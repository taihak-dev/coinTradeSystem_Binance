# api/bybit/order.py

import logging

from pybit.exceptions import InvalidRequestError  # <-- pybit 전용 예외 클래스를 import 합니다.

from api.bybit.client import get_bybit_client

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _safe_float_convert(value, default=0.0):
    if value and isinstance(value, str):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    return default


def set_leverage(market: str, leverage: int):
    """
    지정된 마켓(코인)에 대해 레버리지 배수를 설정합니다.
    """
    client = get_bybit_client()
    leverage_str = str(leverage)
    try:
        logging.info(f"🔧 Bybit 레버리지 설정 시도: {market}, {leverage_str}x")
        client.set_leverage(
            category="linear",
            symbol=market,
            buyLeverage=leverage_str,
            sellLeverage=leverage_str,
        )
        logging.info(f"✅ {market} 레버리지 {leverage_str}x 설정 완료.")

    # --- 👇👇👇 여기가 핵심 수정 부분입니다 👇👇👇 ---
    except InvalidRequestError as e:
        # Bybit API 오류 중, 'leverage not modified'(110043) 오류는
        # 이미 해당 레버리지로 설정된 상태이므로 오류가 아닙니다.
        # 이 경우, 경고만 로깅하고 다음 작업을 계속하도록 예외를 발생시키지 않습니다.
        if "110043" in str(e) or "leverage not modified" in str(e).lower():
            logging.warning(f"⚠️ {market} 레버리지가 이미 {leverage_str}x로 설정되어 있어 건너뜁니다.")
            # 정상적인 상황이므로 여기서 함수를 종료하고 다음 단계로 넘어갑니다.
        else:
            # 그 외의 다른 API 오류는 심각한 문제일 수 있으므로 오류를 발생시킵니다.
            logging.error(f"❌ {market} 레버리지 설정 실패: {e}", exc_info=True)
            raise
    # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆

    except Exception as e:
        logging.error(f"❌ {market} 레버리지 설정 중 예상치 못한 오류 발생: {e}", exc_info=True)
        raise


def send_order(market: str, side: str, volume: float, price: float, **kwargs) -> dict:
    """
    Bybit에 지정가 주문을 제출합니다.
    """
    client = get_bybit_client()
    qty_str = str(volume)
    price_str = str(price)

    try:
        logging.info(f"➡️ Bybit 주문 제출 시도: {market}, {side}, 수량: {qty_str}, 가격: {price_str}")

        response = client.place_order(
            category="linear",
            symbol=market,
            side="Buy" if side.lower() == 'buy' else "Sell",
            orderType="Limit",
            qty=qty_str,
            price=price_str,
            # --- 👇👇👇 여기가 핵심 수정 부분입니다 👇👇👇 ---
            # 'PostOnly'는 즉시 체결될 경우 주문을 취소시키므로,
            # 'GTC'(Good-Til-Cancelled)로 변경하여 반드시 체결되도록 합니다.
            timeInForce="GTC",
            # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆
        )

        if response and response.get('retCode') == 0:
            order_id = response['result']['orderId']
            logging.info(f"✅ 주문 제출 성공. Order ID: {order_id}")
            return {"orderId": order_id}
        else:
            logging.error(f"❌ 주문 제출 실패: {response.get('retMsg')}")
            raise Exception(f"Bybit order placement failed: {response.get('retMsg')}")

    except Exception as e:
        logging.error(f"❌ Bybit 주문 제출 중 오류 발생: {e}", exc_info=True)
        raise


# (이하 get_order_result, cancel_order 함수는 기존과 동일하므로 생략)

def get_order_result(market: str, order_uuid: str) -> dict:
    client = get_bybit_client()

    try:
        history_response = client.get_order_history(
            category="linear", orderId=order_uuid, limit=1
        )

        order_data = None
        if history_response and history_response['result']['list']:
            order_data = history_response['result']['list'][0]

        if not order_data:
            open_orders_response = client.get_open_orders(
                category="linear", orderId=order_uuid, limit=1
            )
            if open_orders_response and open_orders_response['result']['list']:
                order_data = open_orders_response['result']['list'][0]

        if order_data:
            status = order_data.get('orderStatus')
            state_map = {
                "New": "wait", "PartiallyFilled": "wait", "Filled": "done",
                "Cancelled": "cancel", "Rejected": "error",
            }

            return {
                "uuid": order_data.get("orderId"),
                "state": state_map.get(status, "unknown"),
                "market": order_data.get("symbol"),
                "side": order_data.get("side"),
                "price": _safe_float_convert(order_data.get("price")),
                "avg_price": _safe_float_convert(order_data.get("avgPrice")),
                "executed_qty": _safe_float_convert(order_data.get("cumExecQty")),
                "cum_quote": _safe_float_convert(order_data.get("cumExecValue")),
            }
        else:
            logging.warning(f"ⓘ 주문 상태 조회: {market}(id:{order_uuid}) - 주문이 존재하지 않음. 'done'으로 간주합니다.")
            return {"state": "done"}

    except Exception as e:
        logging.error(f"❌ Bybit 주문({order_uuid}) 조회 중 오류: {e}", exc_info=True)
        return {"state": "wait"}


def cancel_order(market: str, order_uuid: str) -> dict:
    client = get_bybit_client()
    try:
        logging.info(f"🚫 Bybit 주문 취소 시도: {market}, UUID: {order_uuid}")
        response = client.cancel_order(
            category="linear",
            symbol=market,
            orderId=order_uuid,
        )

        if response and response.get('retCode') == 0:
            logging.info(f"✅ 주문 취소 성공. Order ID: {response['result']['orderId']}")
            return response['result']
        else:
            logging.warning(f"⚠️ 주문 취소 실패 또는 이미 처리된 주문: {response.get('retMsg')}")
            return {}

    except Exception as e:
        logging.error(f"❌ Bybit 주문({order_uuid}) 취소 중 오류: {e}", exc_info=True)
        raise