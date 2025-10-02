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


def send_order(market: str, side: str, quantity: float, price: float = None):
    """
    바이비트에 주문을 전송합니다.
    - 매수(bid)는 시장가(Market)로, 매도(ask)는 지정가(Limit)로 자동 처리합니다.
    """
    client = get_bybit_client()
    side_map = {"bid": "Buy", "ask": "Sell"}
    order_side = side_map[side]

    try:
        # API 요청을 위한 기본 파라미터 구성
        params = {
            'category': "linear",
            'symbol': market,
            'side': order_side,
            'qty': str(quantity),
        }

        # 주문의 종류(side)에 따라 주문 유형(orderType)을 동적으로 결정
        if order_side == "Buy":
            # 매수 주문일 경우, 시장가(Market)로 설정
            params['orderType'] = "Market"
            logging.info(f"➡️ 바이비트 시장가 매수 주문 전송 시도: {market}, 수량={quantity}")
            # 시장가 주문에는 가격(price) 파라미터가 필요 없습니다.

        else:  # order_side == "Sell"
            # 매도 주문일 경우, 지정가(Limit)로 설정
            params['orderType'] = "Limit"
            if price is None:
                raise ValueError("지정가(Limit) 매도 주문에는 가격(price)이 반드시 필요합니다.")
            params['price'] = str(price)
            params['timeInForce'] = 'GTC'
            logging.info(f"➡️ 바이비트 지정가 매도 주문 전송 시도: {market}, 수량={quantity}, 가격={price}")

        # 구성된 파라미터로 주문 실행
        order_result = client.place_order(**params)

        order_id = order_result.get('result', {}).get('orderId')
        logging.info(f"✅ 주문 제출 성공. Order ID: {order_id}")
        return order_id

    except Exception as e:
        logging.error(f"❌ 바이비트 주문 실패: {e}", exc_info=True)
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