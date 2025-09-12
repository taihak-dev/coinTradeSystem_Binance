# api/bybit/order.py

import logging
import uuid
from api.bybit.client import get_bybit_client

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _safe_float_convert(value, default=0.0):
    if value and isinstance(value, str):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    return default


# --- 👇👇👇 레버리지 설정 함수 (신규 추가) 👇👇👇 ---
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
    except Exception as e:
        # 이미 해당 레버리지로 설정되어 있을 경우에도 오류가 발생할 수 있으므로, 경고로 처리하고 계속 진행합니다.
        # (e.g., "Leverage has not been modified")
        if "Leverage has not been modified" in str(e):
            logging.warning(f"⚠️ {market} 레버리지가 이미 {leverage_str}x로 설정되어 있습니다.")
        else:
            logging.error(f"❌ {market} 레버리지 설정 실패: {e}", exc_info=True)
            raise


# --- 👆👆👆 여기까지 추가 --- 👆👆👆


def send_order(market: str, side: str, volume: float, price: float, **kwargs) -> dict:
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
            timeInForce="GTC",
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
# (기존 파일에서 위의 set_leverage 함수만 추가하시면 됩니다.)

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