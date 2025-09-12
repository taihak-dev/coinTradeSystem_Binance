# api/bybit/order.py

import logging
import uuid
from api.bybit.client import get_bybit_client

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _safe_float_convert(value, default=0.0):
    """
    문자열을 float으로 안전하게 변환합니다.
    문자열이 비어 있거나 None이면 default 값을 반환합니다.
    """
    if value and isinstance(value, str):
        return float(value)
    if isinstance(value, (int, float)):
        return value
    return default


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


def get_order_result(market: str, order_uuid: str) -> dict:
    """
    Bybit에서 특정 주문의 상태를 조회합니다. (안전 변환 로직 추가)
    """
    client = get_bybit_client()

    try:
        history_response = client.get_order_history(
            category="linear", orderId=order_uuid, limit=1
        )

        order_data = None
        if history_response and history_response['result']['list']:
            order_data = history_response['result']['list'][0]
            logging.debug(f"주문 ID {order_uuid}를 history에서 찾았습니다. 상태: {order_data.get('orderStatus')}")

        if not order_data:
            open_orders_response = client.get_open_orders(
                category="linear", orderId=order_uuid, limit=1
            )
            if open_orders_response and open_orders_response['result']['list']:
                order_data = open_orders_response['result']['list'][0]
                logging.debug(f"주문 ID {order_uuid}를 open_orders에서 찾았습니다. 상태: {order_data.get('orderStatus')}")

        if order_data:
            status = order_data.get('orderStatus')
            state_map = {
                "New": "wait", "PartiallyFilled": "wait", "Filled": "done",
                "Cancelled": "cancel", "Rejected": "error",
            }

            # --- 👇👇👇 여기가 핵심 수정 부분입니다 👇👇👇 ---
            # 모든 숫자 변환에 _safe_float_convert 함수를 적용하여 ValueError 방지
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
            # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆
        else:
            logging.warning(f"ⓘ 주문 상태 조회: {market}(id:{order_uuid}) - 주문이 존재하지 않음. 'done'으로 간주합니다.")
            return {"state": "done"}

    except Exception as e:
        logging.error(f"❌ Bybit 주문({order_uuid}) 조회 중 오류: {e}", exc_info=True)
        return {"state": "wait"}


def cancel_order(market: str, order_uuid: str) -> dict:
    """
    Bybit에 제출된 주문을 취소합니다.
    """
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