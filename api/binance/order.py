# api/binance/order.py

import logging
import uuid
from binance.error import ClientError
from api.binance.client import get_binance_client
from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _place_order(params: dict) -> dict:
    """
    최종적으로 정리된 파라미터 딕셔너리를 그대로 받아
    API에 주문을 전송하는 역할만 수행합니다.
    """
    client = get_binance_client()
    try:
        # ✅✅✅ 핵심 수정 부분 ✅✅✅
        # 라이브러리와의 충돌을 피하기 위해, 우리가 만든 파라미터 딕셔너리를
        # 'params'라는 이름 대신 'order_params'로 변경하여 직접 전달합니다.
        order_params = params.copy() # 원본 딕셔너리 보호를 위해 복사
        logging.info(f"DEBUG: 최종 API 전송 파라미터: {order_params}")
        response = client.new_order(**order_params)
        return response
    except Exception as e:
        logging.error(f"❌ _place_order에서 API 호출 실패 (전송 파라미터: {params}): {e}", exc_info=True)
        raise e


def send_order(market: str, side: str, type: str,
               volume: float = None, price: float = None,
               position_side: str = "BOTH", closePosition: bool = None) -> dict:
    binance_side = "BUY" if side == "bid" else "SELL"
    binance_type = type.upper()

    order_params = {
        'symbol': market,
        'side': binance_side,
        'positionSide': position_side,
        'type': binance_type,
        'newClientOrderId': f"A_{uuid.uuid4().hex}"
    }

    if volume is not None:
        order_params['quantity'] = adjust_quantity_to_step(market, volume)
    if price is not None:
        order_params['price'] = price
        order_params['timeInForce'] = "GTC"
    if closePosition:
        order_params['closePosition'] = 'true'

    return _place_order(params=order_params)


def get_order_result(order_uuid: str, market: str) -> dict:
    """
    바이낸스에서 특정 주문의 상세 정보를 조회합니다.
    단순 상태 조회뿐만 아니라, 체결 관련 상세 정보를 포함하여 반환합니다.
    """
    client = get_binance_client()
    try:
        response = client.query_order(symbol=market, orderId=str(order_uuid))

        state_map = {
            "NEW": "wait", "PARTIALLY_FILLED": "wait", "FILLED": "done",
            "CANCELED": "cancel", "PENDING_CANCEL": "wait",
            "REJECTED": "error", "EXPIRED": "cancel",
        }

        # ✅ 반환할 정보에 체결 수량, 평균 체결가, 수수료 등을 추가합니다.
        return {
            "uuid": response.get("orderId"),
            "state": state_map.get(response.get("status"), "unknown"),
            "market": response.get("symbol"),
            "order_type": response.get("type"),
            "side": response.get("side"),
            "price": float(response.get("price", 0.0)),  # 주문 가격
            "avg_price": float(response.get("avgPrice", 0.0)),  # 평균 체결 가격
            "executed_qty": float(response.get("executedQty", 0.0)),  # 체결된 수량
            "cum_quote": float(response.get("cumQuote", 0.0)),  # 체결된 총 금액 (USDT)
            # 수수료 정보는 별도 trade list 조회가 필요할 수 있으나, 우선은 생략.
            # 필요 시 client.get_account_trades(symbol=market)를 통해 조회 가능
        }
    except ClientError as e:
        if e.error_code == -2013:
            # 주문이 존재하지 않는 경우 (이미 오래전에 체결되고 정리됨)
            logging.warning(f"ⓘ 주문 상태 조회: {market}(id:{order_uuid}) - 주문이 존재하지 않음. 'done'으로 간주.")
            return {"uuid": order_uuid, "state": "done"}
        logging.error(f"❌ 주문 상태 조회 실패 {market}(id:{order_uuid}): {e}", exc_info=True)
        raise e


def cancel_order(order_uuid: str, market: str) -> dict:
    client = get_binance_client()
    try:
        response = client.cancel_order(symbol=market, orderId=str(order_uuid))
        return response
    except ClientError as e:
        raise e