# api/bybit/order.py

import logging
from api.bybit.client import get_bybit_client
from pybit.exceptions import InvalidRequestError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def set_leverage(market, leverage):
    """지정된 마켓에 대해 레버리지를 설정합니다."""
    client = get_bybit_client()
    leverage_str = str(leverage)
    try:
        logging.info(f"🔧 Bybit 레버리지 설정 시도: {market}, {leverage}x")
        client.set_leverage(
            category="linear",
            symbol=market,
            buyLeverage=leverage_str,
            sellLeverage=leverage_str,
        )
        logging.info(f"✅ {market} 레버리지 {leverage}x 설정 완료.")
    except InvalidRequestError as e:
        if "leverage not modified" in str(e):
            logging.info(f"ℹ️ {market} 레버리지가 이미 {leverage}x로 설정되어 있어 건너뜁니다.")
        else:
            logging.error(f"❌ {market} 레버리지 설정 실패: {e}", exc_info=True)
            raise
    except Exception as e:
        logging.error(f"❌ {market} 레버리지 설정 중 알 수 없는 오류: {e}", exc_info=True)
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


def get_order_result(market, uuid):
    """
    Bybit에서 주문 ID(uuid)를 사용하여 주문 상세 정보를 조회합니다.
    주문이 체결되었는지, 대기 중인지, 취소되었는지 등의 상태를 반환합니다.
    """
    client = get_bybit_client()
    try:
        # 먼저 활성 주문 목록에서 찾아봅니다.
        order_info = client.get_open_orders(category="linear", symbol=market, orderId=uuid)
        if order_info and order_info['result']['list']:
            order = order_info['result']['list'][0]
            status_map = {
                "New": "wait",
                "PartiallyFilled": "wait",
                "Created": "wait",
                "Filled": "done"
            }
            state = status_map.get(order.get("orderStatus"), "unknown")

            if state == "done":
                # --- ▼▼▼ 여기가 핵심 수정 부분입니다 ▼▼▼ ---
                # API 응답(문자)을 숫자(float)로 변환하여 반환합니다.
                return {
                    "state": "done",
                    "executed_qty": float(order.get("cumExecQty") or 0),
                    "avg_price": float(order.get("avgPrice") or 0),
                    "cum_quote": float(order.get("cumExecValue") or 0)
                }
                # --- ▲▲▲ 수정 완료 ▲▲▲ ---
            return {"state": state}

        # 활성 주문에 없으면, 체결/취소된 주문 기록에서 찾아봅니다.
        order_history = client.get_order_history(category="linear", symbol=market, orderId=uuid)
        if order_history and order_history['result']['list']:
            order = order_history['result']['list'][0]
            status_map = {
                "Filled": "done",
                "Cancelled": "cancel",
                "PartiallyFilledCanceled": "cancel",
                "Rejected": "error"
            }
            state = status_map.get(order.get("orderStatus"), "unknown")
            if state == "done":
                return {
                    "state": "done",
                    "executed_qty": float(order.get("cumExecQty") or 0),
                    "avg_price": float(order.get("avgPrice") or 0),
                    "cum_quote": float(order.get("cumExecValue") or 0)
                }
            return {"state": state}

        logging.warning(f"⚠️ {market} 주문(ID: {uuid})을 찾을 수 없습니다. 이미 오래전에 처리된 주문일 수 있습니다. 'done'으로 간주합니다.")
        return {"state": "done"}

    except Exception as e:
        logging.error(f"❌ {market} 주문(ID: {uuid}) 상태 조회 실패: {e}", exc_info=True)
        raise


def cancel_order(market, order_id):
    """Bybit에서 지정된 주문 ID의 주문을 취소합니다."""
    client = get_bybit_client()
    try:
        logging.info(f"🚫 {market} 주문 취소 시도 (ID: {order_id})")
        result = client.cancel_order(
            category="linear",
            symbol=market,
            orderId=order_id,
        )
        logging.info(f"✅ {market} 주문(ID: {order_id}) 취소 요청 성공.")
        return result
    except Exception as e:
        logging.error(f"❌ {market} 주문(ID: {order_id}) 취소 실패: {e}", exc_info=True)
        raise