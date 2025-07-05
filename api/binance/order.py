# api/binance/order.py
import logging
from api.binance.client import get_binance_client
from binance.error import ClientError
import config


# --- 내부 헬퍼 함수 ---
def _place_order(symbol, side, position_side, quantity, order_type='MARKET', price=None):
    client = get_binance_client()
    try:
        params = {
            'symbol': symbol,
            'side': side,
            'positionSide': position_side,
            'quantity': quantity,
            'type': order_type
        }
        if order_type == 'LIMIT':
            if price is None:
                raise ValueError("지정가(LIMIT) 주문에는 가격(price)이 반드시 필요합니다.")
            params['price'] = price
            params['timeInForce'] = 'GTC'  # Good 'Til Canceled

        logging.info(f"바이낸스 주문 시도: {params}")
        order_response = client.new_order(**params)
        logging.info(f"바이낸스 주문 성공: {order_response}")
        return order_response

    except ClientError as e:
        logging.error(f"바이낸스 주문 실패: {e.error_message} (ErrorCode: {e.error_code})")
        # 이미 취소되었거나 존재하지 않는 주문을 정정하려 할 때 발생하는 에러
        if e.error_code == -2011:
            return {"error": "order_not_found", "message": e.error_message}
        raise e


# --- 기존 프로그램과 호환되는 함수들 ---

def send_order(market: str, side: str, ord_type: str, volume: float = None, unit_price: float = None,
               amount_krw: float = None) -> dict:
    """
    Upbit의 send_order와 호환성을 위한 함수.
    선물 거래의 개념에 맞게 내부적으로 변환하여 주문을 실행한다.

    - side 'bid'(매수) -> LONG 포지션 진입
    - side 'ask'(매도) -> LONG 포지션 종료 (전체 수량 매도)
    - ord_type 'limit' -> 지정가, 'market'/'price' -> 시장가
    """

    # 1. 포지션 및 주문 방향 결정
    # 이 프로그램의 매수(bid)는 롱 포지션 진입, 매도(ask)는 롱 포지션 종료를 의미.
    position_side = "LONG"
    order_side = "BUY" if side == "bid" else "SELL"

    # 2. 주문 유형 결정
    binance_ord_type = "LIMIT" if ord_type == "limit" else "MARKET"

    # 3. 수량 결정
    # 시장가 매수(initial) 시 amount_krw(USDT) 기준으로 수량 계산
    if binance_ord_type == 'MARKET' and side == 'bid' and amount_krw is not None:
        current_price = float(get_binance_client().ticker_price(symbol=market)['price'])
        quantity = round(amount_krw / current_price, 3)  # USDT로 살 수 있는 코인 수량. 소수점 3자리로 반올림
    else:
        quantity = volume

    if quantity is None or quantity <= 0:
        raise ValueError("주문 수량(volume)이 유효하지 않습니다.")

    # 레버리지 설정
    try:
        get_binance_client().change_leverage(symbol=market, leverage=config.DEFAULT_LEVERAGE)
    except ClientError as e:
        # 이미 해당 레버리지로 설정된 경우(-4046)는 정상으로 간주
        if e.error_code != -4046:
            logging.warning(f"{market} 레버리지 설정 실패: {e.error_message}")
            raise e

    # 주문 실행
    response = _place_order(market, order_side, position_side, quantity, binance_ord_type, unit_price)

    # Upbit과 유사한 응답 형태로 변환
    return {
        "uuid": response.get('orderId'),
        "side": side,
        "ord_type": ord_type,
        "price": response.get('price'),
        "volume": response.get('origQty'),
        "market": response.get('symbol'),
        "state": "wait"  # 바이낸스는 주문 즉시 wait 상태로 간주
    }


def cancel_order(uuid: str, symbol: str) -> dict:
    """단일 주문을 취소합니다."""
    client = get_binance_client()
    try:
        logging.info(f"주문 취소 시도: {symbol}, orderId={uuid}")
        response = client.cancel_order(symbol=symbol, orderId=uuid)
        logging.info(f"주문 취소 성공: {response}")
        return response
    except ClientError as e:
        logging.error(f"주문 취소 실패: {e.error_message}")
        # 이미 체결/취소된 주문
        if e.error_code == -2011:
            return {"error": "already_done_or_canceled", "message": e.error_message}
        raise e


def cancel_and_new_order(prev_order_uuid: str, market: str, price: float, amount: float) -> dict:
    """
    기존 주문을 취소하고 신규 주문을 넣습니다. (Upbit의 정정주문 대체)
    """
    # 1. 기존 주문 취소
    cancel_response = cancel_order(prev_order_uuid, market)

    # 이미 처리된 주문이라 취소에 실패한 경우, 신규 주문을 넣지 않고 에러를 발생시켜
    # order_executor가 이를 'done'으로 처리하도록 유도할 수 있음.
    # 여기서는 'order_not_found' 에러를 반환하여 신규 주문을 시도하게 함.
    if cancel_response.get("error") == "already_done_or_canceled":
        logging.warning(f"정정 주문 실패: 기존 주문({prev_order_uuid})이 이미 체결/취소되어 신규 주문을 넣지 않습니다.")
        return {"error": "done_order", "message": "Original order already processed."}

    # 2. 신규 주문 (지정가 매수)
    # side='bid', ord_type='limit'으로 고정 (기존 로직 기반)
    new_order_response = send_order(market=market, side='bid', ord_type='limit', volume=amount, unit_price=price)

    return {
        "new_order_uuid": new_order_response.get("uuid"),
        "cancel_response": cancel_response,
        "new_order_response": new_order_response
    }


def get_order_result(uuid: str, symbol: str) -> dict:
    """
    단일 주문의 상태를 조회합니다. Upbit의 state('wait', 'done', 'cancel')와 유사하게 변환합니다.
    """
    client = get_binance_client()
    try:
        order = client.get_order(symbol=symbol, orderId=uuid)
        status = order['status']

        # 상태 매핑
        if status == 'FILLED':
            state = 'done'
        elif status in ['CANCELED', 'EXPIRED']:
            state = 'cancel'
        else:  # NEW, PARTIALLY_FILLED
            state = 'wait'

        return {
            "uuid": order['orderId'],
            "state": state,
            "market": order['symbol'],
            "volume": order.get('origQty'),
            "executed_volume": order.get('executedQty'),
            "trades_count": 1  # 단순화
        }
    except ClientError as e:
        # 존재하지 않는 주문이면 'cancel'로 간주하여 로그에서 정리되도록 함
        if e.error_code == -2013:
            logging.warning(f"주문({uuid})을 찾을 수 없습니다. 취소된 것으로 간주합니다.")
            return {"uuid": uuid, "state": "cancel"}
        logging.error(f"주문 조회 실패: {e.error_message}")
        raise e