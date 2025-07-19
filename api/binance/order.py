# api/binance/order.py

import logging
import uuid
import time
from binance.error import ClientError
from api.binance.client import get_binance_client
from api.binance.price import get_current_ask_price, get_current_bid_price  # <--- 이 라인이 중요!
from utils.binance_price_utils import adjust_quantity_to_step

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _place_order(symbol: str, side: str, positionSide: str, type: str,
                 quantity: float = None, quoteOrderQty: float = None, price: float = None,
                 timeInForce: str = None, closePosition: bool = None) -> dict:
    """
    바이낸스 선물 거래소에 주문을 제출하는 내부 함수.

    :param symbol: 거래 쌍 (예: BTCUSDT)
    :param side: BUY 또는 SELL
    :param positionSide: LONG, SHORT 또는 BOTH (헷지 모드 사용 시 필수)
    :param type: MARKET, LIMIT, STOP 등 주문 유형
    :param quantity: 주문 수량 (코인 개수)
    :param quoteOrderQty: 시장가 매수 시 매수할 USDT 금액
    :param price: 지정가 주문 시 가격
    :param timeInForce: GTC, IOC, FOK 등 (지정가 주문 시 사용)
    :return: 주문 응답 딕셔너리
    """
    client = get_binance_client()

    params = {
        'symbol': symbol,
        'side': side,
        'positionSide': positionSide,
        'type': type,
        'newClientOrderId': f"A_{uuid.uuid4().hex}"
    }

    if quantity is not None:
        params['quantity'] = quantity
    if quoteOrderQty is not None:
        params['quoteOrderQty'] = quoteOrderQty
    if price is not None:
        params['price'] = price
    if timeInForce is not None:
        params['timeInForce'] = timeInForce
    if closePosition is not None:  # <-- closePosition이 있으면 params에 추가
        params['closePosition'] = str(closePosition).lower()  # API는 'true'/'false' 문자열을 요구할 수 있음

    try:
        response = client.new_order(**params)
        logging.info(f"✅ 주문 제출 성공: {response}")
        time.sleep(0.1)  # API 요청 간 딜레이
        return response
    except ClientError as e:
        logging.error(f"❌ 바이낸스 주문 제출 실패 (ClientError: {e.error_code}): {e.error_message}")
        raise
    except Exception as e:
        logging.error(f"❌ 바이낸스 주문 제출 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise


def send_order(market: str, side: str, type: str,
               amount_usdt: float = None, price: float = None, volume: float = None,
               position_side: str = "BOTH") -> dict:
    binance_side = "BUY" if side == "bid" else "SELL"
    binance_type = type.upper()

    order_params = {
        'symbol': market,
        'side': binance_side,
        'positionSide': position_side,
        'type': binance_type,
    }

    if binance_type == "MARKET":
        if binance_side == "BUY":
            if amount_usdt is None:
                raise ValueError("시장가 매수 주문은 'amount_usdt'(매수 금액)를 필수로 지정해야 합니다.")
            order_params['quoteOrderQty'] = amount_usdt
            response = _place_order(**order_params)

        else:  # binance_side == "SELL" (시장가 매도)
            if volume is None:
                raise ValueError("시장가 매도 주문은 'volume'(수량)을 필수로 지정해야 합니다.")
            adjusted_volume = adjust_quantity_to_step(market, volume)
            if adjusted_volume <= 0:
                logging.warning(f"⚠️ {market} 시장가 매도 수량 조정 결과 0이하. 주문 취소. (원본: {volume})")
                return {"error": "adjusted_quantity_zero"}

            # ⭐⭐ 수정: closePosition=True일 때는 quantity를 보내지 않음 ⭐⭐
            # order_params['quantity'] = adjusted_volume # 이 줄을 삭제하거나 조건부로 변경
            order_params['closePosition'] = True  # 전량 매도

            # 아래와 같이 변경하여 quantity를 보내지 않도록 합니다.
            # quantity가 필수 파라미터가 아니므로, closePosition=True가 있다면 quantity를 추가하지 않음.
            if 'closePosition' not in order_params or not order_params[
                'closePosition']:  # closePosition이 없거나 False일 때만 quantity 추가
                order_params['quantity'] = adjusted_volume

            response = _place_order(**order_params)


    elif binance_type == "LIMIT":
        if price is None or volume is None:
            raise ValueError("지정가 주문은 'price'와 'volume'을 필수로 지정해야 합니다.")

        adjusted_volume = adjust_quantity_to_step(market, volume)
        if adjusted_volume <= 0:
            logging.warning(f"⚠️ {market} 지정가 주문 수량 조정 결과 0이하. 주문 취소. (원본: {volume})")
            return {"error": "adjusted_quantity_zero"}

        order_params['quantity'] = adjusted_volume
        order_params['price'] = price
        order_params['timeInForce'] = "GTC"

        response = _place_order(**order_params)

    else:
        raise ValueError(f"지원하지 않는 주문 유형입니다: {type}")
    return response


def cancel_order(order_uuid: str, market: str) -> dict:
    """
    지정된 UUID의 주문을 취소합니다.

    :param order_uuid: 취소할 주문의 UUID (바이낸스 orderId)
    :param market: 주문이 제출된 마켓 심볼
    :return: 취소 응답 딕셔너리
    """
    client = get_binance_client()
    try:
        response = client.cancel_open_orders(symbol=market, orderId=order_uuid)
        logging.info(f"✅ 주문 취소 요청 성공 (UUID: {order_uuid}, Market: {market}): {response}")
        time.sleep(0.1)
        return response
    except ClientError as e:
        # 주문이 이미 체결되었거나 존재하지 않는 경우 등
        if e.error_code == -2011:  # Unknown order sent
            logging.warning(
                f"⚠️ 주문 취소 실패: 이미 처리되었거나 존재하지 않는 주문입니다. (UUID: {order_uuid}, Market: {market}, Error: {e.error_message})")
            return {"error": "done_order"}  # 이미 체결된 주문으로 간주하여 처리
        logging.error(f"❌ 주문 취소 실패 (ClientError: {e.error_code}): {e.error_message}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"❌ 주문 취소 중 알 수 없는 오류 발생 (UUID: {order_uuid}, Market: {market}): {e}", exc_info=True)
        raise


def get_order_result(order_uuid: str, market: str) -> dict:
    """
    지정된 UUID의 주문 상태를 조회합니다.

    :param order_uuid: 조회할 주문의 UUID (바이낸스 orderId)
    :param market: 주문이 제출된 마켓 심볼
    :return: 주문 상태를 포함하는 딕셔너리 {'state': 'wait' or 'done' or 'cancel'}
    """
    client = get_binance_client()
    try:
        response = client.query_order(symbol=market, orderId=order_uuid)
        status = response.get('status')
        filled_qty = float(response.get('executedQty', '0'))
        orig_qty = float(response.get('origQty', '0'))

        state = "wait"
        if status == "FILLED":
            state = "done"
        elif status in ["CANCELED", "EXPIRED", "REJECTED"]:
            state = "cancel"
        elif status == "PARTIALLY_FILLED":
            # 부분 체결된 경우, 나머지 수량이 남아있으므로 'wait' 상태로 유지
            if filled_qty > 0 and filled_qty < orig_qty:
                state = "wait"
            else:  # 혹시 모를 상황을 대비해 'done'으로 처리
                state = "done"

        logging.debug(f"🔍 주문 상태 조회 (UUID: {order_uuid}, Market: {market}): 바이낸스 상태={status}, 로컬 상태={state}")
        return {"state": state, "response": response}
    except ClientError as e:
        # 주문이 존재하지 않는 경우 (이미 취소되었거나 체결 완료 후 기록이 사라진 경우)
        if e.error_code == -2013:  # Order does not exist
            logging.warning(
                f"⚠️ 주문 상태 조회 실패: 주문이 존재하지 않음 (UUID: {order_uuid}, Market: {market}, Error: {e.error_message}). 'cancel' 상태로 간주합니다.")
            return {"state": "cancel", "error": "Order does not exist"}
        logging.error(f"❌ 주문 상태 조회 실패 (ClientError: {e.error_code}): {e.error_message}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"❌ 주문 상태 조회 중 알 수 없는 오류 발생 (UUID: {order_uuid}, Market: {market}): {e}", exc_info=True)
        raise


def cancel_and_new_order_binance(prev_order_uuid: str, symbol: str, price: float, quantity: float,
                                 position_side: str = "LONG") -> dict:
    """
    기존 주문을 취소하고 새로운 지정가 매수 주문을 제출합니다.
    바이낸스에서는 정정 주문 API가 없으므로 이 방식으로 구현합니다.

    :param prev_order_uuid: 취소할 기존 주문의 UUID (바이낸스 orderId)
    :param symbol: 거래 쌍 (예: BTCUSDT)
    :param price: 새로 제출할 지정가 주문의 가격
    :param quantity: 새로 제출할 지정가 주문의 수량
    :param position_side: LONG, SHORT 또는 BOTH (새로운 주문의 positionSide, 기본값 LONG)
    :return: 새로운 주문의 응답 딕셔너리 (Upbit의 new_order_uuid와 유사)
    """
    logging.info(f"🔄 바이낸스 정정 매수 주문 시도 (기존 UUID: {prev_order_uuid}, Market: {symbol})")
    try:
        # 1. 기존 주문 취소 시도
        cancel_response = cancel_order(prev_order_uuid, symbol)
        # 기존 주문이 이미 'done_order' (체결 완료 또는 존재하지 않음)로 처리되었다면
        if cancel_response.get("error") == "done_order":
            logging.info(f"ℹ️ 기존 주문({prev_order_uuid})은 이미 체결 완료되었거나 존재하지 않습니다. 새로운 주문을 제출하지 않습니다.")
            return {"error": "done_order"}  # 이미 완료된 주문임을 상위 함수에 알림

        # 2. 새로운 주문 제출
        # send_order 호출 시 인자 이름을 올바르게 사용하도록 수정
        response = send_order(
            market=symbol,
            side="bid",  # 매수
            type="limit",  # 지정가
            price=price,  # 가격
            volume=quantity,  # 수량
            position_side=position_side  # 매개변수 사용
        )
        logging.info(f"✅ 기존 주문({prev_order_uuid}) 취소 후 새로운 매수 주문 제출 완료. 새로운 orderId: {response.get('uuid')}")
        return response  # 새로운 주문의 응답 반환
    except ClientError as e:
        logging.error(f"❌ 바이낸스 정정 주문 실패 (ClientError: {e.error_code}): {e.error_message}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"❌ 바이낸스 정정 주문 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise