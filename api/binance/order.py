# api/binance/order.py

import logging
from binance.error import ClientError
from api.binance.client import get_binance_client
from utils.binance_price_utils import adjust_quantity_to_step, adjust_price_to_tick # 임포트 유지
import time
from typing import Optional, Dict
from api.binance.price import get_current_ask_price, get_current_bid_price


# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _place_order(
    symbol: str,
    side: str, # BUY/SELL
    position_side: str, # LONG/SHORT/BOTH
    order_type: str, # MARKET/LIMIT
    quantity: Optional[float] = None,
    price: Optional[float] = None,
    amount_usdt: Optional[float] = None, # 시장가 매수 시 USDT 금액
    client_order_id: Optional[str] = None
) -> dict:
    """
    바이낸스 선물 시장에 실제 주문을 제출하는 내부 헬퍼 함수.
    모든 주문 유형에 공통적으로 사용됩니다.
    """
    binance_client = get_binance_client()
    params = {
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "type": order_type,
        "newClientOrderId": client_order_id # 클라이언트 주문 ID (추적 용이)
    }

    # 주문 유형에 따른 파라미터 설정
    if order_type == "MARKET":
        if amount_usdt: # 시장가 매수 (USDT 금액 기준)
            params["quoteOrderQty"] = amount_usdt # 매수할 USDT 금액
            logging.info(f"🆕 시장가 매수 주문 준비: {symbol}, {amount_usdt} USDT")
        elif quantity: # 시장가 매도 또는 수량 지정 시장가 매수
            params["quantity"] = quantity
            logging.info(f"🆕 시장가 {'매도' if side == 'SELL' else '매수'} 주문 준비: {symbol}, 수량={quantity}")
        else:
            raise ValueError("시장가 주문은 quantity 또는 amount_usdt(매수 시)가 필요합니다.")
    elif order_type == "LIMIT":
        if quantity is None or price is None:
            raise ValueError("지정가 주문은 quantity와 price가 모두 필요합니다.")
        params["quantity"] = quantity
        params["price"] = price
        params["timeInForce"] = "GTC" # Good Till Cancelled
        logging.info(f"🆕 지정가 {'매도' if side == 'SELL' else '매수'} 주문 준비: {symbol}, 수량={quantity}, 가격={price}")
    else:
        raise ValueError(f"지원하지 않는 주문 유형: {order_type}")

    try:
        response = binance_client.new_order(**params)
        logging.info(f"✅ 주문 제출 성공: Symbol={symbol}, Side={side}, Type={order_type}, OrderId={response.get('orderId')}")
        # Upbit의 uuid와 유사하게 'orderId'를 'uuid'로 매핑하여 반환
        response['uuid'] = response.get('orderId')
        return response
    except ClientError as e:
        logging.error(f"❌ 주문 제출 실패 (ClientError: {e.error_code}): {e.error_message}, Params: {params}")
        raise e
    except Exception as e:
        logging.error(f"❌ 주문 제출 중 알 수 없는 오류 발생: {e}, Params: {params}", exc_info=True)
        raise e


def send_order(
        market: str,
        side: str,  # "bid" (매수) or "ask" (매도)
        type: str,  # "price" (시장가) or "limit" (지정가)
        amount_usdt: Optional[float] = None,  # USDT 기준 금액 (시장가 주문 시)
        price: Optional[float] = None,  # 지정가 주문 시 가격
        volume: Optional[float] = None,  # 수량 (코인 개수)
        position_side: Optional[str] = None  # LONG, SHORT (헷지 모드일 경우 필수)
) -> Dict:
    binance_side = "BUY" if side == "bid" else "SELL"
    final_position_side = position_side

    # ... (position_side 결정 로직 - 기존과 동일) ...

    if type == "price":  # 시장가 주문
        logging.info(f"🆕 시장가 {binance_side} 주문 준비: {market}, {amount_usdt} USDT")

        if amount_usdt is not None:
            try:
                # 시장가 매수(BUY)는 매도 호가(Ask Price)를, 시장가 매도(SELL)는 매수 호가(Bid Price)를 사용
                if binance_side == "BUY":
                    current_market_price = get_current_ask_price(market)
                else:  # SELL
                    current_market_price = get_current_bid_price(market)

                if current_market_price is None or current_market_price <= 0:
                    logging.error(f"❌ {market} 현재 시장 가격을 가져올 수 없거나 유효하지 않습니다 ({current_market_price}). 주문을 제출할 수 없습니다.")
                    raise ValueError(f"현재 시장 가격 오류로 주문 제출 불가: {market}")

                calculated_quantity = amount_usdt / current_market_price
                logging.debug(f"계산된 수량: {amount_usdt} USDT / {current_market_price} = {calculated_quantity}")

                # ⚠️ 수정: 인자 순서 변경 (symbol, quantity)
                adjusted_quantity = adjust_quantity_to_step(market, calculated_quantity)

                if adjusted_quantity == 0:
                    logging.error(f"❌ {market} 계산된 수량({calculated_quantity})이 너무 작아 주문할 수 없습니다. 보정 후 0이 되었습니다.")
                    raise ValueError(f"계산된 수량이 너무 작아 주문 불가: {market}")

                return _place_order(market, binance_side, final_position_side, "MARKET", quantity=adjusted_quantity)
            except Exception as e:
                logging.error(f"❌ 시장가 주문 수량 계산 중 오류 발생: {e}", exc_info=True)
                raise e
        elif volume is not None:  # volume (quantity)이 직접 주어진 경우
            # 수량 보정
            # ⚠️ 수정: 인자 순서 변경 (symbol, quantity)
            adjusted_quantity = adjust_quantity_to_step(market, volume)
            if adjusted_quantity == 0:
                logging.error(f"❌ {market} 직접 지정된 수량({volume})이 너무 작아 주문할 수 없습니다. 보정 후 0이 되었습니다.")
                raise ValueError(f"직접 지정된 수량이 너무 작아 주문 불가: {market}")
            return _place_order(market, binance_side, final_position_side, "MARKET", quantity=adjusted_quantity)
        else:
            logging.error("❌ 시장가 주문에는 amount_usdt 또는 volume 중 하나가 반드시 필요합니다.")
            raise ValueError("시장가 주문에는 amount_usdt 또는 volume 중 하나가 반드시 필요합니다.")

    elif type == "limit":  # 지정가 주문
        if price is None or volume is None:
            logging.error("❌ 지정가 주문에는 가격과 수량이 모두 필요합니다.")
            raise ValueError("지정가 주문에는 가격과 수량이 모두 필요합니다.")

        # ⚠️ 수정: 인자 순서 변경 (symbol, price)
        adjusted_price = adjust_price_to_tick(market, price)
        # ⚠️ 수정: 인자 순서 변경 (symbol, quantity)
        adjusted_quantity = adjust_quantity_to_step(market, volume)

        if adjusted_price == 0 or adjusted_quantity == 0:
            logging.error(f"❌ {market} 보정된 가격({adjusted_price}) 또는 수량({adjusted_quantity})이 0이 되어 주문할 수 없습니다.")
            raise ValueError(f"보정된 가격 또는 수량이 0이 되어 주문 불가: {market}")

        return _place_order(market, binance_side, final_position_side, "LIMIT", price=adjusted_price,
                            quantity=adjusted_quantity)
    else:
        logging.error(f"❌ 알 수 없는 주문 타입: {type}")
        raise ValueError(f"알 수 없는 주문 타입: {type}")


def cancel_order(order_id: str, symbol: str):
    """
    지정된 UUID(orderId)를 가진 바이낸스 선물 주문을 취소합니다.
    """
    logging.info(f"🗑️ 주문 취소 시도: OrderId={order_id}, Symbol={symbol}")
    client = get_binance_client()
    try:
        response = client.cancel_open_orders(symbol=symbol, orderId=order_id)
        logging.info(f"✅ 주문 취소 성공: OrderId={order_id}, Response={response}")
        time.sleep(0.1)  # ⚠️ 주문 취소 후 딜레이 추가
        return response
    except ClientError as e:
        logging.error(f"❌ 주문 취소 실패 (ClientError: {e.error_code}): {e.error_message}, OrderId={order_id}, Symbol={symbol}")
        raise e
    except Exception as e:
        logging.error(f"❌ 주문 취소 중 알 수 없는 오류 발생: {e}, OrderId={order_id}, Symbol={symbol}", exc_info=True)
        raise e


def get_order_result(order_id: str, symbol: str) -> dict:
    """
    지정된 UUID(orderId)를 가진 바이낸스 선물 주문의 상태를 조회합니다.
    Upbit의 상태("wait", "done", "cancel")와 유사하게 변환하여 반환합니다.
    """
    logging.debug(f"🔍 주문 상태 조회 시도: OrderId={order_id}, Symbol={symbol}")
    client = get_binance_client()

    # ⚠️ 수정: upbit_state 변수를 try 블록 외부에서 초기화하여 모든 실행 경로에서 접근 가능하도록 보장
    upbit_state = "unknown"

    try:
        order_info = client.query_order(symbol=symbol, orderId=order_id)
        status = order_info.get('status') # 'status' 키가 없을 경우 None 반환
        executed_qty = float(order_info.get('executedQty', 0))
        orig_qty = float(order_info.get('origQty', 0))

        # 바이낸스 상태를 Upbit 유사 상태로 매핑
        if status == "NEW":
            upbit_state = "wait" # 새로 생성된 주문, 미체결
        elif status == "PARTIALLY_FILLED":
            upbit_state = "wait" # 부분 체결, 잔여 수량 미체결
        elif status == "FILLED":
            upbit_state = "done" # 전체 체결
        elif status == "CANCELED":
            upbit_state = "cancel" # 취소됨
        elif status == "EXPIRED":
            upbit_state = "cancel" # 만료됨 (시간조건부 주문 등)
        elif status == "REJECTED":
            upbit_state = "cancel" # 거부됨

        # 추가 검증: 체결 수량이 0이 아니고 원래 수량과 같다면 'done'으로 최종 확인
        if executed_qty > 0 and executed_qty == orig_qty:
            upbit_state = "done"
        # 체결 수량이 0이고, 상태가 NEW/PARTIALLY_FILLED가 아니면 'cancel'로 간주 (방어적 로직)
        elif executed_qty == 0 and status not in ["NEW", "PARTIALLY_FILLED"]:
            upbit_state = "cancel"


        logging.debug(f"✅ 주문 상태 조회 성공: OrderId={order_id}, Binance Status={status} -> Mapped Status={upbit_state}")
        time.sleep(0.05)
        return {"uuid": order_id, "state": upbit_state}

    except ClientError as e:
        # 주문이 존재하지 않는 경우 (예: 이미 취소되었거나 오래된 주문)
        if e.error_code == -2013: # Order does not exist, orderId was invalid
            logging.warning(f"⚠️ 주문 조회 실패 - OrderId={order_id}, Symbol={symbol}: 존재하지 않는 주문 또는 이미 처리됨. (Error: {e.error_message})")
            return {"uuid": order_id, "state": "cancel"} # 존재하지 않으므로 취소된 것으로 간주
        logging.error(f"❌ 주문 상태 조회 실패 (ClientError: {e.error_code}): {e.error_message}, OrderId={order_id}, Symbol={symbol}")
        raise e # 다른 종류의 에러는 다시 발생

    except Exception as e:
        logging.error(f"❌ 주문 상태 조회 중 알 수 없는 오류 발생: {e}, OrderId={order_id}, Symbol={symbol}", exc_info=True)
        raise e # 다른 종류의 에러는 다시 발생

# --- 신규: 바이낸스용 정정 주문 (취소 후 신규 주문) ---
def cancel_and_new_order_binance(prev_order_uuid: str, symbol: str, price: float, quantity: float) -> dict:
    """
    바이낸스 선물 시장에서 기존 주문을 취소하고 새로운 지정가 매수 주문을 제출합니다.
    바이낸스 API는 Upbit처럼 '정정' 기능을 직접 제공하지 않으므로, 취소 후 신규 주문으로 처리합니다.

    :param prev_order_uuid: 취소할 기존 주문의 UUID (orderId)
    :param symbol: 심볼 (예: BTCUSDT)
    :param price: 새로운 주문의 지정가
    :param quantity: 새로운 주문의 수량
    :return: 새로운 주문의 응답 딕셔너리 (새로운 주문 UUID 또는 오류 정보)
    """
    logging.info(f"🔁 바이낸스 매수 정정 주문 시도: 기존 {prev_order_uuid} 취소 후 신규 주문 ({symbol}, 가격:{price}, 수량:{quantity})")

    # 1. 기존 주문 취소 시도
    try:
        # cancel_order는 내부적으로 로깅을 수행
        cancel_order(prev_order_uuid, symbol)
        # 취소는 성공했지만, 실제 API는 주문 상태를 바로 갱신하지 않을 수 있음
        time.sleep(0.1) # 짧은 딜레이로 API 처리 시간 확보
        logging.info(f"✅ 기존 주문 {prev_order_uuid} 취소 요청 성공.")
    except ClientError as e:
        # -2011: Unknown orderId / Order does not exist -> 이미 체결되었거나 취소되었을 수 있음
        # -2022: Order would immediately match -> 주문이 이미 즉시 체결될 수 있어 취소 불가 (실질적으로 체결됨)
        if e.error_code in [-2011, -2022]:
            logging.warning(f"⚠️ 기존 주문 {prev_order_uuid}는 이미 체결되었거나 존재하지 않아 취소할 수 없습니다. (Error: {e.error_message})")
            return {"error": "done_order"} # 이미 체결된 것으로 간주하고 새로운 주문을 시도하지 않음
        else:
            logging.error(f"❌ 기존 주문 {prev_order_uuid} 취소 실패 (ClientError: {e.error_code}): {e.error_message}")
            raise e # 다른 종류의 에러는 다시 발생시킴
    except Exception as e:
        logging.error(f"❌ 기존 주문 {prev_order_uuid} 취소 실패 (일반 오류): {e}", exc_info=True)
        raise e

    # 2. 새로운 주문 제출 (지정가 매수)
    # 취소가 성공했거나, 주문이 이미 없어서 취소할 필요가 없었던 경우에만 신규 주문 진행
    try:
        # send_order 함수는 이미 레버리지/마진 타입 설정을 포함하고 있습니다.
        # 따라서 여기서는 순수하게 주문만 전송합니다.
        new_order_response = send_order(
            market=symbol,
            side="bid",         # 매수 (Upbit 호환) -> send_order 내부에서 "BUY"로 변환
            ord_type="limit",   # 지정가 (Upbit 호환) -> send_order 내부에서 "LIMIT"으로 변환
            unit_price=price,
            volume=quantity,
            position_side="LONG" # 매수 시 롱 포지션 진입 (전략에 따라 조절)
        )
        logging.info(f"✅ 새로운 매수 주문 제출 성공: UUID={new_order_response.get('uuid')}")
        return {"new_order_uuid": new_order_response.get("uuid")}

    except Exception as e:
        logging.error(f"❌ 새로운 매수 주문 제출 실패 ({symbol}, 가격:{price}, 수량:{quantity}): {e}", exc_info=True)
        raise e


def send_order(
        market: str,
        side: str,  # "bid" (매수) or "ask" (매도)
        type: str,  # "price" (시장가) or "limit" (지정가)
        amount_usdt: Optional[float] = None,  # USDT 기준 금액 (시장가 주문 시)
        price: Optional[float] = None,  # 지정가 주문 시 가격
        volume: Optional[float] = None,  # 수량 (코인 개수)
        position_side: Optional[str] = None  # LONG, SHORT (헷지 모드일 경우 필수)
) -> Dict:
    binance_side = "BUY" if side == "bid" else "SELL"
    final_position_side = position_side

    # ... (position_side 결정 로직 - 기존과 동일) ...

    if type == "price":  # 시장가 주문
        logging.info(f"🆕 시장가 {binance_side} 주문 준비: {market}, {amount_usdt} USDT")

        if amount_usdt is not None:
            try:
                # 시장가 매수(BUY)는 매도 호가(Ask Price)를, 시장가 매도(SELL)는 매수 호가(Bid Price)를 사용
                if binance_side == "BUY":
                    current_market_price = get_current_ask_price(market)
                else:  # SELL
                    current_market_price = get_current_bid_price(market)

                if current_market_price is None or current_market_price <= 0:
                    logging.error(f"❌ {market} 현재 시장 가격을 가져올 수 없거나 유효하지 않습니다 ({current_market_price}). 주문을 제출할 수 없습니다.")
                    raise ValueError(f"현재 시장 가격 오류로 주문 제출 불가: {market}")

                calculated_quantity = amount_usdt / current_market_price
                logging.debug(f"계산된 수량: {amount_usdt} USDT / {current_market_price} = {calculated_quantity}")

                adjusted_quantity = adjust_quantity_to_step(market, calculated_quantity)

                if adjusted_quantity == 0:
                    logging.error(f"❌ {market} 계산된 수량({calculated_quantity})이 너무 작아 주문할 수 없습니다. 보정 후 0이 되었습니다.")
                    raise ValueError(f"계산된 수량이 너무 작아 주문 불가: {market}")

                # _place_order에 계산된 quantity를 전달
                return _place_order(market, binance_side, final_position_side, "MARKET", quantity=adjusted_quantity)
            except Exception as e:
                logging.error(f"❌ 시장가 주문 수량 계산 중 오류 발생: {e}", exc_info=True)
                raise e
        elif volume is not None:  # volume (quantity)이 직접 주어진 경우
            # 수량 보정
            adjusted_quantity = adjust_quantity_to_step(market, volume)
            if adjusted_quantity == 0:
                logging.error(f"❌ {market} 직접 지정된 수량({volume})이 너무 작아 주문할 수 없습니다. 보정 후 0이 되었습니다.")
                raise ValueError(f"직접 지정된 수량이 너무 작아 주문 불가: {market}")
            return _place_order(market, binance_side, final_position_side, "MARKET", quantity=adjusted_quantity)
        else:
            logging.error("❌ 시장가 주문에는 amount_usdt 또는 volume 중 하나가 반드시 필요합니다.")
            raise ValueError("시장가 주문에는 amount_usdt 또는 volume 중 하나가 반드시 필요합니다.")

    elif type == "limit":  # 지정가 주문
        if price is None or volume is None:
            logging.error("❌ 지정가 주문에는 가격과 수량이 모두 필요합니다.")
            raise ValueError("지정가 주문에는 가격과 수량이 모두 필요합니다.")

        adjusted_price = adjust_price_to_tick(market, price)
        adjusted_quantity = adjust_quantity_to_step(market, volume)

        if adjusted_price == 0 or adjusted_quantity == 0:
            logging.error(f"❌ {market} 보정된 가격({adjusted_price}) 또는 수량({adjusted_quantity})이 0이 되어 주문할 수 없습니다.")
            raise ValueError(f"보정된 가격 또는 수량이 0이 되어 주문 불가: {market}")

        return _place_order(market, binance_side, final_position_side, "LIMIT", price=adjusted_price,
                            quantity=adjusted_quantity)
    else:
        logging.error(f"❌ 알 수 없는 주문 타입: {type}")
        raise ValueError(f"알 수 없는 주문 타입: {type}")


def _place_order(
        market: str,
        side: str,  # "BUY" or "SELL"
        position_side: str,  # "LONG" or "SHORT"
        order_type: str,  # "MARKET", "LIMIT"
        price: Optional[float] = None,
        quantity: Optional[float] = None,  # 코인 수량 (이제 필수로 사용)
        amount_usdt: Optional[float] = None  # 이 매개변수는 이제 send_order에서 처리되므로 _place_order에서는 사용하지 않음
) -> Dict:
    binance_client = get_binance_client()
    params = {
        "symbol": market,
        "side": side,
        "positionSide": position_side,
        "type": order_type,
        "newClientOrderId": None  # 클라이언트 주문 ID (선택 사항)
    }

    if order_type == "MARKET":
        # send_order에서 quantity를 이미 계산하여 넘겨주므로, 여기서는 quantity가 None일 수 없음
        if quantity is not None:
            params["quantity"] = quantity
            logging.debug(f"시장가 주문 (수량 지정): {quantity}")
        else:
            logging.error("❌ 시장가 주문에는 quantity가 반드시 필요합니다. (send_order에서 계산되어야 함)")
            raise ValueError("시장가 주문에는 quantity가 반드시 필요합니다.")

    elif order_type == "LIMIT":
        if price is None or quantity is None:
            logging.error("❌ 지정가 주문에는 가격과 수량이 모두 필요합니다.")
            raise ValueError("지정가 주문에는 가격과 수량이 모두 필요합니다.")
        params["price"] = price
        params["quantity"] = quantity
        params["timeInForce"] = "GTC"  # Good Till Cancel (지정가 주문에만 해당)
        logging.debug(f"지정가 주문: 가격={price}, 수량={quantity}")
    else:
        logging.error(f"❌ 지원하지 않는 주문 타입: {order_type}")
        raise ValueError(f"지원하지 않는 주문 타입: {order_type}")

    logging.info(f"🆕 바이낸스 주문 제출: {params}")
    try:
        response = binance_client.new_order(**params)
        logging.info(f"✅ 주문 제출 성공: {response}")
        time.sleep(0.2)  # ⚠️ 주문 제출 후 딜레이 추가 (가중치 높음)
        return response
    except ClientError as e:
        logging.error(f"❌ 주문 제출 실패 (ClientError: {e.error_code}): {e.error_message}, Params: {params}", exc_info=True)
        raise e
    except Exception as e:
        logging.error(f"❌ 주문 제출 중 알 수 없는 오류 발생: {e}, Params: {params}", exc_info=True)
        raise e