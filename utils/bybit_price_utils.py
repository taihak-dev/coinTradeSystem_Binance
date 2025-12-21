# utils/bybit_price_utils.py

import logging
from api.bybit.client import get_bybit_client
from decimal import Decimal, getcontext, ROUND_HALF_UP

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 거래소 규칙 캐싱을 위한 변수 ---
_instrument_info_cache = {}

# Decimal 모듈 정밀도 설정
getcontext().prec = 20


def get_instrument_info(symbol: str) -> dict:
    """
    Bybit에서 특정 심볼의 거래 규칙(instrument info)을 가져와 캐시에 저장하고 반환합니다.
    """
    global _instrument_info_cache
    if symbol in _instrument_info_cache:
        return _instrument_info_cache[symbol]

    logging.info(f"🌐 Bybit 거래소 규칙 정보 로드 중 ({symbol})...")
    try:
        client = get_bybit_client()
        response = client.get_instruments_info(category="linear", symbol=symbol)

        if response and response['result']['list']:
            info = response['result']['list'][0]
            _instrument_info_cache[symbol] = info
            logging.info(f"✅ {symbol} 거래 규칙 정보 로드 완료.")
            return info
        else:
            raise ValueError(f"{symbol}의 거래 규칙 정보를 찾을 수 없습니다.")

    except Exception as e:
        logging.error(f"❌ {symbol} 거래 규칙 정보 로드 실패: {e}", exc_info=True)
        raise


def adjust_price_to_tick(symbol: str, price: float) -> float:
    """
    Bybit의 가격 규칙(tickSize)에 맞게 가격을 조정합니다.
    """
    try:
        info = get_instrument_info(symbol)
        tick_size_str = info['priceFilter']['tickSize']

        price_dec = Decimal(str(price))
        tick_size_dec = Decimal(tick_size_str)

        # tickSize의 배수로 가격을 조정 (내림 처리)
        adjusted_price_dec = (price_dec / tick_size_dec).to_integral_value(rounding='ROUND_DOWN') * tick_size_dec

        adjusted_price = float(adjusted_price_dec)
        if price != adjusted_price:
            logging.debug(f"🔢 {symbol} 가격 조정: {price} -> {adjusted_price}")
        return adjusted_price

    except Exception as e:
        logging.error(f"{symbol} 가격 조정 실패, 기본 반올림 적용: {e}")
        # 실패 시 안전하게 기본값 처리
        return round(price, 8)


def adjust_quantity_to_step(symbol: str, quantity: float) -> float:
    """
    Bybit의 수량 규칙(qtyStep)에 맞게 수량을 조정합니다.
    목표 금액과의 오차를 줄이기 위해 내림(Floor) 대신 반올림(Round)을 사용합니다.
    """
    try:
        info = get_instrument_info(symbol)
        qty_step_str = info['lotSizeFilter']['qtyStep']

        quantity_dec = Decimal(str(quantity))
        qty_step_dec = Decimal(qty_step_str)

        # --- 👇👇👇 수정된 부분: 반올림(ROUND_HALF_UP) 적용 👇👇👇 ---
        # qtyStep의 배수로 수량을 조정 (반올림 처리하여 목표 금액 오차 최소화)
        adjusted_quantity_dec = (quantity_dec / qty_step_dec).to_integral_value(rounding=ROUND_HALF_UP) * qty_step_dec
        # --- 👆👆👆 수정 완료 --- 👆👆👆

        # 최소/최대 주문 수량 확인
        min_qty = Decimal(info['lotSizeFilter']['minOrderQty'])
        max_qty = Decimal(info['lotSizeFilter']['maxOrderQty'])

        # 조정된 수량이 최소 주문량보다 작으면 0 또는 최소 주문량으로 처리
        if adjusted_quantity_dec < min_qty:
            logging.warning(f"⚠️ {symbol} 조정된 수량({adjusted_quantity_dec})이 최소 주문 수량({min_qty})보다 작아 0으로 처리합니다.")
            return 0.0

        adjusted_quantity_dec = min(adjusted_quantity_dec, max_qty)

        # --- 👇👇👇 수정된 부분: 소수점 자릿수 보정 👇👇👇 ---
        # 부동소수점 오차 제거를 위해 qtyStep의 자릿수만큼 round 처리
        precision = len(qty_step_str.split('.')[1]) if '.' in qty_step_str else 0
        adjusted_quantity = round(float(adjusted_quantity_dec), precision)
        # --- 👆👆👆 수정 완료 --- 👆👆👆

        if quantity != adjusted_quantity:
            logging.debug(f"🔢 {symbol} 수량 조정: {quantity} -> {adjusted_quantity}")
        return adjusted_quantity

    except Exception as e:
        logging.error(f"{symbol} 수량 조정 실패, 기본 반올림 적용: {e}")
        # 실패 시 안전하게 기본값 처리
        return round(quantity, 6)