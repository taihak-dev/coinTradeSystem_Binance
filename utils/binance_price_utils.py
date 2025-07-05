# utils/binance_price_utils.py
import logging
from api.binance.client import get_binance_client

# 거래소의 가격/수량 규칙을 캐싱하여 반복적인 API 호출을 피함
_exchange_info_cache = None


def get_exchange_info():
    """거래소의 모든 심볼에 대한 규칙(필터) 정보를 가져와 캐싱합니다."""
    global _exchange_info_cache
    if _exchange_info_cache is None:
        logging.info("바이낸스 거래소 규칙 정보를 가져옵니다 (최초 1회 실행).")
        _exchange_info_cache = get_binance_client().exchange_info()
    return _exchange_info_cache


def get_symbol_filters(symbol):
    """특정 심볼에 대한 필터(규칙)를 반환합니다."""
    info = get_exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            return {f['filterType']: f for f in s['filters']}
    raise ValueError(f"{symbol}에 대한 거래소 규칙을 찾을 수 없습니다.")


def adjust_price_to_tick(price, symbol):
    """
    주어진 가격을 해당 심볼의 tickSize에 맞게 조정합니다.
    (예: tickSize가 0.01이면, 가격 123.456 -> 123.45로 조정)
    """
    try:
        filters = get_symbol_filters(symbol)
        tick_size = float(filters['PRICE_FILTER']['tickSize'])

        # tick_size에 맞춰 가격 조정
        # (price / tick_size)를 내림한 후 다시 tick_size를 곱함
        adjusted_price = (price // tick_size) * tick_size

        # 소수점 정밀도 계산 (e.g., 0.01 -> 2, 10 -> 0)
        precision = 0
        if '.' in str(tick_size):
            precision = len(str(tick_size).split('.')[1].rstrip('0'))

        return round(adjusted_price, precision)

    except Exception as e:
        logging.error(f"가격 조정 실패 ({symbol}, {price}): {e}. 원본 가격을 반환합니다.")
        return price


def adjust_quantity_to_step(quantity, symbol):
    """
    주어진 수량을 해당 심볼의 stepSize에 맞게 조정합니다.
    (예: stepSize가 0.001이면, 수량 1.2345 -> 1.234로 조정)
    """
    try:
        filters = get_symbol_filters(symbol)
        step_size = float(filters['LOT_SIZE']['stepSize'])

        # step_size에 맞춰 수량 조정
        adjusted_quantity = (quantity // step_size) * step_size

        # 소수점 정밀도 계산
        precision = 0
        if '.' in str(step_size):
            precision = len(str(step_size).split('.')[1].rstrip('0'))

        return round(adjusted_quantity, precision)

    except Exception as e:
        logging.error(f"수량 조정 실패 ({symbol}, {quantity}): {e}. 원본 수량을 반환합니다.")
        return quantity