# utils/binance_price_utils.py
import logging
from api.binance.client import get_binance_client # 인증된 클라이언트 사용 (exchange_info는 인증 불필요하나, 인증 클라이언트가 이미 포함)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 거래소의 모든 심볼에 대한 규칙 정보를 캐싱하여 반복적인 API 호출을 피함
# 이 캐시는 프로그램 실행 중 한 번만 로드됩니다.
_exchange_info_cache = None


def get_exchange_info():
    """
    바이낸스 거래소의 모든 심볼(거래 쌍)에 대한 상세 규칙(필터) 정보를 가져와 캐싱합니다.
    이 정보는 가격(tickSize) 및 수량(stepSize) 조정을 위해 사용됩니다.
    """
    global _exchange_info_cache
    if _exchange_info_cache is None:
        logging.info("🌐 바이낸스 거래소 규칙 정보 로드 중 (최초 1회 실행).")
        try:
            # get_binance_client는 인증된 클라이언트를 반환하며, exchange_info는 인증이 필요 없음.
            # 그러나 이미 생성된 클라이언트를 재사용하는 것이 효율적.
            _exchange_info_cache = get_binance_client().exchange_info()
            logging.info("✅ 바이낸스 거래소 규칙 정보 로드 완료.")
        except Exception as e:
            logging.error(f"❌ 바이낸스 거래소 규칙 정보 로드 실패: {e}", exc_info=True)
            raise RuntimeError(f"바이낸스 거래소 규칙 정보를 가져올 수 없습니다: {e}")
    return _exchange_info_cache


def get_symbol_filters(symbol: str) -> dict:
    """
    특정 심볼(거래 쌍)에 대한 바이낸스 거래 규칙(필터)을 딕셔너리 형태로 반환합니다.

    :param symbol: 조회할 심볼 (예: BTCUSDT)
    :return: 필터 딕셔너리 {filterType: filter_details}
    :raises ValueError: 심볼에 대한 규칙을 찾을 수 없을 때
    """
    info = get_exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            # 각 필터의 타입(예: PRICE_FILTER, LOT_SIZE)을 키로 하는 딕셔너리로 변환
            return {f['filterType']: f for f in s['filters']}
    logging.error(f"❌ {symbol}에 대한 거래소 규칙을 찾을 수 없습니다. (심볼명 확인 필요)")
    raise ValueError(f"{symbol}에 대한 거래소 규칙을 찾을 수 없습니다. 심볼명을 확인해주세요.")


def adjust_price_to_tick(symbol: str, price: float) -> float:  # 파라미터 순서 변경 (symbol, price)
    """
    주어진 가격을 해당 심볼의 'PRICE_FILTER'에 정의된 'tickSize'에 맞게 조정합니다.
    """
    try:
        filters = get_symbol_filters(symbol)
        if 'PRICE_FILTER' not in filters:
            logging.warning(f"⚠️ {symbol}에 PRICE_FILTER가 없습니다. 가격 조정을 건너뛰고 원본 가격 {price}를 반환합니다.")
            return price

        tick_size = float(filters['PRICE_FILTER']['tickSize'])
        adjusted_price = (price // tick_size) * tick_size

        precision = 0
        if '.' in str(tick_size):
            precision = len(str(tick_size).split('.')[1].rstrip('0'))

        final_adjusted_price = round(adjusted_price, precision)
        logging.debug(f"📈 {symbol} 가격 조정: 원본={price:.8f}, tickSize={tick_size}, 조정 후={final_adjusted_price:.8f}")
        return final_adjusted_price

    except Exception as e:
        logging.error(f"❌ {symbol} 가격 조정 실패 (원본 가격: {price}): {e}. 원본 가격을 반환합니다.", exc_info=True)
        return price


def adjust_quantity_to_step(symbol: str, quantity: float) -> float:  # 파라미터 순서 변경 (symbol, quantity)
    """
    주어진 수량을 해당 심볼의 'LOT_SIZE'에 정의된 'stepSize'에 맞게 조정합니다.
    """
    try:
        filters = get_symbol_filters(symbol)
        if 'LOT_SIZE' not in filters:
            logging.warning(f"⚠️ {symbol}에 LOT_SIZE 필터가 없습니다. 수량 조정을 건너뛰고 원본 수량 {quantity}를 반환합니다.")
            return quantity

        step_size = float(filters['LOT_SIZE']['stepSize'])
        min_qty = float(filters['LOT_SIZE']['minQty'])
        max_qty = float(filters['LOT_SIZE']['maxQty'])

        adjusted_quantity = (quantity // step_size) * step_size

        adjusted_quantity = max(adjusted_quantity, min_qty)
        adjusted_quantity = min(adjusted_quantity, max_qty)

        precision = 0
        if '.' in str(step_size):
            precision = len(str(step_size).split('.')[1].rstrip('0'))

        final_adjusted_quantity = round(adjusted_quantity, precision)
        logging.debug(f"🔢 {symbol} 수량 조정: 원본={quantity:.8f}, stepSize={step_size}, 조정 후={final_adjusted_quantity:.8f}")

        if final_adjusted_quantity < min_qty and quantity > 0:
            logging.warning(
                f"⚠️ {symbol} 최종 조정 수량({final_adjusted_quantity})이 최소 거래 수량({min_qty}) 미만입니다. 최소 수량으로 설정합니다.")
            return min_qty

        return final_adjusted_quantity

    except Exception as e:
        logging.error(f"❌ {symbol} 수량 조정 실패 (원본 수량: {quantity}): {e}. 원본 수량을 반환합니다.", exc_info=True)
        return quantity