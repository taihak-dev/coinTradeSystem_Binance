# utils/binance_price_utils.py

import logging
from api.binance.client import get_binance_client
# decimal 모듈 추가
from decimal import Decimal, getcontext

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

_exchange_info_cache = None

# Decimal 모듈의 정밀도 설정 (바이낸스 가격 정밀도에 맞춰 충분히 높게 설정)
getcontext().prec = 20  # 20자리 정밀도로 설정. 필요에 따라 더 높게 설정 가능.


def get_exchange_info():
    global _exchange_info_cache
    if _exchange_info_cache is None:
        logging.info("🌐 바이낸스 거래소 규칙 정보 로드 중 (최초 1회 실행).")
        try:
            _exchange_info_cache = get_binance_client().exchange_info()
            logging.info("✅ 바이낸스 거래소 규칙 정보 로드 완료.")
        except Exception as e:
            logging.error(f"❌ 바이낸스 거래소 규칙 정보 로드 실패: {e}", exc_info=True)
            raise RuntimeError(f"바이낸스 거래소 규칙 정보를 가져올 수 없습니다: {e}")
    return _exchange_info_cache


def get_symbol_filters(symbol: str) -> dict:
    info = get_exchange_info()
    for s in info['symbols']:
        if s['symbol'] == symbol:
            return {f['filterType']: f for f in s['filters']}
    logging.error(f"❌ {symbol}에 대한 거래소 규칙을 찾을 수 없습니다. (심볼명 확인 필요)")
    raise ValueError(f"{symbol}에 대한 거래소 규칙을 찾을 수 없습니다. 심볼명을 확인해주세요.")


def adjust_price_to_tick(symbol: str, price: float) -> float:
    """
    주어진 가격을 해당 심볼의 'PRICE_FILTER'에 정의된 'tickSize'에 맞게 조정합니다.
    조정된 가격이 0이 될 경우, 유효한 최소 틱 사이즈 가격을 반환하도록 로직을 강화합니다.
    """
    try:
        filters = get_symbol_filters(symbol)
        if 'PRICE_FILTER' not in filters:
            logging.warning(f"⚠️ {symbol}에 PRICE_FILTER가 없습니다. 가격 조정을 건너뛰고 원본 가격 {price}를 반환합니다.")
            return price

        # Decimal을 사용하여 부동소수점 오차를 최소화
        tick_size_dec = Decimal(str(filters['PRICE_FILTER']['tickSize']))
        min_price_dec = Decimal(str(filters['PRICE_FILTER'].get('minPrice', '0')))
        price_dec = Decimal(str(price))

        # 가격을 tickSize의 배수로 조정
        # Decimal에서는 // 연산자가 정의되어 있지 않으므로, quantize를 사용
        # to_nearest_zero는 0 방향으로 내림 (float의 // 와 유사)
        adjusted_price_dec = (price_dec / tick_size_dec).quantize(Decimal('1'),
                                                                  rounding=getcontext().rounding) * tick_size_dec

        # ⭐⭐⭐ 핵심 수정: 조정된 가격이 0 이하일 때의 처리 강화 ⭐⭐⭐
        if adjusted_price_dec <= 0 and price_dec > 0:  # 원본 가격이 양수였는데 0 이하로 조정된 경우
            logging.warning(
                f"⚠️ {symbol} 가격 조정 결과가 0 이하입니다 (원본: {price:.10f}, 조정 후: {float(adjusted_price_dec):.10f}). 최소 가격으로 보정합니다.")

            # min_price_dec와 tick_size_dec 중 큰 값을 선택
            corrected_price_candidate = max(min_price_dec, tick_size_dec)

            # 최종적으로 tick_size의 배수이면서 0보다 큰 최소 가격 보장
            if corrected_price_candidate <= 0:  # 혹시 minPrice나 tickSize 자체가 0이거나 음수인 경우 방지
                corrected_price_candidate = Decimal('0.00000001')  # 아주 작은 양수 값으로 강제 설정 (극단적인 경우)

            # 다시 한번 tick_size에 맞춰 조정 (혹시라도 corrected_price_candidate가 tick_size의 배수가 아닐 수 있으므로)
            final_corrected_price_dec = (corrected_price_candidate / tick_size_dec).quantize(Decimal('1'),
                                                                                             rounding=getcontext().rounding) * tick_size_dec

            # 최종 결과가 여전히 0 이하인지 다시 확인
            if final_corrected_price_dec <= 0:
                final_corrected_price_dec = tick_size_dec if tick_size_dec > 0 else Decimal('0.00000001')

            logging.info(f"✅ {symbol} 가격 0 이하 조정 완료: {float(final_corrected_price_dec):.10f}")
            return float(final_corrected_price_dec)  # float으로 변환하여 반환

        # 최종 정밀도 조정 (Decimal의 quantize 사용)
        # tick_size의 소수점 이하 자릿수 계산
        precision_str = str(tick_size_dec).split('.')
        precision = len(precision_str[1]) if len(precision_str) > 1 else 0
        quantized_adjusted_price_dec = adjusted_price_dec.quantize(Decimal('1e-' + str(precision)))

        logging.debug(
            f"📈 {symbol} 가격 조정: 원본={price:.10f}, tickSize={float(tick_size_dec)}, 조정 후={float(quantized_adjusted_price_dec):.10f}")
        return float(quantized_adjusted_price_dec)

    except Exception as e:
        logging.error(f"❌ {symbol} 가격 조정 실패 (원본 가격: {price}): {e}. 원본 가격을 반환합니다.", exc_info=True)
        return price


def adjust_quantity_to_step(symbol: str, quantity: float) -> float:
    """
    주어진 수량을 해당 심볼의 'LOT_SIZE'에 정의된 'stepSize'에 맞게 조정합니다.
    """
    try:
        filters = get_symbol_filters(symbol)
        if 'LOT_SIZE' not in filters:
            logging.warning(f"⚠️ {symbol}에 LOT_SIZE 필터가 없습니다. 수량 조정을 건너뛰고 원본 수량 {quantity}를 반환합니다.")
            return quantity

        # Decimal을 사용하여 부동소수점 오차를 최소화
        step_size_dec = Decimal(str(filters['LOT_SIZE']['stepSize']))
        min_qty_dec = Decimal(str(filters['LOT_SIZE']['minQty']))
        max_qty_dec = Decimal(str(filters['LOT_SIZE']['maxQty']))
        quantity_dec = Decimal(str(quantity))

        # 수량을 stepSize의 배수로 조정
        adjusted_quantity_dec = (quantity_dec / step_size_dec).quantize(Decimal('1'),
                                                                        rounding=getcontext().rounding) * step_size_dec

        # minQty와 maxQty 범위 내로 조정
        adjusted_quantity_dec = max(adjusted_quantity_dec, min_qty_dec)
        adjusted_quantity_dec = min(adjusted_quantity_dec, max_qty_dec)

        # 최종 정밀도 조정 (step_size의 소수점 이하 자릿수)
        precision_str = str(step_size_dec).split('.')
        precision = len(precision_str[1]) if len(precision_str) > 1 else 0
        final_adjusted_quantity_dec = adjusted_quantity_dec.quantize(Decimal('1e-' + str(precision)))

        logging.debug(
            f"🔢 {symbol} 수량 조정: 원본={quantity:.10f}, stepSize={float(step_size_dec)}, 조정 후={float(final_adjusted_quantity_dec):.10f}")

        # 조정된 수량이 min_qty 미만이고, 원본 수량이 0보다 컸다면 경고 후 min_qty 반환
        if final_adjusted_quantity_dec < min_qty_dec and quantity_dec > 0:
            logging.warning(
                f"⚠️ {symbol} 최종 조정 수량({float(final_adjusted_quantity_dec)})이 최소 거래 수량({float(min_qty_dec)}) 미만입니다. 최소 수량으로 설정합니다.")
            return float(min_qty_dec)

        return float(final_adjusted_quantity_dec)

    except Exception as e:
        logging.error(f"❌ {symbol} 수량 조정 실패 (원본 수량: {quantity}): {e}. 원본 수량을 반환합니다.", exc_info=True)
        return quantity