# api/bybit/price.py

import logging
import time
from datetime import datetime, timezone
from api.bybit.client import get_bybit_client

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_current_ask_price(symbol: str) -> float:
    """
    Bybit 선물 시장의 지정된 심볼에 대한 현재 매도 호가(Ask Price)를 반환합니다.
    """
    logging.debug(f"📊 {symbol} 현재 매도 호가 조회 시도 중...")
    client = get_bybit_client()
    try:
        ticker = client.get_tickers(category="linear", symbol=symbol)
        ask_price = float(ticker['result']['list'][0]['ask1Price'])
        logging.debug(f"✅ {symbol} 현재 매도 호가: {ask_price}")
        time.sleep(0.1)  # API Rate Limit 방지를 위한 짧은 대기
        return ask_price
    except Exception as e:
        logging.error(f"❌ {symbol} 현재 매도 호가 조회 실패: {e}", exc_info=True)
        raise


def get_minute_candles(market, to=None, count=200, unit=1):
    """
    Bybit에서 분봉 캔들 데이터를 가져옵니다.
    Binance API의 반환 형식과 동일한 구조로 가공하여 반환합니다.
    """
    client = get_bybit_client()

    # Bybit는 end 타임스탬프(ms)를 파라미터로 받음
    end_timestamp = None
    if to:
        # 'YYYY-MM-DD HH:MM:SS' 형식의 문자열을 timestamp(ms)로 변환
        end_dt = datetime.strptime(to, '%Y-%m-%d %H:%M:%S')
        end_timestamp = int(end_dt.timestamp() * 1000)

    try:
        response = client.get_kline(
            category="linear",
            symbol=market,
            interval=str(unit),  # 분 단위
            limit=count,
            end=end_timestamp,
        )

        klines = response['result']['list']
        processed_candles = []
        for kline in klines:
            # [중요] Bybit 타임스탬프(ms)를 Binance와 같은 날짜/시간 문자열 형식으로 변환
            start_dt_utc = datetime.fromtimestamp(int(kline[0]) / 1000, tz=timezone.utc)
            start_dt_kst = start_dt_utc.astimezone(timezone(datetime.now(timezone.utc).astimezone().tzinfo))

            processed_candles.append({
                "candle_date_time_utc": start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "candle_date_time_kst": start_dt_kst.strftime("%Y-%m-%dT%H:%M:%S"),
                "opening_price": float(kline[1]),
                "high_price": float(kline[2]),
                "low_price": float(kline[3]),
                "trade_price": float(kline[4]),  # 종가
                "candle_acc_trade_volume": float(kline[5]),  # 거래량
            })

        # Bybit는 최신 데이터가 먼저 오므로, 과거->현재 순서로 뒤집어줌
        return processed_candles[::-1]

    except Exception as e:
        logging.error(f"❌ {market} 캔들 데이터 조회 실패: {e}", exc_info=True)
        return []