# api/binance/price.py
import logging
from binance.error import ClientError
from api.binance.client import get_public_binance_client # 공용 클라이언트 사용
from typing import List, Dict, Optional
import pandas as pd
import time
from datetime import timedelta

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_current_ask_price(symbol: str) -> float:
    """
    바이낸스 선물 시장의 지정된 심볼(예: BTCUSDT)에 대한 현재 매도 호가(Ask Price)를 반환합니다.
    이는 시장에 존재하는 가장 낮은 매도 가격입니다.
    """
    logging.debug(f"📊 {symbol} 현재 매도 호가 조회 시도 중...")
    client = get_public_binance_client()
    try:
        ticker = client.book_ticker(symbol=symbol)
        ask_price = float(ticker['askPrice'])
        logging.debug(f"✅ {symbol} 현재 매도 호가: {ask_price}")
        time.sleep(0.05)  # ⚠️ API 호출 후 작은 딜레이 추가 (예: 50ms)
        return ask_price
    except ClientError as e:
        logging.error(f"❌ {symbol} 현재 매도 호가 조회 실패 (ClientError: {e.error_code}): {e.error_message}")
        raise e
    except Exception as e:
        logging.error(f"❌ {symbol} 현재 매도 호가 조회 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise e


def get_current_bid_price(symbol: str) -> float:
    """
    바이낸스 선물 시장의 지정된 심볼(예: BTCUSDT)에 대한 현재 매수 호가(Bid Price)를 반환합니다.
    이는 시장에 존재하는 가장 높은 매수 가격입니다.
    """
    logging.debug(f"📊 {symbol} 현재 매수 호가 조회 시도 중...")
    client = get_public_binance_client()
    try:
        ticker = client.book_ticker(symbol=symbol)
        bid_price = float(ticker['bidPrice'])
        logging.debug(f"✅ {symbol} 현재 매수 호가: {bid_price}")
        time.sleep(0.05) # ⚠️ API 호출 후 작은 딜레이 추가 (예: 50ms)
        return bid_price
    except ClientError as e:
        logging.error(f"❌ {symbol} 현재 매수 호가 조회 실패 (ClientError: {e.error_code}): {e.error_message}")
        raise e
    except Exception as e:
        logging.error(f"❌ {symbol} 현재 매수 호가 조회 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise e


def get_minute_candles(symbol: str, unit: int = 1, to: Optional[str] = None, count: int = 200) -> List[Dict]:
    """
    바이낸스 선물 시장에서 분(Minute) 단위 캔들 데이터를 가져옵니다.
    Upbit의 캔들 데이터 형식과 유사하게 가공하여 반환합니다.

    :param symbol: 마켓 심볼 (예: BTCUSDT)
    :param unit: 분 단위 (바이낸스는 '1m', '3m' 등으로 표기되나 여기서는 숫자로 받음)
                 (UMFutures API는 'interval' 파라미터에 '1m', '3m' 등을 요구)
    :param to: 마지막 캔들 시각 (exclusive) - ISO8601 또는 "YYYY-MM-DD HH:MM:SS" 포맷 문자열
               (API는 'endTime'에 milliseconds timestamp를 요구)
    :param count: 요청할 캔들 개수 (최대 1500)
    :return: 캔들 리스트 (Upbit 유사 형식의 dict)
    """
    logging.debug(f"🕯️ {symbol} {unit}분봉 캔들 {count}개 조회 시도 중 (to: {to})...")
    client = get_public_binance_client()
    interval = f"{unit}m" # 바이낸스 API 형식에 맞게 변환

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": count # 가져올 캔들의 최대 개수
    }

    if to:
        # 'to' 시각을 밀리초 유닉스 타임스탬프로 변환
        # 바이낸스 API는 endTime 이전의 캔들을 반환 (exclusive)
        to_dt = pd.to_datetime(to)
        params["endTime"] = int(to_dt.timestamp() * 1000)
        logging.debug(f"  -> endTime (ms): {params['endTime']}")

    try:
        klines = client.klines(**params)
        logging.debug(f"✅ {symbol} {unit}분봉 캔들 {len(klines)}개 조회 성공.")
        time.sleep(0.1)  # ⚠️ API 호출 후 딜레이 추가 (캔들 데이터는 가중치가 더 높을 수 있음)
        # Upbit 캔들 형식으로 가공
        # ['opentime', 'open', 'high', 'low', 'close', 'volume', 'closetime', ...]
        processed_candles = []
        for kline in klines:
            # 바이낸스 API의 캔들 close time은 해당 캔들 종료 시각의 밀리초 타임스탬프
            # Upbit의 candle_date_time_kst는 해당 캔들 시작 시각 (KST)
            # KST로 변환 및 1분 이전으로 조정하여 Upbit의 'candle_date_time_kst'와 유사하게 만듦
            close_time_ms = kline[6]
            candle_start_dt_utc = pd.to_datetime(close_time_ms, unit='ms') - timedelta(minutes=unit)
            candle_start_dt_kst = candle_start_dt_utc.tz_localize('UTC').tz_convert('Asia/Seoul')

            processed_candles.append({
                "candle_date_time_utc": candle_start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "candle_date_time_kst": candle_start_dt_kst.strftime("%Y-%m-%dT%H:%M:%S"),
                "opening_price": float(kline[1]),
                "high_price": float(kline[2]),
                "low_price": float(kline[3]),
                "trade_price": float(kline[4]), # 종가
                "candle_acc_trade_volume": float(kline[5]), # 거래량
                # Upbit에 없는 필드는 생략 또는 None으로 처리
            })
        # Upbit는 최신 데이터가 마지막에 오므로 순서를 뒤집음 (만약 API가 과거순으로 준다면)
        # 바이낸스 klines는 기본적으로 과거에서 현재 순서로 정렬되어 제공
        # Upbit get_minute_candles는 최신 캔들이 리스트의 첫 번째 요소로 옴 (역순)
        # 따라서 바이낸스 klines 결과를 역순으로 반환해야 Upbit get_minute_candles와 동일한 동작 기대
        return processed_candles[::-1] # 역순으로 반환
    except ClientError as e:
        logging.error(f"❌ {symbol} {unit}분봉 캔들 조회 실패 (ClientError: {e.error_code}): {e.error_message}")
        raise e
    except Exception as e:
        logging.error(f"❌ {symbol} {unit}분봉 캔들 조회 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise e