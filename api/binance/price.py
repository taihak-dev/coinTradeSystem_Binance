# api/binance/price.py
import logging
import pandas as pd
from api.binance.client import get_public_binance_client # 신규: get_public_binance_client를 import

def get_current_ask_price(market: str) -> float:
    """
    바이낸스 선물 시장의 현재가(최근 체결가)를 반환합니다.
    """
    client = get_public_binance_client() # 신규: 공용 클라이언트 사용
    try:
        ticker_info = client.ticker_price(symbol=market)
        current_price = float(ticker_info['price'])
        return current_price
    except Exception as e:
        logging.error(f"바이낸스 현재가 조회 실패 - {market}: {e}")
        raise e

def get_minute_candles(market: str, unit: int = 1, to: str = None, count: int = 200) -> list[dict]:
    """
    바이낸스 선물 시장의 분봉 캔들 데이터를 가져옵니다.
    """
    client = get_public_binance_client() # 신규: 공용 클라이언트 사용
    interval = f"{unit}m" if unit < 60 else f"{unit//60}h"

    end_time_ms = None
    if to:
        end_time_ms = int(pd.to_datetime(to).timestamp() * 1000)

    try:
        klines = client.klines(symbol=market, interval=interval, limit=count, endTime=end_time_ms)

        formatted_candles = []
        for k in klines:
            formatted_candles.append({
                "market": market,
                "candle_date_time_kst": pd.to_datetime(k[0], unit='ms').strftime("%Y-%m-%d %H:%M:%S"),
                "opening_price": float(k[1]),
                "high_price": float(k[2]),
                "low_price": float(k[3]),
                "trade_price": float(k[4]),
                "candle_acc_trade_volume": float(k[5]),
            })
        return formatted_candles
    except Exception as e:
        logging.error(f"바이낸스 분봉 조회 실패 - {market}: {e}")
        raise e