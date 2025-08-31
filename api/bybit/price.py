# api/bybit/price.py
import logging
from api.bybit.client import get_public_bybit_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_current_ask_price(symbol: str) -> float:
    """
    Bybit 선물(USDT-Perp, category='linear') 현재 매도호가.
    """
    http = get_public_bybit_client()
    # v5 tickers
    data = http.get_tickers(category="linear", symbol=symbol)
    item = (data.get("result", {}) or {}).get("list", [{}])[0]
    # bybit는 'ask1Price' 제공
    return float(item.get("ask1Price") or item.get("askPrice") or item.get("lastPrice") or 0)

def get_current_bid_price(symbol: str) -> float:
    http = get_public_bybit_client()
    data = http.get_tickers(category="linear", symbol=symbol)
    item = (data.get("result", {}) or {}).get("list", [{}])[0]
    return float(item.get("bid1Price") or item.get("bidPrice") or item.get("lastPrice") or 0)

def get_minute_candles(symbol: str, unit: int = 1, to: str | None = None, count: int = 200):
    """
    바이낸스용 get_minute_candles와 동일한 형태의 리스트[dict]를 반환.
    - keys: opening_price, high_price, low_price, trade_price, candle_acc_trade_volume
    - 최신 캔들이 리스트의 첫 요소가 되도록 '역순' 반환
    """
    http = get_public_bybit_client()
    # interval: '1','3','5','15','30','60',...
    resp = http.get_kline(category="linear", symbol=symbol, interval=str(unit), limit=count)
    raw = (resp.get("result", {}) or {}).get("list", [])  # Bybit는 과거→현재 순
    processed = []
    for it in raw:
        # it: [start, open, high, low, close, volume, turnover]
        # (문자열로 오므로 float 캐스팅)
        processed.append({
            "opening_price": float(it[1]),
            "high_price": float(it[2]),
            "low_price": float(it[3]),
            "trade_price": float(it[4]),
            "candle_acc_trade_volume": float(it[5]),
        })
    return processed[::-1]
