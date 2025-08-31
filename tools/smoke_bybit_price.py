import os
os.environ["EXCHANGE"] = "bybit"   # 1) 가장 먼저
from dotenv import load_dotenv
load_dotenv()                      # 2) .env 병행 사용 시

import importlib
import config                      # 3) config가 env를 읽어둔다
importlib.reload(config)           #   (보수적으로 리로드)

# 4) 이후에 서비스 임포트
from services.exchange_service import (
    get_current_ask_price, get_current_bid_price, get_minute_candles
)

def main():
    os.environ["EXCHANGE"] = "bybit"  # 런타임 강제 전환(메인 봇은 건드리지 않음)
    symbol = "BTCUSDT"
    ask = get_current_ask_price(symbol)
    bid = get_current_bid_price(symbol)
    candles = get_minute_candles(symbol, unit=1, count=5)
    print("BYBIT ASK/BID:", ask, bid)
    print("BYBIT 1m CANDLES(5):", candles)

if __name__ == "__main__":
    main()
