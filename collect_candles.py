# collect_candles.py

from data.candle_collector import collect_minute_candles

if __name__ == "__main__":
    MARKET_TO_COLLECT = "XRPUSDT"  # 예: "BTCUSDT", "ETHUSDT" 등

    # ✅ 수집 시작 시각 (형식: "YYYY-MM-DD HH:MM:SS")
    START_DATE = "2024-01-01 00:00:00"

    # ✅ 수집 종료 시각
    END_DATE = "2024-12-31 23:59:59"

    print(f"⏳ {MARKET_TO_COLLECT}의 1분봉 데이터를 {START_DATE} ~ {END_DATE}까지 수집합니다.")
    collect_minute_candles(MARKET_TO_COLLECT, START_DATE, END_DATE)
    print("🎉 데이터 수집 완료")
