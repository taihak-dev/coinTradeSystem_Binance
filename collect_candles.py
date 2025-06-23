# collect_candles.py

from data.candle_collector import collect_minute_candles

if __name__ == "__main__":
    # ✅ 수집할 코인 마켓명 (예: KRW-BTC, KRW-DOGE, KRW-ETH 등)
    market = "KRW-ETH"

    # ✅ 수집 시작 시각 (형식: "YYYY-MM-DD HH:MM:SS")
    start_time = "2023-01-01 00:00:00"

    # ✅ 수집 종료 시각
    end_time = "2025-06-10 00:00:00"

    print(f"⏳ {market}의 1분봉 데이터를 {start_time} ~ {end_time}까지 수집합니다.")
    collect_minute_candles(market, start_time, end_time)
    print("🎉 데이터 수집 완료")
