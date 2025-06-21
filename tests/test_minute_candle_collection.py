import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# ✅ 업비트 API: 1분봉 데이터 요청
def get_minute_candles(market: str, to: str, count: int = 200, unit: int = 1) -> list:
    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"
    headers = {"accept": "application/json"}
    params = {
        "market": market,
        "to": to,
        "count": count
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ 요청 실패: {response.status_code} - {response.text}")
        return []

# ✅ 1차 수집: 최신 → 과거 방향으로 분봉 수집
def fetch_minute_candles_basic(market: str, start: str, end: str) -> pd.DataFrame:
    start_time = pd.to_datetime(start)
    end_time = pd.to_datetime(end)
    all_candles = []
    timestamps_collected = set()
    current_to = end_time

    while True:
        to_str = current_to.strftime("%Y-%m-%dT%H:%M:%S+09:00")
        candles = get_minute_candles(market, to=to_str, count=200, unit=1)
        if not candles:
            break

        for candle in candles:
            ts = pd.to_datetime(candle["candle_date_time_kst"])
            if ts < start_time:
                return pd.DataFrame(all_candles)
            if start_time <= ts <= end_time and ts not in timestamps_collected:
                all_candles.append({
                    "timestamp": ts,
                    "open": candle["opening_price"],
                    "high": candle["high_price"],
                    "low": candle["low_price"],
                    "close": candle["trade_price"],
                    "volume": candle["candle_acc_trade_volume"]
                })
                timestamps_collected.add(ts)

        current_to = pd.to_datetime(candles[-1]["candle_date_time_kst"]) - timedelta(minutes=1)
        if current_to < start_time:
            break

        time.sleep(0.11)

    return pd.DataFrame(all_candles)

# ✅ 2차 보정: 누락된 분봉 재조회
def fetch_missing_candles(market: str, missing: pd.DatetimeIndex) -> list:
    filled_candles = []
    for ts in missing:
        to_str = (ts + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S+09:00")
        candle = get_minute_candles(market, to=to_str, count=1)
        if candle:
            c = candle[0]
            filled_candles.append({
                "timestamp": pd.to_datetime(c["candle_date_time_kst"]),
                "open": c["opening_price"],
                "high": c["high_price"],
                "low": c["low_price"],
                "close": c["trade_price"],
                "volume": c["candle_acc_trade_volume"]
            })
        time.sleep(0.11)
    return filled_candles

# ✅ 실행 설정
if __name__ == "__main__":
    market = "KRW-DOGE"
    start = "2025-06-19 09:55:00"
    end = "2025-06-19 10:10:00"

    print(f"⏳ 분봉 수집 시작: {market} {start} ~ {end}")
    df_base = fetch_minute_candles_basic(market, start, end)
    df_base.sort_values("timestamp", inplace=True)
    print(f"📊 기본 수집된 분봉 수: {len(df_base)}")

    # ✅ 누락 시각 확인
    expected = pd.date_range(start=start, end=end, freq="1min")
    actual = pd.to_datetime(df_base["timestamp"])
    missing = expected.difference(actual)

    # ✅ 누락 보정
    recovered = []
    missing_failed = []

    for ts in missing:
        to_str = (ts + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S+09:00")
        candle = get_minute_candles(market, to=to_str, count=1)
        if candle:
            c = candle[0]
            recovered.append({
                "timestamp": pd.to_datetime(c["candle_date_time_kst"]),
                "open": c["opening_price"],
                "high": c["high_price"],
                "low": c["low_price"],
                "close": c["trade_price"],
                "volume": c["candle_acc_trade_volume"]
            })
        else:
            missing_failed.append(ts)
        time.sleep(0.11)

    df_missing = pd.DataFrame(recovered)
    df_all = pd.concat([df_base, df_missing]).sort_values("timestamp").drop_duplicates("timestamp")

    print(f"✅ 보정 성공 분봉 수: {len(df_missing)}")
    print(f"❌ 보정 실패한 분봉 수: {len(missing_failed)}")
    for ts in missing_failed:
        print(f"   ⛔ 존재하지 않는 분봉: {ts}")

    # ✅ 저장
    filename = f"doge_분봉_{pd.to_datetime(start).strftime('%Y%m%d_%H%M')}_보정포함.xlsx"
    df_all.to_excel(filename, index=False)
    print(f"📁 저장 완료: {filename}")
