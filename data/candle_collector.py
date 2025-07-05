# data/candle_collector.py

import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import config
import os

# --- 설정에 따라 다른 API 모듈을 가져옴 ---
if config.EXCHANGE == 'binance':
    from api.binance.price import get_minute_candles

    print("[SYSTEM] 데이터 수집기: 바이낸스 모드로 실행합니다.")
else:
    from api.upbit.price import get_minute_candles

    print("[SYSTEM] 데이터 수집기: 업비트 모드로 실행합니다.")

# --- 사용자가 쉽게 수정할 수 있도록 설정 변수를 위로 옮김 ---
MARKET_TO_COLLECT = "XRPUSDT"
START_DATE = "2025-01-01 00:00:00"
END_DATE = "2025-06-30 23:59:59"

# DB 경로: 사용자님이 변경하신 경로를 반영합니다.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


def ensure_table_exists():
    # DB 경로의 디렉토리가 존재하지 않으면 생성
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS minute_candles (
            market TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (market, timestamp)
        )
    """)
    conn.commit()
    conn.close()


def get_existing_timestamps(market: str, start: datetime, end: datetime) -> set:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT timestamp FROM minute_candles WHERE market = ? AND timestamp BETWEEN ? AND ?"
    cursor.execute(query, (market, start.isoformat(), end.isoformat()))
    rows = cursor.fetchall()
    conn.close()
    return {r[0] for r in rows}


def save_candles_to_db(market: str, candles: list[dict]):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for c in candles:
        ts = pd.to_datetime(c["candle_date_time_kst"]).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT OR IGNORE INTO minute_candles
            (market, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            market, ts,
            c["opening_price"], c["high_price"], c["low_price"],
            c["trade_price"], c["candle_acc_trade_volume"]
        ))
    conn.commit()
    conn.close()


def collect_minute_candles(market: str, start: str, end: str):
    ensure_table_exists()
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    print(f"🕯️ {market} 분봉 데이터 수집 시작: {start} ~ {end}")
    existing = get_existing_timestamps(market, start_dt, end_dt)
    print(f"🧩 DB에 이미 저장된 분봉 수: {len(existing)}")

    current_time = end_dt
    all_new_candles_count = 0

    while current_time > start_dt:
        to_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            candles = get_minute_candles(market=market, unit=1, to=to_str, count=200)
            if not candles:
                print("더 이상 가져올 데이터가 없습니다.")
                break

            new_candles = []
            for c in candles:
                candle_time = pd.to_datetime(c["candle_date_time_kst"])
                if start_dt <= candle_time <= end_dt and candle_time.strftime("%Y-%m-%d %H:%M:%S") not in existing:
                    new_candles.append(c)

            if new_candles:
                save_candles_to_db(market, new_candles)
                new_timestamps = {pd.to_datetime(c["candle_date_time_kst"]).strftime("%Y-%m-%d %H:%M:%S") for c in
                                  new_candles}
                existing.update(new_timestamps)
                all_new_candles_count += len(new_candles)
                print(f"💾 {len(new_candles)}개 신규 분봉 저장 완료... (현재까지 총 {all_new_candles_count}개)")

            # --- 여기가 수정된 핵심 부분 ---
            # API 응답의 첫 번째 캔들(가장 과거)의 시간을 기준으로 다음 요청 시간을 설정
            oldest_candle_time = pd.to_datetime(candles[0]["candle_date_time_kst"])
            # --- 여기까지 ---

            current_time = oldest_candle_time - timedelta(minutes=1)
            time.sleep(0.2)

        except Exception as e:
            print(f"❌ 데이터 수집 중 오류 발생: {e}")
            print("5초 후 다시 시도합니다...")
            time.sleep(5)

    print(f"✅ 총 {all_new_candles_count}개의 신규 분봉을 저장했습니다. 수집을 종료합니다.")


if __name__ == "__main__":
    collect_minute_candles(
        market=MARKET_TO_COLLECT,
        start=START_DATE,
        end=END_DATE
    )