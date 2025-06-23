# data/candle_collector.py

import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
from api.price import get_minute_candles
import time

DB_PATH = "db/candle_db.sqlite"

def ensure_table_exists():
    """DB와 테이블이 없으면 생성"""
    os.makedirs("db", exist_ok=True)
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
    query = """
        SELECT timestamp FROM minute_candles
        WHERE market = ? AND timestamp BETWEEN ? AND ?
    """
    cursor.execute(query, (market, start.isoformat(), end.isoformat()))
    rows = cursor.fetchall()
    conn.close()
    return {r[0] for r in rows}

def save_candles_to_db(market: str, candles: list[dict]):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for c in candles:
        # ✅ 기존: ISO 8601 형식 (T 포함)
        # ts = pd.to_datetime(c["candle_date_time_kst"]).isoformat()

        # ✅ 수정: 일반적인 날짜-시간 문자열 형식
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

    existing = get_existing_timestamps(market, start_dt, end_dt)
    print(f"🧩 기존에 저장된 분봉 수: {len(existing)}")

    current_time = end_dt
    all_new = []

    while current_time > start_dt:
        to_str = current_time.strftime("%Y-%m-%dT%H:%M:%S+09:00")
        try:
            candles = get_minute_candles(market=market, unit=1, to=to_str, count=200)
            if not candles:
                break

            # 최신 → 과거 정렬
            candles = sorted(candles, key=lambda c: c["candle_date_time_kst"])
            new_candles = [
                c for c in candles
                if pd.to_datetime(c["candle_date_time_kst"]).isoformat() not in existing
            ]
            save_candles_to_db(market, new_candles)
            all_new.extend(new_candles)

            last_time = pd.to_datetime(candles[0]["candle_date_time_kst"])
            current_time = last_time - timedelta(minutes=1)
            time.sleep(0.1)

        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            time.sleep(5)

    print(f"✅ 신규 저장된 분봉 수: {len(all_new)}")

