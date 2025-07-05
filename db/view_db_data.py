# view_db_data.py

import sqlite3
import pandas as pd
import os

# DB 경로
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")

def load_candle_data(market: str, start: str = None, end: str = None) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)

    query = "SELECT * FROM minute_candles WHERE market = ?"
    params = [market]

    if start and end:
        query += " AND timestamp BETWEEN ? AND ?"
        params.extend([start, end])
    elif start:
        query += " AND timestamp >= ?"
        params.append(start)
    elif end:
        query += " AND timestamp <= ?"
        params.append(end)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# 예시 사용
if __name__ == "__main__":
    # df = load_candle_data("KRW-DOGE")  # ← start, end 없이 전체 로드
    df = load_candle_data("XRPUSDT", "2025-06-01 00:20:00", "2025-06-01 00:30:00")
    print(df.head(10))  # 앞부분만 보기
