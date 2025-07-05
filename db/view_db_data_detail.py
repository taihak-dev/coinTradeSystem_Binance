import sqlite3
import pandas as pd
import os

# DB 경로
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


# 연결 및 조회
with sqlite3.connect(DB_PATH) as conn:
    query = """
    SELECT market,
           MIN(timestamp) AS start_time,
           MAX(timestamp) AS end_time
    FROM minute_candles
    GROUP BY market;
    """
    df = pd.read_sql(query, conn)

print(df)
