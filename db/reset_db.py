# db/reset_db.py

import sqlite3
import os

DB_PATH = "../db/candle_db.sqlite"

def reset_minute_candle_table():
    if not os.path.exists(DB_PATH):
        print(f"❌ DB 파일이 존재하지 않아 새로 생성합니다: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 테이블 삭제 및 재생성
    cursor.execute("DROP TABLE IF EXISTS minute_candles")
    cursor.execute("""
        CREATE TABLE minute_candles (
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

    print("✅ 'minute_candles' 테이블 초기화 완료")

if __name__ == "__main__":
    reset_minute_candle_table()
