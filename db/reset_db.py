# db/reset_db.py

import sqlite3
import os

DB_PATH = "candle_db.sqlite"

def reset_database():
    """데이터베이스 파일을 삭제하고, 새로운 스키마로 테이블을 다시 생성합니다."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️ 기존 데이터베이스({DB_PATH})를 삭제했습니다.")

    # candle_collector.py와 동일한 스키마로 테이블 생성
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
    print("✅ 새로운 데이터베이스 테이블을 생성했습니다.")

if __name__ == '__main__':
    reset_database()