# db/reset_db.py

import sqlite3
import os

# --- 경로 설정 수정 ---
# 현재 파일의 위치를 기준으로 프로젝트 루트 디렉토리의 절대 경로를 계산
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite") # 수정됨
# --- 여기까지 ---

def reset_database():
    """데이터베이스 파일을 삭제하고, 새로운 스키마로 테이블을 다시 생성합니다."""
    # DB 디렉토리가 존재하지 않으면 생성 (candle_collector.py와 일관성 유지)
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"📁 데이터베이스 디렉토리({db_dir})를 생성했습니다.") # 로그 추가

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️ 기존 데이터베이스({DB_PATH})를 삭제했습니다.")
    else:
        print(f"ℹ️ 기존 데이터베이스({DB_PATH})가 없어 삭제할 필요가 없습니다.") # 로그 추가

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