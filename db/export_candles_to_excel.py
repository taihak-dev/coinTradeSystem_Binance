# db/export_candles_to_excel.py

import sqlite3
import pandas as pd
import os
from datetime import datetime

def export_all_candles_to_excel(
    db_path: str = "../db/candle_db.sqlite",
    output_dir: str = "."
):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ DB 파일을 찾을 수 없습니다: {db_path}")

    # 현재 일시로 파일명 생성
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"candles_export_{now_str}.xlsx"
    output_path = os.path.join(output_dir, filename)

    # DB 연결 및 데이터 로드
    conn = sqlite3.connect(db_path)
    try:
        query = "SELECT * FROM minute_candles ORDER BY market, timestamp"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        raise RuntimeError(f"❌ SQL 실행 오류: {e}")
    finally:
        conn.close()

    if df.empty:
        print("⚠️ 가져올 데이터가 없습니다.")
    else:
        df.to_excel(output_path, index=False)
        print(f"✅ {len(df)}개의 데이터를 '{output_path}'로 저장했습니다.")

# 직접 실행할 경우
if __name__ == "__main__":
    export_all_candles_to_excel()
