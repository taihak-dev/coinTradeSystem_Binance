# db/export_candles_to_excel.py

import sqlite3
import pandas as pd
import os
from datetime import datetime

def export_candles_to_excel(
    db_path: str = "../db/candle_db.sqlite",
    output_dir: str = ".",
    start_date: str = None,  # 예: '2024-01-01 00:00:00'
    end_date: str = None,    # 예: '2024-01-31 23:59:59'
    market: str = None       # 예: 'BTCUSDT'
):
    """
    DB에서 캔들 데이터를 조회하여 엑셀로 저장합니다.
    start_date, end_date, market을 지정하여 데이터를 필터링할 수 있습니다.
    """
    if not os.path.exists(db_path):
        # 현재 스크립트 위치 기준으로 상대 경로 재시도 (실행 위치에 따라 다를 수 있음)
        alt_path = os.path.join(os.path.dirname(__file__), "candle_db.sqlite")
        if os.path.exists(alt_path):
            db_path = alt_path
        else:
            # 프로젝트 루트 기준 경로 시도
            root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "db", "candle_db.sqlite"))
            if os.path.exists(root_path):
                db_path = root_path
            else:
                raise FileNotFoundError(f"❌ DB 파일을 찾을 수 없습니다: {db_path}")

    # 현재 일시로 파일명 생성
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    market_str = f"_{market}" if market else "_ALL"
    filename = f"candles_export{market_str}_{now_str}.xlsx"
    output_path = os.path.join(output_dir, filename)

    # DB 연결 및 데이터 로드
    conn = sqlite3.connect(db_path)
    try:
        query = "SELECT * FROM minute_candles"
        params = []
        conditions = []

        if market:
            conditions.append("market = ?")
            params.append(market)

        if start_date:
            conditions.append("timestamp >= ?")
            params.append(start_date)
        
        if end_date:
            conditions.append("timestamp <= ?")
            params.append(end_date)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY market, timestamp"
        
        print(f"🔍 실행 쿼리: {query}")
        print(f"🔍 파라미터: {params}")

        df = pd.read_sql_query(query, conn, params=params)
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
    # 예시 1: 전체 데이터 추출
    # export_candles_to_excel()
    
    # 예시 2: 특정 기간 데이터 추출
    # export_candles_to_excel(start_date="2025-01-01 00:00:00", end_date="2025-01-31 23:59:59")

    # 예시 3: 특정 코인 및 기간 데이터 추출
    export_candles_to_excel(market="BTCUSDT", start_date="2025-01-01 00:00:00", end_date="2025-12-04 23:59:59")