# data/candle_collector.py

import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import config
import logging # 로깅 모듈 임포트

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# 설정 변수 (사용자가 쉽게 수정)
# 이 값들은 collect_candles.py에서 전달받거나, 여기서 직접 설정할 수 있습니다.
# MARKET_TO_COLLECT = "XRPUSDT" # collect_candles.py에서 전달받음
# START_DATE = "2025-01-01 00:00:00"
# END_DATE = "2025-06-30 23:59:59"

# DB 경로: 프로젝트 루트를 기준으로 절대 경로를 사용 (reset_db.py와 일관성)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


# config 설정에 따라 다른 API 모듈을 가져옴
if config.EXCHANGE == 'binance':
    from api.binance.price import get_minute_candles
    logging.info("[SYSTEM] 데이터 수집기: 바이낸스 모드로 실행합니다.")
else:
    from api.upbit.price import get_minute_candles
    logging.info("[SYSTEM] 데이터 수집기: 업비트 모드로 실행합니다.")


def ensure_table_exists():
    """SQLite 데이터베이스 파일과 'minute_candles' 테이블이 존재하는지 확인하고,
    없다면 생성합니다.
    """
    # DB 경로의 디렉토리가 존재하지 않으면 생성
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logging.info(f"📁 데이터베이스 디렉토리({db_dir})를 생성했습니다.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
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
        logging.info("✅ 'minute_candles' 테이블 존재 확인 또는 생성 완료.")
    except sqlite3.Error as e:
        logging.error(f"❌ 데이터베이스 테이블 생성 중 오류 발생: {e}")
        raise e
    finally:
        conn.close()


def get_existing_timestamps(market: str, start: datetime, end: datetime) -> set:
    """
    데이터베이스에서 지정된 마켓과 기간에 이미 저장된 캔들 데이터의 타임스탬프를 가져옵니다.
    이는 중복 저장을 방지하기 위함입니다.
    """
    logging.debug(f"🔍 {market} 기존 캔들 타임스탬프 조회 중 ({start} ~ {end})...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT timestamp FROM minute_candles WHERE market = ? AND timestamp BETWEEN ? AND ?"
    try:
        cursor.execute(query, (market, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")))
        rows = cursor.fetchall()
        existing_count = len(rows)
        logging.info(f"🧩 DB에 이미 저장된 {market} 캔들 수: {existing_count}개.")
        return {r[0] for r in rows}
    except sqlite3.Error as e:
        logging.error(f"❌ 기존 타임스탬프 조회 중 오류 발생: {e}")
        raise e
    finally:
        conn.close()


def save_candles_to_db(market: str, candles: list[dict]):
    """
    새로 수집된 캔들 데이터를 데이터베이스에 저장합니다.
    이미 존재하는 캔들은 무시(IGNORE)합니다.
    """
    if not candles:
        logging.debug("저장할 캔들 데이터가 없습니다.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    insert_count = 0
    try:
        for c in candles:
            # Upbit 유사 형식의 캔들 데이터를 DB 스키마에 맞게 매핑
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
            # 삽입 또는 무시된 행 수를 확인하여 실제로 삽입된 경우만 카운트
            if cursor.rowcount > 0:
                insert_count += 1
        conn.commit()
        logging.info(f"💾 {market} 신규 분봉 {insert_count}개 저장 완료.")
    except sqlite3.Error as e:
        logging.error(f"❌ 캔들 데이터 DB 저장 중 오류 발생: {e}")
        conn.rollback() # 오류 발생 시 롤백
        raise e
    finally:
        conn.close()


def collect_minute_candles(market: str, start: str, end: str):
    """
    지정된 마켓에 대해 시작일부터 종료일까지의 분봉 캔들 데이터를 수집하여 DB에 저장합니다.
    과거 데이터부터 순차적으로 수집하며, 이미 존재하는 데이터는 건너뜁니다.
    """
    logging.info(f"--- 🕯️ {market} 분봉 데이터 수집 시작: {start} ~ {end} ---")
    ensure_table_exists() # 테이블이 없으면 생성

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    # 이미 저장된 타임스탬프를 미리 로드하여 API 요청 및 DB 저장을 최적화
    existing = get_existing_timestamps(market, start_dt, end_dt)

    current_time = end_dt # 종료 시각부터 역순으로 조회
    all_new_candles_count = 0
    total_api_calls = 0

    while current_time >= start_dt: # 'current_time > start_dt' 대신 '>=' 로 변경하여 시작 시간 포함
        to_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        logging.debug(f"API 요청 시도: {market}, to={to_str}, count=200")
        try:
            # API로부터 캔들 데이터 요청 (최대 200개)
            candles = get_minute_candles(market=market, unit=1, to=to_str, count=200)
            total_api_calls += 1

            if not candles:
                logging.info(f"더 이상 가져올 데이터가 없습니다. (현재 시각: {current_time})")
                break # 더 이상 캔들이 없으면 종료

            new_candles_for_db = []
            for c in candles:
                candle_time_kst_str = pd.to_datetime(c["candle_date_time_kst"]).strftime("%Y-%m-%d %H:%M:%S")
                # 요청 범위 내에 있고, 아직 DB에 없는 캔들만 필터링
                if start_dt <= pd.to_datetime(candle_time_kst_str) <= end_dt and candle_time_kst_str not in existing:
                    new_candles_for_db.append(c)

            if new_candles_for_db:
                save_candles_to_db(market, new_candles_for_db)
                # 새로 저장된 캔들의 타임스탬프를 existing set에 추가하여 중복 방지
                new_timestamps = {pd.to_datetime(c["candle_date_time_kst"]).strftime("%Y-%m-%d %H:%M:%S") for c in new_candles_for_db}
                existing.update(new_timestamps)
                all_new_candles_count += len(new_candles_for_db)
            else:
                logging.debug(f"현재 요청에서 새로운 캔들을 찾지 못했습니다. (to: {to_str})")


            # API 응답의 첫 번째 캔들(가장 과거)의 시간을 기준으로 다음 요청 시간을 설정
            # 다음 요청은 이 시간 1분 전부터 시작
            oldest_candle_time = pd.to_datetime(candles[-1]["candle_date_time_kst"]) # 바이낸스는 과거->현재, Upbit는 현재->과거
            # Upbit get_minute_candles가 최신 데이터부터 주기 때문에, candles[-1]이 가장 오래된 캔들.
            # 바이낸스 get_minute_candles는 최신 데이터가 인덱스 0에 오도록 처리했으므로, candles[-1]이 가장 오래된 캔들.
            current_time = oldest_candle_time - timedelta(minutes=1)
            logging.debug(f"다음 조회 시작 시간: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

            # API 요청 간 딜레이
            time.sleep(0.2)

        except Exception as e:
            logging.error(f"❌ {market} 데이터 수집 중 오류 발생: {e}", exc_info=True)
            logging.info("5초 후 다시 시도합니다...")
            time.sleep(5) # 오류 발생 시 재시도 딜레이

    logging.info(f"--- ✅ {market} 총 {all_new_candles_count}개의 신규 분봉 저장 완료. 수집을 종료합니다. ---")
    logging.info(f"총 API 호출 횟수: {total_api_calls}회.")


if __name__ == "__main__":
    # collect_candles.py 스크립트에서 이 함수를 호출하므로,
    # 여기서는 테스트 용도로만 사용하거나, 실제 사용 시에는 이 부분을 주석 처리
    # MARKET_TO_COLLECT, START_DATE, END_DATE는 collect_candles.py에서 정의됨
    print("💡 이 스크립트는 일반적으로 'collect_candles.py'를 통해 실행됩니다.")
    print("직접 실행하는 경우, MARKET_TO_COLLECT, START_DATE, END_DATE를 확인하세요.")
    # collect_minute_candles(
    #     market=MARKET_TO_COLLECT,
    #     start=START_DATE,
    #     end=END_DATE
    # )