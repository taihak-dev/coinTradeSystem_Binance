import os
import sqlite3
import time
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from binance.um_futures import UMFutures
from binance.error import ClientError

# --- 기본 설정 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 사용자 설정 ---
MARKET_TO_COLLECT = "DOGEUSDT"
START_DATE_STR = "2025-11-22 00:00:00"
END_DATE_STR = "2025-12-04 23:59:59"

# --- DB 설정 ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


# --- DB 관련 함수 ---
def ensure_table_exists():
    """DB와 테이블이 없으면 생성"""
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logging.info(f"📁 데이터베이스 디렉토리({db_dir})를 생성했습니다.")
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS minute_candles (
                market TEXT, timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL,
                PRIMARY KEY (market, timestamp)
            )
        """)
        logging.info("✅ 'minute_candles' 테이블 준비 완료.")


def save_candles_to_db(candles_df: pd.DataFrame):
    """데이터프레임을 DB에 저장"""
    if candles_df.empty:
        return 0
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        insert_count = 0
        for _, row in candles_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO minute_candles (market, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, tuple(row))
                insert_count += 1
            except sqlite3.IntegrityError:
                continue
        conn.commit()
        logging.info(f"💾 신규 캔들 {insert_count}개 저장 완료.")
        return insert_count


def get_last_timestamp_from_db(market: str) -> datetime | None:
    """DB에서 특정 마켓의 가장 마지막 타임스탬프를 조회"""
    if not os.path.exists(DB_PATH):
        return None
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(timestamp) FROM minute_candles WHERE market = ?", (market,))
            result = cursor.fetchone()[0]
            if result:
                # 저장된 timestamp 문자열을 UTC datetime 객체로 변환
                return datetime.strptime(result, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


# --- 메인 수집 함수 ---
def collect_all_candles():
    """설정된 기간 동안의 모든 1분봉 데이터를 수집"""
    ensure_table_exists()

    user_start_dt_utc = datetime.strptime(START_DATE_STR, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_dt_utc = datetime.strptime(END_DATE_STR, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    # --- 👇👇👇 여기가 수정된 부분입니다 👇👇👇 ---
    # DB의 마지막 시간을 확인하는 로직을 삭제하고,
    # 항상 사용자가 설정한 시작 시간(user_start_dt_utc)을 사용하도록 강제합니다.
    start_dt_utc = user_start_dt_utc
    logging.info(f"ℹ️ 사용자가 설정한 시작 시간({start_dt_utc})부터 수집을 시작합니다.")

    # (선택 사항) DB 마지막 시간은 참고용으로 로그만 남깁니다.
    last_saved_dt = get_last_timestamp_from_db(MARKET_TO_COLLECT)
    if last_saved_dt:
        logging.info(f"🔍 (참고) DB에 이미 저장된 마지막 데이터 시점: {last_saved_dt}")
    # --- 👆👆👆 수정 완료 --- 👆👆👆

    if start_dt_utc >= end_dt_utc:
        logging.info("✅ 이미 모든 데이터가 최신 상태입니다. 수집을 종료합니다.")
        return

    logging.info(f"--- 🕯️ {MARKET_TO_COLLECT} 분봉 데이터 수집 시작 (UTC 기준) ---")
    logging.info(f"기간: {start_dt_utc} ~ {end_dt_utc}")

    try:
        client = UMFutures()
        client.session.timeout = 15
        logging.info("✅ 바이낸스 공용 클라이언트 연결 성공!")
    except Exception as e:
        logging.error(f"❌ 클라이언트 생성 실패: {e}")
        return

    current_dt = start_dt_utc
    total_saved_count = 0

    while current_dt < end_dt_utc:
        start_time_ms = int(current_dt.timestamp() * 1000)

        logging.info(f"🔄 {current_dt.strftime('%Y-%m-%d %H:%M:%S')}부터 1000개 캔들 요청...")

        try:
            klines = client.klines(
                symbol=MARKET_TO_COLLECT,
                interval='1m',
                startTime=start_time_ms,
                limit=1000
            )

            if not klines:
                logging.info("API로부터 더 이상 데이터를 받지 못했습니다. 수집 종료.")
                break

            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                'taker_buy_quote_asset_volume', 'ignore'
            ])

            df['market'] = MARKET_TO_COLLECT

            # --- 👇👇👇 여기가 수정된 부분입니다 👇👇👇 ---
            # 1. '날짜 객체' 컬럼을 먼저 만듭니다.
            df['dt_timestamp'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)

            # 2. '날짜 객체'로 즉시 필터링합니다. (경고가 발생하던 이중 변환을 제거)
            df_filtered = df[df['dt_timestamp'] <= end_dt_utc].copy()

            if df_filtered.empty:
                logging.info("남은 캔들이 모두 수집 기간 이후의 데이터이므로 수집을 종료합니다.")
                break

            # 3. DB에 저장할 '텍스트' 컬럼을 *필터링이 끝난 후에* 만듭니다.
            df_filtered['timestamp'] = df_filtered['dt_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # 4. 최종 저장할 데이터프레임을 선택합니다.
            df_to_save = df_filtered[['market', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]
            # --- 👆👆👆 수정 완료 --- 👆👆👆

            saved_count = save_candles_to_db(df_to_save)
            total_saved_count += saved_count

            # 'df' (필터링 전 원본)의 마지막 시간을 기준으로 다음 루프를 결정합니다.
            last_open_time_ms = df.iloc[-1]['open_time']
            current_dt = datetime.fromtimestamp(last_open_time_ms / 1000, tz=timezone.utc) + timedelta(minutes=1)

            time.sleep(0.5)

        except ClientError as e:
            logging.error(f"API 오류 발생 (Code: {e.error_code}): {e.error_message}. 5초 후 재시도...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"알 수 없는 오류 발생: {e}. 5초 후 재시도...")
            time.sleep(5)

    logging.info(f"--- ✅ 수집 완료. 총 {total_saved_count}개의 신규 캔들이 저장되었습니다. ---")


if __name__ == "__main__":
    collect_all_candles()