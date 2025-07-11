# main.py

import os
import sys
import pandas as pd
from strategy.entry import run_casino_entry
import logging
import time  # time 모듈 임포트

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 필요 열 정의 (기존과 동일)
REQUIRED_COLUMNS = {
    "setting.csv": [
        "market", "unit_size", "small_flow_pct", "small_flow_units",
        "large_flow_pct", "large_flow_units", "take_profit_pct",
        "leverage", "margin_type"
    ],
    "buy_log.csv": [
        "time", "market", "target_price", "buy_amount",
        "buy_units", "buy_type", "buy_uuid", "filled"
    ],
    "sell_log.csv": [
        "market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"
    ],
}


def ensure_csv_files():
    """
    프로그램 실행에 필요한 CSV 파일들이 존재하는지 확인하고,
    없다면 기본 형태로 새로 생성합니다.
    기존 파일이 있을 경우, 필수 컬럼들이 올바른지 검증합니다.
    """
    logging.info("CSV 파일 검사 시작")

    for filename, expected_columns in REQUIRED_COLUMNS.items():
        if not os.path.exists(filename):
            logging.warning(f"📄 '{filename}' 파일이 없어 새로 생성합니다.")
            df = pd.DataFrame(columns=expected_columns)
            df.to_csv(filename, index=False)
        else:
            df = pd.read_csv(filename)
            existing_columns = df.columns.tolist()
            if existing_columns != expected_columns:
                logging.error(f"❌ '{filename}' 파일의 열이 예상과 다릅니다.")
                logging.error(f"    ▶ 예상: {expected_columns}")
                logging.error(f"    ▶ 실제: {existing_columns}")
                logging.error("🚫 프로그램이 필수 CSV 파일 형식 문제로 종료됩니다.")
                sys.exit(1)
            else:
                logging.info(f"✅ '{filename}' 파일이 정상입니다.")


def main():
    """자동 매매 프로그램의 메인 진입점. 주기적으로 매매 로직을 실행합니다."""
    logging.info("========== 자동 매매 프로그램 시작 ==========")
    ensure_csv_files()  # CSV 파일 존재 여부 및 형식 검사

    INTERVAL_SECONDS = 5  # ⚠️ 매매 로직 실행 간격 (초 단위) - 5초에 한 번 실행

    while True:  # 무한 루프
        try:
            logging.info(f"\n--- 매매 로직 실행 주기 시작 (다음 실행까지 {INTERVAL_SECONDS}초 대기) ---")
            run_casino_entry()  # 매매 전략 실행
            logging.info("--- 매매 로직 실행 주기 완료 ---")

        except Exception as e:
            logging.critical(f"🔥 매매 로직 실행 중 치명적인 오류 발생: {e}", exc_info=True)
            # 오류 발생 시 프로그램이 완전히 종료되지 않고 일정 시간 후 재시도
            logging.info(f"⚠️ 오류 발생! {INTERVAL_SECONDS}초 후 다시 시도합니다...")

        time.sleep(INTERVAL_SECONDS)  # 지정된 시간만큼 대기


if __name__ == "__main__":
    main()