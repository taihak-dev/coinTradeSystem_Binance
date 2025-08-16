# main.py

import pandas as pd
import time
import os
import sys
# /// [추가] logging 모듈 임포트 ///
import logging

from strategy.entry import run_casino_entry
from utils.telegram_notifier import notify_bot_status, notify_error

# /// [추가 시작] 로그 파일 저장 기능 ///
# 로그 파일을 저장할 'logs' 디렉토리 생성
if not os.path.exists('logs'):
    os.makedirs('logs')

# 로거(logger) 설정
# 파일과 콘솔에 모두 로그를 출력하도록 핸들러(handler)를 설정합니다.
logger = logging.getLogger()
logger.setLevel(logging.INFO) # 로그 레벨 설정

# 포맷 설정
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# 1. 파일 핸들러: 날짜별로 로그 파일을 생성합니다.
file_handler = logging.FileHandler(f"logs/trades_{time.strftime('%Y-%m-%d')}.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 2. 스트림 핸들러: 파이참 콘솔 등 실행 환경에 로그를 출력합니다.
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# /// [추가 끝] ///


def check_csv_files():
    logging.info("CSV 파일 검사 시작") # 이제 logging.info를 사용합니다.
    required_files = ["buy_log.csv", "sell_log.csv", "setting.csv"]
    all_files_ok = True
    for filename in required_files:
        if not os.path.exists(filename):
            logging.error(f"🚨 필수 파일이 없습니다: {filename}. 프로그램을 종료합니다.")
            all_files_ok = False
        else:
            try:
                # 파일이 비어있는 경우를 대비한 예외 처리
                if os.stat(filename).st_size == 0:
                     logging.warning(f"⚠️ '{filename}' 파일이 비어있습니다. 정상 파일로 간주하고 계속합니다.")
                else:
                    pd.read_csv(filename)
                logging.info(f"✅ '{filename}' 파일이 정상입니다.")
            except pd.errors.EmptyDataError:
                 logging.warning(f"⚠️ '{filename}' 파일이 비어있지만 헤더만 있습니다. 정상 파일로 간주하고 계속합니다.")
            except Exception as e:
                logging.error(f"🚨 '{filename}' 파일 읽기 오류: {e}. 프로그램을 종료합니다.")
                all_files_ok = False
    if not all_files_ok:
        sys.exit(1)


if __name__ == '__main__':
    # 기존에 다른 파일에서 basicConfig를 호출할 수 있으므로,
    # 이 메인 로거가 우선권을 가지도록 강제 설정
    logging.getLogger().handlers.clear()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"logs/trades_{time.strftime('%Y-%m-%d')}.log", encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )

    check_csv_files()

    try:
        logging.info("자동 매매 프로그램을 시작합니다.")
        notify_bot_status("시작", "자동 매매 프로그램이 실행되었습니다.")
        while True:
            # print("[main.py] ▶ 카지노 매매 시스템 시작") # 로깅으로 대체
            logging.info("="*20 + " 새로운 사이클 시작 " + "="*20)
            run_casino_entry()
            logging.info("="*20 + " 사이클 종료, 5초 대기 " + "="*20 + "\n")
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info("프로그램을 수동으로 종료합니다.")
        notify_bot_status("종료", "사용자에 의해 프로그램이 중지되었습니다.")
    except Exception as e:
        logging.error(f"🚨 메인 루프에서 치명적인 오류 발생: {e}", exc_info=True)
        notify_error("Main Loop", f"프로그램이 비정상 종료되었습니다: {e}")