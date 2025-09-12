# strategy/entry.py

import logging
from datetime import datetime, timedelta

import config
from api.binance.account import get_accounts as get_binance_accounts
from api.bybit.account import get_accounts as get_bybit_accounts
from strategy.buy_entry import run_buy_entry_flow
from strategy.sell_entry import run_sell_entry_flow
from utils.telegram_notifier import notify_position_summary, notify_error


def send_periodic_summary():
    """
    설정된 시간 간격(예: 4시간)마다 현재 계좌 상태 요약을 텔레그램으로 보냅니다.
    """
    SUMMARY_INTERVAL_HOURS = 4
    STATUS_FILE_PATH = "last_summary_time.txt"

    now = datetime.now()
    should_send = False

    try:
        with open(STATUS_FILE_PATH, 'r') as f:
            last_summary_time_str = f.read().strip()
            if not last_summary_time_str:
                should_send = True
            else:
                last_summary_time = datetime.fromisoformat(last_summary_time_str)
                if now - last_summary_time >= timedelta(hours=SUMMARY_INTERVAL_HOURS):
                    should_send = True
    except FileNotFoundError:
        logging.info(f"'{STATUS_FILE_PATH}' 파일이 없어 최초 상태 보고를 시도합니다.")
        should_send = True
    except Exception as e:
        logging.error(f"'{STATUS_FILE_PATH}' 파일 처리 중 오류 발생: {e}")
        should_send = True

    if should_send:
        logging.info(f" 주기적 상태 보고 시간 도달. 텔레그램으로 요약 정보를 전송합니다.")
        try:
            summary_data = get_bybit_accounts() if config.EXCHANGE == 'bybit' else get_binance_accounts()
            notify_position_summary(summary_data)
            with open(STATUS_FILE_PATH, 'w') as f:
                f.write(now.isoformat())
            logging.info(f"상태 보고 완료. 다음 보고 시간은 약 {SUMMARY_INTERVAL_HOURS}시간 후입니다.")
        except Exception as e:
            logging.error(f"주기적 상태 보고 중 오류 발생: {e}", exc_info=True)
            notify_error("Periodic Summary", f"주기적 상태 보고 실패: {e}")


def run_casino_entry():
    """
    매매 로직의 전체 사이클을 실행하고, 오류 발생 시에도 메인 루프가 중단되지 않도록 처리합니다.
    """
    print("\n[entry.py] ▶ 카지노 매매 시스템 사이클 시작")

    try:
        # 1. 매수 전략 실행
        print("[entry.py] ▶ 매수 전략 실행")
        run_buy_entry_flow()

        # 2. 매도 전략 실행
        print("[entry.py] ▶ 매도 전략 실행")
        run_sell_entry_flow()

        # 3. 주기적 상태 보고 실행 (매 사이클마다 체크)
        # print("[entry.py] ▶ 주기적 상태 보고 확인")
        # send_periodic_summary() # main.py로 이동됨

    except Exception as e:
        # 이 try-except 블록은 각 사이클의 안정성을 보장합니다.
        # buy/sell_entry_flow 내부의 오류가 여기까지 올라오면 로깅하고, main.py의 루프는 계속됩니다.
        logging.critical(f"[entry.py] ⚠️ 매매 로직 사이클 중 예외 발생: {e}", exc_info=True)
        notify_error("Casino Entry Cycle", f"매매 로직 실행 중 오류가 발생했습니다: {e}")