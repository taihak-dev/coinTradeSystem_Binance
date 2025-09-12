# main.py

import logging
import time
import pandas as pd
import os
import sys
import config
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- 👇👇👇 거래소 선택 로직 (핵심 수정) 👇👇👇 ---
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] Main: 바이낸스 API 모드를 사용합니다.")
    from api.binance.account import get_accounts
elif config.EXCHANGE == 'bybit':
    logging.info("[SYSTEM] Main: 바이빗 API 모드를 사용합니다.")
    from api.bybit.account import get_accounts
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")
# --- 👆👆👆 여기까지 수정 --- 👆👆👆

from strategy.entry import run_casino_entry
from utils.telegram_notifier import (
    notify_bot_status,
    notify_error,
    notify_position_summary,
    notify_liquidation_warning,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 상태 관리 변수 ---
last_health_check_time = 0
last_summary_time = 0
last_liquidation_warning_times = {}


def check_and_notify_status():
    """주기적으로 봇 상태, 계좌 요약, 청산 위험을 체크하고 알림을 보냅니다."""
    global last_health_check_time, last_summary_time, last_liquidation_warning_times
    current_time = time.time()

    try:
        # 1. 봇 생존 신고 (예: 1시간마다)
        if current_time - last_health_check_time >= config.HEALTH_CHECK_INTERVAL_SECONDS:
            notify_bot_status("정상 동작 중", f"거래소: {config.EXCHANGE.upper()}")
            last_health_check_time = current_time

        # get_accounts 함수는 이제 설정에 따라 바이낸스 또는 바이빗의 함수가 됩니다.
        account_data = get_accounts()

        # 2. 포지션 현황 요약 (예: 6시간마다)
        if current_time - last_summary_time >= config.POSITION_SUMMARY_INTERVAL_SECONDS:
            notify_position_summary(account_data)
            last_summary_time = current_time

        # 3. 청산 위험 감지
        open_positions = account_data.get("open_positions", [])
        for pos_info in open_positions:
            market = pos_info['symbol']
            mark_price = pos_info['markPrice']
            liquidation_price = pos_info['liquidationPrice']
            entry_price = pos_info['entryPrice']
            roe = pos_info.get('roe', 0.0)

            if liquidation_price > 0 and mark_price > 0:
                # 롱 포지션(가격 하락 시 청산) 기준
                gap_to_liquidation = mark_price - liquidation_price
                price_range = entry_price - liquidation_price if entry_price > liquidation_price else 0.00000001

                remaining_pct = (gap_to_liquidation / price_range) if price_range > 0 else 0

                # 1단계 경고
                if 0 < remaining_pct <= config.LIQUIDATION_WARNING_PCT_1:
                    if market not in last_liquidation_warning_times or \
                            current_time - last_liquidation_warning_times.get(market, {}).get('level1',
                                                                                              0) >= 1800:  # 30분
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 1)
                        last_liquidation_warning_times.setdefault(market, {})['level1'] = current_time

                # 2단계 경고
                if 0 < remaining_pct <= config.LIQUIDATION_WARNING_PCT_2:
                    if market not in last_liquidation_warning_times or \
                            current_time - last_liquidation_warning_times.get(market, {}).get('level2', 0) >= 300:  # 5분
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 2)
                        last_liquidation_warning_times.setdefault(market, {})['level2'] = current_time

    except Exception as e:
        logging.error(f"상태 확인 및 알림 중 오류 발생: {e}", exc_info=True)
        notify_error("Status Check", f"상태 확인 중 오류 발생: {e}")


def main():
    """메인 실행 함수"""
    notify_bot_status("시작", f"거래소: {config.EXCHANGE.upper()}")

    while True:
        try:
            logging.info("\n" + "=" * 50)
            logging.info(f"== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 메인 루프 시작 ==")
            logging.info("=" * 50)

            # 주기적인 상태 확인 및 알림
            check_and_notify_status()

            # 매매 전략 실행
            run_casino_entry()

            # 다음 실행까지 대기
            logging.info(f"== 메인 루프 종료. {config.RUN_INTERVAL_SECONDS}초 후 다음 루프 시작 ==")
            time.sleep(config.RUN_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logging.info("사용자에 의해 프로그램이 중단되었습니다.")
            notify_bot_status("종료", "사용자 직접 중단")
            break
        except Exception as e:
            logging.critical(f"메인 루프에서 치명적인 오류 발생: {e}", exc_info=True)
            notify_error("Main Loop", f"프로그램이 비정상적으로 종료될 수 있습니다: {e}")
            time.sleep(60)  # 오류 발생 시 60초 대기 후 재시도


if __name__ == "__main__":
    main()