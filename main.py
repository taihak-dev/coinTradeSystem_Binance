# main.py

import logging
import time
import pandas as pd
import os
import sys
import config
from datetime import datetime
from dotenv import load_dotenv
from manager.hwm_manager import hwm_manager
from manager.cooldown_manager import cooldown_manager # 쿨다운 매니저 임포트
from manager.order_executor import close_all_positions # 전체 청산 함수 임포트
from utils.telegram_notifier import send_telegram_message # 일반 메시지 전송용

load_dotenv()

if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] Main: 바이낸스 API 모드를 사용합니다.")
    from api.binance.account import get_accounts
elif config.EXCHANGE == 'bybit':
    logging.info("[SYSTEM] Main: 바이빗 API 모드를 사용합니다.")
    from api.bybit.account import get_accounts
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")

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
last_profit_reset_alert_time = 0 # 수익 리셋 알림 중복 방지용
last_margin_refill_alert_time = 0 # 증거금 보충 알림 중복 방지용


def check_and_notify_status(account_data: dict):
    global last_health_check_time, last_summary_time, last_liquidation_warning_times, last_profit_reset_alert_time
    current_time = time.time()

    try:
        # 1. 봇 생존 신고
        if current_time - last_health_check_time >= config.HEALTH_CHECK_INTERVAL_SECONDS:
            notify_bot_status("정상 동작 중", f"거래소: {config.EXCHANGE.upper()}")
            last_health_check_time = current_time

        # 2. 포지션 현황 요약
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
                gap_to_liquidation = mark_price - liquidation_price
                price_range = entry_price - liquidation_price if entry_price > liquidation_price else 0.00000001
                remaining_pct = (gap_to_liquidation / price_range) if price_range > 0 else 0

                if 0 < remaining_pct <= config.LIQUIDATION_WARNING_PCT_1:
                    if market not in last_liquidation_warning_times or \
                            current_time - last_liquidation_warning_times.get(market, {}).get('level1', 0) >= 1800:
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 1)
                        last_liquidation_warning_times.setdefault(market, {})['level1'] = current_time

                if 0 < remaining_pct <= config.LIQUIDATION_WARNING_PCT_2:
                    if market not in last_liquidation_warning_times or \
                            current_time - last_liquidation_warning_times.get(market, {}).get('level2', 0) >= 300:
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 2)
                        last_liquidation_warning_times.setdefault(market, {})['level2'] = current_time
        
        # 4. 수익 리셋 알림 (단순 알림만)
        total_equity = account_data.get('total_equity', 0)
        target_equity = config.ORIGINAL_INITIAL_CASH * (1 + config.PROFIT_RESET_TARGET)
        
        if total_equity >= target_equity:
            # 1시간(3600초)마다 알림
            if current_time - last_profit_reset_alert_time >= 3600:
                msg = f"🎉 *[목표 수익 달성]*\n"
                msg += f"현재 자산: `{total_equity:.2f}` USDT\n"
                msg += f"목표 자산: `{target_equity:.2f}` USDT\n"
                msg += f"수익률: `{(total_equity - config.ORIGINAL_INITIAL_CASH) / config.ORIGINAL_INITIAL_CASH * 100:.2f}`%\n"
                msg += "수익 실현 및 리셋을 고려하세요!"
                send_telegram_message(msg)
                last_profit_reset_alert_time = current_time

    except Exception as e:
        logging.error(f"상태 확인 및 알림 중 오류 발생: {e}", exc_info=True)
        notify_error("Status Check", f"상태 확인 중 오류 발생: {e}")


def main():
    global last_margin_refill_alert_time
    notify_bot_status("시작", f"거래소: {config.EXCHANGE.upper()}")

    while True:
        try:
            logging.info("\n" + "=" * 50)
            logging.info(f"== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 메인 루프 시작 ==")
            logging.info("=" * 50)

            account_data = get_accounts()
            total_equity = account_data.get('total_equity', 0)
            
            # --- 👇👇👇 쿨다운 및 손절 로직 추가 👇👇👇 ---
            
            # 1. 쿨다운 상태 확인
            if cooldown_manager.is_cooldown_active():
                end_time = cooldown_manager.get_end_time()
                
                # 쿨다운 시간이 지났는지 확인 (24시간 경과)
                if end_time and datetime.now() >= end_time:
                    # 증거금 충족 여부 확인
                    if total_equity >= config.ORIGINAL_INITIAL_CASH:
                        cooldown_manager.end_cooldown()
                        send_telegram_message("🔥 *[쿨다운 종료]*\n증거금이 충족되어 매매를 재개합니다.")
                    else:
                        # 증거금 부족 알림 (1시간마다)
                        current_time = time.time()
                        if current_time - last_margin_refill_alert_time >= 3600:
                            shortage = config.ORIGINAL_INITIAL_CASH - total_equity
                            msg = f"⚠️ *[증거금 보충 필요]*\n"
                            msg += f"쿨다운 시간은 지났으나 증거금이 부족합니다.\n"
                            msg += f"현재: `{total_equity:.2f}` / 목표: `{config.ORIGINAL_INITIAL_CASH:.2f}`\n"
                            msg += f"부족분: `{shortage:.2f}` USDT\n"
                            msg += "매매 재개를 위해 입금이 필요합니다."
                            send_telegram_message(msg)
                            last_margin_refill_alert_time = current_time
                        
                        logging.info(f"❄️ 쿨다운 중 (증거금 부족). 현재: {total_equity:.2f}, 목표: {config.ORIGINAL_INITIAL_CASH:.2f}")
                        time.sleep(config.RUN_INTERVAL_SECONDS)
                        continue
                else:
                    logging.info(f"❄️ 쿨다운 중... 종료 예정: {end_time}")
                    time.sleep(config.RUN_INTERVAL_SECONDS)
                    continue

            # 2. 손절 조건 확인
            stop_loss_level = config.ORIGINAL_INITIAL_CASH * (1 - config.STOP_LOSS_THRESHOLD) # 예: 3000 * (1 - 0.35) = 1950
            # 주의: STOP_LOSS_THRESHOLD가 0.65라면 (1-0.65)=0.35가 됨. 
            # config.py에는 STOP_LOSS_THRESHOLD=0.65 (65% 이하 시 손절)로 되어 있음.
            # 따라서 조건은 total_equity <= config.ORIGINAL_INITIAL_CASH * config.STOP_LOSS_THRESHOLD 가 맞음.
            
            if total_equity <= config.ORIGINAL_INITIAL_CASH * config.STOP_LOSS_THRESHOLD:
                logging.warning(f"🚨 손절 조건 도달! 현재 자산: {total_equity:.2f}, 기준: {config.ORIGINAL_INITIAL_CASH * config.STOP_LOSS_THRESHOLD:.2f}")
                
                # 모든 포지션 청산
                close_all_positions()
                
                # 쿨다운 시작
                cooldown_manager.start_cooldown()
                
                # 알림 전송
                msg = f"🚨 *[손절 실행 및 쿨다운]*\n"
                msg += f"자산이 손절 기준 이하로 하락하여 모든 포지션을 청산하고 매매를 중단합니다.\n"
                msg += f"현재 자산: `{total_equity:.2f}` USDT\n"
                msg += f"손절 기준: `{config.ORIGINAL_INITIAL_CASH * config.STOP_LOSS_THRESHOLD:.2f}` USDT\n"
                msg += f"쿨다운 종료 예정: {cooldown_manager.get_end_time()}"
                send_telegram_message(msg)
                
                time.sleep(config.RUN_INTERVAL_SECONDS)
                continue
            
            # --- 👆👆👆 추가 완료 --- 👆👆👆

            check_and_notify_status(account_data)

            try:
                setting_df = pd.read_csv('setting.csv')
                base_unit_size = setting_df['unit_size'].iloc[0]
            except Exception as e:
                logging.error(f"setting.csv 파일에서 unit_size를 읽는 중 오류 발생: {e}. 기본값 100을 사용합니다.")
                base_unit_size = 100

            current_unit_size = base_unit_size
            
            if config.ENABLE_DYNAMIC_UNIT:
                if total_equity > config.ORIGINAL_INITIAL_CASH:
                    current_unit_size = base_unit_size * (total_equity / config.ORIGINAL_INITIAL_CASH)
                    logging.info(f"📈 동적 유닛 활성화: 자산 증가로 유닛 사이즈 상향 조정: {current_unit_size:.2f} (기본: {base_unit_size})")
                else:
                    logging.info(f"📉 동적 유닛 활성화: 자산이 기준보다 작으므로 기본 유닛 사이즈 유지: {current_unit_size:.2f}")
            else:
                logging.info(f"🛠️ 동적 유닛 비활성화: 고정 유닛 사이즈 사용: {current_unit_size:.2f}")
            
            run_casino_entry(current_unit_size=current_unit_size)

            logging.info(f"== 메인 루프 종료. {config.RUN_INTERVAL_SECONDS}초 후 다음 루프 시작 ==")
            time.sleep(config.RUN_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logging.info("사용자에 의해 프로그램이 중단되었습니다.")
            notify_bot_status("종료", "사용자 직접 중단")
            break
        except Exception as e:
            logging.critical(f"메인 루프에서 치명적인 오류 발생: {e}", exc_info=True)
            notify_error("Main Loop", f"프로그램이 비정상적으로 종료될 수 있습니다: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()