# main.py

import logging
import time
import pandas as pd
import os
import sys
import config
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드 (가장 먼저 실행되는 코드 중 하나여야 함)
load_dotenv()

# common_utils 모듈 자체를 임포트
import utils.common_utils

# 바이낸스 모듈
from api.binance.client import get_binance_client
from api.binance.account import get_accounts as get_binance_accounts
from api.binance.order import get_order_result, cancel_order
from api.binance.price import get_current_ask_price, get_current_bid_price

# 매매 로직
from strategy.entry import run_casino_entry

# 텔레그램 알림 모듈
from utils.telegram_notifier import (
    notify_bot_status,
    notify_error,
    notify_order_event,
    notify_position_summary,
    notify_liquidation_warning,
    notify_liquidation_occurred
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# CSV 파일 경로
BUY_LOG_FILE = "buy_log.csv"
SELL_LOG_FILE = "sell_log.csv"
SETTING_FILE = "setting.csv"

# --- 봇 상태 및 알림 주기 관리를 위한 전역 변수 ---
last_health_check_time = 0
last_position_summary_time = 0
last_liquidation_warning_times = {}  # {symbol: {level: timestamp}}


def initialize_log_file(filename, columns):
    """CSV 로그 파일이 없으면 헤더와 함께 생성합니다."""
    if not os.path.exists(filename):
        pd.DataFrame(columns=columns).to_csv(filename, index=False)
        logging.info(f"ℹ️ '{filename}' 파일이 없어 새로 생성했습니다.")


def check_csv_file_format(filename, expected_columns):
    """CSV 파일의 형식을 검사합니다."""
    try:
        df = pd.read_csv(filename)
        if not all(col in df.columns for col in expected_columns):
            logging.error(f"❌ '{filename}' 파일의 컬럼 형식이 올바르지 않습니다.")
            return False
        logging.info(f"✅ '{filename}' 파일이 정상입니다.")
        return True
    except FileNotFoundError:
        logging.error(f"❌ '{filename}' 파일을 찾을 수 없습니다. 프로그램을 종료합니다.")
        return False
    except pd.errors.EmptyDataError:
        logging.warning(f"⚠️ '{filename}' 파일이 비어 있습니다. 헤더만 있는 것으로 간주합니다.")
        return True  # 비어있으면 일단 정상으로 간주
    except Exception as e:
        logging.error(f"❌ '{filename}' 파일 검사 중 오류 발생: {e}", exc_info=True)
        return False


def ensure_csv_files():
    """모든 필수 CSV 파일의 존재 여부와 형식을 검사하고, 없으면 생성합니다."""
    logging.info("CSV 파일 검사 시작")

    # buy_log.csv
    initialize_log_file(BUY_LOG_FILE,
                        ["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])
    if not check_csv_file_format(BUY_LOG_FILE,
                                 ["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid",
                                  "filled"]):
        return False

    # sell_log.csv
    initialize_log_file(SELL_LOG_FILE,
                        ["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"])
    if not check_csv_file_format(SELL_LOG_FILE,
                                 ["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"]):
        return False

    # setting.csv
    initialize_log_file(SETTING_FILE, ["market", "unit_size", "small_flow_pct", "small_flow_units", "large_flow_pct",
                                       "large_flow_units", "take_profit_pct", "leverage", "margin_type"])
    if not check_csv_file_format(SETTING_FILE,
                                 ["market", "unit_size", "small_flow_pct", "small_flow_units", "large_flow_pct",
                                  "large_flow_units", "take_profit_pct", "leverage", "margin_type"]):
        return False

    return True


def main():
    global last_health_check_time, last_position_summary_time, last_liquidation_warning_times  # 전역 변수 선언

    # 시작 시점 기준으로 초기화
    last_health_check_time = time.time()
    last_position_summary_time = time.time()
    last_liquidation_warning_times = {}  # 매번 main() 시작 시 초기화

    # ⭐⭐ UnboundLocalError 해결: open_positions_for_summary 변수를 미리 초기화 ⭐⭐
    open_positions_for_summary = {}

    # CSV 파일 검사 및 초기화
    if not ensure_csv_files():
        logging.error("필수 CSV 파일 검사 실패. 프로그램을 종료합니다.")
        notify_error("System", "필수 CSV 파일 검사 실패. 프로그램 종료.")
        sys.exit(1)

    # 봇 시작 알림
    notify_bot_status("시작", "자동 매매 프로그램이 실행되었습니다.")

    while True:
        current_time = time.time()

        # 봇 정상 동작 주기 알림
        if current_time - last_health_check_time >= config.HEALTH_CHECK_INTERVAL_SECONDS:
            notify_bot_status("정상 동작", "봇이 활성화되어 있습니다.")
            last_health_check_time = current_time

        # 주기적인 포지션 현황 요약 알림
        # 이 블록이 실행되지 않는 첫 루프에도 open_positions_for_summary가 정의되어 있도록 위에서 초기화함.
        if current_time - last_position_summary_time >= config.POSITION_SUMMARY_INTERVAL_SECONDS:
            account_data = get_binance_accounts()  # api.binance.account에서 계좌 정보 가져옴
            usdt_balance = account_data.get("usdt_balance", 0.0)

            open_positions_raw = account_data.get("open_positions", [])

            # 총 포트폴리오 가치 계산 (현금 + 포지션 가치)
            total_portfolio_value = usdt_balance
            # 이 초기화는 최신 데이터를 받아 덮어쓰는 역할을 합니다.
            open_positions_for_summary = {}

            for pos in open_positions_raw:
                market = pos.get('symbol')
                position_amt = abs(float(pos.get('positionAmt', '0.0')))

                if market and position_amt > 0:
                    entry_price = float(pos.get('entryPrice', '0.0'))
                    mark_price = float(pos.get('markPrice', '0.0'))
                    unrealized_profit = float(pos.get('unRealizedProfit', '0.0'))
                    liquidation_price = float(pos.get('liquidationPrice', '0.0'))
                    leverage = int(pos.get('leverage', '1'))
                    margin_type = pos.get('marginType', 'UNKNOWN')
                    position_side = pos.get('positionSide', 'UNKNOWN')

                    total_portfolio_value += position_amt * mark_price  # 시장가 기준 포지션 가치 추가
                    open_positions_for_summary[market] = {
                        "quantity": position_amt,
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "unrealized_pnl": unrealized_profit,
                        "roe": (unrealized_profit / (position_amt * entry_price) * 100) if (
                                                                                                       position_amt * entry_price) > 0 else 0,
                        "liquidation_price": liquidation_price,
                        "leverage": leverage,
                        "margin_type": margin_type,
                        "position_side": position_side
                    }

            holdings_summary = {
                'usdt_balance': usdt_balance,
                'total_portfolio_value': total_portfolio_value,
                'open_positions': open_positions_for_summary
            }

            notify_position_summary(holdings_summary)
            last_position_summary_time = current_time

        # 청산 위험 알림 로직 (open_positions_for_summary 활용)
        for market, pos_info in open_positions_for_summary.items():
            liquidation_price = pos_info.get('liquidation_price', 0.0)
            mark_price = pos_info.get('mark_price', 0.0)
            entry_price = pos_info.get('entry_price', 0.0)
            roe = pos_info.get('roe', 0.0)
            position_side = pos_info.get('position_side', 'UNKNOWN')

            # 롱 포지션 청산 위험 (가격 하락 시 청산)
            if position_side == 'LONG':  # Long 전용 전략이므로 LONG만 체크
                # 청산까지 남은 가격 폭 계산
                price_diff_to_liq = mark_price - liquidation_price

                # 진입가와 청산가 사이의 총 가격 폭 (0으로 나누는 것 방지)
                total_price_range = entry_price - liquidation_price
                if total_price_range <= 0:  # 진입가 <= 청산가 (즉시 청산되거나 PNL이 양수인 비정상 상태)
                    total_price_range = 0.00000001  # 0으로 나누는 것을 방지하기 위한 최소값

                # 청산까지 남은 비율 (백분율)
                # 현재가가 청산가보다 낮으면 음수값이 나와 청산에 가까워질수록 0%에 수렴.
                # 반대로 청산가에서 멀어질수록 퍼센트가 커짐.
                # 직관적인 이해를 위해 '청산까지 남은 %'를 계산.
                if mark_price > liquidation_price:  # 아직 청산가 위에 있을 때
                    remaining_pct = (mark_price - liquidation_price) / (entry_price - liquidation_price) * 100 if (
                                                                                                                              entry_price - liquidation_price) > 0 else 0
                else:  # 이미 청산가에 도달했거나 넘어섰을 때
                    remaining_pct = 0  # 0% 남음 또는 이미 초과

                # 경고 레벨 1
                if remaining_pct <= config.LIQUIDATION_WARNING_PCT_1 * 100 and remaining_pct > config.LIQUIDATION_WARNING_PCT_2 * 100:
                    if market not in last_liquidation_warning_times or 'level1' not in last_liquidation_warning_times[
                        market] or \
                            current_time - last_liquidation_warning_times[market][
                        'level1'] >= config.HEALTH_CHECK_INTERVAL_SECONDS:
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 1)
                        last_liquidation_warning_times.setdefault(market, {})['level1'] = current_time

                # 경고 레벨 2 (더 긴급)
                elif remaining_pct <= config.LIQUIDATION_WARNING_PCT_2 * 100 and remaining_pct > 0:
                    if market not in last_liquidation_warning_times or 'level2' not in last_liquidation_warning_times[
                        market] or \
                            current_time - last_liquidation_warning_times[market]['level2'] >= 300:  # 2단계 알림 주기 (5분)
                        notify_liquidation_warning(market, mark_price, liquidation_price, entry_price, roe, 2)
                        last_liquidation_warning_times.setdefault(market, {})['level2'] = current_time

                # 강제 청산 발생 (Mark Price가 청산 가격 이하로 떨어졌고 손실이 있는 경우)
                elif mark_price <= liquidation_price and pos_info['unrealized_pnl'] < 0:
                    if market not in last_liquidation_warning_times or 'occurred' not in last_liquidation_warning_times[
                        market] or \
                            current_time - last_liquidation_warning_times[market][
                        'occurred'] >= 600:  # 청산 발생 알림 주기 (10분)
                        notify_liquidation_occurred(market, pos_info['unrealized_pnl'])
                        last_liquidation_warning_times.setdefault(market, {})['occurred'] = current_time

        # 매매 전략 실행
        run_casino_entry()

        # 다음 실행까지 대기
        time.sleep(config.RUN_INTERVAL_SECONDS)


# 프로그램 메인 실행
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"❌ 자동 매매 프로그램 실행 중 치명적인 오류 발생: {e}", exc_info=True)
        notify_error("System", f"자동 매매 프로그램 실행 중 치명적인 오류 발생: {e}")
        sys.exit(1)
    finally:
        logging.info("========== 자동 매매 프로그램 종료 ==========")
        notify_bot_status("종료", "자동 매매 프로그램이 종료되었습니다.")