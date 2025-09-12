# strategy/sell_entry.py

import logging
import os

import pandas as pd

import config
from manager.order_executor import execute_sell_orders
from strategy.casino_strategy import generate_sell_orders
from utils.common_utils import get_current_holdings
from utils.telegram_notifier import notify_order_event, notify_error

# --- 👇👇👇 거래소 선택 로직 (핵심 수정) 👇👇👇 ---
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] Sell Entry: 바이낸스 모드로 설정합니다.")
    from api.binance.order import get_order_result
elif config.EXCHANGE == 'bybit':
    logging.info("[SYSTEM] Sell Entry: 바이빗 모드로 설정합니다.")
    from api.bybit.order import get_order_result
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")
# --- 👆👆👆 여기까지 수정 --- 👆👆👆

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def update_sell_log_status(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    sell_log.csv에 기록된 'wait' 상태의 주문들이 실제로 체결되었는지 확인하고 상태를 업데이트합니다.
    체결이 확인되면 텔레그램으로 상세 알림을 전송합니다.
    """
    logging.info("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")

    if 'sell_uuid' not in sell_log_df.columns or sell_log_df['sell_uuid'].isnull().all():
        return sell_log_df

    wait_orders = sell_log_df[sell_log_df['filled'] == 'wait'].copy()
    if wait_orders.empty:
        logging.info("  - 확인할 'wait' 상태의 매도 주문이 없습니다.")
        return sell_log_df

    logging.info(f"  - 총 {len(wait_orders)}건의 'wait' 상태 매도 주문 확인 중...")
    for idx, row in wait_orders.iterrows():
        market = row['market']
        uuid = row['sell_uuid']

        try:
            order_info = get_order_result(market, str(uuid))
            current_state = order_info.get("state")

            if current_state != 'wait':
                logging.info(f"  - 주문 상태 변경 감지: {market} (UUID: {uuid}) -> {current_state}")
                sell_log_df.loc[idx, 'filled'] = current_state

                if current_state == 'done':
                    avg_buy_price = float(row.get('avg_buy_price', 0))
                    filled_qty = float(order_info.get('executed_qty', 0))
                    avg_sell_price = float(order_info.get('avg_price', 0))

                    pnl = 0
                    if avg_buy_price > 0 and filled_qty > 0 and avg_sell_price > 0:
                        pnl = (avg_sell_price - avg_buy_price) * filled_qty

                    details = {
                        'filled_qty': filled_qty,
                        'price': avg_sell_price,
                        'total_amount': order_info.get('cum_quote', 0),
                        'fee': 0,
                        'pnl': pnl
                    }
                    notify_order_event("체결", market, details)

        except Exception as e:
            logging.error(f"  - ❌ 주문 상태 확인 중 오류: {market} (UUID: {uuid}): {e}")
            notify_error("update_sell_log_status", f"{market} 주문({uuid}) 상태 확인 실패: {e}")
            continue

    return sell_log_df


def run_sell_entry_flow():
    """
    매도 전략의 전체 흐름을 실행합니다.
    """
    # 1. 설정 및 로그 파일 로드
    try:
        setting_df = pd.read_csv("setting.csv")
        sell_log_df = pd.read_csv("sell_log.csv") if os.path.exists("sell_log.csv") else pd.DataFrame()
    except Exception as e:
        logging.error(f"❌ 설정 또는 로그 파일 로드 실패: {e}")
        return

    # 2. 'wait' 상태인 기존 주문들의 최종 상태를 확인하고 업데이트
    if not sell_log_df.empty:
        sell_log_df = update_sell_log_status(sell_log_df)

    # 3. 현재 보유 자산 정보 가져오기
    try:
        holdings = get_current_holdings()
    except Exception as e:
        logging.error(f"❌ 보유 자산 정보 조회 실패: {e}")
        return

    # 4. 보유 자산을 기준으로 신규/정정 매도 주문 목록 생성
    orders_to_action_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    # 5. 실행 로직 진행
    if not orders_to_action_df.empty:
        logging.info(f"🆕 신규/정정 매도 주문 {len(orders_to_action_df)}건 생성됨. 주문 실행을 시작합니다.")

        # 'update'가 필요한 주문의 UUID 목록
        uuids_to_update = orders_to_action_df['sell_uuid'].dropna().tolist()

        # 기존 로그에서 처리 대상(정정/신규) 주문들을 제외하고, 새로운 주문 목록과 합침
        sell_log_df_filtered = sell_log_df[~sell_log_df['sell_uuid'].isin(uuids_to_update)]
        combined_sell_log_df = pd.concat([sell_log_df_filtered, orders_to_action_df], ignore_index=True)

        try:
            final_sell_log_df = execute_sell_orders(combined_sell_log_df)
            final_sell_log_df.to_csv("sell_log.csv", index=False)
            logging.info("[sell_entry.py] sell_log.csv 파일 저장 완료.")
        except Exception as e:
            logging.error(f"❌ 매도 주문 실행 또는 로그 저장 중 오류 발생: {e}")
    else:
        if not sell_log_df.empty:
            sell_log_df.to_csv("sell_log.csv", index=False)
        logging.info("[sell_entry.py] 신규 생성된 매도 주문이 없습니다.")