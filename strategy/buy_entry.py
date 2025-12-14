# strategy/buy_entry.py

import logging
import pandas as pd
import os
import sys
import config
from utils.telegram_notifier import notify_order_event, notify_error
from datetime import datetime
from manager.hwm_manager import hwm_manager

if config.EXCHANGE == 'binance':
    print("[SYSTEM] Buy Entry: 바이낸스 모드로 설정합니다.")
    from api.binance.order import get_order_result, cancel_order
    from api.binance.price import get_current_ask_price
    from api.binance.account import get_accounts
elif config.EXCHANGE == 'bybit':
    print("[SYSTEM] Buy Entry: 바이빗 모드로 설정합니다.")
    from api.bybit.order import get_order_result, cancel_order
    from api.bybit.price import get_current_ask_price
    from api.bybit.account import get_accounts
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")

from manager.order_executor import execute_buy_orders
from strategy.casino_strategy import generate_buy_orders


def update_buy_log_status(buy_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[buy_entry.py] buy_log.csv 주문 상태 확인 및 정리 중...")
    if 'buy_uuid' not in buy_log_df.columns or buy_log_df['buy_uuid'].isnull().all():
        return buy_log_df

    wait_orders = buy_log_df[buy_log_df['filled'] == 'wait'].copy()
    if wait_orders.empty:
        print("  - 확인할 'wait' 상태의 매수 주문이 없습니다.")
        return buy_log_df

    print(f"  - 총 {len(wait_orders)}건의 'wait' 상태 매수 주문 확인 중...")
    for idx, row in wait_orders.iterrows():
        market, uuid = row['market'], row['buy_uuid']
        try:
            order_info = get_order_result(market, str(uuid))
            current_state = order_info.get("state")
            if current_state != 'wait':
                print(f"  - 주문 상태 변경 감지: {market} (UUID: {uuid}) -> {current_state}")
                buy_log_df.loc[idx, 'filled'] = current_state
                if current_state == 'done':
                    avg_price = float(order_info.get('avg_price', 0))
                    # --- 👇👇👇 HWM 리셋 로직 추가 👇👇👇 ---
                    hwm_manager.reset_hwm(market, avg_price)
                    # --- 👆👆👆 추가 완료 --- 👆👆👆
                    details = {
                        'filled_qty': order_info.get('executed_qty', 0),
                        'price': avg_price,
                        'total_amount': order_info.get('cum_quote', 0), 'fee': 0
                    }
                    notify_order_event("체결", market, details)
        except Exception as e:
            print(f"  - ❌ 주문 상태 확인 중 오류: {market} (UUID: {uuid}): {e}")
            notify_error("update_buy_log_status", f"{market} 주문({uuid}) 상태 확인 실패: {e}")
            continue
    return buy_log_df


def run_buy_entry_flow(current_unit_size: float):
    try:
        setting_df = pd.read_csv("setting.csv")
        buy_log_df = pd.read_csv("buy_log.csv") if os.path.exists("buy_log.csv") else pd.DataFrame(columns=["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "base_unit_size", "filled"])
    except Exception as e:
        print(f"❌ 설정 또는 로그 파일 로드 실패: {e}")
        return

    if not buy_log_df.empty:
        buy_log_df = update_buy_log_status(buy_log_df)

    try:
        account_data = get_accounts()
        usdt_balance = account_data.get("usdt_balance", 0.0)
        open_positions = account_data.get("open_positions", [])

        holdings = {}
        for pos in open_positions:
            market = pos['symbol']
            balance = abs(float(pos.get('positionAmt', 0)))
            avg_price = float(pos.get('entryPrice', 0))
            if balance * avg_price > 5:
                holdings[market] = {"balance": balance, "avg_price": avg_price}

    except Exception as e:
        print(f"❌ 보유 자산 및 잔고 정보 조회 실패: {e}")
        return

    markets_to_check = setting_df['market'].unique()
    current_prices = {}
    for market in markets_to_check:
        try:
            price = get_current_ask_price(market)
            current_prices[market] = price
            # --- 👇👇👇 HWM 갱신 로직 추가 👇👇👇 ---
            if market in holdings: # 포지션이 있을 때만 HWM 갱신
                hwm_manager.update_hwm(market, price)
            # --- 👆👆👆 추가 완료 --- 👆👆👆
        except Exception as e:
            print(f"❌ {market} 현재가 조회 실패: {e}")
            current_prices[market] = None

    setting_df['unit_size'] = current_unit_size

    new_orders_df = generate_buy_orders(setting_df, buy_log_df, current_prices, holdings, usdt_balance)

    if not new_orders_df.empty:
        print(f"[buy_entry.py] 신규 매수 주문 {len(new_orders_df)}건 생성됨. 주문 실행을 시작합니다.")
        combined_buy_log_df = pd.concat([buy_log_df, new_orders_df], ignore_index=True)
        try:
            final_buy_log_df = execute_buy_orders(combined_buy_log_df, setting_df)
            final_buy_log_df.to_csv("buy_log.csv", index=False)
            print("[buy_entry.py] buy_log.csv 파일 저장 완료.")
        except Exception as e:
            print(f"❌ 매수 주문 실행 또는 로그 저장 중 오류 발생: {e}")
    else:
        if not buy_log_df.empty:
            buy_log_df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] 신규 생성된 매수 주문이 없습니다.")