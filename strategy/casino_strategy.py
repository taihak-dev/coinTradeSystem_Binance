# strategy/casino_strategy.py
import pandas as pd
from datetime import datetime
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_last_small_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    if market_buy_log.empty: return None
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "small_flow"]))
        ]
    return filtered_log.iloc[-1]["target_price"] if not filtered_log.empty else None


def get_last_large_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    if market_buy_log.empty: return None
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "large_flow"]))
        ]
    return filtered_log.iloc[-1]["target_price"] if not filtered_log.empty else None


def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict, holdings: dict,
                        usdt_balance: float) -> pd.DataFrame:
    new_orders = []

    for _, setting in setting_df.iterrows():
        market = setting["market"]
        current_price = current_prices.get(market)
        if current_price is None:
            logging.warning(f"⚠️ {market}의 현재 가격 정보가 없어 매수 주문 생성을 건너뜁니다.")
            continue

        market_buy_log = buy_log_df[buy_log_df["market"] == market] if not buy_log_df.empty else pd.DataFrame()

        # --- 1. 최초 매수 로직 (기존과 동일) ---
        if market_buy_log.empty and market not in holdings:
            buy_amount = float(setting["unit_size"])
            if usdt_balance >= buy_amount:
                logging.info(f"🆕 {market}: 최초 매수 주문 생성을 시도합니다.")
                new_orders.append({
                    "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                    "target_price": current_price, "buy_amount": buy_amount,
                    "buy_units": 0, "buy_type": "initial", "filled": "update"
                })
            else:
                logging.warning(f"⚠️ {market} 최초 매수 실패 (잔고 부족). 필요: {buy_amount:.2f}, 보유: {usdt_balance:.2f}")
            continue

        # --- 2. 기준가 확인 (기존과 동일) ---
        last_small_flow_price = get_last_small_flow_or_initial_price(market_buy_log)
        last_large_flow_price = get_last_large_flow_or_initial_price(market_buy_log)

        if last_small_flow_price is None or last_large_flow_price is None:
            logging.debug(f"ℹ️ {market}: 이전 체결 기록이 부족하여 추가 매수 주문을 생성하지 않습니다.")
            continue

        # --- 👇👇👇 3. small_flow 로직 수정 👇👇👇 ---
        # 1) for 루프 제거 -> '횟수' 제한 사라짐
        # 2) target_price 계산 시 '* i' 제거 -> 단순 '연쇄' 방식 적용
        # 3) buy_amount 계산 시 'units'를 '배율'로만 사용
        # 4) 중복 주문 방지 로직 변경

        # 'small_flow_units'를 배율로 사용
        small_flow_multiplier = float(setting["small_flow_units"])
        small_target_price = round(last_small_flow_price * (1 - float(setting["small_flow_pct"])), 8)

        if current_price <= small_target_price:
            # 이미 'wait' 또는 'update' 상태인 small_flow 주문이 있는지 확인
            if not market_buy_log[
                (market_buy_log["buy_type"] == "small_flow") &
                (market_buy_log["filled"].isin(["wait", "update"]))
            ].empty:
                logging.debug(f"ℹ️ {market}: 이미 대기 중인 small_flow 주문이 있어 건너뜁니다.")
            else:
                # 'unit_size' * '배율'
                buy_amount = float(setting["unit_size"]) * small_flow_multiplier
                if usdt_balance >= buy_amount:
                    new_orders.append({
                        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                        "target_price": small_target_price, "buy_amount": buy_amount,
                        "buy_units": 1,  # 'buy_units' 컬럼은 더 이상 단계 의미가 없으므로 1로 고정
                        "buy_type": "small_flow", "filled": "update"
                    })
                else:
                    logging.warning(
                        f"⚠️ {market} small_flow 매수 실패 (잔고 부족). 필요: {buy_amount:.2f}, 보유: {usdt_balance:.2f}")

        # --- 👇👇👇 4. large_flow 로직 수정 (small_flow와 동일) 👇👇👇 ---

        # 'large_flow_units'를 배율로 사용
        large_flow_multiplier = float(setting["large_flow_units"])
        large_target_price = round(last_large_flow_price * (1 - float(setting["large_flow_pct"])), 8)

        if current_price <= large_target_price:
            # 이미 'wait' 또는 'update' 상태인 large_flow 주문이 있는지 확인
            if not market_buy_log[
                (market_buy_log["buy_type"] == "large_flow") &
                (market_buy_log["filled"].isin(["wait", "update"]))
            ].empty:
                logging.debug(f"ℹ️ {market}: 이미 대기 중인 large_flow 주문이 있어 건너뜁니다.")
            else:
                # 'unit_size' * '배율'
                buy_amount = float(setting["unit_size"]) * large_flow_multiplier
                if usdt_balance >= buy_amount:
                    new_orders.append({
                        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                        "target_price": large_target_price, "buy_amount": buy_amount,
                        "buy_units": 1,  # 'buy_units' 컬럼은 더 이상 단계 의미가 없으므로 1로 고정
                        "buy_type": "large_flow", "filled": "update"
                    })
                else:
                    logging.warning(
                        f"⚠️ {market} large_flow 매수 실패 (잔고 부족). 필요: {buy_amount:.2f}, 보유: {usdt_balance:.2f}")
        # --- 👆👆👆 수정 완료 👆👆👆 ---

    return pd.DataFrame(new_orders)


def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
    orders_to_action = []
    processed_markets = set()

    if not sell_log_df.empty:
        wait_sell_orders = sell_log_df[sell_log_df['filled'] == 'wait'].copy()
        for _, row in wait_sell_orders.iterrows():
            market = row['market']
            processed_markets.add(market)
            if market not in holdings: continue
            info, setting = holdings[market], setting_df[setting_df['market'] == market].iloc[0]
            avg_buy_price, quantity_to_sell = info['avg_price'], info['balance']
            target_price = round(avg_buy_price * (1 + float(setting['take_profit_pct'])), 8)
            if not np.isclose(row['target_sell_price'], target_price) or not np.isclose(row['quantity'],
                                                                                        quantity_to_sell):
                row['target_sell_price'], row['quantity'], row['filled'] = target_price, quantity_to_sell, 'update'
                orders_to_action.append(row.to_dict())

    for market, info in holdings.items():
        if market in processed_markets: continue
        setting = setting_df[setting_df['market'] == market]
        if setting.empty: continue
        target_price = round(info['avg_price'] * (1 + float(setting.iloc[0]['take_profit_pct'])), 8)
        new_order = {
            "market": market, "avg_buy_price": info['avg_price'], "quantity": info['balance'],
            "target_sell_price": target_price, "sell_uuid": "new", "filled": "new",
            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        orders_to_action.append(new_order)

    # --- 👇👇👇 여기가 핵심 수정 부분입니다 👇👇👇 ---
    # if orders_to_action:  <-- 이 불필요한 조건문을 제거합니다.
    return pd.DataFrame(orders_to_action)  # 주문 목록이 비어있더라도 항상 DataFrame을 반환합니다.
    # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆