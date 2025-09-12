# strategy/casino_strategy.py
import pandas as pd
from datetime import datetime
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_last_small_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    # --- 👇👇👇 안전장치 추가 👇👇👇 ---
    if market_buy_log.empty:
        return None
    # --- 👆👆👆 여기까지 추가 --- 👆👆👆
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "small_flow"]))
        ]
    if not filtered_log.empty:
        return filtered_log.iloc[-1]["target_price"]
    return None


def get_last_large_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    # --- 👇👇👇 안전장치 추가 👇👇👇 ---
    if market_buy_log.empty:
        return None
    # --- 👆👆👆 여기까지 추가 --- 👆👆👆
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "large_flow"]))
        ]
    if not filtered_log.empty:
        return filtered_log.iloc[-1]["target_price"]
    return None


# (이하 generate_buy_orders, generate_sell_orders 함수는 이전과 동일하므로 생략)
# (기존 파일에서 위의 두 함수만 수정하시면 됩니다)

def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict,
                        holdings: dict) -> pd.DataFrame:
    new_orders = []

    for _, setting in setting_df.iterrows():
        market = setting["market"]
        current_price = current_prices.get(market)
        if current_price is None:
            logging.warning(f"⚠️ {market}의 현재 가격 정보가 없어 매수 주문 생성을 건너뜁니다.")
            continue

        market_buy_log = buy_log_df[buy_log_df["market"] == market] if not buy_log_df.empty else pd.DataFrame()

        if market_buy_log.empty and market not in holdings:
            logging.info(f"🆕 {market}: 최초 매수 주문 생성을 시도합니다.")
            new_orders.append({
                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "market": market,
                "target_price": current_price,
                "buy_amount": float(setting["unit_size"]),
                "buy_units": 0,
                "buy_type": "initial",
                "filled": "update"
            })
            continue

        last_small_flow_price = get_last_small_flow_or_initial_price(market_buy_log)
        last_large_flow_price = get_last_large_flow_or_initial_price(market_buy_log)

        if last_small_flow_price is None or last_large_flow_price is None:
            logging.debug(f"ℹ️ {market}: 이전 체결 기록이 부족하여 추가 매수 주문을 생성하지 않습니다.")
            continue

        for i in range(1, int(setting["small_flow_units"]) + 1):
            target_price = round(last_small_flow_price * (1 - float(setting["small_flow_pct"]) * i), 8)
            if current_price <= target_price:
                if not market_buy_log[
                    (market_buy_log["buy_type"] == "small_flow") & (market_buy_log["buy_units"] == i)].empty:
                    continue
                new_orders.append({
                    "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "market": market, "target_price": target_price,
                    "buy_amount": float(setting["unit_size"]),
                    "buy_units": i, "buy_type": "small_flow", "filled": "update"
                })
                break

        for i in range(1, int(setting["large_flow_units"]) + 1):
            target_price = round(last_large_flow_price * (1 - float(setting["large_flow_pct"]) * i), 8)
            if current_price <= target_price:
                if not market_buy_log[
                    (market_buy_log["buy_type"] == "large_flow") & (market_buy_log["buy_units"] == i)].empty:
                    continue
                new_orders.append({
                    "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "market": market, "target_price": target_price,
                    "buy_amount": float(setting["unit_size"]),
                    "buy_units": i, "buy_type": "large_flow", "filled": "update"
                })
                break

    return pd.DataFrame(new_orders)


def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
    orders_to_action = []
    processed_markets = set()

    if not sell_log_df.empty:
        wait_sell_orders = sell_log_df[sell_log_df['filled'] == 'wait'].copy()
        for idx, row in wait_sell_orders.iterrows():
            market = row['market']
            processed_markets.add(market)

            if market not in holdings:
                continue

            info = holdings[market]
            setting = setting_df[setting_df['market'] == market].iloc[0]
            avg_buy_price = info['avg_price']
            quantity_to_sell = info['balance']
            target_price = round(avg_buy_price * (1 + float(setting['take_profit_pct'])), 8)

            if np.isclose(row['target_sell_price'], target_price) and np.isclose(row['quantity'], quantity_to_sell):
                continue

            row['target_sell_price'] = target_price
            row['quantity'] = quantity_to_sell
            row['filled'] = 'update'
            orders_to_action.append(row.to_dict())

    for market, info in holdings.items():
        if market in processed_markets:
            continue

        setting = setting_df[setting_df['market'] == market]
        if setting.empty: continue

        target_price = round(info['avg_price'] * (1 + float(setting.iloc[0]['take_profit_pct'])), 8)
        new_order = {
            "market": market,
            "avg_buy_price": info['avg_price'],
            "quantity": info['balance'],
            "target_sell_price": target_price,
            "sell_uuid": "new",
            "filled": "new",
            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        orders_to_action.append(new_order)

    return pd.DataFrame(orders_to_action)