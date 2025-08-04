# strategy/casino_strategy.py
import pandas as pd
from datetime import datetime
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_last_small_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    """'initial' 또는 'small_flow' 타입의 주문 중 가장 최근 체결가를 반환합니다."""
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "small_flow"]))
        ]
    if not filtered_log.empty:
        return filtered_log.iloc[-1]["target_price"]
    return None


def get_last_large_flow_or_initial_price(market_buy_log: pd.DataFrame) -> float | None:
    """'initial' 또는 'large_flow' 타입의 주문 중 가장 최근 체결가를 반환합니다."""
    filtered_log = market_buy_log[
        (market_buy_log["filled"] == "done") &
        (market_buy_log["buy_type"].isin(["initial", "large_flow"]))
        ]
    if not filtered_log.empty:
        return filtered_log.iloc[-1]["target_price"]
    return None


def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict,
                        holdings: dict) -> pd.DataFrame:
    """
    [최종 수정] 보유 현황 기반 'initial' 매수 및 '단순 하락률' 전략
    """
    new_orders = []

    for _, setting in setting_df.iterrows():
        market = setting["market"]

        is_holding = market in holdings and holdings.get(market, {}).get('balance', 0) > 0

        # --- 최초 매수 로직 수정 ---
        if not is_holding:
            # 현재 보유량이 없다면 최초 매수 진행
            # 단, 아직 처리되지 않은 'update' 상태의 initial 주문이 있는지 확인하여 중복 생성 방지
            existing_initial_update = buy_log_df[
                (buy_log_df['market'] == market) & (buy_log_df['buy_type'] == 'initial') & (
                            buy_log_df['filled'] == 'update')]
            if existing_initial_update.empty:
                unit_size = float(setting["unit_size"])
                current_price = current_prices.get(market)
                if current_price:
                    new_orders.append({
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "market": market,
                        "target_price": current_price, "buy_amount": unit_size, "buy_units": 0,
                        "buy_type": "initial", "buy_uuid": "", "filled": "update"
                    })
            continue

        # --- Flow 매수 로직 (보유 중일 때만 실행) ---
        market_buy_log = buy_log_df[buy_log_df['market'] == market]
        unit_size = float(setting["unit_size"])
        small_flow_pct = float(setting["small_flow_pct"])
        small_flow_units_as_multiplier = int(setting["small_flow_units"])
        large_flow_pct = float(setting["large_flow_pct"])
        large_flow_units_as_multiplier = int(setting["large_flow_units"])
        current_price = current_prices.get(market)

        if current_price is None:
            continue

        # 1. small_flow 로직 (단순 하락률)
        small_flow_base_price = get_last_small_flow_or_initial_price(market_buy_log)
        if small_flow_base_price:
            target_price = round(small_flow_base_price * (1 - small_flow_pct), 8)
            if current_price <= target_price and market_buy_log[
                np.isclose(market_buy_log["target_price"], target_price)].empty:
                buy_amount = unit_size * small_flow_units_as_multiplier
                new_orders.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "market": market,
                    "target_price": target_price, "buy_amount": buy_amount, "buy_units": 1,
                    "buy_type": "small_flow", "buy_uuid": "", "filled": "update"
                })

        # 2. large_flow 로직 (단순 하락률)
        large_flow_base_price = get_last_large_flow_or_initial_price(market_buy_log)
        if large_flow_base_price:
            target_price = round(large_flow_base_price * (1 - large_flow_pct), 8)
            if current_price <= target_price and market_buy_log[
                np.isclose(market_buy_log["target_price"], target_price)].empty:
                buy_amount = unit_size * large_flow_units_as_multiplier
                new_orders.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "market": market,
                    "target_price": target_price, "buy_amount": buy_amount, "buy_units": 1,
                    "buy_type": "large_flow", "buy_uuid": "", "filled": "update"
                })

    return pd.DataFrame(new_orders)


def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    [백테스트 최적화] 신규 또는 수정이 필요한 매도 주문만 반환합니다.
    """
    orders_to_update = []

    existing_wait_orders = sell_log_df[sell_log_df['filled'] == 'wait'].copy()
    processed_markets = set()

    for idx, row in existing_wait_orders.iterrows():
        market = row['market']
        processed_markets.add(market)

        # 보유하지 않는데 매도 주문이 남은 경우, 취소 대상으로 반환
        if market not in holdings:
            row['filled'] = 'update'
            orders_to_update.append(row.to_dict())
            continue

        info = holdings[market]
        setting = setting_df[setting_df['market'] == market].iloc[0]
        avg_buy_price = info['avg_price']
        quantity_to_sell = info['balance']
        target_price = round(avg_buy_price * (1 + float(setting['take_profit_pct'])), 8)

        # 💡 [핵심 수정 1] 컬럼명을 'target_sell_price'와 'quantity'로 바로잡습니다.
        if not np.isclose(row['target_sell_price'], target_price) or not np.isclose(row['quantity'], quantity_to_sell):
            row['target_sell_price'] = target_price
            row['quantity'] = quantity_to_sell
            row['filled'] = 'update'
            orders_to_update.append(row.to_dict())

    # 신규 보유 코인에 대한 매도 주문 생성
    for market, info in holdings.items():
        if market in processed_markets:
            continue

        setting = setting_df[setting_df['market'] == market]
        if setting.empty: continue

        target_price = round(info['avg_price'] * (1 + float(setting.iloc[0]['take_profit_pct'])), 8)

        # 💡 [핵심 수정 2] 신규 주문 생성 시에도 올바른 컬럼명을 사용하고, 빠져있던 'avg_buy_price'를 추가합니다.
        orders_to_update.append({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market": market,
            "avg_buy_price": info['avg_price'],
            "quantity": info['balance'],
            "target_sell_price": target_price,
            "sell_uuid": "new",
            "filled": "update"
        })

    return pd.DataFrame(orders_to_update)