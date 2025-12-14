# strategy/casino_strategy.py
import pandas as pd
from datetime import datetime
import logging
import numpy as np
import config
from manager.hwm_manager import hwm_manager

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
                        usdt_balance: float, enable_rebalance: bool = False) -> pd.DataFrame:
    new_orders = []

    for _, setting in setting_df.iterrows():
        market = setting["market"]
        current_price = current_prices.get(market)
        if current_price is None:
            logging.warning(f"⚠️ {market}의 현재 가격 정보가 없어 매수 주문 생성을 건너뜁니다.")
            continue

        try:
            leverage = float(setting["leverage"])
            if leverage <= 0: leverage = 1.0
        except (KeyError, TypeError, ValueError):
            logging.warning(f"⚠️ {market}의 레버리지 설정이 없거나 잘못되었습니다. [1.0]배로 간주합니다.")
            leverage = 1.0

        market_buy_log = buy_log_df[buy_log_df["market"] == market] if not buy_log_df.empty else pd.DataFrame()

        if market_buy_log.empty and market not in holdings:
            base_unit_size = float(setting["unit_size"])
            initial_entry_multiplier = float(setting.get("initial_entry_units", 1.0))
            buy_amount = base_unit_size * initial_entry_multiplier
            
            required_margin = (buy_amount / leverage) * config.MARGIN_BUFFER_FACTOR

            if usdt_balance >= required_margin:
                logging.info(f"🆕 {market}: 최초 매수 주문 생성을 시도합니다. (Buy Amount: {buy_amount:.2f}, Base Unit: {base_unit_size})")
                new_orders.append({
                    "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                    "target_price": current_price, "buy_amount": buy_amount,
                    "buy_units": 0, "buy_type": "initial", "filled": "update",
                    "base_unit_size": base_unit_size
                })
            else:
                logging.warning(
                    f"⚠️ {market} 최초 매수 실패 (잔고 부족). 필요 증거금(버퍼 포함): {required_margin:.2f}, 보유: {usdt_balance:.2f}")
            continue

        base_unit_size_for_flow = None
        if not market_buy_log.empty:
            initial_buys = market_buy_log[market_buy_log['buy_type'] == 'initial']
            if not initial_buys.empty:
                base_unit_size_for_flow = initial_buys.iloc[-1].get('base_unit_size')

        if pd.isna(base_unit_size_for_flow):
            base_unit_size_for_flow = float(setting["unit_size"])

        last_small_flow_price = get_last_small_flow_or_initial_price(market_buy_log)
        last_large_flow_price = get_last_large_flow_or_initial_price(market_buy_log)

        if last_small_flow_price is None or last_large_flow_price is None:
            logging.debug(f"ℹ️ {market}: 이전 체결 기록이 부족하여 추가 매수 주문을 생성하지 않습니다.")
            continue

        hwm = hwm_manager.get_hwm(market)
        small_flow_pct = float(setting["small_flow_pct"])
        large_flow_pct = float(setting["large_flow_pct"])
        
        rebalance_small_target_price = None
        if enable_rebalance and hwm > last_small_flow_price * (1 + (small_flow_pct * 0.5)):
            rebalance_small_target_price = round(hwm * (1 - small_flow_pct), 8)
            logging.info(f"🔄 [Rebalance] {market}: small_flow HWM({hwm}) 기반 타겟 조정 -> {rebalance_small_target_price} (기존 매수가: {last_small_flow_price})")

        rebalance_large_target_price = None
        if enable_rebalance and hwm > last_large_flow_price * (1 + (large_flow_pct * 0.5)):
            rebalance_large_target_price = round(hwm * (1 - large_flow_pct), 8)
            logging.info(f"🔄 [Rebalance] {market}: large_flow HWM({hwm}) 기반 타겟 조정 -> {rebalance_large_target_price} (기존 매수가: {last_large_flow_price})")

        small_target_price = rebalance_small_target_price if rebalance_small_target_price is not None else round(last_small_flow_price * (1 - small_flow_pct), 8)
        large_target_price = rebalance_large_target_price if rebalance_large_target_price is not None else round(last_large_flow_price * (1 - large_flow_pct), 8)
        
        if current_price <= small_target_price:
            if not market_buy_log[(market_buy_log["buy_type"] == "small_flow") & (market_buy_log["filled"].isin(["wait", "update"]))].empty:
                logging.debug(f"ℹ️ {market}: 이미 대기 중인 small_flow 주문이 있어 건너뜁니다.")
            else:
                buy_amount = base_unit_size_for_flow * float(setting["small_flow_units"])
                required_margin = (buy_amount / leverage) * config.MARGIN_BUFFER_FACTOR
                if usdt_balance >= required_margin:
                    new_orders.append({
                        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                        "target_price": small_target_price, "buy_amount": buy_amount,
                        "buy_units": 1, "buy_type": "small_flow", "filled": "update",
                        "base_unit_size": np.nan
                    })
                else:
                    logging.warning(f"⚠️ {market} small_flow 매수 실패 (잔고 부족). 필요 증거금(버퍼 포함): {required_margin:.2f}, 보유: {usdt_balance:.2f}")

        if current_price <= large_target_price:
            if not market_buy_log[(market_buy_log["buy_type"] == "large_flow") & (market_buy_log["filled"].isin(["wait", "update"]))].empty:
                logging.debug(f"ℹ️ {market}: 이미 대기 중인 large_flow 주문이 있어 건너뜁니다.")
            else:
                buy_amount = base_unit_size_for_flow * float(setting["large_flow_units"])
                required_margin = (buy_amount / leverage) * config.MARGIN_BUFFER_FACTOR
                if usdt_balance >= required_margin:
                    new_orders.append({
                        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "market": market,
                        "target_price": large_target_price, "buy_amount": buy_amount,
                        "buy_units": 1, "buy_type": "large_flow", "filled": "update",
                        "base_unit_size": np.nan
                    })
                else:
                    logging.warning(f"⚠️ {market} large_flow 매수 실패 (잔고 부족). 필요 증거금(버퍼 포함): {required_margin:.2f}, 보유: {usdt_balance:.2f}")

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
            if not np.isclose(row['target_sell_price'], target_price) or not np.isclose(row['quantity'], quantity_to_sell):
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

    return pd.DataFrame(orders_to_action)