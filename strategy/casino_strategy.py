# import pandas as pd
#
#
# def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict) -> pd.DataFrame:
#     """
#     카지노 매매 전략에 따라 상황을 판단하고,
#     각 상황에 따른 매수 주문 내역을 buy_log 형태로 생성/수정하여 리턴한다.
#     """
#     print("[casino_strategy.py] generate_buy_orders() 호출됨")
#
#     new_logs = []
#
#     for _, setting in setting_df.iterrows():
#         market = setting["market"]
#         unit_size = setting["unit_size"]
#         small_pct = setting["small_flow_pct"]
#         small_units = setting["small_flow_units"]
#         large_pct = setting["large_flow_pct"]
#         large_units = setting["large_flow_units"]
#
#         current_price = current_prices.get(market)
#         if current_price is None:
#             print(f"❌ 현재 가격 없음 → {market}")
#             continue
#
#         coin_logs = buy_log_df[buy_log_df["market"] == market]
#         initial_logs = coin_logs[coin_logs["buy_type"] == "initial"]
#         flow_logs = coin_logs[coin_logs["buy_type"].isin(["small_flow", "large_flow"])]
#
#         # 수정된 부분 (generate_buy_orders 내부)
#
#         # ✅ [상황1] 최초 주문 없음
#         if coin_logs.empty:
#             print(f"📌 {market} → 상황1: 최초 주문 생성")
#
#             # 데이터 1 - initial
#             new_logs.append({
#                 "time": pd.Timestamp.now(),
#                 "market": market,
#                 "target_price": current_price,
#                 "buy_amount": unit_size,
#                 "buy_units": 1,
#                 "buy_type": "initial",
#                 "buy_uuid": None,
#                 "filled": "update"  # 수정됨
#             })
#
#             # 데이터 2 - small_flow
#             small_price = round(current_price * (1 - small_pct))
#             new_logs.append({
#                 "time": pd.Timestamp.now(),
#                 "market": market,
#                 "target_price": small_price,
#                 "buy_amount": unit_size * small_units,
#                 "buy_units": small_units,
#                 "buy_type": "small_flow",
#                 "buy_uuid": None,
#                 "filled": "update"  # 수정됨
#             })
#
#             # 데이터 3 - large_flow
#             large_price = round(current_price * (1 - large_pct))
#             new_logs.append({
#                 "time": pd.Timestamp.now(),
#                 "market": market,
#                 "target_price": large_price,
#                 "buy_amount": unit_size * large_units,
#                 "buy_units": large_units,
#                 "buy_type": "large_flow",
#                 "buy_uuid": None,
#                 "filled": "update"  # 수정됨
#             })
#
#     # ✅ 수정된 상황2: initial filled == done인 코인
#         elif not initial_logs.empty:
#             print(f"📌 {market} → 수정된 상황2: flow 주문 개별 처리 시작")
#
#             for _, row in flow_logs.iterrows():
#                 buy_type = row["buy_type"]
#                 target_price = row["target_price"]
#                 raw_filled = row["filled"]
#                 filled = "" if pd.isna(raw_filled) else str(raw_filled).strip()
#                 row_index = row.name
#
#                 if pd.isna(target_price) or pd.isna(row["buy_amount"]) or pd.isna(row["buy_units"]):
#                     raise ValueError(f"[❌ 에러] {market} - {buy_type} 주문에 누락된 값이 있습니다. 행: {row.to_dict()}")
#
#                 target_price = float(target_price)
#                 unit_pct = small_pct if buy_type == "small_flow" else large_pct
#
#                 # case1: wait 상태 → 가격 상향 후 재조정
#                 if filled == "wait":
#                     # 가격이 기준 이상으로 상승한 경우 → 매수 기준 재조정
#                     A = target_price / (1 - unit_pct)
#                     C = A * (1 + unit_pct / 2)
#
#                     if current_price > C:
#                         new_price = round(C * (1 - unit_pct))
#                         print(f"↗ {market} {buy_type} 가격 재조정: {target_price} → {new_price}")
#                         buy_log_df.loc[row_index, "target_price"] = new_price
#                         buy_log_df.loc[row_index, "filled"] = "update"
#
#
#                 # case2: done 상태 → 동일 비율로 다시 내려서 주문 재생성
#                 elif filled == "done":
#                     buy_log_df.at[row_index, "buy_uuid"] = None
#
#                     new_price = round(target_price * (1 - unit_pct))
#                     print(f"🔁 {market} {buy_type} 연속 주문: {target_price} → {new_price}")
#                     buy_log_df.loc[row_index, "target_price"] = new_price
#                     buy_log_df.loc[row_index, "filled"] = "update"
#
#
#                 elif pd.isna(filled) or filled == "":
#                     print(f"📝 {market} {buy_type} 수동 주문 → 필드 유효성 검사")
#
#                     # 필수 항목 확인: market, target_price, buy_amount, buy_units, buy_type
#                     required_columns = ["market", "target_price", "buy_amount", "buy_units", "buy_type"]
#                     missing_columns = [col for col in required_columns if pd.isna(row[col]) or row[col] == ""]
#
#                     if missing_columns:
#                         raise ValueError(f"[❌ 에러] {market} - {buy_type} 수동 주문에 누락된 필드가 있습니다: {missing_columns}")
#
#                     # 이상 없으면 update 처리
#                     buy_log_df.loc[row_index, "filled"] = "update"
#
#                 # case4: cancel 등 기타 상태 → 예외 처리
#                 else:
#                     raise ValueError(f"[❌ 에러] {market} - {buy_type} 주문의 filled 상태가 예외적입니다: '{filled}'")
#
#     # 새로운 주문이 있다면 기존 로그와 결합
#     if new_logs:
#         new_df = pd.DataFrame(new_logs)
#         buy_log_df = pd.concat([buy_log_df, new_df], ignore_index=True)
#
#     return buy_log_df
#
#
# def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
#     print("[casino_strategy.py] generate_sell_orders() 호출됨")
#
#     # 기존 sell_log_df를 복사해서 시작
#     updated_df = sell_log_df.copy()
#
#     for _, row in setting_df.iterrows():
#         market = row["market"]
#
#         # 보유 중인 코인만 대상
#         if market not in holdings:
#             continue
#
#         h = holdings[market]
#         avg_buy_price = round(h["avg_price"], 8)
#         quantity = round(h["balance"] + h["locked"], 8)  # ✅ 수정된 부분
#         take_profit_pct = row["take_profit_pct"]
#         target_price = round(avg_buy_price * (1 + take_profit_pct), 8)
#
#         # 기존 sell_log에서 해당 market 데이터 있는지 확인
#         existing_idx = updated_df[updated_df["market"] == market].index
#
#         if not existing_idx.empty:
#             idx = existing_idx[0]
#             existing = updated_df.loc[idx]
#
#             is_same = (
#                 round(existing["avg_buy_price"], 8) == avg_buy_price and
#                 round(existing["quantity"], 8) == quantity and
#                 round(existing["target_sell_price"], 2) == target_price
#             )
#
#             if is_same:
#                 print(f"✅ {market} → 보유 정보와 동일 → 유지")
#                 continue
#
#             print(f"✏️ {market} → 기존과 차이 있음 → 수정")
#             updated_df.loc[idx, "avg_buy_price"] = avg_buy_price
#             updated_df.loc[idx, "quantity"] = quantity
#             updated_df.loc[idx, "target_sell_price"] = target_price
#             updated_df.loc[idx, "filled"] = "update"
#
#         else:
#             print(f"🆕 {market} → 새로운 sell_log 생성")
#             new_row = {
#                 "market": market,
#                 "avg_buy_price": avg_buy_price,
#                 "quantity": quantity,
#                 "target_sell_price": target_price,
#                 "sell_uuid": None,
#                 "filled": "update"
#             }
#             updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)
#
#     return updated_df

# 에러 수정 - gemini 제안에 따라 수정처리 (2025-7-3)

# strategy/casino_strategy.py

import pandas as pd
from datetime import datetime


def get_last_filled_price(buy_log_df, market):
    filled_orders = buy_log_df[(buy_log_df['market'] == market) & (buy_log_df['filled'] == 'done')]
    if not filled_orders.empty:
        return filled_orders.iloc[-1]['target_price']
    return None


def generate_buy_orders(setting_df, buy_log_df, current_prices):
    print("[casino_strategy.py] generate_buy_orders() 호출됨")
    new_orders = []

    for _, setting in setting_df.iterrows():
        market = setting['market']
        current_price = current_prices.get(market)
        if current_price is None:
            continue

        market_buy_log = buy_log_df[buy_log_df['market'] == market].copy()
        initial_order = market_buy_log[market_buy_log['buy_type'] == 'initial']

        if initial_order.empty:
            print(f"📌 {market} → 상황1: 최초 주문 생성")
            new_order = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "market": market,
                "target_price": current_price,
                "buy_amount": setting['unit_size'],
                "buy_units": 0,
                "buy_type": "initial",
                "buy_uuid": "",
                "filled": "update"
            }
            new_orders.append(new_order)
            continue

        last_filled_price = get_last_filled_price(market_buy_log, market)
        if last_filled_price is None:
            continue

        print(f"📌 {market} → 수정된 상황2: flow 주문 개별 처리 시작")
        for i in range(1, setting['small_flow_units'] + 1):
            target_price = last_filled_price * (1 - setting['small_flow_pct'] * i)
            if current_price <= target_price:
                # --- 여기가 수정된 핵심 부분 ---
                # 이미 해당 단계의 미체결 flow 주문이 있는지 확인
                existing_flow_order = market_buy_log[
                    (market_buy_log['buy_type'] == 'small_flow') &
                    (market_buy_log['buy_units'] == i) &
                    (market_buy_log['filled'].isin(['update', 'wait']))
                    ]
                if not existing_flow_order.empty:
                    continue  # 이미 주문이 있으므로 건너뜀
                # --- 여기까지 ---

                new_order = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market": market,
                    "target_price": target_price,
                    "buy_amount": setting['unit_size'],
                    "buy_units": i,
                    "buy_type": "small_flow",
                    "buy_uuid": "",
                    "filled": "update"
                }
                new_orders.append(new_order)

        for i in range(1, setting['large_flow_units'] + 1):
            target_price = last_filled_price * (1 - setting['large_flow_pct'] * i)
            if current_price <= target_price:
                # --- 여기가 수정된 핵심 부분 ---
                existing_flow_order = market_buy_log[
                    (market_buy_log['buy_type'] == 'large_flow') &
                    (market_buy_log['buy_units'] == i) &
                    (market_buy_log['filled'].isin(['update', 'wait']))
                    ]
                if not existing_flow_order.empty:
                    continue
                # --- 여기까지 ---

                new_order = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market": market,
                    "target_price": target_price,
                    "buy_amount": setting['unit_size'],
                    "buy_units": i,
                    "buy_type": "large_flow",
                    "buy_uuid": "",
                    "filled": "update"
                }
                new_orders.append(new_order)

    if new_orders:
        new_df = pd.DataFrame(new_orders)
        buy_log_df = pd.concat([buy_log_df, new_df], ignore_index=True)

    return buy_log_df


def generate_sell_orders(setting_df, holdings, sell_log_df):
    print("[casino_strategy.py] generate_sell_orders() 호출됨")
    updated_df = sell_log_df.copy()

    for market, info in holdings.items():
        if info['balance'] <= 0:
            continue

        setting = setting_df[setting_df['market'] == market]
        if setting.empty:
            continue
        setting = setting.iloc[0]

        avg_buy_price = info['avg_price']
        take_profit_pct = setting['take_profit_pct']

        # 소수점 8자리까지 계산하여 정밀도를 높임
        target_price = round(avg_buy_price * (1 + take_profit_pct), 8)

        existing_sell = updated_df[updated_df['market'] == market]
        if not existing_sell.empty:
            existing_row_idx = existing_sell.index[0]
            if updated_df.at[existing_row_idx, 'target_sell_price'] != target_price:
                updated_df.at[existing_row_idx, 'target_sell_price'] = target_price
                updated_df.at[existing_row_idx, 'quantity'] = info['balance']
                updated_df.at[existing_row_idx, 'filled'] = "update"
                print(f"🔁 {market} → sell_log 가격 업데이트")
        else:
            new_row = {
                "market": market,
                "avg_buy_price": avg_buy_price,
                "quantity": info['balance'],
                "target_sell_price": target_price,
                "sell_uuid": "",
                "filled": "update"
            }
            print(f"🆕 {market} → 새로운 sell_log 생성")
            updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)

    return updated_df