# strategy/buy_entry.py

import pandas as pd
import os
import sys
import config # 신규/변경

# --- 신규/변경: config 설정에 따라 다른 모듈을 불러오도록 변경 ---
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 매수 로직을 설정합니다.")
    from api.binance.account import get_accounts
    from api.binance.order import get_order_result, cancel_order
    from api.binance.price import get_current_ask_price
else:
    print("[SYSTEM] 업비트 모드로 매수 로직을 설정합니다.")
    from api.upbit.account import get_accounts
    from api.upbit.order import get_order_results_by_uuids, cancel_orders_by_uuids
    from api.upbit.price import get_current_ask_price
# --- 여기까지 ---

from manager.order_executor import execute_buy_orders
from strategy.casino_strategy import generate_buy_orders


def clean_buy_log_for_fully_sold_coins(buy_log_df: pd.DataFrame, holdings: dict) -> pd.DataFrame:
    print("[buy_entry.py] ✅ 보유하지 않은 코인의 매수 주문 정리 중...")
    valid_markets = set(holdings.keys())
    all_markets_in_log = set(buy_log_df["market"].unique())
    sold_out_markets = all_markets_in_log - valid_markets

    if not sold_out_markets:
        return buy_log_df

    uuids_to_cancel_map = {}
    for market in sold_out_markets:
        coin_logs = buy_log_df[(buy_log_df["market"] == market) & (buy_log_df["filled"] == "wait")]
        uuids = coin_logs["buy_uuid"].dropna().tolist()
        if uuids:
            uuids_to_cancel_map[market] = uuids

    if not uuids_to_cancel_map:
         return buy_log_df[buy_log_df["market"].isin(valid_markets)]

    # --- 신규/변경: 거래소별 주문 취소 로직 ---
    if config.EXCHANGE == 'binance':
        success_count = 0
        for market, uuids in uuids_to_cancel_map.items():
            for uuid in uuids:
                try:
                    cancel_order(str(uuid), market)
                    success_count += 1
                except Exception as e:
                    print(f"⚠️ {market} 주문(id:{uuid}) 취소 실패: {e}")
        print(f"🗑️ 매도 완료된 코인들의 매수 주문 총 {success_count}건 취소 완료")
    else: # 업비트
        all_uuids = [uuid for uuids in uuids_to_cancel_map.values() for uuid in uuids]
        try:
            result = cancel_orders_by_uuids(all_uuids)
            print(f"🗑️ 매도 완료된 코인들의 매수 주문 취소 요청 완료: {result}")
        except Exception as e:
            print(f"⚠️ 주문 취소 요청 실패: {e}")
    # --- 여기까지 ---

    # 최종적으로 보유 코인에 대한 로그만 남김
    return buy_log_df[buy_log_df["market"].isin(valid_markets)]


def load_setting_data():
    print("[buy_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


def get_current_holdings():
    print("[buy_entry.py] 현재 보유 자산 조회 중")
    accounts = get_accounts()
    holdings = {}

    # 신규/변경: 기준 통화 변경 (KRW -> USDT)
    base_currency = 'USDT' if config.EXCHANGE == 'binance' else 'KRW'

    for acc in accounts:
        if acc['currency'] == base_currency:
            continue

        # 신규/변경: 바이낸스는 market 이름이 이미 'BTCUSDT' 형태임
        if config.EXCHANGE == 'binance':
             market = acc['currency']
        else:
             market = f"{base_currency}-{acc['currency']}"

        balance = float(acc['balance']) + float(acc['locked'])
        avg_price = float(acc['avg_buy_price'])

        if balance * avg_price < 1: # 1 USDT 또는 1 KRW 미만은 무시
            continue

        try:
            current_price = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 현재가 조회 실패: {e}")
            continue

        total_value = balance * current_price

        # 신규/변경: 최소 보유금액 기준 상향 (100원 -> 5 USDT)
        min_value = 5 if config.EXCHANGE == 'binance' else 100
        if total_value < min_value:
            continue

        holdings[market] = {
            "balance": balance, "avg_price": avg_price,
            "current_price": current_price, "total_value": total_value
        }

    print(f"[buy_entry.py] 현재 보유 중인 코인 수: {len(holdings)}개")
    return holdings


def update_buy_log_status():
    print("[buy_entry.py] buy_log.csv 주문 체결 여부 확인 중")
    try:
        df = pd.read_csv("buy_log.csv")
        if df.empty: return
    except FileNotFoundError:
        return

    pending_df = df[df["filled"] == "wait"]
    if pending_df.empty:
        print("[buy_entry.py] 확인할 주문이 없습니다.")
        return

    changed = False
    # --- 신규/변경: 거래소별 주문 상태 조회 로직 ---
    if config.EXCHANGE == 'binance':
        for idx, row in pending_df.iterrows():
            uuid = str(row["buy_uuid"])
            market = row["market"]
            try:
                result = get_order_result(uuid, market)
                if df.at[idx, "filled"] != result['state']:
                    df.at[idx, "filled"] = result['state']
                    print(f"주문 상태 변경: {market} (id:{uuid}) -> {result['state']}")
                    changed = True
            except Exception as e:
                print(f"주문 상태 조회 실패 {market}(id:{uuid}): {e}")
    else: # 업비트
        uuid_list = pending_df["buy_uuid"].tolist()
        try:
            status_map = get_order_results_by_uuids(uuid_list)
            for idx, row in df.iterrows():
                uuid = row["buy_uuid"]
                if uuid in status_map and df.at[idx, "filled"] != status_map[uuid]:
                    df.at[idx, "filled"] = status_map[uuid]
                    changed = True
        except Exception as e:
            print(f"주문 상태 조회 중 오류: {e}")
    # --- 여기까지 ---

    if changed:
        df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] buy_log.csv 업데이트 완료")


def run_buy_entry_flow():
    print("[buy_entry.py] 카지노 매매 전략 - 매수 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings()

    update_buy_log_status()

    try:
        buy_log_df = pd.read_csv("buy_log.csv")
    except FileNotFoundError:
        buy_log_df = pd.DataFrame(columns=[
            "time", "market", "target_price", "buy_amount",
            "buy_units", "buy_type", "buy_uuid", "filled"
        ])

    # 매도된 코인의 미체결 매수 주문 정리
    buy_log_df = clean_buy_log_for_fully_sold_coins(buy_log_df, holdings)

    print("[buy_entry.py] 현재 가격 수집 중...")
    current_prices = {}
    for market in setting_df["market"].unique():
        try:
            current_prices[market] = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 가격 조회 실패: {e}")

    updated_buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

    try:
        # 신규/변경: execute_buy_orders 호출 시 setting_df 추가
        updated_buy_log_df = execute_buy_orders(updated_buy_log_df, setting_df)
        updated_buy_log_df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] 모든 주문 완료 → buy_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 주문 실패: {e}", file=sys.stderr)
        sys.exit(1)

    print("[buy_entry.py] 매수 전략 흐름 종료")