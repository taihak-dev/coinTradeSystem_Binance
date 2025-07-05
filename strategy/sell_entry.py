# strategy/sell_entry.py

import pandas as pd
import sys
import config # 신규/변경

# --- 신규/변경: config 설정에 따라 다른 모듈을 불러오도록 변경 ---
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 매도 로직을 설정합니다.")
    from api.binance.account import get_accounts
    from api.binance.order import get_order_result
    from api.binance.price import get_current_ask_price
else:
    print("[SYSTEM] 업비트 모드로 매도 로직을 설정합니다.")
    from api.upbit.account import get_accounts
    from api.upbit.order import get_order_results_by_uuids
    from api.upbit.price import get_current_ask_price
# --- 여기까지 ---

from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders


def update_sell_log_status(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")
    pending_df = sell_log_df[sell_log_df["filled"] == "wait"]

    if pending_df.empty:
        print("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        return sell_log_df

    indices_to_drop = []

    # --- 신규/변경: 거래소별 주문 상태 조회 로직 ---
    if config.EXCHANGE == 'binance':
        for idx, row in pending_df.iterrows():
            uuid = str(row["sell_uuid"])
            market = row["market"]
            try:
                result = get_order_result(uuid, market)
                if result['state'] in ["done", "cancel"]:
                    print(f"✅ {market} 매도 주문(id:{uuid}) 완료/취소됨 → 로그에서 제거")
                    indices_to_drop.append(idx)
            except Exception as e:
                print(f"매도 주문 상태 조회 실패 {market}(id:{uuid}): {e}")
    else: # 업비트
        uuid_list = pending_df["sell_uuid"].tolist()
        try:
            status_map = get_order_results_by_uuids(uuid_list)
            for idx, row in pending_df.iterrows():
                uuid = row["sell_uuid"]
                if uuid in status_map and status_map[uuid] in ["done", "cancel"]:
                    indices_to_drop.append(idx)
        except Exception as e:
            print(f"❌ 주문 상태 조회 중 오류 발생: {e}")
    # --- 여기까지 ---

    if indices_to_drop:
        sell_log_df = sell_log_df.drop(index=indices_to_drop).reset_index(drop=True)
        print(f"[sell_entry.py] 완료된 {len(indices_to_drop)}건 삭제 처리 완료")

    return sell_log_df


def load_setting_data():
    return pd.read_csv("setting.csv")


def get_current_holdings():
    # 이 함수는 buy_entry.py의 것과 거의 동일하므로 그쪽 것을 사용해도 무방
    # 여기서는 sell_entry에 맞게 약간 간소화된 버전을 유지
    print("[sell_entry.py] 현재 보유 자산 조회 중")
    accounts = get_accounts()
    holdings = {}
    base_currency = 'USDT' if config.EXCHANGE == 'binance' else 'KRW'

    for acc in accounts:
        if acc['currency'] == base_currency:
            continue

        market = acc['currency'] if config.EXCHANGE == 'binance' else f"{base_currency}-{acc['currency']}"
        balance = float(acc['balance'])
        locked = float(acc['locked'])
        total_balance = balance + locked
        avg_price = float(acc['avg_buy_price'])

        if total_balance * avg_price < 1:
            continue

        holdings[market] = {
            "balance": balance,
            "locked": locked,
            "avg_price": avg_price,
        }
    return holdings


def run_sell_entry_flow():
    print("[sell_entry.py] 카지노 매매 전략 - 매도 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings()

    if not holdings:
        print("[sell_entry.py] 보유 코인이 없어 매도 로직을 종료합니다.")
        return

    try:
        sell_log_df = pd.read_csv("sell_log.csv")
    except FileNotFoundError:
        sell_log_df = pd.DataFrame(columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"])

    sell_log_df = update_sell_log_status(sell_log_df)

    valid_markets = set(holdings.keys())
    sell_log_df = sell_log_df[sell_log_df["market"].isin(valid_markets)]

    updated_sell_log_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    try:
        updated_sell_log_df = execute_sell_orders(updated_sell_log_df)
        updated_sell_log_df.to_csv("sell_log.csv", index=False)
        print("[sell_entry.py] 매도 주문 완료 → sell_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 매도 주문 실패: {e}", file=sys.stderr)
        sys.exit(1)

    print("[sell_entry.py] 매도 전략 흐름 종료")