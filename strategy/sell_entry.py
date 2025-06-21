# strategy/sell_entry.py

from api.account import get_accounts
from api.price import get_current_ask_price
from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders
import pandas as pd
from api.order import get_order_results_by_uuids


def update_sell_log_status_by_uuid(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")

    # 확인 대상: filled가 공백/대기(wait)이며, uuid 존재
    pending_df = sell_log_df[
        sell_log_df["filled"].isin(["wait", ""]) & sell_log_df["sell_uuid"].notna()
    ]

    if pending_df.empty:
        print("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        return sell_log_df

    uuid_list = pending_df["sell_uuid"].tolist()

    try:
        # UUID로 상태 조회
        status_map = get_order_results_by_uuids(uuid_list)

        # 삭제 대상 인덱스 수집
        indices_to_drop = []

        for idx, row in sell_log_df.iterrows():
            uuid = row["sell_uuid"]
            if uuid in status_map:
                state = status_map[uuid]
                if state in ["done", "cancel"]:
                    print(f"✅ {row['market']} 주문 완료됨 → 제거 처리 (상태: {state})")
                    indices_to_drop.append(idx)

        # 삭제 처리
        if indices_to_drop:
            sell_log_df.drop(index=indices_to_drop, inplace=True)
            sell_log_df.reset_index(drop=True, inplace=True)
            sell_log_df.to_csv("sell_log.csv", index=False)
            print(f"[sell_entry.py] 완료된 {len(indices_to_drop)}건 삭제 및 sell_log.csv 저장 완료")
        else:
            print("[sell_entry.py] 삭제할 완료 주문 없음")

    except Exception as e:
        print(f"❌ 주문 상태 조회 중 오류 발생: {e}")
        import sys
        sys.exit(1)

    return sell_log_df




def load_setting_data():
    print("[sell_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


def get_current_holdings():
    print("[...] 현재 보유 자산 조회 중")
    accounts = get_accounts()
    holdings = {}

    for acc in accounts:
        if acc['currency'] == 'KRW':
            continue

        market = f"KRW-{acc['currency']}"
        balance = float(acc['balance'])        # 실제 주문 가능한 잔고
        locked = float(acc['locked'])          # 주문에 묶인 잔고
        avg_price = float(acc['avg_buy_price'])

        try:
            current_price = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 현재가 조회 실패: {e}")
            continue

        total_value = (balance + locked) * current_price
        if total_value < 100:
            continue

        holdings[market] = {
            "balance": balance,            # 🟢 실제 사용 가능한 잔고
            "locked": locked,              # 🔒 주문 대기 중 수량
            "avg_price": avg_price,
            "current_price": current_price,
            "total_value": total_value
        }

    print(f"[...] 보유 중인 코인 수: {len(holdings)}개")
    return holdings


def run_sell_entry_flow():
    print("[sell_entry.py] 카지노 매매 전략 - 매도 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings()

    try:
        sell_log_df = pd.read_csv("sell_log.csv")
    except FileNotFoundError:
        sell_log_df = pd.DataFrame(columns=[
            "market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"
        ])

    # ✅ UUID 기반 매도 주문 상태 확인
    sell_log_df = update_sell_log_status_by_uuid(sell_log_df)

    # ✅ 보유하지 않은 코인의 매도 로그 제거
    valid_markets = set(holdings.keys())
    sell_log_df = sell_log_df[sell_log_df["market"].isin(valid_markets)]

    # 전략 실행
    updated_sell_log_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    try:
        updated_sell_log_df = execute_sell_orders(updated_sell_log_df)
        updated_sell_log_df.to_csv("sell_log.csv", index=False)
        print("[sell_entry.py] 매도 주문 완료 → sell_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 매도 주문 실패: {e}")
        import sys
        sys.exit(1)

    print("[sell_entry.py] 매도 전략 흐름 종료")