# strategy/sell_entry.py

import pandas as pd
import sys
import config

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 매도 로직을 설정합니다.")
    # from api.binance.account import get_accounts # 제거
    from api.binance.order import get_order_result
    # from api.binance.price import get_current_ask_price # 제거
else:
    print("[SYSTEM] 업비트 모드로 매도 로직을 설정합니다.")
    # from api.upbit.account import get_accounts # 제거
    from api.upbit.order import get_order_results_by_uuids
    # from api.upbit.price import get_current_ask_price # 제거

# 추가: common_utils에서 get_current_holdings를 import
from utils.common_utils import get_current_holdings

from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders


def update_sell_log_status(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")
    pending_df = sell_log_df[sell_log_df["filled"] == "wait"]

    if pending_df.empty:
        print("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        return sell_log_df

    indices_to_drop = []

    # --- 기존 코드와 동일 ---
    if config.EXCHANGE == 'binance':
        for idx, row in pending_df.iterrows():
            uuid = str(row["sell_uuid"])
            market = row["market"]
            try:
                result = get_order_result(uuid, market)
                if result['state'] in ["done", "cancel"]:
                    print(f"✅ {market} 매도 주문(id:{uuid}) 완료/취소됨 → 로그에서 제거")
                    indices_to_drop.append(idx)
                else:
                    print(f"ⓘ {market} 매도 주문(id:{uuid}) 상태: {result['state']}") # 현재 상태 로그 추가
            except Exception as e:
                print(f"매도 주문 상태 조회 실패 {market}(id:{uuid}): {e}")
    else: # 업비트
        uuid_list = pending_df["sell_uuid"].tolist()
        try:
            status_map = get_order_results_by_uuids(uuid_list)
            for idx, row in pending_df.iterrows():
                uuid = row["sell_uuid"]
                if uuid in status_map and status_map[uuid] in ["done", "cancel"]:
                    print(f"✅ {row['market']} 매도 주문(id:{uuid}) 완료/취소됨 → 로그에서 제거") # 로그 추가
                    indices_to_drop.append(idx)
                elif uuid in status_map:
                    print(f"ⓘ {row['market']} 매도 주문(id:{uuid}) 상태: {status_map[uuid]}") # 현재 상태 로그 추가
        except Exception as e:
            print(f"❌ 주문 상태 조회 중 오류 발생: {e}")
    # --- 여기까지 ---

    if indices_to_drop:
        sell_log_df = sell_log_df.drop(index=indices_to_drop).reset_index(drop=True)
        print(f"[sell_entry.py] 완료/취소된 {len(indices_to_drop)}건 삭제 처리 완료")
    else:
        print("[sell_entry.py] sell_log.csv에 변경사항 없음.") # 로그 추가

    return sell_log_df


def load_setting_data():
    print("[sell_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


# --- 기존 get_current_holdings 함수는 utils/common_utils.py로 이동 ---


def run_sell_entry_flow():
    print("[sell_entry.py] 카지노 매매 전략 - 매도 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings() # common_utils에서 import된 함수 호출

    if not holdings:
        print("[sell_entry.py] 현재 보유 코인이 없어 매도 로직을 종료합니다.")
        # 만약 sell_log에 미체결 주문이 남아있다면 clear
        try:
            sell_log_df = pd.read_csv("sell_log.csv")
            if not sell_log_df.empty:
                # 보유 코인이 없으면 모든 미체결 매도 주문을 취소하거나 done 처리? (전략에 따라 다름)
                # 현재는 단순히 로그에서 제거 (clean_buy_log_for_fully_sold_coins 유사 로직 필요할 수도)
                print("[sell_entry.py] 보유 코인이 없으므로 sell_log.csv를 초기화합니다.")
                pd.DataFrame(columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"]).to_csv("sell_log.csv", index=False)
        except FileNotFoundError:
            pass # 파일이 없으면 초기화할 필요 없음
        return

    try:
        sell_log_df = pd.read_csv("sell_log.csv")
    except FileNotFoundError:
        sell_log_df = pd.DataFrame(columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"])

    sell_log_df = update_sell_log_status(sell_log_df)

    # 보유하지 않은 마켓의 sell_log는 정리
    valid_markets = set(holdings.keys())
    initial_sell_log_count = len(sell_log_df) # 로그 추가
    sell_log_df = sell_log_df[sell_log_df["market"].isin(valid_markets)].reset_index(drop=True)
    if len(sell_log_df) < initial_sell_log_count: # 로그 추가
        print(f"[sell_entry.py] 보유하지 않은 마켓의 매도 주문 {initial_sell_log_count - len(sell_log_df)}건 정리 완료.")

    updated_sell_log_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    try:
        updated_sell_log_df = execute_sell_orders(updated_sell_log_df)
        updated_sell_log_df.to_csv("sell_log.csv", index=False)
        print("[sell_entry.py] 매도 주문 완료 → sell_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 매도 주문 실행 중 치명적인 오류 발생: {e}", file=sys.stderr) # 오류 메시지 명확화
        sys.exit(1)

    print("[sell_entry.py] 매도 전략 흐름 종료")