# strategy/buy_entry.py
import logging
import pandas as pd
import os
import sys
import config
from utils.telegram_notifier import notify_order_event, notify_error
from datetime import datetime
# from api.binance.account import get_accounts # 제거
# from api.binance.order import get_order_result, cancel_order # 기존
# from api.binance.price import get_current_ask_price # 제거
# from api.upbit.account import get_accounts # 제거
# from api.upbit.order import get_order_results_by_uuids, cancel_orders_by_uuids # 기존
# from api.upbit.price import get_current_ask_price # 제거

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 매수 로직을 설정합니다.")
    from api.binance.order import get_order_result, cancel_order
else:
    print("[SYSTEM] 업비트 모드로 매수 로직을 설정합니다.")
    from api.upbit.order import get_order_results_by_uuids, cancel_orders_by_uuids

# 추가: common_utils에서 get_current_holdings를 import
from utils.common_utils import get_current_holdings

from manager.order_executor import execute_buy_orders
from strategy.casino_strategy import generate_buy_orders


def clean_buy_log_for_fully_sold_coins(buy_log_df: pd.DataFrame, holdings: dict) -> pd.DataFrame:
    print("[buy_entry.py] ✅ 보유하지 않은 코인의 매수 주문 정리 중...")
    valid_markets = set(holdings.keys())
    all_markets_in_log = set(buy_log_df["market"].unique())
    sold_out_markets = all_markets_in_log - valid_markets

    if not sold_out_markets:
        print("[buy_entry.py] 정리할 매수 주문이 없습니다.") # 로그 추가
        return buy_log_df[buy_log_df["market"].isin(valid_markets)] # 이미 보유하지 않은 코인이 없다면 바로 필터링

    uuids_to_cancel_map = {}
    for market in sold_out_markets:
        coin_logs = buy_log_df[(buy_log_df["market"] == market) & (buy_log_df["filled"] == "wait")]
        uuids = coin_logs["buy_uuid"].dropna().tolist()
        if uuids:
            uuids_to_cancel_map[market] = uuids

    if not uuids_to_cancel_map:
         print("[buy_entry.py] 취소할 매수 주문이 없습니다.") # 로그 추가
         return buy_log_df[buy_log_df["market"].isin(valid_markets)]

    # --- 기존 코드와 동일 ---
    if config.EXCHANGE == 'binance':
        success_count = 0
        for market, uuids in uuids_to_cancel_map.items():
            for uuid in uuids:
                try:
                    cancel_order(str(uuid), market)
                    notify_order_event("취소", market, {"reason": "매도 완료된 코인 매수 주문 취소", "order_id": uuid})
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
    print(f"[buy_entry.py] 정리 후 남은 매수 주문 수: {len(buy_log_df[buy_log_df['market'].isin(valid_markets)])}") # 로그 추가
    return buy_log_df[buy_log_df["market"].isin(valid_markets)]


def load_setting_data():
    print("[buy_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


def reconcile_holdings_with_logs(holdings: dict, buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    """
    거래소의 실제 보유 현황과 로컬 로그 파일을 비교하여,
    누락된 코인 정보를 buy_log.csv에 자동으로 추가하여 동기화합니다.
    """
    logging.info("⚙️ 실제 보유 현황과 로그 파일의 동기화를 시작합니다...")

    coins_in_holdings = set(holdings.keys())
    coins_in_buy_log = set(buy_log_df['market'].unique())
    coins_in_settings = set(setting_df['market'].unique())

    missing_coins = (coins_in_settings & coins_in_holdings) - coins_in_buy_log

    if not missing_coins:
        logging.info("✅ 모든 보유 코인이 로그 파일과 동기화되어 있습니다.")
        return buy_log_df

    logging.warning(f"⚠️ 로그 파일과 동기화되지 않은 코인을 발견했습니다: {missing_coins}")
    new_buy_logs = []

    for market in missing_coins:
        logging.info(f"  -> '{market}' 코인의 매수 기록을 자동으로 생성합니다.")
        holding_info = holdings[market]
        avg_price = holding_info['avg_price']
        balance = holding_info['balance']

        new_buy_log_entry = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market": market,
            "target_price": avg_price,
            "buy_amount": avg_price * balance,
            "buy_units": 0,
            "buy_type": "initial",
            "buy_uuid": f"re-synced-{int(datetime.now().timestamp())}",
            "filled": "done"
        }
        new_buy_logs.append(new_buy_log_entry)
        logging.info(f"    - 생성된 매수 기록: {new_buy_log_entry}")

    if new_buy_logs:
        new_logs_df = pd.DataFrame(new_buy_logs)
        updated_buy_log_df = pd.concat([buy_log_df, new_logs_df], ignore_index=True)
        logging.info(f"✅ 총 {len(new_buy_logs)}개의 누락된 코인 정보를 buy_log_df에 추가했습니다.")
        return updated_buy_log_df

    return buy_log_df


def update_buy_log_status():
    print("[buy_entry.py] buy_log.csv 주문 체결 여부 확인 중")
    try:
        df = pd.read_csv("buy_log.csv")
        if df.empty:
            print("[buy_entry.py] buy_log.csv가 비어있습니다. 확인할 주문이 없습니다.") # 로그 추가
            return
    except FileNotFoundError:
        print("[buy_entry.py] buy_log.csv 파일이 없습니다. 생성될 예정입니다.") # 로그 추가
        return

    pending_df = df[df["filled"] == "wait"]
    if pending_df.empty:
        print("[buy_entry.py] 확인할 미체결 매수 주문이 없습니다.") # 로그 추가
        return

    changed = False
    # --- 기존 코드와 동일 ---
    if config.EXCHANGE == 'binance':
        for idx, row in pending_df.iterrows():
            uuid = str(row["buy_uuid"])
            market = row["market"]
            try:
                result = get_order_result(uuid, market)
                # 바이낸스 API 응답 상태와 로컬 상태가 다를 경우 업데이트
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
            for idx, row in df.iterrows(): # 전체 df를 순회하며 상태 업데이트
                uuid = row["buy_uuid"]
                if uuid in status_map and df.at[idx, "filled"] != status_map[uuid]:
                    df.at[idx, "filled"] = status_map[uuid]
                    print(f"주문 상태 변경: {row['market']} (id:{uuid}) -> {status_map[uuid]}") # 로그 추가
                    changed = True
        except Exception as e:
            print(f"주문 상태 조회 중 오류: {e}")
    # --- 여기까지 ---

    if changed:
        df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] buy_log.csv 업데이트 완료")
    else:
        print("[buy_entry.py] buy_log.csv 변경 사항 없음.") # 로그 추가


def run_buy_entry_flow():
    print("[buy_entry.py] 카지노 매매 전략 - 매수 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings() # common_utils에서 import된 함수 호출

    update_buy_log_status()

    try:
        buy_log_df = pd.read_csv("buy_log.csv")
    except FileNotFoundError:
        buy_log_df = pd.DataFrame(columns=[
            "time", "market", "target_price", "buy_amount",
            "buy_units", "buy_type", "buy_uuid", "filled"
        ])

    # 모든 로직 시작 전에, 실제 보유 현황을 기준으로 buy_log.csv를 먼저 동기화합니다.
    buy_log_df = reconcile_holdings_with_logs(holdings, buy_log_df, setting_df)

    # 매도된 코인의 미체결 매수 주문 정리
    buy_log_df = clean_buy_log_for_fully_sold_coins(buy_log_df, holdings)

    print("[buy_entry.py] 현재 가격 수집 중...")
    current_prices = {}
    # settings_df에 있는 모든 market에 대한 현재 가격을 조회
    for market in setting_df["market"].unique():
        try:
            # common_utils에서 get_current_ask_price를 import하지 않았으므로
            # api.binance.price.get_current_ask_price (또는 upbit)를 직접 호출
            if config.EXCHANGE == 'binance':
                from api.binance.price import get_current_ask_price
            else:
                from api.upbit.price import get_current_ask_price
            current_prices[market] = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 현재가 조회 실패: {e}")

    updated_buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

    try:
        updated_buy_log_df = execute_buy_orders(updated_buy_log_df, setting_df)
        updated_buy_log_df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] 모든 주문 완료 → buy_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 주문 실행 중 치명적인 오류 발생: {e}") # 오류 메시지 명확화
        sys.exit(1)

    print("[buy_entry.py] 매수 전략 흐름 종료")