# strategy/buy_entry.py
import logging
import sys
from datetime import datetime
import pandas as pd
import config
from utils.telegram_notifier import notify_order_event, notify_error

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 매수 로직을 설정합니다.")
    from services.exchange_service import get_order_result, cancel_order
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
                    logging.info(f"🎉 [{market}] 매수 주문 체결! 텔레그램 알림을 전송합니다.")
                    notify_order_event(
                        "체결", market,
                        {
                            "filled_qty": result.get('executed_qty'),
                            "price": result.get('avg_price'),
                            "total_amount": result.get('cum_quote'),
                            "fee": 0  # 수수료 정보는 별도 조회가 필요하여 우선 0으로 표시
                        }
                    )
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

    # ✅✅✅ --- 1단계 안전장치: 자산 조회 실패 시 긴급 정지 --- ✅✅✅
    try:
        holdings = get_current_holdings()
        # get_current_holdings 함수가 실패하면 Exception을 발생시킨다고 가정
    except Exception as e:
        logging.critical(f"🚨 [CRITICAL] 보유 자산 조회 실패: {e}. 안전을 위해 매수 로직을 즉시 중단합니다.")
        notify_error("CRITICAL HOLDINGS CHECK", f"Failed to fetch holdings: {e}. Buy cycle aborted.")
        return  # 함수를 즉시 종료하여 의도치 않은 매매 방지
    # ✅✅✅ --- 여기까지 추가 --- ✅✅✅

    try:
        buy_log_df = pd.read_csv("buy_log.csv", dtype={'buy_uuid': str})
    except FileNotFoundError:
        buy_log_df = pd.DataFrame(columns=[
            "time", "market", "target_price", "buy_amount",
            "buy_units", "buy_type", "buy_uuid", "filled"
        ])

    # ✅✅✅ --- 2단계 안전장치: 로그와 실제 자산 동기화 --- ✅✅✅
    # 매매 로직 시작 전, 항상 실제 보유 현황을 기준으로 buy_log.csv를 먼저 동기화합니다.
    # 이 로직이 먼저 실행되면, 로그가 비워져도 자산 조회만 성공하면 복구 가능합니다.
    buy_log_df = reconcile_holdings_with_logs(holdings, buy_log_df, setting_df)
    # ✅✅✅ --- 여기까지 위치 조정 및 강조 --- ✅✅✅

    # 매도된 코인의 미체결 매수 주문 정리
    buy_log_df = clean_buy_log_for_fully_sold_coins(buy_log_df, holdings)

    update_buy_log_status()

    try:
        buy_log_df = pd.read_csv("buy_log.csv", dtype={'buy_uuid': str})
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
    if config.EXCHANGE == 'binance':
        from services.exchange_service import get_current_ask_price as _get_price
    else:
        from api.upbit.price import get_current_ask_price as _get_price
    for market in setting_df["market"].unique():
        try:
            current_prices[market] = _get_price(market)
        except Exception as e:
            # 실패한 심볼만 스킵하고 다음으로 진행
            print(f"❌ {market} 현재가 조회 실패: {e}")
            # 필요하면 기본값을 넣고 계속
            # current_prices[market] = None
            continue


    # 1. 현재 가격을 기준으로 신규 매수 주문 목록을 생성합니다.
    new_orders_df = generate_buy_orders(setting_df, buy_log_df, current_prices, holdings)

    # 2. 신규 생성된 주문이 있을 경우에만 실행 로직을 진행합니다.
    if not new_orders_df.empty:
        print(f"[buy_entry.py] 신규 매수 주문 {len(new_orders_df)}건 생성됨. 주문 실행을 시작합니다.")

        # 💡 [핵심 수정 1] 기존 로그와 신규 주문을 하나로 합칩니다.
        combined_buy_log_df = pd.concat([buy_log_df, new_orders_df], ignore_index=True)

        try:
            # 💡 [핵심 수정 2] 합쳐진 전체 로그를 실행기에 전달합니다.
            final_buy_log_df = execute_buy_orders(combined_buy_log_df, setting_df)

            # 💡 [핵심 수정 3] 최종 업데이트된 전체 로그를 저장하여 데이터 유실을 방지합니다.
            final_buy_log_df.to_csv("buy_log.csv", index=False)
            print("[buy_entry.py] 모든 주문 완료 → buy_log.csv 저장 완료")

        except Exception as e:
            print(f"🚨 주문 실행 중 치명적인 오류 발생: {e}")
            # 오류 발생 시에도 현재까지의 로그는 유지됩니다.
            sys.exit(1)

    else:
        # 💡 [핵심 수정 4] 신규 주문이 없으면 없다고 명확히 로그를 남기고 종료합니다.
        print("[buy_entry.py] 신규 매수 주문이 없습니다. 현재 상태를 유지합니다.")
        # 만약을 위해 현재 상태의 buy_log_df를 저장하여 일관성을 유지합니다.
        buy_log_df.to_csv("buy_log.csv", index=False)

    print("[buy_entry.py] 매수 전략 흐름 종료")
