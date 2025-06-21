# strategy/buy_entry.py

# 필요한 외부 라이브러리 및 내부 함수 불러오기
import pandas as pd
import os
from api.account import get_accounts  # 계좌 정보 조회 함수
from api.order import get_order_results_by_uuids  # 여러 주문 UUID로 상태 확인 함수
from api.price import get_current_ask_price  # 현재 매도호가(가격) 조회 함수
from manager.order_executor import execute_buy_orders
from strategy.casino_strategy import generate_buy_orders
import sys
from api.order import cancel_orders_by_uuids


def clean_buy_log_for_fully_sold_coins(buy_log_df: pd.DataFrame, holdings: dict) -> pd.DataFrame:
    print("[buy_entry.py] ✅ 보유하지 않은 코인의 매수 주문 정리 중...")

    valid_markets = set(holdings.keys())
    all_markets = set(buy_log_df["market"].unique())
    sold_out_markets = all_markets - valid_markets

    for market in sold_out_markets:
        coin_logs = buy_log_df[buy_log_df["market"] == market]
        uuids_to_cancel = coin_logs["buy_uuid"].dropna().tolist()

        if uuids_to_cancel:
            try:
                result = cancel_orders_by_uuids(uuids_to_cancel)
                success_count = result.get("success", {}).get("count", 0)
                fail_count = result.get("failed", {}).get("count", 0)

                print(f"🗑️ {market} 매수 주문 취소 요청 완료:")
                print(f"  ✅ 성공: {success_count}개")
                print(f"  ❌ 실패: {fail_count}개")

                if success_count == 0:
                    print(f"⚠️ {market} 매수 주문이 하나도 취소되지 않았습니다 → 유지")
                    continue

            except Exception as e:
                print(f"⚠️ {market} 주문 취소 요청 자체 실패 → 유지: {e}")
                continue

        # 일부라도 성공한 경우 → buy_log에서 삭제
        print(f"📤 {market} → buy_log에서 삭제 완료")

    # 최종적으로 보유 코인만 남기기
    return buy_log_df[buy_log_df["market"].isin(valid_markets)]


def load_setting_data():
    # setting.csv 파일을 읽어서 DataFrame으로 반환
    print("[buy_entry.py] setting.csv 불러오는 중")
    df = pd.read_csv("setting.csv")
    return df


def get_current_holdings():
    """
    보유 중인 코인 정보를 가져오고,
    총 평가금액이 100원 이상인 코인만 반환하는 함수
    """
    print("[buy_entry.py] 현재 보유 자산 조회 중")
    accounts = get_accounts()  # 업비트 API로 전체 계좌 리스트 가져오기
    holdings = {}  # 보유 코인 정보를 담을 딕셔너리

    for acc in accounts:
        if acc['currency'] == 'KRW':
            continue  # 원화는 보유 자산 판단에서 제외

        # KRW-코인형태 (예: KRW-DOGE)로 마켓명을 구성
        market = f"KRW-{acc['currency']}"

        # 보유 수량과 평균 매수 단가를 float으로 변환
        balance = float(acc['balance']) + float(acc['locked'])
        avg_price = float(acc['avg_buy_price'])

        try:
            # 현재 매도 호가(=현재 가격)를 API로 조회
            current_price = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 현재가 조회 실패: {e}")
            continue  # 가격 조회에 실패하면 그 코인은 건너뜀

        # 총 평가 금액 = 보유 수량 * 현재가
        total_value = balance * current_price

        # 총 평가 금액이 100원 미만이면 '보유하지 않은 것으로 간주'
        if total_value < 100:
            continue

        # 보유 중인 코인의 정보 저장
        holdings[market] = {
            "balance": balance,
            "avg_price": avg_price,
            "current_price": current_price,
            "total_value": total_value
        }

    print(f"[buy_entry.py] 현재 보유 중인 코인 수: {len(holdings)}개")
    return holdings  # 예: {"KRW-DOGE": {...}, "KRW-XRP": {...}}


def update_buy_log_status():
    """
    buy_log.csv에 기록된 매수 주문의 상태를 확인하고,
    주문 상태(wait → done/cancel 등) 변경 시 파일을 업데이트하며,
    API 응답에 포함되지 않은 uuid는 삭제한다.
    """
    print("[buy_entry.py] buy_log.csv 주문 체결 여부 확인 중")

    if not os.path.exists("buy_log.csv"):
        print("[buy_entry.py] buy_log.csv 파일이 없습니다.")
        return

    df = pd.read_csv("buy_log.csv")

    if "buy_uuid" not in df.columns or "filled" not in df.columns:
        print("❌ buy_log.csv에 필요한 열이 없습니다.")
        return

    # 1. 'filled' 열을 문자열로 만들고 NaN → 빈 문자열 처리
    filled_str = df["filled"].fillna("").astype(str)

    # 2. 아직 체결되지 않은 상태("wait", "")인 것만 필터링
    cond_filled_wait_or_empty = filled_str.isin(["wait", ""])

    # 3. UUID가 존재하는 행 필터링
    cond_has_uuid = df["buy_uuid"].notna()

    # 4. 두 조건을 만족하는 "확인 대상 주문" 추출
    pending_df = df[cond_filled_wait_or_empty & cond_has_uuid]

    if pending_df.empty:
        print("[buy_entry.py] 확인할 주문이 없습니다.")
        return

    uuid_list = pending_df["buy_uuid"].tolist()

    try:
        # ✅ API를 통해 주문 상태 조회 (uuid → 상태)
        status_map = get_order_results_by_uuids(uuid_list)
        changed = False

        # ✅ 응답되지 않은 uuid 리스트 구하기
        received_uuids = set(status_map.keys())
        submitted_uuids = set(uuid_list)
        missing_uuids = submitted_uuids - received_uuids

        if missing_uuids:
            print(f"[buy_entry.py] 응답 없는 잘못된 uuid 삭제 예정: {missing_uuids}")
            # 해당 uuid를 가진 행들을 제거
            df = df[~df["buy_uuid"].isin(missing_uuids)]
            changed = True

        # ✅ 응답된 uuid에 대해 상태 업데이트
        for idx, row in df.iterrows():
            uuid = row["buy_uuid"]
            if uuid in status_map:
                new_state = status_map[uuid]
                if df.at[idx, "filled"] != new_state:
                    df.at[idx, "filled"] = new_state
                    changed = True

        if changed:
            df.to_csv("buy_log.csv", index=False)
            print("[buy_entry.py] buy_log.csv 업데이트 완료")
        else:
            print("[buy_entry.py] 변경 사항 없음")

    except Exception as e:
        print(f"[buy_entry.py] 주문 상태 조회 중 오류 발생: {e}")


def run_buy_entry_flow():
    print("[buy_entry.py] 카지노 매매 전략 - 매수 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings()

    # 현재 보유한 코인에 대해 주문 상태 업데이트
    for _, row in setting_df.iterrows():
        market = row["market"]
        has_coin = market in holdings
        print(f"🪙 {market} 보유 여부: {'보유 중' if has_coin else '미보유'}")

        if has_coin:
            update_buy_log_status()

    # buy_log.csv 불러오기 또는 초기화
    try:
        buy_log_df = pd.read_csv("buy_log.csv")
    except FileNotFoundError:
        buy_log_df = pd.DataFrame(columns=[
            "time", "market", "target_price", "buy_amount",
            "buy_units", "buy_type", "buy_uuid", "filled"
        ])

    # 현재 가격(매도 1호가) 수집
    print("[buy_entry.py] 현재 가격 수집 중...")
    current_prices = {}
    for _, row in setting_df.iterrows():
        market = row["market"]
        try:
            current_prices[market] = get_current_ask_price(market)
        except Exception as e:
            print(f"❌ {market} 가격 조회 실패: {e}")

    buy_log_df = pd.read_csv("buy_log.csv")
    buy_log_df = clean_buy_log_for_fully_sold_coins(buy_log_df, holdings)

    # 전략 실행 → 업데이트된 buy_log 반환
    updated_buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

    # 주문 실행
    try:
        updated_buy_log_df = execute_buy_orders(updated_buy_log_df)
        updated_buy_log_df.to_csv("buy_log.csv", index=False)
        print("[buy_entry.py] 모든 주문 완료 → buy_log.csv 저장 완료")
    except Exception as e:
        print(f"🚨 주문 실패: {e}")
        print("프로그램을 종료합니다.")
        import sys
        sys.exit(1)

    print("[buy_entry.py] 매수 전략 흐름 종료")