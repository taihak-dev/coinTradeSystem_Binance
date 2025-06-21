# manager/order_executor.py

import pandas as pd
from api.order import send_order, cancel_and_new_order
from utils.price_utils import adjust_price_to_tick


def execute_buy_orders(buy_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[order_executor.py] 매수 주문 실행 시작")
    all_success = True

    for idx, row in buy_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = row["buy_uuid"]

        if filled == "done":
            continue

        market = row["market"]
        price = float(row["target_price"])
        amount = float(row["buy_amount"])
        buy_type = row["buy_type"]

        # 호가 단위 보정
        price = adjust_price_to_tick(price, market="KRW", ticker=market)

        # case2: 정정 주문
        if filled == "update" and pd.notna(uuid):
            print(f"🔁 정정 매수 주문: {market}, uuid={uuid}, amount={amount}, price={price}")
            try:
                response = cancel_and_new_order(
                    prev_order_uuid=uuid,
                    market=market,
                    price=price,
                    amount=round(amount / price, 8)
                )
                new_uuid = response.get("new_order_uuid", "")
                if new_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = new_uuid
                    buy_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("정정 매수 주문 uuid 없음")
            except Exception as e:
                print(f"❌ 정정 매수 주문 실패: {e}")
                all_success = False

        # case3: 신규 주문
        elif filled == "update" and pd.isna(uuid):
            print(f"🆕 신규 매수 주문: {market}, amount={amount}, price={price}")
            try:
                if buy_type == "initial":
                    response = send_order(
                        market=market,
                        side="bid",
                        ord_type="price",
                        amount_krw=amount
                    )
                else:
                    volume = round(amount / price, 8)
                    response = send_order(
                        market=market,
                        side="bid",
                        ord_type="limit",
                        unit_price=price,
                        volume=volume,
                        amount_krw=None
                    )
                new_uuid = response.get("uuid", "")
                if new_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = new_uuid
                    buy_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("신규 매수 주문 uuid 없음")
            except Exception as e:
                print(f"❌ 신규 매수 주문 실패: {e}")
                all_success = False

    print("[order_executor.py] 매수 주문 실행 완료")

    if not all_success:
        raise RuntimeError("일부 매수 주문 실패")

    return buy_log_df


import pandas as pd
import sys
from api.order import send_order, cancel_and_new_order
from utils.price_utils import adjust_price_to_tick


def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[order_executor.py] 매도 주문 실행 시작")
    all_success = True

    for idx, row in sell_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = row["sell_uuid"]

        if filled == "done":
            continue  # 이미 완료된 주문은 스킵

        market = row["market"]
        price = float(row["target_sell_price"])
        volume = float(row["quantity"])

        # 호가 단위로 가격 보정
        price = adjust_price_to_tick(price, market="KRW", ticker=market)

        # ✅ update + uuid 존재 → 정정 매도 주문
        if filled == "update" and pd.notna(uuid):
            if volume <= 0:
                print(f"⚠️ {market} 매도할 수량이 0 → 정정 매도 스킵")
                sell_log_df.at[idx, "filled"] = "done"
                continue

            print(f"🔁 정정 매도 주문: {market}, uuid={uuid}, price={price}, volume={volume}")
            try:
                response = cancel_and_new_order(
                    prev_order_uuid=uuid,
                    market=market,
                    price=price,
                    amount=volume
                )
                new_uuid = response.get("new_order_uuid", "")
                if new_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = new_uuid
                    sell_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("정정 매도 주문 new_uuid 없음")
            except Exception as e:
                error_message = str(e)
                if "order_not_found" in error_message:
                    print(f"⚠️ {market} 기존 주문이 없음 → 신규 매도 주문으로 대체")
                    try:
                        response = send_order(
                            market=market,
                            side="ask",
                            ord_type="limit",
                            unit_price=price,
                            volume=volume,
                            amount_krw=None
                        )
                        new_uuid = response.get("uuid", "")
                        if new_uuid:
                            sell_log_df.at[idx, "sell_uuid"] = new_uuid
                            sell_log_df.at[idx, "filled"] = "wait"
                        else:
                            raise ValueError("신규 매도 주문 uuid 없음 (정정 실패 대체)")
                    except Exception as new_e:
                        print(f"❌ 신규 매도 주문 실패: {new_e}")
                        all_success = False
                elif "done_order" in error_message:
                    print(f"✅ {market} 기존 주문은 이미 체결 완료됨 → filled=done 처리")
                    sell_log_df.at[idx, "filled"] = "done"
                    continue
                else:
                    print(f"❌ 정정 매도 주문 실패: {e}")
                    all_success = False

        # ✅ update + uuid 없음 → 신규 매도 주문
        elif filled == "update" and pd.isna(uuid):
            if volume <= 0:
                print(f"⚠️ {market} 매도할 수량이 0 → 신규 매도 스킵")
                sell_log_df.at[idx, "filled"] = "done"
                continue

            print(f"🆕 신규 매도 주문: {market}, price={price}, volume={volume}")
            try:
                response = send_order(
                    market=market,
                    side="ask",
                    ord_type="limit",
                    unit_price=price,
                    volume=volume,
                    amount_krw=None
                )
                new_uuid = response.get("uuid", "")
                if new_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = new_uuid
                    sell_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("신규 매도 주문 uuid 없음")
            except Exception as e:
                print(f"❌ 신규 매도 주문 실패: {e}")
                all_success = False

    print("[order_executor.py] 매도 주문 실행 완료")

    if not all_success:
        raise RuntimeError("일부 매도 주문 실패")

    return sell_log_df