# strategy/sell_entry.py

import logging
import sys
import pandas as pd
import config
from manager.order_executor import execute_sell_orders
from strategy.casino_strategy import generate_sell_orders
from utils.common_utils import get_current_holdings
from utils.telegram_notifier import notify_order_event, notify_error

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] 바이낸스 모드로 매도 로직을 설정합니다.")
    from api.binance.order import get_order_result
else:
    # 업비트 등 다른 거래소 로직 (현재는 비활성)
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def update_sell_log_status(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    sell_log.csv에 기록된 'wait' 상태의 주문들이 실제로 체결되었는지 확인하고 상태를 업데이트합니다.
    체결이 확인되면 텔레그램으로 상세 알림을 전송합니다.
    """
    logging.info("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")

    # 확인할 주문이 없으면 바로 종료
    if 'sell_uuid' not in sell_log_df.columns or sell_log_df['sell_uuid'].isnull().all():
        logging.info("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        return sell_log_df

    pending_df = sell_log_df[sell_log_df["filled"] == "wait"].copy()
    if pending_df.empty:
        logging.info("[sell_entry.py] 확인할 미체결 매도 주문이 없습니다.")
        return sell_log_df

    changed = False
    for idx, row in pending_df.iterrows():
        order_id = str(row["sell_uuid"])
        market = row["market"]

        try:
            result = get_order_result(order_id, market)

            # 💡 --- 알림 로직이 추가된 핵심 부분 --- 💡
            # 주문 상태가 'wait' -> 'done' 으로 변경된 순간을 포착
            if sell_log_df.at[idx, "filled"] == "wait" and result.get("state") == "done":
                sell_log_df.at[idx, "filled"] = "done"
                changed = True

                logging.info(f"🎉 [{market}] 매도 주문 체결! 텔레그램 알림을 전송합니다.")

                # 매도 수익(PNL) 계산
                avg_buy_price = float(row['avg_buy_price'])
                sell_price = result.get('avg_price')
                sold_quantity = result.get('executed_qty')

                # PNL = (판매가 - 구매가) * 수량
                pnl = (sell_price - avg_buy_price) * sold_quantity if avg_buy_price > 0 else 0

                # 텔레그램 알림 전송
                notify_order_event(
                    "체결", market,
                    {
                        "filled_qty": sold_quantity,
                        "price": sell_price,
                        "total_amount": result.get('cum_quote'),
                        "fee": 0,  # 수수료 정보는 별도 조회가 필요하여 우선 0으로 표시
                        "pnl": pnl  # 계산된 수익 정보 추가
                    }
                )
            # 💡 --- 여기까지 알림 로직입니다 --- 💡

        except Exception as e:
            logging.error(f"❌ 매도 주문 상태 조회 실패 {market}(id:{order_id}): {e}")
            notify_error(f"{market} Sell Order Status", f"주문 상태 조회 실패(id:{order_id}): {e}")
            continue

    if changed:
        logging.info("[sell_entry.py] sell_log.csv에 변경사항 있음.")
    else:
        logging.info("[sell_entry.py] sell_log.csv에 변경사항 없음.")

    return sell_log_df


def load_setting_data():
    logging.info("[sell_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


def run_sell_entry_flow():
    logging.info("[sell_entry.py] 카지노 매매 전략 - 매도 로직 시작 (선주문 방식)")

    setting_df = load_setting_data()
    holdings = get_current_holdings()

    try:
        sell_log_df = pd.read_csv("sell_log.csv", dtype={'sell_uuid': str})
    except FileNotFoundError:
        sell_log_df = pd.DataFrame(
            columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"])

    # --- 💡 [핵심 수정 1] 보유하지 않는 코인의 매도 기록을 먼저 정리 ---
    if not sell_log_df.empty:
        markets_in_log = sell_log_df['market'].unique()
        markets_in_holdings = holdings.keys()
        markets_to_remove = [m for m in markets_in_log if m not in markets_in_holdings]

        if markets_to_remove:
            logging.info(f"🧹 보유하지 않는 코인의 매도 기록을 sell_log.csv에서 정리합니다: {markets_to_remove}")
            sell_log_df = sell_log_df[~sell_log_df['market'].isin(markets_to_remove)].copy()
    # --- 여기까지 정리 로직 ---

    # 현재 보유 코인이 없다면 모든 로직 종료
    if not holdings:
        logging.info("[sell_entry.py] 현재 보유 코인이 없어 매도 로직을 종료합니다.")
        # 정리된 sell_log_df (비어있을 것)를 저장
        sell_log_df.to_csv("sell_log.csv", index=False)
        return

    # 1. 거래소에 제출된 'wait' 상태 주문들의 실제 체결 상태를 확인하고 알림을 보냅니다.
    sell_log_df = update_sell_log_status(sell_log_df)

    # --- 💡 [핵심 수정 2] 매수 로직과 동일한 안정적인 데이터 처리 구조로 변경 ---
    # 2. 현재 보유 현황을 기준으로 신규/정정 매도 주문 목록을 생성합니다.
    orders_to_action_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    # 3. 신규/정정 주문이 있을 경우에만 실행 로직을 진행합니다.
    if not orders_to_action_df.empty:
        logging.info(f"🆕 신규/정정 매도 주문 {len(orders_to_action_df)}건 생성됨. 주문 실행을 시작합니다.")

        # 기존 로그에서 'update'가 필요한 주문들을 제거하고, 새로 생성된 주문 목록과 합칩니다.
        # 'new' UUID를 가진 신규 주문과, 기존 UUID를 가진 정정 주문을 모두 처리합니다.
        uuids_to_update = orders_to_action_df['sell_uuid'].tolist()
        sell_log_df = sell_log_df[~sell_log_df['sell_uuid'].isin(uuids_to_update)]
        if sell_log_df.empty:
            combined_sell_log_df = orders_to_action_df
        else:
            combined_sell_log_df = pd.concat([sell_log_df, orders_to_action_df], ignore_index=True)

        try:
            # 합쳐진 전체 로그를 실행기에 전달합니다.
            final_sell_log_df = execute_sell_orders(combined_sell_log_df)
            # 최종 업데이트된 전체 로그를 저장하여 데이터 유실을 방지합니다.
            final_sell_log_df.to_csv("sell_log.csv", index=False)
            logging.info("[sell_entry.py] 모든 주문 완료 → sell_log.csv 저장 완료")
        except Exception as e:
            logging.error(f"🚨 매도 주문 실행 중 치명적인 오류 발생: {e}", exc_info=True)
            notify_error("Sell Execution", f"매도 주문 실행 중 오류: {e}")
            sys.exit(1)
    else:
        logging.info("[sell_entry.py] 신규/정정 매도 주문이 없습니다. 현재 상태를 유지합니다.")
        # 변경사항(체결 상태 업데이트 등)이 있을 수 있으므로 현재 로그를 저장합니다.
        sell_log_df.to_csv("sell_log.csv", index=False)

    logging.info("[sell_entry.py] 매도 전략 흐름 종료")