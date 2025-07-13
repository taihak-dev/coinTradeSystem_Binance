# strategy/sell_entry.py

import pandas as pd
import sys
import config
import logging # 로깅 모듈 임포트

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] 바이낸스 모드로 매도 로직을 설정합니다.")
    from api.binance.order import get_order_result, cancel_order # cancel_order도 사용됨
else:
    logging.info("[SYSTEM] 업비트 모드로 매도 로직을 설정합니다.")
    from api.upbit.order import get_order_results_by_uuids

# 추가: common_utils에서 get_current_holdings를 import
from utils.common_utils import get_current_holdings

from strategy.casino_strategy import generate_sell_orders
from manager.order_executor import execute_sell_orders

# 텔레그램 알림 모듈 임포트
from utils.telegram_notifier import notify_order_event, notify_error

def update_sell_log_status(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("[sell_entry.py] sell_log.csv 주문 상태 확인 및 정리 중...")
    pending_df = sell_log_df[sell_log_df["filled"] == "wait"]

    if pending_df.empty:
        logging.info("[sell_entry.py] 확인할 매도 주문이 없습니다.")
        return sell_log_df

    indices_to_drop = []

    if config.EXCHANGE == 'binance':
        for idx, row in pending_df.iterrows():
            order_id = str(row["sell_uuid"])
            market = row["market"]
            try:
                result = get_order_result(order_id, market)
                state = result['state']
                if state in ["done", "cancel"]:
                    logging.info(f"✅ {market} 매도 주문(id:{order_id}) 완료/취소됨 → 로그에서 제거")
                    indices_to_drop.append(idx)
                    # 텔레그램 알림 추가
                    if state == "done":
                        # 체결된 주문의 상세 정보를 가져와야 하지만, get_order_result에서 바로 제공하지 않으므로
                        # 필요한 정보가 있다면 result['response']에서 파싱해야 함.
                        # 여기서는 간단하게 체결/취소 알림만 보냄.
                        # pnl은 이 단계에서 정확히 알 수 없을 수 있으므로, order_executor에서 매도 체결 시점에 보내는 것이 더 정확합니다.
                        # 여기서는 최소한의 정보만 보냅니다.
                        notify_order_event("체결", market, {
                            "filled_qty": "확인 필요", # get_order_result의 response에서 가져올 수 있다면 파싱하여 사용
                            "price": "확인 필요",      # get_order_result의 response에서 가져올 수 있다면 파싱하여 사용
                            "total_amount": "확인 필요",
                            "fee": "확인 필요",
                            "pnl": "확인 필요"
                        })
                    elif state == "cancel":
                        notify_order_event("취소", market, {"reason": "시스템/사용자 취소", "order_id": order_id})
                else:
                    logging.info(f"ⓘ {market} 매도 주문(id:{order_id}) 상태: {state}")
            except Exception as e:
                logging.error(f"❌ 매도 주문 상태 조회 실패 {market}(id:{order_id}): {e}", exc_info=True)
                notify_error(f"{market} Sell Order Status", f"주문 상태 조회 실패 (ID:{order_id}): {e}")

    else: # 업비트 (기존 로직 유지)
        uuid_list = pending_df["sell_uuid"].tolist()
        try:
            status_map = get_order_results_by_uuids(uuid_list)
            for idx, row in pending_df.iterrows():
                order_id = row["sell_uuid"]
                state = status_map.get(order_id)
                if state in ["done", "cancel"]:
                    logging.info(f"✅ {row['market']} 매도 주문(id:{order_id}) 완료/취소됨 → 로그에서 제거")
                    indices_to_drop.append(idx)
                    # 텔레그램 알림 추가 (업비트도 동일)
                    if state == "done":
                        notify_order_event("체결", row['market'], {"filled_qty": "확인 필요", "price": "확인 필요", "pnl": "확인 필요"})
                    elif state == "cancel":
                        notify_order_event("취소", row['market'], {"reason": "시스템/사용자 취소", "order_id": order_id})
                elif state:
                    logging.info(f"ⓘ {row['market']} 매도 주문(id:{order_id}) 상태: {state}")
        except Exception as e:
            logging.error(f"❌ 주문 상태 조회 중 오류 발생: {e}", exc_info=True)
            notify_error("Sell Order Status Batch", f"주문 상태 일괄 조회 중 오류 발생: {e}")

    if indices_to_drop:
        sell_log_df = sell_log_df.drop(index=indices_to_drop).reset_index(drop=True)
        logging.info(f"[sell_entry.py] 완료/취소된 {len(indices_to_drop)}건 삭제 처리 완료")
    else:
        logging.info("[sell_entry.py] sell_log.csv에 변경사항 없음.")

    return sell_log_df


def load_setting_data():
    logging.info("[sell_entry.py] setting.csv 불러오는 중")
    return pd.read_csv("setting.csv")


def run_sell_entry_flow():
    logging.info("[sell_entry.py] 카지노 매매 전략 - 매도 로직 시작")

    setting_df = load_setting_data()
    holdings = get_current_holdings() # common_utils에서 import된 함수 호출

    if not holdings:
        logging.info("[sell_entry.py] 현재 보유 코인이 없어 매도 로직을 종료합니다.")
        # 만약 sell_log에 미체결 주문이 남아있다면 clear
        try:
            sell_log_df = pd.read_csv("sell_log.csv")
            if not sell_log_df.empty:
                logging.info("[sell_entry.py] 보유 코인이 없으므로 sell_log.csv를 초기화합니다.")
                # notify_bot_status 알림은 너무 잦을 수 있으므로 여기서는 선택적으로 주석 처리.
                # notify_bot_status("초기화", "보유 코인 없어 sell_log.csv 초기화")
                pd.DataFrame(columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"]).to_csv("sell_log.csv", index=False)
        except FileNotFoundError:
            pass
        return

    try:
        sell_log_df = pd.read_csv("sell_log.csv")
    except FileNotFoundError:
        sell_log_df = pd.DataFrame(columns=["market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"])

    # 매도 로그 상태 업데이트 (체결/취소 알림 발생)
    sell_log_df = update_sell_log_status(sell_log_df)

    # 보유하지 않은 마켓의 sell_log는 정리
    valid_markets = set(holdings.keys())
    initial_sell_log_count = len(sell_log_df)
    sell_log_df = sell_log_df[sell_log_df["market"].isin(valid_markets)].reset_index(drop=True)
    if len(sell_log_df) < initial_sell_log_count:
        logging.info(f"[sell_entry.py] 보유하지 않은 마켓의 매도 주문 {initial_sell_log_count - len(sell_log_df)}건 정리 완료.")
        # notify_bot_status 알림 (옵션)
        # notify_bot_status("정리", f"보유하지 않은 매도 주문 {initial_sell_log_count - len(sell_log_df)}건 정리 완료.")


    updated_sell_log_df = generate_sell_orders(setting_df, holdings, sell_log_df)

    try:
        updated_sell_log_df = execute_sell_orders(updated_sell_log_df)
        updated_sell_log_df.to_csv("sell_log.csv", index=False)
        logging.info("[sell_entry.py] 매도 주문 완료 → sell_log.csv 저장 완료")
    except Exception as e:
        logging.error(f"🚨 매도 주문 실행 중 치명적인 오류 발생: {e}", file=sys.stderr, exc_info=True)
        notify_error("Sell Entry Flow", f"매도 주문 실행 중 치명적인 오류 발생: {e}") # 치명적 오류 알림
        sys.exit(1)

    logging.info("[sell_entry.py] 매도 전략 흐름 종료")