# manager/order_executor.py

import pandas as pd
import time
import config
import logging
from binance.error import ClientError  # ClientError 임포트
from api.binance.client import get_binance_client
from api.binance.order import send_order, cancel_order
from utils.telegram_notifier import notify_order_event, notify_error
from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    """
    매수 주문(신규/정정)을 실행합니다.
    (이 함수는 기존 로직을 유지합니다.)
    """
    logging.info("--- 🛒 매수 주문 실행 시작 ---")
    _configured_symbols = set()
    all_success = True

    orders_to_process = buy_log_df[buy_log_df['filled'] == 'update'].copy()
    if orders_to_process.empty:
        logging.info("실행할 신규/정정 매수 주문이 없습니다.")
        return buy_log_df

    for idx, row in orders_to_process.iterrows():
        market = row["market"]
        buy_type = row["buy_type"]
        price = float(row["target_price"])
        buy_amount_usdt = float(row["buy_amount"])
        uuid = str(row["buy_uuid"]) if pd.notna(row["buy_uuid"]) and row["buy_uuid"] else None

        try:
            if config.EXCHANGE == 'binance' and market not in _configured_symbols:
                logging.info(f"⚙️ [{market}] 거래 환경 설정 시작")
                settings = setting_df[setting_df['market'] == market].iloc[0]
                leverage = int(settings['leverage'])
                margin_type = settings.get('margin_type', 'CROSSED').upper()

                client = get_binance_client()

                try:
                    client.change_margin_type(symbol=market, marginType=margin_type)
                    logging.info(f"✅ [{market}] 마진 타입을 {margin_type}으로 설정했습니다.")
                except ClientError as e:
                    if e.error_code == -4046:
                        logging.info(f"ⓘ [{market}] 마진 타입이 이미 {margin_type}입니다. 변경 불필요.")
                    else:
                        raise e

                client.change_leverage(symbol=market, leverage=leverage)
                logging.info(f"✅ [{market}] 레버리지를 {leverage}x로 설정했습니다.")
                _configured_symbols.add(market)
                logging.info(f"⚙️ [{market}] 거래 환경 설정 완료.")

            order_type_map = {'initial': 'market', 'small_flow': 'limit', 'large_flow': 'limit'}
            order_type = order_type_map.get(buy_type)

            volume_to_order = buy_amount_usdt / price if price > 0 else 0

            if order_type == 'market':
                response = send_order(market=market, side="bid", type="market", amount_usdt=buy_amount_usdt,
                                      position_side="LONG")
            else:  # limit
                response = send_order(market=market, side="bid", type="limit", price=price, volume=volume_to_order,
                                      position_side="LONG")

            new_order_uuid = response.get("orderId", "")

            if new_order_uuid:
                buy_log_df.at[idx, "buy_uuid"] = new_order_uuid
                buy_log_df.at[idx, "filled"] = "wait"
                logging.info(f"✅ [{market}] 매수 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
            else:
                raise ValueError(f"매수 주문 후 UUID를 얻지 못했습니다. 응답: {response}")

        except Exception as e:
            logging.error(f"❌ [{market}] 매수 주문 실패: {e}", exc_info=True)
            all_success = False
            continue

    logging.info("--- 🛒 매수 주문 실행 완료 ---")
    if not all_success:
        raise RuntimeError("일부 매수 주문 실행에 실패했습니다. 로그를 확인하세요.")
    return buy_log_df


# 💡💡💡 --- 이 함수가 새롭게 수정된 부분입니다 --- 💡💡💡
def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    매도 주문(신규/정정)을 실행합니다.
    'update' 상태의 주문을 받아, 기존 주문을 모두 취소한 뒤 새로운 지정가 주문을 제출합니다.
    """
    logging.info("--- 💲 지정가 매도 주문 실행 시작 (선주문 방식) ---")
    all_success = True

    # 'update' 상태인 주문(신규 또는 수정이 필요한 주문)만 처리 대상으로 필터링합니다.
    orders_to_process = sell_log_df[sell_log_df['filled'] == 'update'].copy()

    if orders_to_process.empty:
        logging.info("실행할 신규/정정 매도 주문이 없습니다.")
        return sell_log_df

    for idx, row in orders_to_process.iterrows():
        market = row["market"]
        price = float(row["target_sell_price"])
        volume_to_order = float(row["quantity"])

        # 1. 거래소 규칙에 맞게 가격 및 수량 보정
        if config.EXCHANGE == 'binance':
            price = adjust_price_to_tick(market, price)
            volume_to_order = adjust_quantity_to_step(market, volume_to_order)

        # 보정 후 수량이 0 이하면 주문할 필요가 없으므로 건너뜁니다.
        if volume_to_order <= 0:
            logging.warning(f"⚠️ [{market}] 매도할 수량이 0 이하이므로 주문을 건너뜁니다.")
            sell_log_df.at[idx, "filled"] = "done"  # 더 이상 처리할 필요 없으므로 'done'으로 변경
            continue

        try:
            # 2. (핵심) "선(先)취소": 새로운 주문을 내기 전에, 해당 코인의 모든 미체결 주문을 취소하여 깨끗한 상태로 만듭니다.
            # 이것이 "유령 주문"과의 충돌을 막는 가장 안전한 방법입니다.
            try:
                client = get_binance_client()
                client.cancel_open_orders(symbol=market)
                logging.info(f"🧹 [{market}] 모든 미체결 주문을 취소했습니다. (새 주문 준비)")
                time.sleep(0.2)  # API가 취소를 처리할 시간을 줍니다.
            except ClientError as e:
                # 취소할 주문이 없는 경우(-2011)는 정상적인 상황이므로 무시하고 계속 진행합니다.
                if e.error_code == -2011:
                    logging.info(f"ⓘ [{market}] 취소할 미체결 주문이 없습니다.")
                else:
                    # 그 외 다른 에러는 심각한 문제일 수 있으므로 예외를 발생시킵니다.
                    raise e

            # 3. "후(後)주문": 새로운 지정가 매도 주문을 제출합니다.
            logging.info(f"🆕 [{market}] 신규/정정 매도 주문 시도 (가격: {price}, 수량: {volume_to_order})")
            response = send_order(market=market, side="ask", type="limit", price=price, volume=volume_to_order,
                                  position_side="LONG")

            # 4. 주문 제출 결과(새로운 주문 ID 등)를 DataFrame에 반영합니다.
            new_order_uuid = response.get("orderId", "")

            if new_order_uuid:
                # 원본 sell_log_df의 해당 행(idx)에 직접 업데이트합니다.
                sell_log_df.at[idx, "sell_uuid"] = new_order_uuid
                sell_log_df.at[idx, "filled"] = "wait"  # 주문 제출 후 '체결 대기' 상태로 변경
                logging.info(f"✅ [{market}] 매도 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
            else:
                raise ValueError(f"매도 주문 후 UUID를 얻지 못했습니다. 응답: {response}")

        except Exception as e:
            logging.error(f"❌ [{market}] 매도 주문 실패: {e}", exc_info=True)
            all_success = False
            continue  # 한 주문이 실패해도 다른 주문은 계속 시도

    logging.info("--- 💲 지정가 매도 주문 실행 완료 ---")
    if not all_success:
        raise RuntimeError("일부 매도 주문 실행에 실패했습니다. 로그를 확인하세요.")

    return sell_log_df