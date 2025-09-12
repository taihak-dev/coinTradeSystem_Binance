# manager/order_executor.py

import logging

import pandas as pd
from binance.error import ClientError  # 바이낸스 전용 오류는 그대로 유지

import config
from utils.telegram_notifier import notify_order_event, notify_error

# --- 거래소 선택 로직 (기존과 동일) ---
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] Order Executor: 바이낸스 모드로 설정합니다.")
    from api.binance.order import send_order, cancel_order
    from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step
elif config.EXCHANGE == 'bybit':
    logging.info("[SYSTEM] Order Executor: 바이빗 모드로 설정합니다.")
    from api.bybit.order import send_order, cancel_order
    from utils.bybit_price_utils import adjust_price_to_tick, adjust_quantity_to_step
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("--- 🛒 매수 주문 실행 시작 ---")
    all_success = True

    orders_to_process = buy_log_df[buy_log_df['filled'] == 'update'].copy()
    if orders_to_process.empty:
        logging.info("실행할 신규/정정 매수 주문이 없습니다.")
        return buy_log_df

    for idx, row in orders_to_process.iterrows():
        market = row["market"]
        price = float(row["target_price"])
        buy_amount_usdt = float(row["buy_amount"])
        old_uuid = row.get("buy_uuid")

        try:
            # --- 👇👇👇 여기가 수정된 부분입니다 👇👇👇 ---
            # 1. 기존 주문 취소 (old_uuid가 유효한 문자열일 경우에만 실행)
            # pd.notna()로 NaN이 아니고, str 타입이며, 내용이 비어있지 않고, "new"가 아닌지 확인
            if pd.notna(old_uuid) and isinstance(old_uuid, str) and old_uuid and old_uuid != "new":
                # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆
                logging.info(f"🔄 [{market}] 기존 매수 주문(UUID: {old_uuid}) 취소를 시도합니다.")
                try:
                    cancel_order(market=market, order_uuid=str(old_uuid))
                except Exception as cancel_e:
                    logging.warning(f"⚠️ 기존 주문 취소 중 오류 발생 (무시하고 계속): {cancel_e}")

            # 2. 신규 주문 제출
            adjusted_price = adjust_price_to_tick(market, price)

            if adjusted_price > 0:
                quantity_to_buy = buy_amount_usdt / adjusted_price
                adjusted_quantity = adjust_quantity_to_step(market, quantity_to_buy)
            else:
                raise ValueError("주문 가격이 0보다 커야 합니다.")

            if adjusted_quantity <= 0:
                logging.warning(f"⚠️ [{market}] 주문 수량이 0 이하로 조정되어 주문을 제출하지 않습니다. (계산된 수량: {quantity_to_buy})")
                buy_log_df.at[idx, "filled"] = "error"
                buy_log_df.at[idx, "buy_uuid"] = "ADJUSTED_TO_ZERO"
                continue

            logging.info(f"➡️ [{market}] 신규 매수 주문 제출: 가격={adjusted_price}, 수량={adjusted_quantity}")

            response = send_order(
                market=market,
                side='buy',
                volume=adjusted_quantity,
                price=adjusted_price
            )

            # 3. 주문 제출 결과 반영
            new_order_uuid = response.get("orderId") or response.get("uuid")
            if new_order_uuid:
                buy_log_df.at[idx, "buy_uuid"] = new_order_uuid
                buy_log_df.at[idx, "filled"] = "wait"
                logging.info(f"✅ [{market}] 매수 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
                notify_order_event(
                    "제출", market,
                    {"type": "limit_buy", "price": adjusted_price, "quantity": adjusted_quantity, "leverage": "-"}
                )
            else:
                raise ValueError(f"매수 주문 후 UUID를 얻지 못했습니다. 응답: {response}")

        except Exception as e:
            logging.error(f"❌ [{market}] 매수 주문 실패: {e}", exc_info=True)
            notify_error("execute_buy_orders", f"{market} 매수 주문 실패: {e}")
            buy_log_df.at[idx, "filled"] = "error"
            all_success = False
            continue

    logging.info(f"--- 🛒 매수 주문 실행 종료 (성공여부: {all_success}) ---")
    return buy_log_df


def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("--- 💸 매도 주문 실행 시작 ---")
    all_success = True

    orders_to_process = sell_log_df[sell_log_df['filled'].isin(['update', 'new'])].copy()
    if orders_to_process.empty:
        logging.info("실행할 신규/정정 매도 주문이 없습니다.")
        return sell_log_df

    for idx, row in orders_to_process.iterrows():
        market = row["market"]
        price = float(row["target_sell_price"])
        volume_to_order = float(row["quantity"])
        old_uuid = row.get("sell_uuid")

        try:
            # 1. 기존 주문 취소 (buy_orders와 동일한 로직 적용)
            if pd.notna(old_uuid) and isinstance(old_uuid, str) and old_uuid and old_uuid != "new":
                logging.info(f"🔄 [{market}] 기존 매도 주문(UUID: {old_uuid}) 취소를 시도합니다.")
                try:
                    cancel_order(market=market, order_uuid=str(old_uuid))
                except Exception as cancel_e:
                    logging.warning(f"⚠️ 기존 주문 취소 중 오류 발생 (무시하고 계속): {cancel_e}")

            # 2. 신규 주문 제출
            adjusted_price = adjust_price_to_tick(market, price)
            adjusted_quantity = adjust_quantity_to_step(market, volume_to_order)

            if adjusted_quantity <= 0:
                logging.warning(f"⚠️ [{market}] 매도 주문 수량이 0 이하로 조정되어 주문을 제출하지 않습니다.")
                sell_log_df.at[idx, "filled"] = "error"
                sell_log_df.at[idx, "sell_uuid"] = "ADJUSTED_TO_ZERO"
                continue

            logging.info(f"➡️ [{market}] 신규 매도 주문 제출: 가격={adjusted_price}, 수량={adjusted_quantity}")

            response = send_order(
                market=market,
                side='sell',
                price=adjusted_price,
                volume=adjusted_quantity
            )

            # 3. 주문 제출 결과 반영
            new_order_uuid = response.get("orderId") or response.get("uuid")
            if new_order_uuid:
                sell_log_df.at[idx, "sell_uuid"] = new_order_uuid
                sell_log_df.at[idx, "filled"] = "wait"
                logging.info(f"✅ [{market}] 매도 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
                notify_order_event(
                    "제출", market,
                    {"type": "limit_sell", "price": adjusted_price, "quantity": adjusted_quantity, "leverage": "-"}
                )
            else:
                raise ValueError(f"매도 주문 후 UUID를 얻지 못했습니다. 응답: {response}")

        except ClientError as e:
            if e.error_code == -2022:
                logging.warning(f"⚠️ [{market}] 매도 주문이 거절되었습니다(코드: -2022). 이미 포지션이 종료된 것으로 보입니다. 상태를 'done'으로 처리합니다.")
                sell_log_df.at[idx, "filled"] = "done"
            else:
                logging.error(f"❌ [{market}] 매도 주문 실패 (ClientError): {e}", exc_info=True)
                all_success = False
                continue

        except Exception as e:
            logging.error(f"❌ [{market}] 매도 주문 중 알 수 없는 오류 발생: {e}", exc_info=True)
            notify_error("execute_sell_orders", f"{market} 매도 주문 실패: {e}")
            sell_log_df.at[idx, "filled"] = "error"
            all_success = False
            continue

    logging.info(f"--- 💸 매도 주문 실행 종료 (성공여부: {all_success}) ---")
    return sell_log_df