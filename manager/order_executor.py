# manager/order_executor.py

import pandas as pd
import time
import config
import logging
from binance.error import ClientError
from api.binance.client import get_binance_client
from api.binance.order import send_order, cancel_order
from utils.telegram_notifier import notify_order_event, notify_error
from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
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

            # ✅✅✅ 최종 수정: 모든 주문에 대해 quantity를 계산합니다 ✅✅✅
            volume_to_order = buy_amount_usdt / price if price > 0 else 0

            if buy_type == 'initial':
                # 'initial' 주문은 시장가(market)로, 계산된 수량(volume)을 전달합니다.
                response = send_order(
                    market=market,
                    side="bid",
                    type="market",
                    volume=volume_to_order,
                    position_side="LONG"
                )
            else:  # 'small_flow', 'large_flow' 등
                # 그 외 주문은 지정가(limit)로, 수량(volume)과 가격(price)을 전달합니다.
                response = send_order(
                    market=market,
                    side="bid",
                    type="limit",
                    price=price,
                    volume=volume_to_order,
                    position_side="LONG"
                )

            new_order_uuid = response.get("orderId", "")
            if new_order_uuid:
                buy_log_df.at[idx, "buy_uuid"] = new_order_uuid
                buy_log_df.at[idx, "filled"] = "wait"
                logging.info(f"✅ [{market}] 매수 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
            else:
                if isinstance(response, dict) and response.get("error"):
                    logging.warning(f"⚠️ [{market}] 주문이 제출되지 않았습니다: {response.get('error')}")
                else:
                    raise ValueError(f"매수 주문 후 ID를 얻지 못했습니다. 응답: {response}")

        except Exception as e:
            logging.error(f"❌ [{market}] 매수 주문 실패: {e}", exc_info=True)
            all_success = False
            continue

    logging.info("--- 🛒 매수 주문 실행 완료 ---")
    if not all_success:
        raise RuntimeError("일부 매수 주문 실행에 실패했습니다. 로그를 확인하세요.")
    return buy_log_df


def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("--- 💲 지정가 매도 주문 실행 시작 (선주문 방식) ---")
    all_success = True
    orders_to_process = sell_log_df[sell_log_df['filled'] == 'update'].copy()
    if orders_to_process.empty:
        logging.info("실행할 신규/정정 매도 주문이 없습니다.")
        return sell_log_df
    for idx, row in orders_to_process.iterrows():
        market = row["market"]
        price = float(row["target_sell_price"])
        volume_to_order = float(row["quantity"])
        if config.EXCHANGE == 'binance':
            price = adjust_price_to_tick(market, price)
            volume_to_order = adjust_quantity_to_step(market, volume_to_order)
        if volume_to_order <= 0:
            logging.warning(f"⚠️ [{market}] 매도할 수량이 0 이하이므로 주문을 건너뜁니다.")
            sell_log_df.at[idx, "filled"] = "done"
            continue
        try:
            try:
                client = get_binance_client()
                client.cancel_open_orders(symbol=market)
                logging.info(f"🧹 [{market}] 모든 미체결 주문을 취소했습니다. (새 주문 준비)")
                time.sleep(0.2)
            except ClientError as e:
                if e.error_code == -2011:
                    logging.info(f"ⓘ [{market}] 취소할 미체결 주문이 없습니다.")
                else:
                    raise e
            logging.info(f"🆕 [{market}] 신규/정정 매도 주문 시도 (가격: {price}, 수량: {volume_to_order})")
            response = send_order(market=market, side="ask", type="limit", price=price, volume=volume_to_order,
                                  position_side="LONG")
            new_order_uuid = response.get("orderId", "")
            if new_order_uuid:
                sell_log_df.at[idx, "sell_uuid"] = new_order_uuid
                sell_log_df.at[idx, "filled"] = "wait"
                logging.info(f"✅ [{market}] 매도 주문 제출 완료. 새 UUID: {new_order_uuid}, 상태: 'wait'")
            else:
                raise ValueError(f"매도 주문 후 UUID를 얻지 못했습니다. 응답: {response}")
        except Exception as e:
            logging.error(f"❌ [{market}] 매도 주문 실패: {e}", exc_info=True)
            all_success = False
            continue
    logging.info("--- 💲 지정가 매도 주문 실행 완료 ---")
    if not all_success:
        raise RuntimeError("일부 매도 주문 실행에 실패했습니다.")
    return sell_log_df