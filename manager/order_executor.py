# manager/order_executor.py

import logging
import pandas as pd
import config
from utils.telegram_notifier import notify_order_event, notify_error
from pybit.exceptions import InvalidRequestError as BybitInvalidRequestError
from binance.error import ClientError as BinanceClientError

# --- 거래소별 함수 임포트 ---
if config.EXCHANGE == 'binance':
    from api.binance.order import send_order, cancel_order
    from api.binance.account import get_accounts
    from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step
elif config.EXCHANGE == 'bybit':
    from api.bybit.order import send_order, cancel_order
    from api.bybit.account import get_accounts
    from utils.bybit_price_utils import adjust_price_to_tick, adjust_quantity_to_step
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_configured_symbols = set()


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("--- 🛒 매수 주문 실행 시작 ---")
    update_orders = buy_log_df[buy_log_df['filled'] == 'update'].copy()

    for idx, row in update_orders.iterrows():
        try:
            market = row['market']
            setting = setting_df[setting_df['market'] == market].iloc[0]
            leverage = int(setting['leverage'])

            if market not in _configured_symbols:
                if config.EXCHANGE == 'bybit':
                    from api.bybit.order import set_leverage
                    set_leverage(market, leverage)
                _configured_symbols.add(market)

            quantity = float(row['buy_amount']) / float(row['target_price'])
            quantity = adjust_quantity_to_step(market, quantity)
            price = adjust_price_to_tick(market, float(row['target_price']))

            if quantity <= 0:
                logging.warning(f"⚠️ [{market}] 계산된 주문 수량이 0 이하이므로 주문을 건너뜁니다.")
                continue

            new_uuid = send_order(
                market=market,
                side="bid",
                price=price,
                quantity=quantity
            )

            buy_log_df.loc[idx, 'buy_uuid'] = new_uuid
            buy_log_df.loc[idx, 'filled'] = 'wait'
            logging.info(f"✅ [{market}] 매수 주문 제출 완료. 새 UUID: {new_uuid}, 상태: 'wait'")

        except Exception as e:
            logging.error(f"❌ [{row['market']}] 매수 주문 실패: {e}", exc_info=True)
            notify_error("execute_buy_orders", f"[{row['market']}] 매수 주문 실패: {e}")
            continue

    return buy_log_df


def execute_sell_orders(sell_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("--- 📈 매도 주문 실행 시작 ---")
    action_orders = sell_log_df[sell_log_df['filled'].isin(['new', 'update'])].copy()

    for idx, row in action_orders.iterrows():
        try:
            market = row['market']
            old_uuid = row.get('sell_uuid')

            if pd.notna(old_uuid) and old_uuid != "new":
                try:
                    cancel_order(market, old_uuid)
                except (BybitInvalidRequestError, BinanceClientError) as e:
                    if isinstance(e, BinanceClientError) and e.error_code == -2011:
                        logging.warning(f"⚠️ [{market}] 이전 매도 주문 취소 불필요 (주문 ID: {old_uuid}, 이미 처리된 주문).")
                    else:
                        logging.warning(f"⚠️ [{market}] 이전 매도 주문 취소 실패 (이미 체결/취소되었을 수 있음): {e}")

            price = adjust_price_to_tick(market, float(row['target_sell_price']))
            quantity = adjust_quantity_to_step(market, float(row['quantity']))

            if quantity <= 0:
                logging.warning(f"⚠️ [{market}] 매도 수량이 0 이하이므로 주문을 제출하지 않습니다.")
                continue

            new_uuid = send_order(
                market=market,
                side="ask",
                price=price,
                quantity=quantity
            )

            sell_log_df.loc[idx, 'sell_uuid'] = new_uuid
            sell_log_df.loc[idx, 'filled'] = 'wait'
            logging.info(f"✅ [{market}] 매도 주문 제출/수정 완료. 새 UUID: {new_uuid}, 상태: 'wait'")

        except Exception as e:
            logging.error(f"❌ [{row['market']}] 매도 주문 실패: {e}", exc_info=True)
            notify_error("Execute Sell Order", f"[{row['market']}] 매도 주문 실패: {e}")
            continue

    return sell_log_df

# --- 👇👇👇 모든 포지션 청산 함수 추가 👇👇👇 ---
def close_all_positions():
    """
    현재 보유 중인 모든 포지션을 시장가로 청산합니다.
    """
    logging.warning("🚨 모든 포지션에 대한 시장가 청산을 시도합니다.")
    try:
        account_data = get_accounts()
        open_positions = account_data.get("open_positions", [])
        
        if not open_positions:
            logging.info("✅ 청산할 포지션이 없습니다.")
            return

        for pos in open_positions:
            market = pos['symbol']
            quantity = abs(float(pos.get('positionAmt', 0)))
            
            if quantity > 0:
                try:
                    logging.info(f"  - [{market}] 포지션 청산 시도. 수량: {quantity}")
                    # 시장가 청산을 위해 price=0 또는 None으로 설정
                    send_order(market=market, side="ask", price=0, quantity=quantity)
                    notify_order_event("제출", market, {"type": "시장가 청산", "quantity": quantity})
                except Exception as e:
                    logging.error(f"❌ [{market}] 포지션 청산 주문 실패: {e}")
                    notify_error("Close Position", f"[{market}] 포지션 청산 실패: {e}")
        
        logging.info("✅ 모든 포지션에 대한 청산 주문 제출 완료.")

    except Exception as e:
        logging.error(f"❌ 포지션 정보 조회 또는 청산 중 심각한 오류 발생: {e}", exc_info=True)
        notify_error("Close All Positions", f"전체 포지션 청산 실패: {e}")
# --- 👆👆👆 추가 완료 --- 👆👆👆