# strategy/casino_strategy.py

import pandas as pd
from datetime import datetime
import logging
import numpy as np

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_last_filled_price(buy_log_df: pd.DataFrame, market: str) -> float:
    """
    해당 마켓의 매수 로그에서 가장 최근에 'done'(체결 완료)된 주문의 'target_price'를 반환합니다.
    이는 다음 매수 단계의 기준 가격이 됩니다.
    """
    # 해당 마켓의 체결 완료된 주문들만 필터링
    filled_orders = buy_log_df[(buy_log_df['market'] == market) & (buy_log_df['filled'] == 'done')]
    if not filled_orders.empty:
        # 가장 최근 주문의 target_price 반환
        last_price = float(filled_orders.iloc[-1]['target_price'])
        logging.debug(f"🔍 {market}의 마지막 체결 가격: {last_price}")
        return last_price
    logging.debug(f"ℹ️ {market}에 체결된 이전 매수 주문이 없습니다.")
    return None


def generate_buy_orders(setting_df: pd.DataFrame, buy_log_df: pd.DataFrame, current_prices: dict) -> pd.DataFrame:
    """
    카지노 매매 전략에 따라 현재 상황을 판단하고,
    각 상황에 따른 매수 주문 내역을 buy_log DataFrame 형태로 생성/수정하여 반환합니다.

    :param setting_df: 각 마켓의 전략 설정 (unit_size, small_flow_pct 등)
    :param buy_log_df: 현재까지의 매수 주문 로그 DataFrame
    :param current_prices: 각 마켓의 현재 가격 정보 {market: price}
    :return: 업데이트된 매수 주문 로그 DataFrame
    """
    logging.info("--- ⚙️ 매수 주문 생성 로직 시작 (generate_buy_orders) ---")
    new_orders_to_add = [] # 새로 추가될 주문 목록

    for _, setting in setting_df.iterrows():
        market = setting['market']
        unit_size = float(setting['unit_size']) # 레버리지 적용된 단위 투자금
        small_flow_pct = float(setting['small_flow_pct'])
        small_flow_units = int(setting['small_flow_units'])
        large_flow_pct = float(setting['large_flow_pct'])
        large_flow_units = int(setting['large_flow_units'])

        current_price = current_prices.get(market)
        if current_price is None:
            logging.warning(f"❌ {market}의 현재 가격을 알 수 없어 매수 주문을 생성할 수 없습니다.")
            continue

        market_buy_log = buy_log_df[buy_log_df['market'] == market].copy()
        initial_order_in_log = market_buy_log[market_buy_log['buy_type'] == 'initial']

        # --- 상황 1: 해당 마켓에 대한 최초(initial) 주문이 없는 경우 ---
        if initial_order_in_log.empty:
            logging.info(f"📌 {market}: 최초 주문 생성 시나리오 진입 (Initial Order Missing).")

            # 1. Initial (최초) 매수 주문 생성
            # 현재 가격을 목표가로 하여 시장가 매수될 예정
            new_orders_to_add.append({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "market": market,
                "target_price": current_price, # 현재 가격을 목표 가격으로 설정
                "buy_amount": unit_size, # initial 투자금
                "buy_units": 0, # initial 주문은 0단계
                "buy_type": "initial",
                "buy_uuid": "", # 주문 전이므로 UUID 없음
                "filled": "update" # 새로 생성된 주문이므로 'update' 상태
            })
            logging.info(f"  -> {market} initial 매수 주문 추가: 가격={current_price:.8f}, 금액={unit_size:.2f}")

            # Initial 주문이 없으면 flow 주문도 없으므로 여기서 바로 다음 코인으로 넘어감
            # (Initial 주문이 먼저 체결되어야 다음 flow 주문이 가능)
            continue

        # --- 상황 2: Initial 주문이 존재하고, 다음 Flow 주문들을 관리하는 경우 ---
        # last_filled_price는 가장 최근에 체결된 매수 주문의 가격 (Initial 또는 Flow)
        last_filled_price = get_last_filled_price(market_buy_log, market)
        if last_filled_price is None:
            logging.warning(f"⚠️ {market}: Initial 주문은 있지만 아직 체결된 매수 주문이 없어 다음 flow 주문을 생성할 수 없습니다. (last_filled_price 없음)")
            continue

        logging.info(f"📌 {market}: Flow 주문 관리 시나리오 진입. (최근 체결가: {last_filled_price:.8f})")

        # Small Flow (소액 분할 매수) 주문 생성/관리
        for i in range(1, small_flow_units + 1):
            target_price = round(last_filled_price * (1 - small_flow_pct * i), 8) # 설정된 비율만큼 하락한 목표 가격
            buy_amount = unit_size # Small flow 투자 금액

            # 현재 가격이 매수 목표가보다 낮거나 같고 (매수 조건 충족)
            if current_price <= target_price:
                # 이미 해당 단계의 미체결(wait, update) flow 주문이 있는지 확인하여 중복 생성 방지
                existing_flow_order = market_buy_log[
                    (market_buy_log['buy_type'] == 'small_flow') &
                    (market_buy_log['buy_units'] == i) &
                    (market_buy_log['filled'].isin(['update', 'wait']))
                ]
                if not existing_flow_order.empty:
                    logging.debug(f"  -> {market} small_flow {i}단계: 이미 대기 중인 주문이 있어 건너뜁니다. (가격: {target_price:.8f})")
                    continue # 이미 주문이 있으므로 건너뜀

                # 새로운 small_flow 주문 추가
                new_orders_to_add.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market": market,
                    "target_price": target_price,
                    "buy_amount": buy_amount,
                    "buy_units": i,
                    "buy_type": "small_flow",
                    "buy_uuid": "",
                    "filled": "update"
                })
                logging.info(f"  -> {market} small_flow {i}단계 추가: 목표가={target_price:.8f}, 금액={buy_amount:.2f}")
            else:
                logging.debug(f"  -> {market} small_flow {i}단계: 매수 조건 미달. (현재가:{current_price:.8f} > 목표가:{target_price:.8f})")

        # Large Flow (대액 분할 매수) 주문 생성/관리
        for i in range(1, large_flow_units + 1):
            target_price = round(last_filled_price * (1 - large_flow_pct * i), 8) # 설정된 비율만큼 하락한 목표 가격
            buy_amount = unit_size * (large_flow_units / large_flow_units) # Large flow 투자 금액 (예: unit_size * 단계별 배율)
                                                                       # 현재는 단순히 unit_size만 곱하므로, 설정에 따라 조절 필요
                                                                       # 예시: unit_size * i 로 각 단계마다 투자금 증가시키려면
                                                                       # buy_amount = unit_size * i (setting.csv의 large_flow_units와 관계없이)
            # 여기서는 setting.csv의 large_flow_units를 단순히 '단계 수'로만 사용하고,
            # 각 단계별 투자 금액은 setting.csv의 'large_flow_units'에 명시된 단위와 일치하게
            # 즉, large_flow_units가 3이면 3단계 모두 unit_size를 따르도록 하거나, 총 금액을 나누는 방식 등 전략 명확화 필요
            # 현재는 단순히 unit_size * (large_flow_units / large_flow_units) = unit_size
            # 이 부분은 전략에 따라 적절한 'buy_amount' 계산 로직으로 변경 필요
            buy_amount = unit_size * large_flow_units # 예시: large_flow 총 금액을 한 번에 매수 (아니면 단위별로?)
                                                        # 기존 코드는 'unit_size * large_flow_units'를 매번 추가했음.
                                                        # 변경된 코드에서는 각 단계마다 buy_amount = unit_size로 설정하는 것이 더 일관적.
                                                        # -> setting.csv의 unit_size를 각 단계의 투자금으로 본다면
                                                        # buy_amount = unit_size 로 변경하는 것이 맞음.
            buy_amount = unit_size # 현재 코드는 unit_size를 그대로 사용

            if current_price <= target_price:
                existing_flow_order = market_buy_log[
                    (market_buy_log['buy_type'] == 'large_flow') &
                    (market_buy_log['buy_units'] == i) &
                    (market_buy_log['filled'].isin(['update', 'wait']))
                ]
                if not existing_flow_order.empty:
                    logging.debug(f"  -> {market} large_flow {i}단계: 이미 대기 중인 주문이 있어 건너뜁니다. (가격: {target_price:.8f})")
                    continue

                new_orders_to_add.append({
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "market": market,
                    "target_price": target_price,
                    "buy_amount": buy_amount,
                    "buy_units": i,
                    "buy_type": "large_flow",
                    "buy_uuid": "",
                    "filled": "update"
                })
                logging.info(f"  -> {market} large_flow {i}단계 추가: 목표가={target_price:.8f}, 금액={buy_amount:.2f}")
            else:
                logging.debug(f"  -> {market} large_flow {i}단계: 매수 조건 미달. (현재가:{current_price:.8f} > 목표가:{target_price:.8f})")


    # 새로운 주문이 있다면 기존 로그와 결합
    if new_orders_to_add:
        new_df = pd.DataFrame(new_orders_to_add)
        buy_log_df = pd.concat([buy_log_df, new_df], ignore_index=True)
        logging.info(f"✅ 총 {len(new_orders_to_add)}개의 새로운 매수 주문이 buy_log_df에 추가되었습니다.")
    else:
        logging.info("ℹ️ 현재 시점에서 추가할 새로운 매수 주문이 없습니다.")

    logging.info("--- ⚙️ 매수 주문 생성 로직 완료 ---")
    return buy_log_df


def generate_sell_orders(setting_df: pd.DataFrame, holdings: dict, sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    보유 포지션 및 카지노 매매 전략에 따라 매도 주문 내역을
    sell_log DataFrame 형태로 생성/수정하여 반환합니다.

    :param setting_df: 각 마켓의 전략 설정 (take_profit_pct 등)
    :param holdings: 현재 보유 중인 자산 정보 {market: {"balance": float, "avg_price": float}}
    :param sell_log_df: 현재까지의 매도 주문 로그 DataFrame
    :return: 업데이트된 매도 주문 로그 DataFrame
    """
    logging.info("--- ⚙️ 매도 주문 생성 로직 시작 (generate_sell_orders) ---")
    updated_df = sell_log_df.copy() # 원본 DataFrame을 변경하지 않기 위해 복사

    for market, info in holdings.items():
        # 매도 대상 코인이지만 보유 수량이 0 이하면 건너김
        if info['balance'] <= 0:
            logging.debug(f"ℹ️ {market}: 보유 수량이 0이므로 매도 주문을 생성하지 않습니다.")
            continue

        # 해당 마켓의 전략 설정 가져오기
        setting = setting_df[setting_df['market'] == market]
        if setting.empty:
            logging.warning(f"⚠️ {market}: setting.csv에 대한 전략 설정이 없어 매도 주문을 생성할 수 없습니다.")
            continue
        setting = setting.iloc[0] # 첫 번째 (유일한) 설정값 가져오기

        avg_buy_price = info['avg_price'] # 평균 매수 가격
        quantity_to_sell = info['balance'] # 매도할 수량 (현재 보유량)
        take_profit_pct = float(setting['take_profit_pct']) # 익절 목표 수익률

        # 목표 매도 가격 계산: 평균 매수 가격 + 익절률
        target_price = round(avg_buy_price * (1 + take_profit_pct), 8) # 소수점 8자리까지 정밀도 유지

        # 기존 sell_log에서 해당 market에 대한 데이터가 있는지 확인
        existing_sell = updated_df[updated_df['market'] == market]

        if not existing_sell.empty:
            # 기존 매도 주문이 존재하는 경우 업데이트 여부 확인
            existing_row_idx = existing_sell.index[0]
            existing_avg_buy_price = round(float(updated_df.at[existing_row_idx, 'avg_buy_price']), 8)
            existing_quantity = round(float(updated_df.at[existing_row_idx, 'quantity']), 8)
            existing_target_sell_price = round(float(updated_df.at[existing_row_idx, 'target_sell_price']), 8)
            avg_price_is_close = np.isclose(existing_avg_buy_price, avg_buy_price, atol=1e-9)
            quantity_is_close = np.isclose(existing_quantity, quantity_to_sell, atol=1e-9)
            target_price_is_close = np.isclose(existing_target_sell_price, target_price, atol=1e-9)

            is_same = avg_price_is_close and quantity_is_close and target_price_is_close
            # 💡💡💡 --- 여기까지 수정입니다 --- 💡💡💡

            if is_same:
                logging.debug(f"✅ {market}: 보유 정보와 매도 주문 정보가 동일 → 기존 주문 유지.")
                # filled 상태가 "wait"인 경우 그대로 유지. "done"이면 이미 정리되었을 것.
                continue # 변경 사항이 없으므로 다음 코인으로 넘어감

            # 기존 정보와 다를 경우 업데이트
            logging.info(f"✏️ {market}: 기존 매도 주문과 보유 정보가 다름 → 매도 주문 수정 (update).")
            updated_df.at[existing_row_idx, 'avg_buy_price'] = avg_buy_price
            updated_df.at[existing_row_idx, 'quantity'] = quantity_to_sell
            updated_df.at[existing_row_idx, 'target_sell_price'] = target_price
            updated_df.at[existing_row_idx, 'filled'] = "update" # 'update' 상태로 변경하여 order_executor에서 처리하도록 지시

        else:
            # 새로운 매도 주문 생성
            logging.info(f"🆕 {market}: 새로운 매도 주문 생성.")
            new_row = {
                "market": market,
                "avg_buy_price": avg_buy_price,
                "quantity": quantity_to_sell,
                "target_sell_price": target_price,
                "sell_uuid": "", # 주문 전이므로 UUID 없음
                "filled": "update" # 새로 생성된 주문이므로 'update' 상태
            }
            updated_df.loc[len(updated_df)] = new_row

    logging.info("--- ⚙️ 매도 주문 생성 로직 완료 ---")
    return updated_df