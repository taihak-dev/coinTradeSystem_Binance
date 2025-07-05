# manager/order_executor.py

import pandas as pd
import config
from binance.error import ClientError  # 바이낸스 에러 처리를 위해 import

# --- config 설정에 따라 다른 모듈을 불러오도록 변경 ---
if config.EXCHANGE == 'binance':
    print("[SYSTEM] 바이낸스 모드로 주문 실행기를 설정합니다.")
    from api.binance.order import send_order, cancel_and_new_order
    from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step
    from api.binance.client import get_binance_client
else:
    print("[SYSTEM] 업비트 모드로 주문 실행기를 설정합니다.")
    from api.upbit.order import send_order, cancel_and_new_order
    from utils.price_utils import adjust_price_to_tick

# 이미 거래 환경(레버리지 등)을 설정한 심볼을 추적하기 위한 집합(set)
_configured_symbols = set()


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    """
    매수 주문을 실행합니다.
    바이낸스의 경우, 주문 실행 전 레버리지와 마진 타입을 먼저 설정합니다.
    """
    print("[order_executor.py] 매수 주문 실행 시작")
    all_success = True

    for idx, row in buy_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = str(row["buy_uuid"]) if pd.notna(row["buy_uuid"]) else None

        if filled == "done":
            continue

        market = row["market"]

        # --- 신규: 레버리지 및 마진 타입 설정 로직 ---
        # 프로그램 실행 후 해당 심볼에 대해 한 번만 거래 환경 설정
        if config.EXCHANGE == 'binance' and market not in _configured_symbols:
            try:
                # setting_df에서 현재 market에 맞는 설정값을 찾음
                coin_setting = setting_df[setting_df['market'] == market].iloc[0]
                leverage = int(coin_setting['leverage'])
                margin_type = coin_setting['margin_type'].upper()

                client = get_binance_client()

                print(f"[{market}] 거래 환경 설정 시작 -> 레버리지: {leverage}x, 마진타입: {margin_type}")
                # 1. 마진 타입 설정
                try:
                    client.change_margin_type(symbol=market, marginType=margin_type)
                    print(f"✅ [{market}] 마진 타입을 {margin_type}으로 설정했습니다.")
                except ClientError as e:
                    # 에러코드 -4046: "No need to change margin type" (이미 해당 타입으로 설정됨)
                    if e.error_code == -4046:
                        print(f"ⓘ [{market}] 마진 타입이 이미 {margin_type}입니다.")
                    else:
                        raise e  # 다른 에러는 그대로 발생시킴

                # 2. 레버리지 설정
                try:
                    client.change_leverage(symbol=market, leverage=leverage)
                    print(f"✅ [{market}] 레버리지를 {leverage}x로 설정했습니다.")
                except ClientError as e:
                    # 에러코드 -4028: "Leverage not modified" (이미 해당 레버리지로 설정됨)
                    if e.error_code == -4028:
                        print(f"ⓘ [{market}] 레버리지가 이미 {leverage}x입니다.")
                    else:
                        raise e

                _configured_symbols.add(market)

            except Exception as e:
                print(f"❌ [{market}] 거래 환경 설정 실패: {e}")
                # 설정에 실패하면 해당 주문은 건너뜀
                all_success = False
                continue
        # --- 여기까지 ---

        price = float(row["target_price"])
        amount = float(row["buy_amount"])
        buy_type = row["buy_type"]

        # 거래소별 가격/수량 보정 로직
        if config.EXCHANGE == 'binance':
            price = adjust_price_to_tick(price, symbol=market)
            # 바이낸스는 수량을 코인 기준으로 계산해야 함
            volume = adjust_quantity_to_step(amount / price if price > 0 else 0, symbol=market)
        else:
            price = adjust_price_to_tick(price, market="KRW", ticker=market)
            volume = round(amount / price, 8) if price > 0 else 0

        # case2: 정정 주문
        if filled == "update" and uuid:
            print(f"🔁 정정 매수 주문: {market}, uuid={uuid}, amount={amount}, price={price}")
            try:
                # 바이낸스는 market(symbol) 정보가 추가로 필요함
                response = cancel_and_new_order(
                    prev_order_uuid=uuid, market=market, price=price, amount=volume
                )
                new_uuid = response.get("new_order_uuid", "")
                if new_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = new_uuid
                    buy_log_df.at[idx, "filled"] = "wait"
                else:
                    if response.get("error") == "done_order":
                        buy_log_df.at[idx, "filled"] = "done"
                        print(f"✅ {market} 기존 주문은 이미 체결 완료됨 → filled=done 처리")
                    else:
                        raise ValueError("정정 매수 주문 new_uuid 없음")

            except Exception as e:
                print(f"❌ 정정 매수 주문 실패: {e}")
                all_success = False

        # case3: 신규 주문
        elif filled == "update" and not uuid:
            print(f"🆕 신규 매수 주문: {market}, amount={amount}, price={price}")
            try:
                # 바이낸스는 시장가 매수(initial) 시 amount_krw(USDT)를 전달
                if config.EXCHANGE == 'binance' and buy_type == "initial":
                    response = send_order(market=market, side="bid", ord_type="price", amount_krw=amount)
                else:
                    # 지정가 주문
                    response = send_order(market=market, side="bid", ord_type="limit", unit_price=price, volume=volume)

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


def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    print("[order_executor.py] 매도 주문 실행 시작")
    all_success = True

    for idx, row in sell_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = str(row["sell_uuid"]) if pd.notna(row["sell_uuid"]) else None

        if filled == "done":
            continue

        market = row["market"]
        price = float(row["target_sell_price"])
        volume = float(row["quantity"])

        if config.EXCHANGE == 'binance':
            price = adjust_price_to_tick(price, symbol=market)
            volume = adjust_quantity_to_step(volume, symbol=market)
        else:
            price = adjust_price_to_tick(price, market="KRW", ticker=market)

        if volume <= 0:
            print(f"⚠️ {market} 매도할 수량이 0 → 주문 스킵")
            sell_log_df.at[idx, "filled"] = "done"
            continue

        if filled == "update":
            print(f"🆕 신규/정정 매도 주문: {market}, price={price}, volume={volume}")
            try:
                if config.EXCHANGE == 'binance':
                    if uuid:
                        try:
                            from api.binance.order import cancel_order
                            cancel_order(uuid, market)
                            print(f"🔁 기존 매도 주문({uuid}) 취소 완료")
                        except Exception as e:
                            print(f"⚠️ 기존 매도 주문 취소 실패 (이미 처리되었을 수 있음): {e}")

                response = send_order(market=market, side="ask", ord_type="limit", unit_price=price, volume=volume)

                new_uuid = response.get("uuid", "")
                if new_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = new_uuid
                    sell_log_df.at[idx, "filled"] = "wait"
                else:
                    raise ValueError("신규/정정 매도 주문 uuid 없음")

            except Exception as e:
                print(f"❌ 신규/정정 매도 주문 실패: {e}")
                all_success = False

    print("[order_executor.py] 매도 주문 실행 완료")
    if not all_success:
        raise RuntimeError("일부 매도 주문 실패")
    return sell_log_df