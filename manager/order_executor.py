# manager/order_executor.py

import pandas as pd
import config
from binance.error import ClientError
import logging
import time  # time 모듈 임포트 (딜레이를 위해 필요)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# config 설정에 따라 다른 모듈을 불러오도록 변경
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] 바이낸스 모드로 주문 실행기를 설정합니다.")
    from api.binance.order import send_order, cancel_order, cancel_and_new_order_binance
    from utils.binance_price_utils import adjust_price_to_tick, adjust_quantity_to_step
    from api.binance.client import get_binance_client
else:
    logging.info("[SYSTEM] 업비트 모드로 주문 실행기를 설정합니다.")
    from api.upbit.order import send_order, cancel_and_new_order
    from utils.price_utils import adjust_price_to_tick  # 업비트 전용 가격 조정 유틸리티

# 이미 거래 환경(레버리지, 마진 타입)을 설정한 심볼을 추적하기 위한 집합(set)
_configured_symbols = set()


def execute_buy_orders(buy_log_df: pd.DataFrame, setting_df: pd.DataFrame) -> pd.DataFrame:
    """
    매수 주문을 실행합니다.
    바이낸스의 경우, 주문 실행 전 해당 심볼의 레버리지와 마진 타입을 먼저 설정합니다.

    :param buy_log_df: 매수 주문 정보가 담긴 DataFrame (filled='update' 대상)
    :param setting_df: 각 마켓의 설정 정보 (레버리지, 마진 타입 포함)
    :return: 업데이트된 buy_log_df
    """
    logging.info("--- 🛒 매수 주문 실행 시작 ---")
    all_success = True  # 모든 주문이 성공했는지 추적

    if buy_log_df.empty:
        logging.info("실행할 매수 주문이 없습니다.")
        return buy_log_df

    for idx, row in buy_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = str(row["buy_uuid"]) if pd.notna(row["buy_uuid"]) else None

        # 이미 체결 완료된 주문은 건너김
        if filled == "done":
            logging.debug(f"ℹ️ {row['market']} 주문(id:{uuid})은 이미 체결 완료되어 스킵합니다.")
            continue

        market = row["market"]
        price = float(row["target_price"])
        buy_amount_usdt_or_krw = float(row["buy_amount"])  # 바이낸스 시장가 매수 시 USDT 금액, 업비트 시 KRW 금액

        # --- 바이낸스 전용: 레버리지 및 마진 타입 설정 로직 ---
        # 프로그램 실행 후 해당 심볼에 대해 한 번만 거래 환경 설정
        if config.EXCHANGE == 'binance' and market not in _configured_symbols:
            try:
                # setting_df에서 현재 market에 맞는 설정값을 찾음
                coin_setting = setting_df[setting_df['market'] == market]
                if coin_setting.empty:
                    logging.warning(f"⚠️ setting.csv에 {market}에 대한 설정이 없습니다. 레버리지/마진 설정을 건너뜁니다.")
                    # 설정이 없어도 주문 자체는 시도할 수 있도록 continue
                else:
                    leverage = int(coin_setting.iloc[0]['leverage'])
                    margin_type = str(coin_setting.iloc[0]['margin_type']).upper()

                    client = get_binance_client()  # 인증된 바이낸스 클라이언트 가져오기

                    logging.info(f"⚙️ [{market}] 거래 환경 설정 시작 -> 레버리지: {leverage}x, 마진타입: {margin_type}")
                    # 1. 마진 타입 설정
                    try:
                        client.change_margin_type(symbol=market, marginType=margin_type)
                        logging.info(f"✅ [{market}] 마진 타입을 {margin_type}으로 설정했습니다.")
                    except ClientError as e:
                        if e.error_code == -4046:  # "No need to change margin type" (이미 해당 타입으로 설정됨)
                            logging.info(f"ⓘ [{market}] 마진 타입이 이미 {margin_type}입니다. 변경 불필요.")
                        else:
                            logging.error(f"❌ [{market}] 마진 타입 설정 실패 (ClientError: {e.error_code}): {e.error_message}")
                            raise e  # 다른 에러는 그대로 발생시킴

                    # 2. 레버리지 설정
                    try:
                        client.change_leverage(symbol=market, leverage=leverage)
                        logging.info(f"✅ [{market}] 레버리지를 {leverage}x로 설정했습니다.")
                    except ClientError as e:
                        if e.error_code == -4028:  # "Leverage not modified" (이미 해당 레버리지로 설정됨)
                            logging.info(f"ⓘ [{market}] 레버리지가 이미 {leverage}x입니다. 변경 불필요.")
                        else:
                            logging.error(f"❌ [{market}] 레버리지 설정 실패 (ClientError: {e.error_code}): {e.error_message}")
                            raise e

                    _configured_symbols.add(market)  # 성공적으로 설정된 심볼은 추적
                    logging.info(f"⚙️ [{market}] 거래 환경 설정 완료.")

            except ClientError as e:  # Rate Limit 처리 추가
                if e.error_code == -1003:  # Too much request weight used
                    logging.critical(
                        f"❌ API Rate Limit 초과 (ClientError: {e.error_code}): {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False  # 이번 주문은 실패로 기록
                    continue  # 다음 주문으로 진행
                elif e.status_code == 429:  # Too Many Requests (HTTP status code)
                    logging.critical(f"❌ HTTP 429 Rate Limit 초과: {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                else:  # 다른 ClientError는 그대로 발생시킴
                    logging.error(f"❌ [{market}] 거래 환경 설정 실패 (ClientError: {e.error_code}): {e.error_message}",
                                  exc_info=True)
                    all_success = False
                    continue  # 다음 주문으로 진행
            except Exception as e:
                logging.error(f"❌ [{market}] 거래 환경 설정 중 치명적인 오류 발생: {e}", exc_info=True)
                all_success = False
                continue  # 다음 주문으로 넘어감
        # --- 여기까지 ---

        buy_type = row["buy_type"]  # initial, small_flow, large_flow

        # 바이낸스 시장가 매수(initial)는 amount_usdt (금액)으로, 지정가 매수는 volume (수량)으로 처리
        # 업비트는 시장가 매수 시 KRW 금액 기준으로 수량 계산
        volume_to_order = 0.0  # 실제 주문에 사용될 수량 (코인 개수)

        if config.EXCHANGE == 'binance':
            if buy_type != "initial":  # 지정가 주문
                # 금액 / 가격 = 수량 (buy_amount_usdt_or_krw는 여기서 USDT 금액임)
                volume_to_order = buy_amount_usdt_or_krw / price if price > 0 else 0
                volume_to_order = adjust_quantity_to_step(market, volume_to_order)  # 수량 보정
                price = adjust_price_to_tick(market, price)  # 가격 보정
            # initial (시장가)의 경우 send_order 내부에서 amount_usdt를 통해 수량 계산 및 보정
        else:  # 업비트
            price = adjust_price_to_tick(price, market="KRW", ticker=market)  # 업비트 전용 가격 조정 유틸리티 사용
            # 업비트 시장가 매수는 amount_krw(금액)으로, 지정가 매수는 volume(수량)으로.
            # 여기서는 buy_amount_usdt_or_krw가 KRW 금액이므로, 지정가일 경우 수량 계산
            volume_to_order = round(buy_amount_usdt_or_krw / price, 8) if price > 0 else 0

        # case1: 기존 주문을 취소하고 새로운 주문을 제출하는 정정 주문 (Upbit의 cancel_and_new 또는 Binance의 취소+신규)
        if filled == "update" and uuid:
            logging.info(
                f"🔁 정정 매수 주문 시도: {market}, 기존 UUID={uuid}, 요청 금액/수량={buy_amount_usdt_or_krw:.2f}/{volume_to_order:.4f}, 가격={price:.8f}")
            try:
                if config.EXCHANGE == 'binance':
                    # 바이낸스는 cancel_and_new_order_binance 함수를 사용 (직접 구현)
                    response = cancel_and_new_order_binance(
                        prev_order_uuid=uuid, symbol=market, price=price, quantity=volume_to_order  # 'quantity' 사용
                    )
                else:  # 업비트 (업비트 고유의 정정 주문 API)
                    response = cancel_and_new_order(
                        prev_order_uuid=uuid, market=market, price=price, amount=volume_to_order  # 업비트는 'amount' (수량)
                    )

                # 바이낸스 응답에서 'orderId'를 사용하고, 업비트 응답에서 'new_order_uuid'를 사용
                new_order_uuid = response.get("orderId", "") if config.EXCHANGE == 'binance' else response.get(
                    "new_order_uuid", "")

                if new_order_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = new_order_uuid
                    buy_log_df.at[idx, "filled"] = "wait"  # 주문 제출 후 대기 상태로 변경
                    logging.info(f"✅ {market} 정정 매수 주문 제출 완료. 새로운 UUID: {new_order_uuid}")
                else:
                    # cancel_and_new_order_binance에서 {"error": "done_order"}가 반환될 수 있음
                    if response.get("error") == "done_order":
                        buy_log_df.at[idx, "filled"] = "done"
                        logging.info(f"✅ {market} 기존 주문({uuid})은 이미 체결 완료되어 정정 주문 스킵. → filled=done 처리")
                    else:
                        # 예상치 못한 응답 또는 uuid 없음
                        raise ValueError(f"정정 매수 주문 후 새로운 UUID를 얻지 못했습니다. 응답: {response}")

            except ClientError as e:  # Rate Limit 처리 추가
                if e.error_code == -1003:  # Too much request weight used
                    logging.critical(
                        f"❌ API Rate Limit 초과 (ClientError: {e.error_code}): {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                elif e.status_code == 429:  # Too Many Requests (HTTP status code)
                    logging.critical(f"❌ HTTP 429 Rate Limit 초과: {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                else:
                    logging.error(f"❌ {market} 정정 매수 주문 실패 (ClientError: {e.error_code}): {e.error_message}",
                                  exc_info=True)
                    all_success = False
            except Exception as e:
                logging.error(f"❌ {market} 정정 매수 주문 실패 (알 수 없는 오류): {e}", exc_info=True)
                all_success = False

        # case2: 새로운 주문 제출 (filled='update' 이지만 buy_uuid가 없는 경우)
        elif filled == "update" and not uuid:
            logging.info(
                f"🆕 신규 매수 주문 시도: {market}, 타입={buy_type}, 요청 금액/수량={buy_amount_usdt_or_krw:.2f}/{volume_to_order:.4f}, 가격={price:.8f}")
            try:
                if config.EXCHANGE == 'binance':
                    if buy_type == "initial":
                        # 바이낸스 시장가 매수 (USDT 금액 기준)
                        response = send_order(
                            market=market,
                            side="bid",  # 매수 (내부적으로 "BUY"로 변환)
                            type="price",  # 'ord_type' -> 'type' 으로 변경 (내부적으로 "MARKET"으로 변환)
                            amount_usdt=buy_amount_usdt_or_krw,  # 'amount_krw' -> 'amount_usdt'로 변경
                            position_side="LONG"  # 롱 포지션 진입 (전략에 따라 조절)
                        )
                    else:
                        # 바이낸스 지정가 매수
                        response = send_order(
                            market=market,
                            side="bid",  # 매수
                            type="limit",  # 'ord_type' -> 'type' 으로 변경 (내부적으로 "LIMIT"으로 변환)
                            price=price,  # 'unit_price' -> 'price'로 변경
                            volume=volume_to_order,  # 'volume' 사용
                            position_side="LONG"  # 롱 포지션 진입 (전략에 따라 조절)
                        )
                else:  # 업비트
                    if buy_type == "initial":
                        # 업비트 시장가 매수 (원화 금액 기준)
                        response = send_order(market=market, side="bid", ord_type="price",
                                              amount_krw=buy_amount_usdt_or_krw)
                    else:
                        # 업비트 지정가 매수
                        response = send_order(market=market, side="bid", ord_type="limit", unit_price=price,
                                              volume=volume_to_order)

                # 바이낸스 응답에서 'orderId'를 사용하고, 업비트 응답에서 'uuid'를 사용
                new_order_uuid = response.get("orderId", "") if config.EXCHANGE == 'binance' else response.get("uuid",
                                                                                                               "")

                if new_order_uuid:
                    buy_log_df.at[idx, "buy_uuid"] = new_order_uuid
                    buy_log_df.at[idx, "filled"] = "wait"  # 주문 제출 후 대기 상태로 변경
                    logging.info(f"✅ {market} 신규 매수 주문 제출 완료. UUID: {new_order_uuid}")
                else:
                    raise ValueError(f"신규 매수 주문 후 UUID를 얻지 못했습니다. 응답: {response}")
            except ClientError as e:  # Rate Limit 처리 추가
                if e.error_code == -1003:  # Too much request weight used
                    logging.critical(
                        f"❌ API Rate Limit 초과 (ClientError: {e.error_code}): {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                elif e.status_code == 429:  # Too Many Requests (HTTP status code)
                    logging.critical(f"❌ HTTP 429 Rate Limit 초과: {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                else:
                    logging.error(f"❌ {market} 신규 매수 주문 실패 (ClientError: {e.error_code}): {e.error_message}",
                                  exc_info=True)
                    all_success = False
            except Exception as e:
                logging.error(f"❌ {market} 신규 매수 주문 실패 (알 수 없는 오류): {e}", exc_info=True)
                all_success = False

    logging.info("--- 🛒 매수 주문 실행 완료 ---")
    if not all_success:
        # 전체 매수 주문 중 하나라도 실패했다면 RuntimeError 발생시켜 상위 로직에 알림
        raise RuntimeError("일부 매수 주문 실행에 실패했습니다. 로그를 확인하세요.")
    return buy_log_df


def execute_sell_orders(sell_log_df: pd.DataFrame) -> pd.DataFrame:
    """
    매도 주문을 실행합니다.
    바이낸스의 경우, 기존 매도 주문을 취소하고 새로운 주문을 제출하는 방식으로 처리됩니다.

    :param sell_log_df: 매도 주문 정보가 담긴 DataFrame (filled='update' 대상)
    :return: 업데이트된 sell_log_df
    """
    logging.info("--- 💲 매도 주문 실행 시작 ---")
    all_success = True  # 모든 주문이 성공했는지 추적

    if sell_log_df.empty:
        logging.info("실행할 매도 주문이 없습니다.")
        return sell_log_df

    for idx, row in sell_log_df.iterrows():
        filled = str(row["filled"]).strip()
        uuid = str(row["sell_uuid"]) if pd.notna(row["sell_uuid"]) else None

        # 이미 체결 완료된 주문은 건너김
        if filled == "done":
            logging.debug(f"ℹ️ {row['market']} 매도 주문(id:{uuid})은 이미 체결 완료되어 스킵합니다.")
            continue

        market = row["market"]
        price = float(row["target_sell_price"])
        volume_to_order = float(row["quantity"])  # 매도할 수량 (코인 수)

        # 거래소별 가격/수량 보정 로직 적용
        if config.EXCHANGE == 'binance':
            price = adjust_price_to_tick(market, price)
            volume_to_order = adjust_quantity_to_step(market, volume_to_order)  # 수량 보정
        else:  # 업비트
            price = adjust_price_to_tick(price, market="KRW", ticker=market)

        # 매도할 수량이 0이거나 음수이면 주문을 건너김
        if volume_to_order <= 0:
            logging.warning(f"⚠️ {market} 매도할 수량({volume_to_order})이 0 이하입니다. 주문을 스킵하고 'done' 처리합니다.")
            sell_log_df.at[idx, "filled"] = "done"  # 이 주문은 더 이상 처리할 필요 없음
            continue

        # 'update' 상태인 주문 (신규 또는 정정)
        if filled == "update":
            logging.info(f"🆕/🔁 매도 주문 시도: {market}, 요청 수량={volume_to_order:.4f}, 가격={price:.8f}")
            try:
                if config.EXCHANGE == 'binance':
                    # 바이낸스는 정정 기능이 없으므로, 기존 주문이 있다면 취소 후 신규 주문
                    if uuid:
                        try:
                            # 기존 주문 취소 시도
                            cancel_order(uuid, market)
                            logging.info(f"🔁 {market} 기존 매도 주문({uuid}) 취소 요청 완료.")
                            time.sleep(0.1)  # 취소 API 처리 시간 확보
                        except Exception as e:
                            # 취소 실패 (예: 이미 체결 또는 존재하지 않음)는 경고로 처리하고 신규 주문 시도
                            logging.warning(f"⚠️ {market} 기존 매도 주문({uuid}) 취소 실패 (이미 처리되었을 수 있음): {e}")

                    # 새로운 지정가 매도 주문 제출
                    response = send_order(
                        market=market,
                        side="ask",  # 매도 (내부적으로 "SELL"로 변환)
                        type="limit",  # 'ord_type' -> 'type' 으로 변경 (내부적으로 "LIMIT"으로 변환)
                        price=price,  # 'unit_price' -> 'price'로 변경
                        volume=volume_to_order,  # 'volume' 사용
                        position_side="LONG"  # 롱 포지션 청산 (전략에 따라 조절)
                    )
                else:  # 업비트 (업비트는 매도 정정 주문 API가 없으므로 항상 신규 주문만)
                    response = send_order(market=market, side="ask", ord_type="limit", unit_price=price,
                                          volume=volume_to_order)

                # 바이낸스 응답에서 'orderId'를 사용하고, 업비트 응답에서 'uuid'를 사용
                new_order_uuid = response.get("orderId", "") if config.EXCHANGE == 'binance' else response.get("uuid",
                                                                                                               "")

                if new_order_uuid:
                    sell_log_df.at[idx, "sell_uuid"] = new_order_uuid
                    sell_log_df.at[idx, "filled"] = "wait"  # 주문 제출 후 대기 상태로 변경
                    logging.info(f"✅ {market} 매도 주문 제출 완료. UUID: {new_order_uuid}")
                else:
                    raise ValueError(f"매도 주문 후 UUID를 얻지 못했습니다. 응답: {response}")

            except ClientError as e:  # Rate Limit 처리 추가
                if e.error_code == -1003:  # Too much request weight used
                    logging.critical(
                        f"❌ API Rate Limit 초과 (ClientError: {e.error_code}): {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                elif e.status_code == 429:  # Too Many Requests (HTTP status code)
                    logging.critical(f"❌ HTTP 429 Rate Limit 초과: {e.error_message}. 60초 후 다시 시도합니다.")
                    time.sleep(60)  # 긴 딜레이 후 재시도
                    all_success = False
                    continue
                else:
                    logging.error(f"❌ {market} 매도 주문 실패 (ClientError: {e.error_code}): {e.error_message}",
                                  exc_info=True)
                    all_success = False
            except Exception as e:
                logging.error(f"❌ {market} 매도 주문 실패 (알 수 없는 오류): {e}", exc_info=True)
                all_success = False

    logging.info("--- 💲 매도 주문 실행 완료 ---")
    if not all_success:
        # 전체 매도 주문 중 하나라도 실패했다면 RuntimeError 발생시켜 상위 로직에 알림
        raise RuntimeError("일부 매도 주문 실행에 실패했습니다. 로그를 확인하세요.")
    return sell_log_df