# utils/common_utils.py

import pandas as pd
import config
import logging
import time  # time 모듈 import 추가

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if config.EXCHANGE == 'binance':
    from api.binance.account import get_accounts as get_binance_accounts
    from api.binance.price import get_current_ask_price

    logging.info("[SYSTEM] common_utils: 바이낸스 API 모드를 사용합니다.")


# ✅✅✅ --- 핵심 수정 함수 --- ✅✅✅
def get_current_holdings(retries=3, delay=5) -> dict:
    """
    바이낸스에서 현재 보유 포지션을 조회합니다.
    일시적인 API 오류에 대비해 재시도 로직을 포함합니다.

    :param retries: 최대 재시도 횟수
    :param delay: 재시도 간 대기 시간 (초)
    :return: 보유 자산 딕셔너리. 최종 실패 시 예외 발생
    """
    for attempt in range(retries):
        try:
            logging.info(f"[common_utils.py] 현재 보유 자산 조회 중... (시도 {attempt + 1}/{retries})")

            # --- 기존 핵심 로직 시작 ---
            holdings = {}
            if config.EXCHANGE == 'binance':
                account_data = get_binance_accounts()
                open_positions = account_data.get("open_positions", [])

                logging.info(f"[common_utils.py] get_accounts에서 조회된 포지션 수: {len(open_positions)}개")

                for pos in open_positions:
                    market = pos.get('symbol')
                    if not market:
                        logging.warning(f"⚠️ 포지션에 심볼 정보가 없습니다: {pos}. 건너뜁니다.")
                        continue

                    balance_abs = abs(float(pos.get('positionAmt', '0.0')))
                    avg_price = float(pos.get('entryPrice', '0.0'))
                    mark_price = float(pos.get('markPrice', '0.0'))
                    notional_value = balance_abs * mark_price

                    if notional_value < 0.001:
                        continue

                    try:
                        current_price = get_current_ask_price(market)
                        if current_price <= 0:
                            logging.warning(f"⚠️ {market} 현재 가격이 유효하지 않습니다 ({current_price}).")
                            # 현재가 조회가 안되면 이 포지션은 건너뛰고 다음 포지션으로
                            continue
                    except Exception as e:
                        logging.error(f"❌ {market} 현재가 조회 실패: {e}. 해당 포지션은 제외됩니다.", exc_info=True)
                        continue

                    total_value = balance_abs * current_price
                    if total_value < 5:
                        continue

                    holdings[market] = {
                        "balance": balance_abs,
                        "avg_price": avg_price,
                        "current_price": current_price,
                        "total_value": total_value,
                        "position_side": pos.get('positionSide', 'UNKNOWN')
                    }
            # --- 기존 로직 끝 ---

            # API 호출은 성공했으나, 결과가 비어있는 경우 재확인
            if not holdings and attempt < retries - 1:
                logging.warning(f"API가 빈 포지션 목록을 반환했습니다. {delay}초 후 재확인합니다...")
                time.sleep(delay)
                continue  # 다음 재시도 실행

            logging.info(f"✅ 최종 조회된 보유 코인 수: {len(holdings)}개")
            return holdings  # 성공적으로 결과를 얻었거나, 마지막 시도에서 빈 값을 얻었을 때 반환

        except Exception as e:
            logging.warning(f"보유 자산 조회 실패 (시도 {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # 다음 재시도 전 대기
            else:
                logging.error("모든 재시도 후에도 보유 자산 조회에 최종 실패했습니다.")
                raise e  # 모든 재시도 실패 시, 오류를 발생시켜 봇을 멈춤
    raise RuntimeError("get_current_holdings 함수가 재시도 로직을 비정상적으로 빠져나왔습니다.")

