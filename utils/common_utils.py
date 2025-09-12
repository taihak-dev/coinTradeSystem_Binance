# utils/common_utils.py

import pandas as pd
import config
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 👇👇👇 거래소 선택 로직 (핵심 수정) 👇👇👇 ---
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] Common Utils: 바이낸스 API 모드를 사용합니다.")
    from api.binance.account import get_accounts
    from api.binance.price import get_current_ask_price
elif config.EXCHANGE == 'bybit':
    logging.info("[SYSTEM] Common Utils: 바이빗 API 모드를 사용합니다.")
    from api.bybit.account import get_accounts
    from api.bybit.price import get_current_ask_price
else:
    raise ValueError(f"지원하지 않는 거래소입니다: {config.EXCHANGE}")


# --- 👆👆👆 여기까지 수정 --- 👆👆👆


def get_current_holdings(retries=3, delay=5) -> dict:
    """
    선택된 거래소에서 현재 보유 포지션을 조회합니다.
    일시적인 API 오류에 대비해 재시도 로직을 포함합니다.
    """
    for attempt in range(retries):
        try:
            logging.info(f"[common_utils.py] 현재 보유 자산 조회 중... (시도 {attempt + 1}/{retries})")

            # get_accounts 함수는 이제 설정에 따라 바이낸스 또는 바이빗의 함수가 됩니다.
            account_data = get_accounts()
            open_positions = account_data.get("open_positions", [])

            holdings = {}
            for pos in open_positions:
                market = pos['symbol']
                balance_abs = abs(float(pos['positionAmt']))
                avg_price = float(pos['entryPrice'])

                # 포지션 가치가 5 USDT 미만이면 무시 (더스트 포지션)
                if balance_abs * avg_price < 5:
                    continue

                holdings[market] = {
                    "balance": balance_abs,
                    "avg_price": avg_price
                }

            if not holdings and attempt < retries - 1:
                logging.warning(f"API가 빈 포지션 목록을 반환했습니다. {delay}초 후 재확인합니다...")
                time.sleep(delay)
                continue

            logging.info(f"✅ 최종 조회된 보유 코인 수: {len(holdings)}개")
            return holdings

        except Exception as e:
            logging.warning(f"보유 자산 조회 실패 (시도 {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logging.error("최종 보유 자산 조회에 실패했습니다.", exc_info=True)
                raise  # 재시도 모두 실패 시 예외 발생

    # 루프가 정상적으로 끝났지만 (그럴 일은 없지만) holdings가 없는 경우
    return {}