# utils/common_utils.py

import pandas as pd
import config
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if config.EXCHANGE == 'binance':
    from api.binance.account import get_accounts as get_binance_accounts
    from api.binance.price import get_current_ask_price

    logging.info("[SYSTEM] common_utils: 바이낸스 API 모드를 사용합니다.")


def get_current_holdings() -> dict:
    logging.info("[common_utils.py] 현재 보유 자산 조회 중...")

    holdings = {}

    try:
        if config.EXCHANGE == 'binance':
            account_data = get_binance_accounts()
            usdt_balance = account_data.get("usdt_balance", 0.0)
            open_positions = account_data.get("open_positions", [])

            base_currency = 'USDT'

            logging.info(f"[common_utils.py] get_accounts에서 조회된 포지션 수: {len(open_positions)}개")  # 추가된 로그

            for pos in open_positions:
                # --- 디버그 로깅 시작 ---
                logging.debug(f"--- 포지션 처리 시작: {pos.get('symbol', 'UNKNOWN')} ---")
                logging.debug(f"원본 포지션 데이터: {pos}")
                # --- 디버그 로깅 끝 ---

                market = pos.get('symbol')
                if not market:  # symbol이 없는 경우 건너뛰기
                    logging.warning(f"⚠️ 포지션에 심볼 정보가 없습니다: {pos}. 건너뜁니다.")
                    continue

                balance = float(pos.get('positionAmt', '0.0'))
                avg_price = float(pos.get('entryPrice', '0.0'))
                balance_abs = abs(balance)

                # --- 디버그 로깅 시작 ---
                logging.debug(f"[{market}] balance_abs: {balance_abs:.6f}, avg_price: {avg_price:.6f}")
                # --- 디버그 로깅 끝 ---

                # 아주 작은 잔고는 무시 (명목 가치가 극히 낮은 경우)
                mark_price = float(pos.get('markPrice', '0.0'))  # markPrice를 안전하게 가져옴
                notional_value = balance_abs * mark_price
                min_value_threshold = 0.001

                # --- 디버그 로깅 시작 ---
                logging.debug(
                    f"[{market}] notional_value: {notional_value:.6f}, min_value_threshold: {min_value_threshold}")
                # --- 디버그 로깅 끝 ---

                if notional_value < min_value_threshold:
                    logging.info(
                        f"ⓘ {market} 보유 가치({notional_value:.6f}{base_currency})가 최소 기준({min_value_threshold}{base_currency}) 미만 → 무시")
                    continue

                try:
                    current_price = get_current_ask_price(market)
                    # --- 디버그 로깅 시작 ---
                    logging.debug(f"[{market}] current_price: {current_price:.6f}")
                    # --- 디버그 로깅 끝 ---

                    if current_price <= 0:
                        logging.warning(f"⚠️ {market} 현재 가격이 유효하지 않습니다 ({current_price}). 보유 코인 목록에서 제외.")
                        continue
                except Exception as e:
                    logging.error(f"❌ {market} 현재가 조회 실패: {e}. 보유 코인 목록에서 제외.", exc_info=True)
                    continue

                total_value = balance_abs * current_price
                min_account_value_ignore = 5  # 5 USDT 미만 포지션은 무시

                # --- 디버그 로깅 시작 ---
                logging.debug(
                    f"[{market}] total_value: {total_value:.6f}, min_account_value_ignore: {min_account_value_ignore}")
                # --- 디버그 로깅 끝 ---

                if total_value < min_account_value_ignore:
                    logging.info(
                        f"ⓘ {market} 보유 가치({total_value:.6f}{base_currency})가 최소 계좌 기준({min_account_value_ignore}{base_currency}) 미만 → 무시")
                    continue

                holdings[market] = {
                    "balance": balance_abs,
                    "avg_price": avg_price,
                    "current_price": current_price,
                    "total_value": total_value,
                    "position_side": pos.get('positionSide', 'UNKNOWN')
                }
                logging.info(
                    f"✅ {market} 포지션이 보유 코인 목록에 추가되었습니다. (수량: {balance_abs:.6f}, 가치: {total_value:.2f}{base_currency})")  # 추가된 로그

        logging.info(f"[common_utils.py] 최종 보유 중인 코인 수: {len(holdings)}개")  # 로그 메시지 변경
        return holdings

    except Exception as e:
        logging.error(f"❌ 현재 보유 자산 조회 중 오류 발생: {e}", exc_info=True)
        return {}