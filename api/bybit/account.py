# api/bybit/account.py

import logging
from api.bybit.client import get_bybit_client

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _safe_float_convert(value, default=0.0):
    """
    문자열을 float으로 안전하게 변환합니다.
    문자열이 비어 있거나 None이면 default 값을 반환합니다.
    """
    if value and isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return default
    if isinstance(value, (int, float)):
        return value
    return default


def get_accounts():
    """
    Bybit 통합 계좌(Unified Trading)의 잔고 및 현재 포지션 정보를 조회합니다.
    """
    logging.info("💰 Bybit 통합 계좌 정보 조회 시도 중...")
    client = get_bybit_client()

    try:
        # 1. 통합 계좌의 자산 정보를 조회합니다.
        wallet_info = client.get_wallet_balance(accountType="UNIFIED")

        usdt_balance = 0.0
        total_wallet_balance = 0.0
        total_unrealized_pnl = 0.0

        # --- ▼▼▼ 최종 수정 부분 ▼▼▼ ---
        # 진단 스크립트를 통해 확인된 정확한 API 응답 구조를 기반으로 잔고를 파싱합니다.
        if wallet_info and wallet_info.get('retCode') == 0 and wallet_info['result']['list']:

            # 'result'->'list' 안에는 단 하나의 요약 객체만 존재합니다.
            summary_data = wallet_info['result']['list'][0]

            # [해결] 이 요약 객체에서 'totalAvailableBalance' 키를 직접 읽어옵니다.
            # 이것이 USDT 보유 여부와 상관없이 실제 선물 거래에 사용할 수 있는 총 증거금입니다.
            usdt_balance = _safe_float_convert(summary_data.get('totalAvailableBalance'))

            # 나머지 정보들도 동일한 위치에서 가져옵니다.
            total_wallet_balance = _safe_float_convert(summary_data.get('totalWalletBalance'))
            total_unrealized_pnl = _safe_float_convert(
                summary_data.get('totalPerpUPL'))  # 선물 미실현 손익은 'totalPerpUPL'이 더 정확할 수 있습니다.

            logging.info(f"✅ 계좌 총 자산: {total_wallet_balance:.2f} USDT")
            logging.info(f"✅ 선물 미실현 손익: {total_unrealized_pnl:.2f} USDT")
            logging.info(f"✅ >> 거래에 사용 가능한 총 잔고(USDT 환산): {usdt_balance:.2f} USDT <<")

        else:
            logging.warning("⚠️ Bybit 계좌에서 자산 정보를 가져오지 못했거나 비어있습니다.")
        # --- ▲▲▲ 최종 수정 완료 ▲▲▲ ---

        # 2. 현재 열려있는 포지션 상세 정보 조회
        positions_info = client.get_positions(category="linear", settleCoin="USDT")

        open_positions = []
        if positions_info and positions_info.get('retCode') == 0 and positions_info['result']['list']:
            for pos in positions_info['result']['list']:
                if _safe_float_convert(pos.get('size')) > 0:
                    entry_price = _safe_float_convert(pos.get('avgPrice'))
                    position_size = _safe_float_convert(pos.get('size'))
                    unrealized_pnl = _safe_float_convert(pos.get('unrealisedPnl'))
                    leverage = _safe_float_convert(pos.get('leverage'), default=1.0)
                    mark_price = _safe_float_convert(pos.get('markPrice'))
                    liquidation_price = _safe_float_convert(pos.get('liqPrice'))
                    initial_margin = (position_size * entry_price) / leverage if leverage > 0 else 0
                    roe = (unrealized_pnl / initial_margin) * 100 if initial_margin > 0 else 0

                    processed_pos = {
                        'symbol': pos.get('symbol'), 'positionAmt': position_size,
                        'entryPrice': entry_price, 'markPrice': mark_price,
                        'unRealizedProfit': unrealized_pnl, 'liquidationPrice': liquidation_price,
                        'leverage': int(leverage), 'roe': roe,
                    }
                    open_positions.append(processed_pos)

        logging.info(f"✅ 현재 보유 중인 선물 포지션 수: {len(open_positions)}개")

        return {
            "usdt_balance": usdt_balance,
            "total_wallet_balance": total_wallet_balance,
            "total_unrealized_pnl": total_unrealized_pnl,
            "open_positions": open_positions,
        }

    except Exception as e:
        logging.error(f"❌ Bybit 계좌 정보 조회 중 심각한 오류 발생: {e}", exc_info=True)
        return {
            "usdt_balance": 0.0, "total_wallet_balance": 0.0,
            "total_unrealized_pnl": 0.0, "open_positions": [],
        }