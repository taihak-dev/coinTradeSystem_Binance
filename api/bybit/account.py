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
    Bybit 선물 계좌의 잔고 및 현재 포지션 정보를 조회합니다.
    """
    logging.info("💰 Bybit 선물 계좌 정보 조회 시도 중...")
    client = get_bybit_client()

    try:
        # 1. 통합 계좌의 '전체' 자산 정보를 조회합니다.
        wallet_info = client.get_wallet_balance(accountType="UNIFIED")

        usdt_balance = 0.0
        total_wallet_balance = 0.0
        total_unrealized_pnl = 0.0

        # --- 👇👇👇 여기가 핵심 수정 부분입니다 👇👇👇 ---
        if wallet_info and wallet_info['result']['list']:
            asset_list = wallet_info['result']['list']

            # 총 자산 가치와 총 미실현 손익은 목록의 첫 번째 항목에서 가져옵니다. (이 값들은 모든 자산 항목에 동일하게 포함됨)
            summary_data = asset_list[0]
            total_wallet_balance = _safe_float_convert(summary_data.get('totalWalletBalance'))
            total_unrealized_pnl = _safe_float_convert(summary_data.get('totalUnrealisedPnl'))

            # 사용 가능 USDT 잔고를 찾기 위해 전체 자산 목록을 순회합니다.
            for asset in asset_list:
                if asset.get('coin') == 'USDT':
                    # 'availableToWithdraw'는 출금 가능액, 'availableBalance'는 거래에 사용 가능한 증거금입니다.
                    # 거래 목적이므로 'availableBalance'를 사용하는 것이 더 적합할 수 있습니다.
                    usdt_balance = _safe_float_convert(asset.get('availableBalance'))
                    break  # USDT를 찾았으면 루프 종료
        # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆

        logging.info(f"✅ 사용 가능 잔고: {usdt_balance:.2f} USDT, 총 자산: {total_wallet_balance:.2f} USDT")

        # 2. 현재 열려있는 포지션 상세 정보 조회
        positions_info = client.get_positions(category="linear", settleCoin="USDT")

        open_positions = []
        if positions_info and positions_info['result']['list']:
            for pos in positions_info['result']['list']:
                position_size = _safe_float_convert(pos.get('size'))

                if position_size > 0:
                    entry_price = _safe_float_convert(pos.get('avgPrice'))
                    unrealized_pnl = _safe_float_convert(pos.get('unrealisedPnl'))
                    leverage = _safe_float_convert(pos.get('leverage'), default=1.0)
                    mark_price = _safe_float_convert(pos.get('markPrice'))
                    liquidation_price = _safe_float_convert(pos.get('liqPrice'))

                    initial_margin = (position_size * entry_price) / leverage if leverage > 0 else 0
                    roe = (unrealized_pnl / initial_margin) * 100 if initial_margin > 0 else 0

                    processed_pos = {
                        'symbol': pos.get('symbol'),
                        'positionAmt': position_size,
                        'entryPrice': entry_price,
                        'markPrice': mark_price,
                        'unRealizedProfit': unrealized_pnl,
                        'liquidationPrice': liquidation_price,
                        'leverage': int(leverage),
                        'roe': roe,
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
        logging.error(f"❌ Bybit 계좌 정보 조회 중 오류 발생: {e}", exc_info=True)
        raise