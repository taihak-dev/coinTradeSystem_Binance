# api/binance/account.py

import logging
from binance.error import ClientError
from api.binance.client import get_binance_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_accounts():
    """
    바이낸스 선물 계좌의 잔고 및 현재 포지션 정보를 조회합니다.
    """
    logging.info("💰 바이낸스 선물 계좌 정보 조회 시도 중...")
    client = get_binance_client() # 인증된 바이낸스 클라이언트 가져오기

    try:
        # 1. USDT 잔고 조회 (account_info에서 추출)
        # client.account()는 /fapi/v2/account 엔드포인트를 호출하며, 자산 정보를 포함합니다.
        account_info = client.account()
        usdt_balance = 0.0
        for asset in account_info.get('assets', []):
            if asset.get('asset') == 'USDT':
                usdt_balance = float(asset.get('availableBalance', '0.0'))
                break
        logging.info(f"✅ USDT 사용 가능 잔고: {usdt_balance:.2f} USDT")

        # 2. 현재 열려있는 포지션 상세 정보 조회 (get_position_risk 사용)
        # client.get_position_risk()는 /fapi/v2/positionRisk 엔드포인트를 호출하며,
        # 각 포지션에 대한 entryPrice, markPrice, unrealizedProfit 등을 직접 제공합니다.
        raw_positions_risk = client.get_position_risk()

        open_positions = []
        for pos in raw_positions_risk:
            # positionAmt가 0이 아닌 경우에만 유효한 포지션으로 간주
            if float(pos.get('positionAmt', '0.0')) != 0:
                try:
                    symbol = pos.get('symbol', 'UNKNOWN')
                    position_amt = float(pos.get('positionAmt', '0.0'))
                    entry_price = float(pos.get('entryPrice', '0.0'))
                    mark_price = float(pos.get('markPrice', '0.0')) # <-- 이 부분이 핵심. get_position_risk()에서 제공
                    unrealized_profit = float(pos.get('unRealizedProfit', '0.0')) # API 응답에서 'unRealizedProfit' (대문자 R) 임
                    liquidation_price = float(pos.get('liquidationPrice', '0.0'))
                    leverage = int(pos.get('leverage', '1'))
                    margin_type = pos.get('marginType', 'UNKNOWN')
                    position_side = pos.get('positionSide', 'UNKNOWN')
                    isolated_wallet = float(pos.get('isolatedWallet', '0.0'))

                    open_positions.append({
                        'symbol': symbol,
                        'positionAmt': position_amt,
                        'entryPrice': entry_price,
                        'markPrice': mark_price, # <-- 추가된 핵심 정보
                        'unRealizedProfit': unrealized_profit, # API 응답 그대로의 키 사용
                        'liquidationPrice': liquidation_price,
                        'leverage': leverage,
                        'marginType': margin_type,
                        'positionSide': position_side,
                        'isolatedWallet': isolated_wallet
                    })
                except Exception as e:
                    logging.error(f"❌ 바이낸스 계좌 정보 조회 중 포지션 데이터 처리 오류 발생: {e}. 해당 포지션: {pos}", exc_info=True)
                    continue

        logging.info(f"✅ 현재 보유 중인 선물 포지션 수: {len(open_positions)}개")
        if not open_positions:
            logging.info("ℹ️ 현재 열려있는 선물 포지션이 없습니다.")

        return {
            "usdt_balance": usdt_balance,
            "open_positions": open_positions # 이제 markPrice가 포함된 상세 포지션 정보
        }

    except ClientError as e:
        logging.error(f"❌ 바이낸스 API 오류 발생 (Code: {e.error_code}): {e.error_message}")
        raise
    except Exception as e:
        logging.error(f"❌ 바이낸스 계좌 정보 조회 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise

def get_position_mode():
    """
    바이낸스 선물 계좌의 포지션 모드 (헷지 모드 또는 단일 모드)를 조회합니다.
    """
    logging.info("🌐 바이낸스 포지션 모드 조회 시도 중...")
    client = get_binance_client()

    try:
        response = client.get_position_mode()
        is_hedge_mode = response.get('dualSidePosition', False)
        if is_hedge_mode:
            logging.info("✅ 포지션 모드가 이미 헷지 모드(Dual-Side)입니다.")
        else:
            logging.warning("⚠️ 포지션 모드가 단일 모드(One-way)입니다. 헷지 모드로 변경하는 것을 고려하세요.")
        return is_hedge_mode
    except ClientError as e:
        logging.error(f"❌ 바이낸스 포지션 모드 조회 API 오류 (Code: {e.error_code}): {e.error_message}")
        raise
    except Exception as e:
        logging.error(f"❌ 바이낸스 포지션 모드 조회 중 알 수 없는 오류 발생: {e}", exc_info=True)
        raise