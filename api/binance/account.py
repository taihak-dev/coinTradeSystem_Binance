# api/binance/account.py
import logging
from api.binance.client import get_binance_client


def get_accounts():
    """
    바이낸스 선물 계좌의 자산과 포지션 정보를 조회합니다.
    기존 프로그램과 호환되도록 Upbit API 형식과 유사하게 가공하여 반환합니다.
    """
    client = get_binance_client()
    try:
        # 1. 계좌 정보(자산) 조회
        account_info = client.account()
        balances = account_info.get('assets', [])

        # 2. 포지션 정보 조회 (현재 보유 코인)
        positions = account_info.get('positions', [])

        # 업비트 형식에 맞춘 결과 리스트
        formatted_accounts = []

        # 사용 가능한 USDT 잔고 추가 (업비트의 KRW 역할)
        for asset in balances:
            if asset['asset'] == 'USDT':
                formatted_accounts.append({
                    "currency": "USDT",  # 업비트의 'KRW'와 동일한 역할
                    "balance": asset.get('availableBalance', '0'),  # 주문 가능한 잔고
                    "locked": "0.0",  # 선물에서는 locked 개념이 다르므로 0으로 통일
                    "avg_buy_price": "0"
                })
                break

        # 보유 중인 포지션 정보 추가
        for pos in positions:
            # positionAmt가 0이 아닌 것만 (실제 보유 포지션)
            if float(pos['positionAmt']) != 0:
                # 롱/숏 포지션에 따라 수량을 양수/음수로 표현할 수 있지만, 여기서는 절대값으로 통일
                # 어차피 매도 로직에서는 전체 수량을 매도함
                position_amt_abs = abs(float(pos['positionAmt']))

                formatted_accounts.append({
                    "currency": pos['symbol'],  # 'BTCUSDT'에서 'USDT'를 뺀 'BTC'와 같은 개념
                    "balance": str(position_amt_abs),  # 주문 가능 수량. 여기서는 전체 보유 수량
                    "locked": "0.0",  # 주문에 묶인 수량. 여기서는 0으로 통일
                    "avg_buy_price": pos.get('entryPrice', '0'),  # 평단가
                    "unrealizedProfit": pos.get('unrealizedProfit', '0')  # 미실현 손익 (참고용)
                })

        return formatted_accounts

    except Exception as e:
        logging.error(f"바이낸스 계좌 정보 조회 실패: {e}")
        raise e