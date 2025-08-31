# api/bybit/account.py
import logging
from api.bybit.client import get_bybit_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_accounts():
    http = get_bybit_client()

    # 지갑 잔고
    wallet = http.get_wallet_balance(accountType="UNIFIED")  # unified 계정
    coins = (wallet.get("result", {}) or {}).get("list", [{}])[0].get("coin", [])
    usdt = next((c for c in coins if c.get("coin") == "USDT"), {"walletBalance": "0", "availableToWithdraw": "0"})
    usdt_balance = float(usdt.get("availableToWithdraw") or usdt.get("walletBalance") or 0)

    # 포지션(USDT perp)
    pos_resp = http.get_positions(category="linear")
    pos_list = (pos_resp.get("result", {}) or {}).get("list", [])

    open_positions = []
    for p in pos_list:
        symbol = p.get("symbol")
        side = (p.get("side") or "").lower()  # "Buy"|"Sell"
        size = float(p.get("size") or 0)
        entry = float(p.get("avgPrice") or 0)
        mark  = float(p.get("markPrice") or 0)
        # 공용 스키마로 변환: positionAmt 부호로 방향 표현
        positionAmt = size if side == "buy" else -size
        if abs(positionAmt) < 1e-12:
            continue
        open_positions.append({
            "symbol": symbol,
            "positionAmt": positionAmt,
            "entryPrice": entry,
            "markPrice": mark,
        })

    return {
        "usdt_balance": usdt_balance,
        "open_positions": open_positions,
    }
