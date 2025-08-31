# api/bybit/account.py
import logging
from pybit.exceptions import FailedRequestError
from api.bybit.client import get_bybit_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_accounts():
    http = get_bybit_client()

    # 1) 지갑 잔고: UNIFIED 먼저, 실패 시 CONTRACT로 재시도
    try:
        wallet = http.get_wallet_balance(accountType="UNIFIED")
    except FailedRequestError as e:
        logging.warning(f"[bybit] UNIFIED 잔고 조회 실패 → CONTRACT로 재시도: {e}")
        wallet = http.get_wallet_balance(accountType="CONTRACT")

    coins = (wallet.get("result", {}) or {}).get("list", [{}])[0].get("coin", [])
    usdt = next((c for c in coins if c.get("coin") == "USDT"), {})
    usdt_balance = float(usdt.get("availableToWithdraw") or usdt.get("walletBalance") or 0)

    # 2) 포지션(USDT-Perp)
    try:
        pos_resp = http.get_positions(category="linear", settleCoin="USDT")
        pos_list = (pos_resp.get("result", {}) or {}).get("list", [])
        # 비정상적으로 빈 결과라면 USDC 마켓도 한 번 시도(선택)
        if not pos_list:
            try:
                alt = http.get_positions(category="linear", settleCoin="USDC")
                pos_list = (alt.get("result", {}) or {}).get("list", [])
            except FailedRequestError as e:
                # USDC 마켓이 없을 수 있으니 조용히 패스
                pass
    except FailedRequestError as e:
        # 파라미터/권한 이슈 등 상세 로그
        logging.error(f"[bybit] 포지션 조회 실패: {e}", exc_info=True)
        pos_list = []

    open_positions = []
    for p in pos_list:
        symbol = p.get("symbol")
        side = (p.get("side") or "").lower()      # "buy" | "sell"
        size = float(p.get("size") or 0)
        entry = float(p.get("avgPrice") or 0)
        mark  = float(p.get("markPrice") or 0)
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
