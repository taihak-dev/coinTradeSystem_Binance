# utils/price_utils.py

def get_tick_size(price: float, market: str = "KRW", ticker: str = "") -> float:
    special_tickers_100_1000 = {
        "ADA", "ALGO", "BLUR", "CELO", "ELF", "EOS", "GRS", "GRT", "ICX",
        "MANA", "MINA", "POL", "SAND", "SEI", "STG", "TRX"
    }

    if market != "KRW":
        raise ValueError("현재는 KRW 마켓만 지원됩니다.")

    # 특이 마켓 우선 체크
    if 100 <= price < 1000 and ticker in special_tickers_100_1000:
        return 1
    if 1000 <= price < 10000 and ticker in {"USDT", "USDC"}:
        return 0.5

    # 일반 호가단위 규칙
    if price >= 2000000:
        return 1000
    elif price >= 1000000:
        return 500
    elif price >= 500000:
        return 100
    elif price >= 100000:
        return 50
    elif price >= 10000:
        return 10
    elif price >= 1000:
        return 1
    elif price >= 100:
        return 0.1
    elif price >= 10:
        return 0.01
    elif price >= 1:
        return 0.001
    elif price >= 0.1:
        return 0.0001
    elif price >= 0.01:
        return 0.00001
    elif price >= 0.001:
        return 0.000001
    elif price >= 0.0001:
        return 0.0000001
    else:
        return 0.00000001


def adjust_price_to_tick(price: float, market: str = "KRW", ticker: str = "") -> float:
    tick_size = get_tick_size(price, market, ticker)
    return float(f"{round(price / tick_size) * tick_size:.10f}")
