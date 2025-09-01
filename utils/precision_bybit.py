# utils/precision_bybit.py
from decimal import Decimal, ROUND_DOWN

def _floor_to_step(value: float, step: str) -> str:
    # step 문자열(예: "0.001") 기준으로 내림 맞춤 후 문자열 반환
    d = Decimal(str(value))
    s = Decimal(step)
    return str((d // s) * s)

def adjust_price_to_tick(price: float, tick_size: str, min_price: str) -> str:
    p = Decimal(_floor_to_step(price, tick_size))
    return str(max(p, Decimal(min_price)))

def adjust_qty_to_step(qty: float, qty_step: str, min_qty: str) -> str:
    q = Decimal(_floor_to_step(qty, qty_step))
    return str(max(q, Decimal(min_qty)))
