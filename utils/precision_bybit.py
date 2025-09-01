# utils/precision_bybit.py

from decimal import Decimal, ROUND_DOWN, ROUND_UP

def _floor_to_step(value: float | str, step: str) -> str:
    d = Decimal(str(value))
    s = Decimal(step)
    return str((d // s) * s)

def adjust_price_to_tick(price: float | str, tick_size: str, min_price: str) -> str:
    p = Decimal(_floor_to_step(price, tick_size))
    return str(max(p, Decimal(min_price)))

def adjust_qty_to_step(qty: float | str, qty_step: str, min_qty: str) -> str:
    q = Decimal(_floor_to_step(qty, qty_step))
    return str(max(q, Decimal(min_qty)))

def _ceil_to_step(value: float | str, step: str) -> str:
    d = Decimal(str(value))
    s = Decimal(step)
    m = (d / s).to_integral_value(rounding=ROUND_UP)
    return str(m * s)

def ensure_min_notional(price: float | str,
                        qty: float | str,
                        min_notional: float | str,
                        qty_step: str,
                        min_qty: str) -> str:
    """
    가격*수량이 최소 주문가치(min_notional: 메인넷 5, 테스트넷 1 USDT)를
    만족하도록 '수량'을 step에 맞춰 '올림' 보정해 문자열로 반환.
    """
    p = Decimal(str(price))
    q = Decimal(str(qty))
    target = Decimal(str(min_notional))

    if q * p >= target:
        return str(max(q, Decimal(min_qty)))

    need = target / p
    q_up = Decimal(_ceil_to_step(need, qty_step))
    if q_up < Decimal(min_qty):
        q_up = Decimal(min_qty)
    return str(q_up)
