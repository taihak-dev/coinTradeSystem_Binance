# services/exchange_service.py
from typing import Any, Dict, List, Optional

# ---------- 계정/포지션 ----------
def get_accounts() -> Dict[str, Any]:
    raise NotImplementedError("phase1: interface only")

def get_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    raise NotImplementedError("phase1: interface only")

# ---------- 시세 ----------
def get_current_price(symbol: str) -> Dict[str, float]:
    raise NotImplementedError("phase1: interface only")

def get_minute_candles(symbol: str, to: Optional[str], count: int) -> List[Dict[str, Any]]:
    raise NotImplementedError("phase1: interface only")

# ---------- 주문 ----------
def ensure_trading_env(symbol: str, **kwargs) -> None:
    raise NotImplementedError("phase1: interface only")

def send_order(
    symbol: str,
    side: str,             # 'buy' | 'sell'
    order_type: str,       # 'market' | 'limit'
    qty: float,
    price: Optional[float] = None,
    reduce_only: bool = False,
    time_in_force: str = "GTC",
    client_id: Optional[str] = None,
    close_position: bool = False,
) -> Dict[str, Any]:
    raise NotImplementedError("phase1: interface only")

def cancel_open_orders(symbol: str) -> None:
    raise NotImplementedError("phase1: interface only")

def get_order_result(symbol: str, order_id: str) -> Dict[str, Any]:
    raise NotImplementedError("phase1: interface only")

# ---------- 정밀도/라운딩 ----------
def adjust_price_to_tick(symbol: str, price: float) -> float:
    raise NotImplementedError("phase1: interface only")

def adjust_quantity_to_step(symbol: str, qty: float) -> float:
    raise NotImplementedError("phase1: interface only")
