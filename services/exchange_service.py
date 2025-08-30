# services/exchange_service.py
"""
거래소 라우팅 레이어.
- 현재는 Binance만 실제 구현에 위임하고, Bybit는 NotImplemented로 막아 둠.
- 이후 Bybit 구현을 추가해도 상위 로직 변경을 최소화하기 위한 얇은 추상화.
"""
import config
from api.binance.client import get_binance_client as _bn_client

# --- Binance 구현 위임 ---
from api.binance.account import get_accounts as _bn_get_accounts
from api.binance.order import (
    send_order as _bn_send_order,
    cancel_order as _bn_cancel_order,
    get_order_result as _bn_get_order_result,
)
from api.binance.price import (
    get_current_bid_price as _bn_get_bid,
    get_current_ask_price as _bn_get_ask,
    get_minute_candles as _bn_get_minutes,
)


# --- 공통 라우팅 함수들 ---

def get_accounts():
    if config.EXCHANGE == "binance":
        return _bn_get_accounts()
    raise NotImplementedError("get_accounts: Bybit 미구현")


def send_order(market: str, side: str, type: str, volume: float | None = None,
               price: float | None = None, position_side: str = "BOTH",
               closePosition: bool | None = None) -> dict:
    if config.EXCHANGE == "binance":
        return _bn_send_order(market, side, type, volume, price, position_side, closePosition)
    raise NotImplementedError("send_order: Bybit 미구현")


def cancel_order(order_uuid: str, market: str) -> dict:
    if config.EXCHANGE == "binance":
        return _bn_cancel_order(order_uuid, market)
    raise NotImplementedError("cancel_order: Bybit 미구현")


def get_order_result(order_uuid: str, market: str) -> dict:
    if config.EXCHANGE == "binance":
        return _bn_get_order_result(order_uuid, market)
    raise NotImplementedError("get_order_result: Bybit 미구현")


def get_current_bid_price(symbol: str) -> float:
    if config.EXCHANGE == "binance":
        return _bn_get_bid(symbol)
    raise NotImplementedError("get_current_bid_price: Bybit 미구현")


def get_current_ask_price(symbol: str) -> float:
    if config.EXCHANGE == "binance":
        return _bn_get_ask(symbol)
    raise NotImplementedError("get_current_ask_price: Bybit 미구현")


def get_minute_candles(symbol: str, unit: int = 1, to: str | None = None, count: int = 200):
    if config.EXCHANGE == "binance":
        return _bn_get_minutes(symbol, unit=unit, to=to, count=count)
    raise NotImplementedError("get_minute_candles: Bybit 미구현")


def cancel_open_orders(symbol: str) -> None:
    if config.EXCHANGE == "binance":
        client = _bn_client()
        client.cancel_open_orders(symbol=symbol)
        return
    raise NotImplementedError("cancel_open_orders: Bybit 미구현")