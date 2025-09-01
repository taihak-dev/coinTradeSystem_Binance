# api/bybit/order.py
import logging
from typing import Any, Dict
from pybit.exceptions import FailedRequestError
from api.bybit.client import get_bybit_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def send_order(market: str, side: str, order_type: str, quantity: float,
               price: float | None = None, reduce_only: bool = False, close_position: bool = False) -> dict:
    """
    공용 시그니처(바이낸스와 동일)에 맞춰 Bybit v5 주문 전송.
    - side: "BUY" | "SELL"
    - order_type: "MARKET" | "LIMIT"
    - quantity: float (서버에서 lotSize에 맞게 반올림/검증 필요)
    - price: 지정가일 때만 사용
    """
    http = get_bybit_client()

    from api.bybit.market import get_instrument_filters
    from utils.precision_bybit import adjust_price_to_tick, adjust_qty_to_step, ensure_min_notional
    import config

    meta = get_instrument_filters(market)
    tick = meta["tickSize"]
    min_price = meta["minPrice"]
    step = meta["qtyStep"]
    min_qty = meta["minOrderQty"]

    ord_type = "Market" if order_type.upper() == "MARKET" else "Limit"
    quantity_s = adjust_qty_to_step(quantity, step, min_qty)  # 문자열
    price_s = None

    if ord_type == "Limit" and price is not None:
        price_s = adjust_price_to_tick(price, tick, min_price)  # 문자열
        min_notional = 1.0 if config.BYBIT_TESTNET else 5.0
        quantity_s = ensure_min_notional(price_s, quantity_s, min_notional, step, min_qty)  # 문자열

    side_bybit = "Buy" if str(side).upper() == "BUY" else "Sell"

    params: Dict[str, Any] = {
        "category": "linear",
        "symbol": market,
        "side": side_bybit,
        "orderType": ord_type,
        "qty": str(quantity_s),
        "timeInForce": "IOC" if ord_type == "Market" else "GTC",
    }
    if price_s is not None and ord_type == "Limit":
        params["price"] = str(price_s)
    if reduce_only:
        params["reduceOnly"] = True
    if close_position:
        params["closeOnTrigger"] = True

    resp = http.place_order(**params)
    oid = ((resp.get("result") or {}).get("orderId")) or ""
    return {"uuid": oid, "state": "wait", "market": market}


def get_order_result(order_uuid: str, market: str) -> dict:
    """
    공용 리턴 스키마로 매핑:
      { uuid, state("wait|done|cancel|error|unknown"), market, avg_price, executed_qty, cum_quote }
    """
    http = get_bybit_client()

    # 1) 미체결에서 조회
    resp = http.get_open_orders(category="linear", symbol=market, orderId=order_uuid)
    lst = (resp.get("result", {}) or {}).get("list", [])
    # 2) 없으면 과거 주문에서 조회
    if not lst:
        try:
            hist = http.get_order_history(category="linear", symbol=market, orderId=order_uuid)
            lst = (hist.get("result", {}) or {}).get("list", [])
        except FailedRequestError:
            lst = []

    item = lst[0] if lst else {}
    status = (item.get("orderStatus") or "").upper()  # NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, ...
    status_key = status.replace(" ", "").replace("_", "")
    state_map = {
        "NEW": "wait",
        "PARTIALLYFILLED": "wait",
        "FILLED": "done",
        "CANCELED": "cancel",
        "CANCELLED": "cancel",  # ← 이 줄 추가
        "REJECTED": "error",
    }
    state = state_map.get(status_key, "unknown")

    avg_price    = float(item.get("avgPrice") or 0)
    executed_qty = float(item.get("cumExecQty") or 0)
    cum_quote    = float(item.get("cumExecValue") or 0)

    return {
        "uuid": order_uuid,
        "state": state,
        "market": market,
        "avg_price": avg_price,
        "executed_qty": executed_qty,
        "cum_quote": cum_quote,
    }


def cancel_open_orders(symbol: str) -> None:
    """심볼 기준 미체결 일괄 취소"""
    http = get_bybit_client()
    http.cancel_all_orders(category="linear", symbol=symbol)
