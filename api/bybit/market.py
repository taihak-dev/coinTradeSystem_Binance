# api/bybit/market.py
from api.bybit.client import get_bybit_client

def get_instrument_filters(symbol: str) -> dict:
    """
    Bybit v5 instruments-info로 심볼 메타(틱/스텝/최소수량 등) 조회.
    반환 예: {"tickSize":"0.1","minPrice":"0.1","qtyStep":"0.001","minOrderQty":"0.001"}
    """
    http = get_bybit_client()
    resp = http.get_instruments_info(category="linear", symbol= symbol)
    item = (resp.get("result",{}) or {}).get("list",[{}])[0]
    price_filter = item.get("priceFilter", {}) or {}
    lot_filter   = item.get("lotSizeFilter", {}) or {}
    return {
        "tickSize":    str(price_filter.get("tickSize", "0")),
        "minPrice":    str(price_filter.get("minPrice", "0")),
        "qtyStep":     str(lot_filter.get("qtyStep", "0")),
        "minOrderQty": str(lot_filter.get("minOrderQty", "0")),
    }
