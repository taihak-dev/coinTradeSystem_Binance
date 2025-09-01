# tools/smoke_bybit_order.py
import os
os.environ["EXCHANGE"] = "bybit"
from dotenv import load_dotenv
load_dotenv()

import importlib, time
import config
importlib.reload(config)

from services.exchange_service import (
    get_current_bid_price, get_current_ask_price,
    send_order, get_order_result, cancel_open_orders
)
from api.bybit.market import get_instrument_filters
from utils.precision_bybit import adjust_price_to_tick, adjust_qty_to_step
from decimal import Decimal, ROUND_UP

SYMBOL = "MNTUSDT"
ENABLE_PLACE = True  # ← 기본 OFF. True로 바꿔야 실제 주문

def main():
    print("EXCHANGE =", config.EXCHANGE, "BYBIT_TESTNET =", config.BYBIT_TESTNET)

    if not ENABLE_PLACE:
        print("DRY-RUN: 주문 전송은 비활성화되어 있습니다. ENABLE_PLACE=True로 변경 시 발주합니다.")
        return

    # 1) 메타(틱/스텝) 조회
    meta = get_instrument_filters(SYMBOL)
    tick = meta["tickSize"]; min_price = meta["minPrice"]
    step = meta["qtyStep"];  min_qty   = meta["minOrderQty"]
    print("META:", meta)

    # 2) 현재 시세
    bid = get_current_bid_price(SYMBOL)
    ask = get_current_ask_price(SYMBOL)
    print("BID/ASK:", bid, ask)

    # 3) 체결되지 않을 가격으로 지정가 BUY (예: bid의 50% 가격)
    raw_qty   = 10     # 아주 소량
    raw_price = bid * 0.5 # 시장가보다 훨씬 낮게 → 체결 방지

    qty = adjust_qty_to_step(raw_qty, step, min_qty)
    price = adjust_price_to_tick(raw_price, tick, min_price)
    print("ADJUSTED qty/price:", qty, price)

    def ceil_to_step(value: float | str, step: str) -> str:
        d = Decimal(str(value))
        s = Decimal(step)
        m = (d / s).to_integral_value(rounding=ROUND_UP)
        return str(m * s)

    # 4) 주문 전송
    resp = send_order(
        SYMBOL, side="BUY", order_type="LIMIT",
        quantity = float(qty), price = float(price)
    )
    oid = resp.get("uuid")
    print("PLACE:", resp)

    # 5) 상태 조회(짧게 2~3회 확인)
    for _ in range(3):
        res = get_order_result(oid, SYMBOL)
        print("STATUS:", res)
        time.sleep(0.8)

    # 6) 미체결 전체 취소
    cancel_open_orders(SYMBOL)
    print("CANCEL OPEN ORDERS: done")

if __name__ == "__main__":
    main()
