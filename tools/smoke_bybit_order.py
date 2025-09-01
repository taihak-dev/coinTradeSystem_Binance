# tools/smoke_bybit_order.py
import os
os.environ["EXCHANGE"] = "bybit"
from dotenv import load_dotenv
load_dotenv()

import importlib, config
importlib.reload(config)

from services.exchange_service import send_order, get_order_result, cancel_open_orders

SYMBOL = "MNTUSDT"
ENABLE_PLACE = False  # ← 기본은 안전하게 OFF

def main():
    print("EXCHANGE =", config.EXCHANGE, "BYBIT_TESTNET =", config.BYBIT_TESTNET)

    if not ENABLE_PLACE:
        print("DRY-RUN: 주문 전송은 비활성화되어 있습니다. ENABLE_PLACE=True로 변경 시 발주합니다.")
        return

    # 1) (예시) 시장가 소량 매수
    resp = send_order(SYMBOL, side="BUY", order_type="MARKET", quantity=10)
    oid = resp.get("uuid")
    print("PLACE:", resp)

    # 2) 상태 조회
    res = get_order_result(oid, SYMBOL)
    print("STATUS:", res)

    # 3) (옵션) 심볼 기준 미체결 일괄취소
    cancel_open_orders(SYMBOL)
    print("CANCEL OPEN ORDERS: done")

if __name__ == "__main__":
    main()
