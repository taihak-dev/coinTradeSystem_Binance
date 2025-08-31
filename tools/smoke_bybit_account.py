# tools/smoke_bybit_account.py
import os
os.environ["EXCHANGE"] = "bybit"   # 1) 가장 먼저
from dotenv import load_dotenv
load_dotenv()                      # 2) .env 같이 쓴다면

import importlib
import config                      # 3) env 반영된 상태로 config 로드
importlib.reload(config)           # 4) 보수적으로 재적용

from services.exchange_service import get_accounts

if __name__ == "__main__":
    print("EXCHANGE =", config.EXCHANGE,
          "BYBIT_TESTNET =", getattr(config, "BYBIT_TESTNET", None),
          "KEY_SET =", bool(getattr(config, "BYBIT_API_KEY", "")),
          "SECRET_SET =", bool(getattr(config, "BYBIT_API_SECRET", "")))
    data = get_accounts()
    print(data)
