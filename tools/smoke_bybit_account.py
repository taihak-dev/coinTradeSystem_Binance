import os
from services.exchange_service import get_accounts

if __name__ == "__main__":
    os.environ["EXCHANGE"] = "bybit"
    data = get_accounts()
    print(data)
