# api/bybit/client.py
import config
from pybit.unified_trading import HTTP

_public_client = None
_private_client = None

def get_public_bybit_client() -> HTTP:
    global _public_client
    if _public_client:
        return _public_client
    base = "https://api-demo.bybit.com" if config.BYBIT_TESTNET else "https://api.bybit.com"
    _public_client = HTTP(testnet=config.BYBIT_TESTNET, api_key=None, api_secret=None, endpoint=base)
    return _public_client

def get_bybit_client() -> HTTP:
    global _private_client
    if _private_client:
        return _private_client
    base = "https://api-demo.bybit.com" if config.BYBIT_TESTNET else "https://api.bybit.com"
    _private_client = HTTP(
        testnet=config.BYBIT_TESTNET,
        api_key=config.BYBIT_API_KEY,
        api_secret=config.BYBIT_API_SECRET,
        endpoint=base,
    )
    return _private_client
