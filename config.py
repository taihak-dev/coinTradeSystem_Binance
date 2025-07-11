# config.py
import os
from dotenv import dotenv_values

# .env 파일 로드
_env_vars = dotenv_values()

# 업비트 설정
UPBIT_OPEN_API_ACCESS_KEY = _env_vars.get("UPBIT_OPEN_API_ACCESS_KEY", "")
UPBIT_OPEN_API_SECRET_KEY = _env_vars.get("UPBIT_OPEN_API_SECRET_KEY", "")
UPBIT_OPEN_API_SERVER_URL = _env_vars.get("UPBIT_OPEN_API_SERVER_URL", "https://api.upbit.com")

# 바이낸스 설정
BINANCE_API_KEY = _env_vars.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = _env_vars.get("BINANCE_API_SECRET", "")

# 거래소 선택 (환경 변수 우선, 없으면 기본값 "binance")
EXCHANGE = _env_vars.get("EXCHANGE", "binance").lower()

# 바이낸스 선물 설정
# .env 파일에서 "True" 또는 "False" 문자열을 읽어와서 실제 boolean 값으로 변환
USE_TESTNET = _env_vars.get("BINANCE_TESTNET", "False").lower() == "true" # 수정됨

# BINANCE_DEFAULT_LEVERAGE는 setting.csv에서 개별 코인별로 설정되므로,
# 일반적으로 이 값은 사용되지 않거나, setting.csv에 명시되지 않은 코인에 대한
# fallback 기본값으로만 사용될 수 있습니다. 현재 코드 흐름에서는 setting.csv의
# leverage 값이 우선합니다.
# BINANCE_DEFAULT_LEVERAGE = int(_env_vars.get("BINANCE_DEFAULT_LEVERAGE", "10")) # 필요 없으면 제거 또는 주석 유지

# 기타 공통 설정 (필요시 추가)
# MIN_TRADE_USDT = 5 # 최소 거래 금액 (예: 5 USDT) - buy_entry.py의 5 USDT와 일관성 유지