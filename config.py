# config.py
import os
from dotenv import dotenv_values
from dotenv import load_dotenv # <-- 추가
load_dotenv() # <-- 추가

# .env 파일 로드
_env_vars = dotenv_values()

# 텔레그램 알림 설정 (추가)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN") # BotFather에서 받은 토큰
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID")     # @get_id_bot 에서 받은 Chat ID

# 기타 설정
RUN_INTERVAL_SECONDS = int(os.getenv("RUN_INTERVAL_SECONDS", "5")) # 메인 루프 실행 주기 (초)

# 알림 주기 설정
HEALTH_CHECK_INTERVAL_SECONDS = int(os.getenv("HEALTH_CHECK_INTERVAL_SECONDS", "3600")) # 봇 정상 동작 알림 주기 (1시간)
POSITION_SUMMARY_INTERVAL_SECONDS = int(os.getenv("POSITION_SUMMARY_INTERVAL_SECONDS", "21600")) # 포지션 현황 요약 알림 주기 (6시간)

# 청산 위험 알림 임계치 설정
LIQUIDATION_WARNING_PCT_1 = float(os.getenv("LIQUIDATION_WARNING_PCT_1", "0.10")) # 1단계 경고: 청산까지 10% 남았을 때
LIQUIDATION_WARNING_PCT_2 = float(os.getenv("LIQUIDATION_WARNING_PCT_2", "0.05")) # 2단계 경고: 청산까지 5% 남았을 때

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