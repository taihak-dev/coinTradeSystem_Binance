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
RUN_INTERVAL_SECONDS = int(os.getenv("RUN_INTERVAL_SECONDS", "10")) # 메인 루프 실행 주기 (초)

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
EXCHANGE = os.getenv("EXCHANGE", _env_vars.get("EXCHANGE", "binance")).strip('"').lower()

# === Bybit 설정 ===
BYBIT_API_KEY    = (os.getenv("BYBIT_API_KEY", "")    or "").strip().strip('"')
BYBIT_API_SECRET = (os.getenv("BYBIT_API_SECRET", "") or "").strip().strip('"')
BYBIT_TESTNET = (os.getenv("BYBIT_TESTNET", None) or _env_vars.get("BYBIT_TESTNET", "False")).strip('"').lower() == "true"



# 바이낸스 선물 설정
# .env 파일에서 "True" 또는 "False" 문자열을 읽어와서 실제 boolean 값으로 변환
USE_TESTNET = _env_vars.get("BINANCE_TESTNET", "False").lower() == "true" # 수정됨

