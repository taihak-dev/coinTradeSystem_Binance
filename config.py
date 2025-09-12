# config.py
import os
from dotenv import load_dotenv

# .env 파일의 환경 변수를 로드합니다.
load_dotenv()

# --- 텔레그램 알림 설정 ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- 거래소 선택 ---
# 봇을 실행할 거래소를 선택합니다: "binance" 또는 "bybit"
EXCHANGE = os.getenv("EXCHANGE", "binance").lower()

# --- 바이낸스 선물 설정 ---
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "False").lower() in ('true', '1', 't')

# --- 👇👇👇 바이빗 선물 설정 (신규 추가) 👇👇👇 ---
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "False").lower() in ('true', '1', 't')
# --- 👆👆👆 여기까지 추가 --- 👆👆👆

# --- 실행 주기 및 알림 설정 ---
RUN_INTERVAL_SECONDS = int(os.getenv("RUN_INTERVAL_SECONDS", "10"))
HEALTH_CHECK_INTERVAL_SECONDS = int(os.getenv("HEALTH_CHECK_INTERVAL_SECONDS", "3600"))
POSITION_SUMMARY_INTERVAL_SECONDS = int(os.getenv("POSITION_SUMMARY_INTERVAL_SECONDS", "21600"))

# --- 청산 위험 알림 임계치 설정 ---
LIQUIDATION_WARNING_PCT_1 = float(os.getenv("LIQUIDATION_WARNING_PCT_1", "0.10"))
LIQUIDATION_WARNING_PCT_2 = float(os.getenv("LIQUIDATION_WARNING_PCT_2", "0.05"))

# (참고) 기존 업비트 설정은 사용되지 않지만, 호환성을 위해 남겨둡니다.
UPBIT_OPEN_API_ACCESS_KEY = os.getenv("UPBIT_OPEN_API_ACCESS_KEY")
UPBIT_OPEN_API_SECRET_KEY = os.getenv("UPBIT_OPEN_API_SECRET_KEY")
UPBIT_OPEN_API_SERVER_URL = os.getenv("UPBIT_OPEN_API_SERVER_URL", "https://api.upbit.com")