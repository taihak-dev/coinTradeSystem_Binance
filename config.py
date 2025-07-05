# config.py (개선된 버전)

import os
import sys  # 프로그램 종료를 위해 import
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# --- 1. 공통 설정: 어떤 거래소를 사용할지 먼저 결정 ---
EXCHANGE = os.getenv('EXCHANGE')
if not EXCHANGE:
    # .env 파일에 EXCHANGE 변수가 아예 없는 경우
    print("오류: .env 파일에 'EXCHANGE' 설정이 없습니다. 'upbit' 또는 'binance' 중 하나를 설정해주세요.", file=sys.stderr)
    sys.exit(1)  # 오류 메시지 출력 후 프로그램 즉시 종료

EXCHANGE = EXCHANGE.lower()
print(f"===== 설정 정보 로드 시작 =====")
print(f"선택된 거래소: {EXCHANGE.upper()}")

# --- 2. 거래소별 필수 환경 변수 로드 및 검증 ---
ACCESS_KEY = None
SECRET_KEY = None
SERVER_URL = None
BINANCE_API_KEY = None
BINANCE_API_SECRET = None

if EXCHANGE == 'upbit':
    ACCESS_KEY = os.getenv('UPBIT_OPEN_API_ACCESS_KEY')
    SECRET_KEY = os.getenv('UPBIT_OPEN_API_SECRET_KEY')
    SERVER_URL = os.getenv('UPBIT_OPEN_API_SERVER_URL')

    # all() 함수를 이용해 세 변수 중 하나라도 없거나 비어있는지 확인
    if not all([ACCESS_KEY, SECRET_KEY, SERVER_URL]):
        print(
            "오류: Upbit을 사용하려면 .env 파일에 UPBIT_OPEN_API_ACCESS_KEY, UPBIT_OPEN_API_SECRET_KEY, UPBIT_OPEN_API_SERVER_URL가 모두 필요합니다.",
            file=sys.stderr)
        sys.exit(1)

elif EXCHANGE == 'binance':
    BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
    BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

    if not all([BINANCE_API_KEY, BINANCE_API_SECRET]):
        print("오류: Binance를 사용하려면 .env 파일에 BINANCE_API_KEY와 BINANCE_API_SECRET가 모두 필요합니다.", file=sys.stderr)
        sys.exit(1)

else:
    print(f"오류: .env 파일에 설정된 EXCHANGE 값 '{EXCHANGE}'를 지원하지 않습니다. 'upbit' 또는 'binance'로 설정해주세요.", file=sys.stderr)
    sys.exit(1)

# --- 3. 바이낸스 전용 추가 설정 (바이낸스 선택 시에만 로드) ---
USE_TESTNET = False
DEFAULT_LEVERAGE = 10

if EXCHANGE == 'binance':
    # .env 파일에 BINANCE_TESTNET이 없으면 기본값 'True'를 사용
    USE_TESTNET_STR = os.getenv('BINANCE_TESTNET', 'True')
    USE_TESTNET = USE_TESTNET_STR.lower() in ('true', '1', 't')
    print(f"테스트넷 사용: {USE_TESTNET}")

    try:
        # .env 파일에 DEFAULT_LEVERAGE가 없으면 기본값 '10'을 사용
        leverage_str = os.getenv('DEFAULT_LEVERAGE', '10')
        # 사용자가 값을 비워두거나 숫자가 아닌 값을 넣었을 경우를 대비
        if not leverage_str or not leverage_str.isdigit():
            raise ValueError("레버리지 값은 숫자여야 합니다.")
        DEFAULT_LEVERAGE = int(leverage_str)
        print(f"기본 레버리지: {DEFAULT_LEVERAGE}x")
    except ValueError as e:
        print(f"오류: .env 파일의 DEFAULT_LEVERAGE 값이 올바르지 않습니다 ({e}).", file=sys.stderr)
        sys.exit(1)

print(f"==============================")