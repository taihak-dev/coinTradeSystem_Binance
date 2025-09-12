# api/bybit/client.py

import logging
import config
from pybit.unified_trading import HTTP

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 클라이언트 인스턴스를 저장할 변수 (싱글톤 패턴)
_bybit_client_instance = None


def get_bybit_client():
    """
    인증된 Bybit 클라이언트 세션을 반환합니다.
    싱글톤 패턴을 사용하여, 이미 생성된 인스턴스가 있으면 재사용합니다.
    """
    global _bybit_client_instance
    if _bybit_client_instance:
        return _bybit_client_instance

    # config.py에서 API 키와 테스트넷 설정 로드
    api_key = config.BYBIT_API_KEY
    api_secret = config.BYBIT_API_SECRET
    testnet = config.BYBIT_TESTNET

    if not api_key or not api_secret:
        logging.error("❌ Bybit API Key 또는 Secret이 설정되지 않았습니다. .env 파일을 확인하세요.")
        raise ValueError("Bybit API Key/Secret missing.")

    try:
        logging.info(f"🌐 Bybit 클라이언트 연결 시도 중... (Testnet: {testnet})")

        # pybit 라이브러리의 HTTP 세션 객체 생성
        session = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
        )

        # --- 👇👇👇 여기가 수정된 부분입니다 👇👇👇 ---
        # 연결 테스트 (API 키 유효성 검사)
        # get_api_key_info -> get_api_key_information 으로 수정
        session.get_api_key_information()
        # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆

        logging.info("✅ Bybit 클라이언트 연결 및 인증 성공!")

        _bybit_client_instance = session
        return _bybit_client_instance

    except Exception as e:
        logging.error(f"❌ Bybit 클라이언트 연결 실패: {e}", exc_info=True)
        raise