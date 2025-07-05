# api/binance/client.py
import logging
from binance.um_futures import UMFutures
from binance.error import ClientError
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 기존 인증된 클라이언트 ---
_authenticated_client_instance = None

def get_binance_client():
    """
    API 키로 인증된 클라이언트를 반환합니다. (주문, 잔고 조회 등)
    """
    global _authenticated_client_instance
    if _authenticated_client_instance:
        return _authenticated_client_instance

    # ... (기존 get_binance_client 함수 내용은 그대로 유지) ...
    api_key = config.BINANCE_API_KEY
    api_secret = config.BINANCE_API_SECRET
    testnet = config.USE_TESTNET

    if not api_key or not api_secret:
        logging.error("바이낸스 API Key 또는 Secret이 설정되지 않았습니다. .env 파일을 확인하세요.")
        raise ValueError("Binance API Key/Secret missing")

    try:
        if testnet:
            client = UMFutures(key=api_key, secret=api_secret, base_url="https://testnet.binancefuture.com")
        else:
            client = UMFutures(key=api_key, secret=api_secret)

        client.account()
        logging.info("바이낸스 인증된 클라이언트 연결 성공!")

        try:
            position_mode = client.get_position_mode()
            if not position_mode['dualSidePosition']:
                 client.change_position_mode(dualSidePosition=True)
        except ClientError as e:
            if e.error_code == -4059:
                logging.warning("열려있는 포지션이 있어 포지션 모드를 변경할 수 없습니다. 수동으로 헷지 모드로 변경해주세요.")
            else:
                logging.error(f"포지션 모드 확인/변경 실패: {e.error_message}")

        _authenticated_client_instance = client
        return _authenticated_client_instance

    except ClientError as e:
        logging.error(f"바이낸스 연결 실패 (API 키 확인 필요): {e.status_code} - {e.error_message}")
        raise e
    except Exception as e:
        logging.error(f"알 수 없는 오류로 바이낸스 연결에 실패했습니다: {e}")
        raise e


# --- 신규: API 키가 필요 없는 공용(Public) 클라이언트 ---
_public_client_instance = None

def get_public_binance_client():
    """
    API 키 없이 연결하는 공용 클라이언트를 반환합니다. (캔들, 현재가 조회 등)
    """
    global _public_client_instance
    if _public_client_instance:
        return _public_client_instance

    testnet = config.USE_TESTNET

    try:
        if testnet:
            client = UMFutures(base_url="https://testnet.binancefuture.com")
        else:
            client = UMFutures()

        # 연결 테스트 (인증 불필요한 ping 사용)
        client.ping()
        logging.info("바이낸스 공용 클라이언트 연결 성공!")
        _public_client_instance = client
        return _public_client_instance

    except Exception as e:
        logging.error(f"바이낸스 공용 클라이언트 연결 실패: {e}")
        raise e