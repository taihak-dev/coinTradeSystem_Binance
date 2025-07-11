# api/binance/client.py
import logging
from binance.um_futures import UMFutures
from binance.error import ClientError
import config

# 로깅 설정 (기존에 이미 잘 되어 있음)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

_authenticated_client_instance = None
_public_client_instance = None


def get_binance_client():
    """
    API 키와 Secret 키로 인증된 바이낸스 선물(UMFutures) 클라이언트를 반환합니다.
    주문, 잔고 조회 등 인증이 필요한 작업에 사용됩니다.
    클라이언트는 싱글톤 패턴으로 한 번만 초기화됩니다.
    """
    global _authenticated_client_instance
    if _authenticated_client_instance:
        logging.debug("인증된 바이낸스 클라이언트 재사용.") # 디버그 로그 추가
        return _authenticated_client_instance

    api_key = config.BINANCE_API_KEY
    api_secret = config.BINANCE_API_SECRET
    testnet = config.USE_TESTNET

    if not api_key or not api_secret:
        logging.error("❌ 바이낸스 API Key 또는 Secret이 설정되지 않았습니다. .env 파일을 확인하세요.")
        raise ValueError("Binance API Key/Secret missing. Please check your .env file.")

    try:
        if testnet:
            logging.info("🌐 바이낸스 테스트넷 인증 클라이언트에 연결 시도 중...")
            # options 파라미터를 통해 추가 설정 가능 (예: default_timeout)
            client = UMFutures(key=api_key, secret=api_secret, base_url="https://testnet.binancefuture.com")
        else:
            logging.info("🌐 바이낸스 실거래 인증 클라이언트에 연결 시도 중...")
            client = UMFutures(key=api_key, secret=api_secret)

        # Note: python-binance library typically handles rate limiting internally
        #       by adding small delays or retries for standard endpoints.
        #       Explicit time.sleep() in calling functions (e.g., price.py, order.py)
        #       is often more effective for frequent calls.

        # API 키 유효성 검증 및 연결 테스트
        client.account() # 계좌 정보 조회로 API 키 유효성 검증
        logging.info("✅ 바이낸스 인증된 클라이언트 연결 성공!")

        # 포지션 모드 설정 (헷지 모드: 롱/숏 동시 보유 가능)
        try:
            # 현재 포지션 모드 조회
            position_mode = client.get_position_mode()
            if not position_mode['dualSidePosition']:
                # 헷지 모드가 아니라면 변경 시도
                logging.info("ℹ️ 현재 포지션 모드가 헷지 모드(Dual-Side)가 아닙니다. 변경을 시도합니다.")
                client.change_position_mode(dualSidePosition=True)
                logging.info("✅ 포지션 모드를 헷지 모드(Dual-Side)로 성공적으로 변경했습니다.")
            else:
                logging.info("✅ 포지션 모드가 이미 헷지 모드(Dual-Side)입니다.")
        except ClientError as e:
            if e.error_code == -4059:
                logging.warning("⚠️ 열려있는 포지션이 있어 포지션 모드를 변경할 수 없습니다. 수동으로 헷지 모드로 변경해주세요.")
            else:
                logging.error(f"❌ 포지션 모드 확인/변경 실패 (ClientError: {e.error_code}): {e.error_message}")
                raise e # 다른 클라이언트 에러는 다시 발생시킴
        except Exception as e:
            logging.error(f"❌ 포지션 모드 설정 중 예상치 못한 오류 발생: {e}")
            raise e

        _authenticated_client_instance = client
        return _authenticated_client_instance

    except ClientError as e:
        logging.error(f"❌ 바이낸스 인증 클라이언트 연결 실패 (API 키/IP 화이트리스트 확인 필요): Status={e.status_code}, Code={e.error_code}, Msg={e.error_message}")
        raise e
    except Exception as e:
        logging.error(f"❌ 알 수 없는 오류로 바이낸스 인증 클라이언트 연결에 실패했습니다: {e}", exc_info=True)
        raise e


def get_public_binance_client():
    """
    API 키가 필요 없는 공용 바이낸스 선물(UMFutures) 클라이언트를 반환합니다.
    캔들, 현재가 조회 등 인증이 불필요한 작업에 사용됩니다.
    클라이언트는 싱글톤 패턴으로 한 번만 초기화됩니다.
    """
    global _public_client_instance
    if _public_client_instance:
        logging.debug("공용 바이낸스 클라이언트 재사용.") # 디버그 로그 추가
        return _public_client_instance

    testnet = config.USE_TESTNET

    try:
        if testnet:
            logging.info("🌐 바이낸스 테스트넷 공용 클라이언트에 연결 시도 중...")
            client = UMFutures(base_url="https://testnet.binancefuture.com")
        else:
            logging.info("🌐 바이낸스 실거래 공용 클라이언트에 연결 시도 중...")
            client = UMFutures()

        # 연결 테스트 (인증 불필요한 ping 사용)
        client.ping()
        logging.info("✅ 바이낸스 공용 클라이언트 연결 성공!")
        _public_client_instance = client
        return _public_client_instance

    except Exception as e:
        logging.error(f"❌ 바이낸스 공용 클라이언트 연결 실패: {e}", exc_info=True)
        raise e