import pprint
from api.bybit.client import get_bybit_client
import logging

# 로깅 기본 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_balance_check():
    """
    Bybit API의 get_wallet_balance를 두 가지 다른 accountType으로 호출하여
    그 응답 결과를 직접 확인하기 위한 진단 스크립트입니다.
    """
    print("=" * 50)
    print("BYBIT 잔고 조회 API 직접 호출 테스트를 시작합니다.")
    print("=" * 50)

    try:
        # 1. API 클라이언트 가져오기
        client = get_bybit_client()
        print("\n✅ API 클라이언트 연결 성공!")

        # 2. accountType='UNIFIED'로 잔고 조회
        print("\n1️⃣ accountType='UNIFIED' (통합계좌)로 잔고를 조회합니다...")
        try:
            unified_balance_info = client.get_wallet_balance(accountType="UNIFIED")
            print("--- UNIFIED 계좌 응답 결과 ---")
            pprint.pprint(unified_balance_info)
            print("-" * 30)
        except Exception as e:
            print(f"❌ 'UNIFIED' 계좌 조회 중 오류 발생: {e}")

        # 3. accountType='CONTRACT'로 잔고 조회
        print("\n2️⃣ accountType='CONTRACT' (선물계좌)로 잔고를 조회합니다...")
        try:
            contract_balance_info = client.get_wallet_balance(accountType="CONTRACT")
            print("--- CONTRACT 계좌 응답 결과 ---")
            pprint.pprint(contract_balance_info)
            print("-" * 30)
        except Exception as e:
            print(f"❌ 'CONTRACT' 계좌 조회 중 오류 발생: {e}")

    except Exception as e:
        print(f"\n🚨 테스트 중 심각한 오류 발생: {e}")

    print("\n=" * 50)
    print("테스트가 완료되었습니다. 위 출력 결과를 모두 복사하여 전달해주세요.")
    print("=" * 50)


if __name__ == "__main__":
    run_balance_check()