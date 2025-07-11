# tests/test_generate_buy_orders.py

import pandas as pd
from strategy.casino_strategy import generate_buy_orders
from datetime import datetime, timedelta

def run_generate_buy_orders_test():
    print("[TEST] generate_buy_orders 테스트 시작")

    # -------- 1. setting.csv 시뮬레이션 (테스트용 간소화) --------
    setting_df = pd.DataFrame([
        {
            "market": "XRPUSDT",
            "unit_size": 100,
            "small_flow_pct": 0.01, # 1% 하락시 매수
            "small_flow_units": 2,  # 총 2단계
            "large_flow_pct": 0.03, # 3% 하락시 매수
            "large_flow_units": 1,  # 총 1단계
            "take_profit_pct": 0.02
        }
    ])

    # -------- 2. 시나리오별 현재 가격 및 기대 결과 정의 --------

    # --- 시나리오 1: 최초 주문 생성 (로그가 비어있을 때) ---
    print("\n--- 시나리오 1: 최초 주문 생성 ---")
    current_prices_s1 = {"XRPUSDT": 0.500}
    buy_log_df_s1 = pd.DataFrame(columns=[
        "time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"
    ])
    updated_df_s1 = generate_buy_orders(setting_df, buy_log_df_s1, current_prices_s1)
    print("Scenario 1 Result:\n", updated_df_s1[['market', 'buy_type', 'target_price', 'buy_units', 'filled']])

    # 검증: initial, small_flow (1, 2), large_flow (1) 주문이 생성되었는지
    assert len(updated_df_s1) == 4, "❌ 시나리오 1: 총 4개의 초기 주문이 생성되어야 합니다."
    assert updated_df_s1[updated_df_s1['buy_type'] == 'initial']['target_price'].iloc[0] == 0.500, "❌ 시나리오 1: initial 가격 불일치"
    assert updated_df_s1[(updated_df_s1['buy_type'] == 'small_flow') & (updated_df_s1['buy_units'] == 1)]['target_price'].iloc[0] == round(0.500 * (1 - 0.01 * 1), 8), "❌ 시나리오 1: small_flow 1 가격 불일치"
    assert updated_df_s1[(updated_df_s1['buy_type'] == 'small_flow') & (updated_df_s1['buy_units'] == 2)]['target_price'].iloc[0] == round(0.500 * (1 - 0.01 * 2), 8), "❌ 시나리오 1: small_flow 2 가격 불일치"
    assert updated_df_s1[(updated_df_s1['buy_type'] == 'large_flow') & (updated_df_s1['buy_units'] == 1)]['target_price'].iloc[0] == round(0.500 * (1 - 0.03 * 1), 8), "❌ 시나리오 1: large_flow 1 가격 불일치"
    assert all(updated_df_s1['filled'] == 'update'), "❌ 시나리오 1: 모든 주문의 filled 상태가 'update'여야 합니다."
    print("✅ 시나리오 1 통과: 초기 주문 생성 확인")


    # --- 시나리오 2: initial 주문 완료 후, 다음 small_flow/large_flow 주문 생성 ---
    # last_filled_price는 initial 주문의 target_price가 될 것임
    print("\n--- 시나리오 2: initial 완료 후 다음 flow 주문 생성 ---")
    current_prices_s2 = {"XRPUSDT": 0.490} # last_filled_price 0.500 기준 1% 하락한 가격
    buy_log_df_s2 = pd.DataFrame([
        {"time": "2025-01-01", "market": "XRPUSDT", "target_price": 0.500, "buy_amount": 100, "buy_units": 0, "buy_type": "initial", "buy_uuid": "uuid_init", "filled": "done"}
    ])
    updated_df_s2 = generate_buy_orders(setting_df, buy_log_df_s2, current_prices_s2)
    print("Scenario 2 Result:\n", updated_df_s2[['market', 'buy_type', 'target_price', 'buy_units', 'filled']])

    # 검증: 기존 initial 주문 + 새로운 small_flow (1, 2), large_flow (1) 주문이 생성되었는지
    assert len(updated_df_s2) == 4, "❌ 시나리오 2: 총 4개의 주문이 있어야 합니다 (initial + 3 flow)."
    assert any((updated_df_s2['buy_type'] == 'small_flow') & (updated_df_s2['buy_units'] == 1) & (updated_df_s2['filled'] == 'update')), "❌ 시나리오 2: small_flow 1 주문 없음 또는 상태 오류"
    assert any((updated_df_s2['buy_type'] == 'small_flow') & (updated_df_s2['buy_units'] == 2) & (updated_df_s2['filled'] == 'update')), "❌ 시나리오 2: small_flow 2 주문 없음 또는 상태 오류"
    assert any((updated_df_s2['buy_type'] == 'large_flow') & (updated_df_s2['buy_units'] == 1) & (updated_df_s2['filled'] == 'update')), "❌ 시나리오 2: large_flow 1 주문 없음 또는 상태 오류"
    assert updated_df_s2[(updated_df_s2['buy_type'] == 'small_flow') & (updated_df_s2['buy_units'] == 1)]['target_price'].iloc[0] == round(0.500 * (1 - 0.01 * 1), 8), "❌ 시나리오 2: small_flow 1 가격 불일치"
    print("✅ 시나리오 2 통과: initial 완료 후 flow 주문 생성 확인")


    # --- 시나리오 3: 이미 'wait' 또는 'update' 상태인 flow 주문이 있을 때 중복 생성 방지 ---
    print("\n--- 시나리오 3: 기존 flow 주문 있을 때 중복 생성 방지 ---")
    current_prices_s3 = {"XRPUSDT": 0.490} # last_filled_price 0.500 기준 1% 하락한 가격
    buy_log_df_s3 = pd.DataFrame([
        {"time": "2025-01-01", "market": "XRPUSDT", "target_price": 0.500, "buy_amount": 100, "buy_units": 0, "buy_type": "initial", "buy_uuid": "uuid_init", "filled": "done"},
        {"time": "2025-01-02", "market": "XRPUSDT", "target_price": round(0.500 * (1 - 0.01 * 1), 8), "buy_amount": 100, "buy_units": 1, "buy_type": "small_flow", "buy_uuid": "uuid_s1", "filled": "wait"},
        {"time": "2025-01-03", "market": "XRPUSDT", "target_price": round(0.500 * (1 - 0.03 * 1), 8), "buy_amount": 100, "buy_units": 1, "buy_type": "large_flow", "buy_uuid": "uuid_l1", "filled": "update"}
    ])
    updated_df_s3 = generate_buy_orders(setting_df, buy_log_df_s3, current_prices_s3)
    print("Scenario 3 Result:\n", updated_df_s3[['market', 'buy_type', 'target_price', 'buy_units', 'filled']])

    # 검증: 새로운 주문이 추가되지 않고 기존 주문만 유지되는지 확인 (small_flow 2단계만 추가되어야 함)
    assert len(updated_df_s3) == 4, "❌ 시나리오 3: 기존 주문 + small_flow 2단계 (총 4개)만 있어야 합니다."
    assert not any((updated_df_s3['buy_type'] == 'small_flow') & (updated_df_s3['buy_units'] == 1) & (updated_df_s3['filled'] == 'update') & (updated_df_s3.duplicated(['market', 'buy_type', 'buy_units'], keep=False))), "❌ 시나리오 3: small_flow 1단계 중복 생성됨"
    assert not any((updated_df_s3['buy_type'] == 'large_flow') & (updated_df_s3['buy_units'] == 1) & (updated_df_s3['filled'] == 'update') & (updated_df_s3.duplicated(['market', 'buy_type', 'buy_units'], keep=False))), "❌ 시나리오 3: large_flow 1단계 중복 생성됨"
    assert any((updated_df_s3['buy_type'] == 'small_flow') & (updated_df_s3['buy_units'] == 2) & (updated_df_s3['filled'] == 'update')), "❌ 시나리오 3: small_flow 2단계 주문이 생성되어야 합니다."
    print("✅ 시나리오 3 통과: 기존 flow 주문 있을 때 중복 생성 방지 확인")


    # --- 시나리오 4: 모든 small_flow/large_flow 주문이 'done' 상태일 때 다음 단계 생성 ---
    print("\n--- 시나리오 4: 모든 flow 주문 'done'일 때 다음 단계 생성 ---")
    # last_filled_price는 initial의 0.500
    current_prices_s4 = {"XRPUSDT": 0.475} # small_flow 2단계보다 더 낮은 가격
    buy_log_df_s4 = pd.DataFrame([
        {"time": "2025-01-01", "market": "XRPUSDT", "target_price": 0.500, "buy_amount": 100, "buy_units": 0, "buy_type": "initial", "buy_uuid": "uuid_init", "filled": "done"},
        {"time": "2025-01-02", "market": "XRPUSDT", "target_price": round(0.500 * (1 - 0.01 * 1), 8), "buy_amount": 100, "buy_units": 1, "buy_type": "small_flow", "buy_uuid": "uuid_s1", "filled": "done"},
        {"time": "2025-01-03", "market": "XRPUSDT", "target_price": round(0.500 * (1 - 0.01 * 2), 8), "buy_amount": 100, "buy_units": 2, "buy_type": "small_flow", "buy_uuid": "uuid_s2", "filled": "done"},
        {"time": "2025-01-04", "market": "XRPUSDT", "target_price": round(0.500 * (1 - 0.03 * 1), 8), "buy_amount": 100, "buy_units": 1, "buy_type": "large_flow", "buy_uuid": "uuid_l1", "filled": "done"}
    ])
    updated_df_s4 = generate_buy_orders(setting_df, buy_log_df_s4, current_prices_s4)
    print("Scenario 4 Result:\n", updated_df_s4[['market', 'buy_type', 'target_price', 'buy_units', 'filled']])

    # 검증: 모든 flow 주문이 done이므로, 이 시나리오에서는 새로운 flow 주문이 생성되지 않아야 함 (설정상 최대 small_flow 2단계, large_flow 1단계)
    # 현재 casino_strategy.py는 'done' 상태의 주문을 '재생성'하는 로직이 없고,
    # 'current_price <= target_price' 조건에 따라 'update'할 새 주문이 없으면 생성하지 않음.
    # 즉, small_flow_units=2, large_flow_units=1 이므로 더 이상 새로운 단계가 생성될 수 없음.
    assert len(updated_df_s4) == len(buy_log_df_s4), "❌ 시나리오 4: 모든 flow 주문이 done 상태일 때 새로운 주문이 추가되어서는 안 됩니다."
    print("✅ 시나리오 4 통과: 모든 flow 주문 done 상태 시 새로운 주문 미생성 확인")

    print("\n✅ generate_buy_orders 전체 테스트 통과")