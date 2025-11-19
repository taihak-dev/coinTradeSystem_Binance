# run_backtest_ft.py

import logging

# --- 👇👇👇 1. V2 엔진 스위치 및 설정 👇👇👇 ---
USE_V2_ENGINE = True  # True: V2(OHLC+Slippage) / False: V1(Close Price)

# V2 엔진을 사용할 경우에만 적용되는 설정
SLIPPAGE_PCT = 0.0005  # (0.05% 슬리피지)
# --- 👆👆👆 ---


# ⭐️ 스위치에 따라 올바른 엔진(함수)을 임포트합니다.
if USE_V2_ENGINE:
    try:
        from manager.simulator_ft_v2 import simulate_futures_with_db

        logging.info("✅ V2 백테스트 엔진(OHLC+Slippage)을 사용합니다.")
    except ImportError:
        logging.error("❌ V2 엔진(simulator_ft_v2.py)을 찾을 수 없습니다. V1 엔진으로 대신 실행합니다.")
        from manager.simulator_ft import simulate_futures_with_db

        USE_V2_ENGINE = False
else:
    from manager.simulator_ft import simulate_futures_with_db

    logging.info("✅ V1 백테스트 엔진(Close Price)을 사용합니다.")

# --- ⚙️ 공통 설정값 정의 ---
common_settings = {
    "save_full_log": False,  # True로 변경 시에만 전체 로그파일(CSV)을 저장합니다.
    "initial_cash": 5_000.0,
    "leverage": 10,
    "buy_fee": 0.0004,
    "sell_fee": 0.0004,
    "maintenance_margin_rate": 0.005,

    "market": "ETHUSDT",
    "start": "2021-01-01 00:00:00",
    "end": "2025-11-18 23:59:59",

    "unit_size": 100,
    "small_flow_pct": 0.04,
    "small_flow_units": 2,
    "large_flow_pct": 0.13,
    "large_flow_units": 14,
    "take_profit_pct": 0.00575
}

# --- 👇👇👇 2. 스위치에 따라 함수를 실행합니다 👇👇👇 ---
if USE_V2_ENGINE:
    # V2 엔진일 경우, 공통 설정에 V2 전용 인자(slippage)를 추가
    common_settings['slippage_pct'] = SLIPPAGE_PCT
    simulate_futures_with_db(**common_settings)
else:
    # V1 엔진일 경우, 공통 설정 그대로 실행
    simulate_futures_with_db(**common_settings)
# --- 👆👆👆 수정 완료 --- 👆👆👆