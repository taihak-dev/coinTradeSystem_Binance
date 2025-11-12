# run_backtest_db.py

from manager.simulator_db import simulate_with_db


simulate_with_db(
    initial_cash=3_000.0, # (예시) 초기 자본
    buy_fee=0.0005,        # (예시) 매수 수수료
    sell_fee=0.0005,       # (예시) 매도 수수료

    market="XRPUSDT",
    start="2024-01-01 00:00:00",
    end="2025-11-04 23:59:59",

    unit_size=100,
    small_flow_pct=0.04,
    small_flow_units=2,
    large_flow_pct=0.13,
    large_flow_units=14,
    take_profit_pct=0.00575,
    leverage=10  # 추가: 원하는 레버리지 배수 설정 (예: 10배)
)