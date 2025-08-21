# run_backtest_db.py

from manager.simulator_db import simulate_with_db

simulate_with_db(
    market="ETHUSDT",
    start="2024-01-01 00:00:00",
    end="2025-08-15 23:59:59",
    unit_size=400,
    small_flow_pct=0.04,
    small_flow_units=2,
    large_flow_pct=0.13,
    large_flow_units=14,
    take_profit_pct=0.00675,
    leverage=15  # 추가: 원하는 레버리지 배수 설정 (예: 10배)
)