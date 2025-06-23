# 시뮬레이터 (DB)

from manager.simulator_db import simulate_with_db

simulate_with_db(
    market="KRW-DOGE",
    start="2025-01-01 00:00:00",
    end="2025-01-01 00:10:00",
    unit_size=5000,
    small_flow_pct=0.04,
    small_flow_units=2,
    large_flow_pct=0.13,
    large_flow_units=7,
    take_profit_pct=0.00575
)