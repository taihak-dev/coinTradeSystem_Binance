# 시뮬레이터 (실시간)

from manager.simulator import simulate_with_strategy

simulate_with_strategy(
    market="KRW-DOGE",
    start="2021-05-08 00:00",
    end="2021-06-25 23:00",
    unit=1,
    unit_size=5000,
    small_flow_pct=0.04,
    small_flow_units=2,
    large_flow_pct=0.13,
    large_flow_units=7,
    take_profit_pct=0.00575
)