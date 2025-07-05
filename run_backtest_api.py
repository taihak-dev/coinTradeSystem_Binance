# run_backtest_api.py

from manager.simulator import simulate_with_api

# 변경: market 이름을 바이낸스 형식으로, leverage 파라미터 추가
simulate_with_api(
    market="DOGEUSDT",
    start="2025-06-01 00:00:00",
    end="2025-06-30 23:59:59",
    unit_size=20,
    small_flow_pct=0.0055,
    small_flow_units=1,
    large_flow_pct=0.0155,
    large_flow_units=3,
    take_profit_pct=0.00575,
    leverage=15
)