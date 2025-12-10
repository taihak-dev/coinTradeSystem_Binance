# run_backtest_dynamic.py

import logging
from manager.simulator_ft_dynamic import simulate_futures_dynamic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ⚙️ 공통 설정값 정의 ---
common_settings = {
    "save_full_log": True,
    "initial_cash": 3_000.0,
    "leverage": 10,
    "buy_fee": 0.0004,
    "sell_fee": 0.0004,
    "maintenance_margin_rate": 0.005,
    "slippage_pct": 0.0005,
    "liquidation_safety_factor": 1.2,

    "market": "BTCUSDT",
    "start": "2020-01-01 00:00:00",
    "end": "2020-03-31 23:59:59",

    "unit_size": 100,
    "small_flow_pct": 0.04,
    "small_flow_units": 2,
    "large_flow_pct": 0.13,
    "large_flow_units": 14,
    "take_profit_pct": 0.00575,
    
    # --- 👇👇👇 동적 유닛 및 수익 리셋 설정 👇👇👇 ---
    "enable_dynamic_unit": True,
    "profit_reset_pct": 2.0  # 200% 수익 달성 시 자본 리셋
    # --- 👆👆👆 설정 완료 --- 👆👆👆
}

if __name__ == "__main__":
    simulate_futures_dynamic(**common_settings)
