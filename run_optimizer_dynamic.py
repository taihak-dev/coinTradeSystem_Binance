# run_optimizer_dynamic.py
import itertools
import logging
import numpy as np
import pandas as pd
from manager.simulator_ft_dynamic import simulate_futures_dynamic

# --- 로깅 설정 ---
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. 최적화할 파라미터 범위 정의 ---
param_grid = {
    # 'enable_rebalance': [True, False],
    # 'take_profit_pct': [0.006, 0.007],
}

# --- 2. 백테스트 기본 설정 ---
base_settings = {
    "enable_dynamic_unit": False,
    "enable_rebalance": True,
    "initial_entry_units": 1.0,  # 초기 매수 배수 추가
    "save_full_log": False,
    "liquidation_safety_factor": 1.5,
    "profit_reset_pct": 1.0,
    "initial_cash": 3_000.0,
    "buy_fee": 0.0004,
    "sell_fee": 0.0004,
    "maintenance_margin_rate": 0.005,
    "slippage_pct": 0.0005,
    "market": "BTCUSDT",
    "start": "2020-01-01 00:00:00",
    "end": "2025-12-04 23:59:59",
    "unit_size": 150,
    "small_flow_units": 2,
    "large_flow_units": 10,
    'small_flow_pct': 0.04,
    'large_flow_pct': 0.17,
    'take_profit_pct': 0.006,
    "leverage": 5,
}

def run_optimizer_dynamic():
    """
    동적 유닛 시뮬레이터의 파라미터 그리드 서치를 수행합니다.
    """
    opt_logger = logging.getLogger("Optimizer")
    opt_logger.setLevel(logging.INFO)
    if not opt_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        opt_logger.addHandler(handler)

    results = []
    
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())
    
    param_combinations = list(itertools.product(*param_values))
    total_runs = len(param_combinations)
    opt_logger.info(f"총 {total_runs}개의 파라미터 조합으로 최적화를 시작합니다.")

    for i, params in enumerate(param_combinations):
        current_params = dict(zip(param_names, params))
        current_settings = {**base_settings, **current_params}

        opt_logger.info(f"--- [{i+1}/{total_runs}] 실행: {current_params} ---")

        try:
            simulation_result = simulate_futures_dynamic(**current_settings)
            full_result = {**current_params, **simulation_result}
            results.append(full_result)
        except Exception as e:
            opt_logger.error(f"파라미터 {current_params} 실행 중 오류 발생: {e}", exc_info=False)

    if not results:
        opt_logger.warning("최적화 실행 결과가 없습니다.")
        return

    # --- 결과 분석 및 리포팅 ---
    results_df = pd.DataFrame(results)
    
    pd.options.display.float_format = '{:,.4f}'.format
    results_df.replace([np.inf, -np.inf], 'inf', inplace=True)

    display_columns = param_names + [
        'Final Balance', 'Total PNL %', 'Accumulated Profit', 'Reset Count', 'Return/MDD', 
        'Profit Factor', 'MDD %', 'Win Rate', 'Total Trades', 'Liquidations'
    ]
    display_columns = [col for col in display_columns if col in results_df.columns]
    
    safe_results = results_df[results_df['Liquidations'] == 0].copy()
    liquidated_results = results_df[results_df['Liquidations'] > 0].copy()

    # --- 안전한 조합 리포트 ---
    opt_logger.info("\n\n" + "="*80)
    opt_logger.info("✅✅✅ 안전한 파라미터 조합 결과 (청산 0회) ✅✅✅")
    opt_logger.info("="*80)
    
    if not safe_results.empty:
        safe_results = safe_results.sort_values(
            by=['Return/MDD', 'Profit Factor', 'Total PNL %'],
            ascending=[False, False, False]
        )
        opt_logger.info(f"총 {len(safe_results)}개의 안전한 조합을 찾았습니다. (Return/MDD 기준 정렬)")
        print(safe_results[display_columns].to_string())
    else:
        opt_logger.warning("⚠️ 청산을 피한 안전한 조합을 찾지 못했습니다.")

    # --- 청산 발생 조합 리포트 ---
    opt_logger.info("\n\n" + "="*80)
    opt_logger.info("🚨🚨🚨 청산 발생 파라미터 조합 결과 (참고용) 🚨🚨🚨")
    opt_logger.info("="*80)

    if not liquidated_results.empty:
        liquidated_results = liquidated_results.sort_values(by='Final Balance', ascending=False)
        opt_logger.info(f"총 {len(liquidated_results)}개의 조합에서 청산이 발생했습니다.")
        print(liquidated_results[display_columns].to_string())
    else:
        opt_logger.info("🎉 모든 조합에서 청산이 발생하지 않았습니다!")


if __name__ == "__main__":
    run_optimizer_dynamic()