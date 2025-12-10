# run_optimizer.py
import itertools
import logging
import numpy as np
import pandas as pd
from manager.simulator_ft_v2 import simulate_futures_with_db

# --- 로깅 설정 ---
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Optimizer 실행 시에는 개별 백테스트 로그를 최소화하기 위해 INFO 대신 WARNING 레벨 사용
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')


# --- 1. 최적화할 파라미터 범위 정의 ---
param_grid = {
    # "small_flow_units": [1, 2],
    # "large_flow_units": [10, 14],
    # 'small_flow_pct': [0.04, 0.05, 0.06],
    # 'large_flow_pct': [0.13, 0.17, 0.20],
    # 'take_profit_pct': [0.005, 0.01],
}

# --- 2. 백테스트 기본 설정 (run_backtest_ft.py에서 가져옴) ---
base_settings = {
    "small_flow_units": 2,
    "large_flow_units": 10,
    'small_flow_pct': 0.04,
    'large_flow_pct': 0.20,
    'take_profit_pct':0.01,
    "leverage": 10,
    "save_full_log": False,
    "initial_cash": 3_000.0,
    "buy_fee": 0.0004,
    "sell_fee": 0.0004,
    "maintenance_margin_rate": 0.005,
    "slippage_pct": 0.0005,  # V2 엔진용 슬리피지 설정
    "liquidation_safety_factor": 1.5, # 기본값 1(안전 마진 없음)
    "market": "BTCUSDT",
    "start": "2020-01-01 00:00:00",
    "end": "2020-03-20 23:59:59",
    "unit_size": 100,
}

def run_optimizer():
    """
    파라미터 그리드 서치를 통해 최적의 조합을 찾고, 우선순위에 따라 분석/리포팅합니다.
    """
    # Optimizer 전용 로거
    opt_logger = logging.getLogger("Optimizer")
    opt_logger.setLevel(logging.INFO)
    # 핸들러가 중복 추가되는 것을 방지
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
            # 백테스트 실행
            simulation_result = simulate_futures_with_db(**current_settings)
            
            # 결과와 파라미터를 합쳐서 저장
            full_result = {**current_params, **simulation_result}
            results.append(full_result)
            
        except Exception as e:
            opt_logger.error(f"파라미터 {current_params} 실행 중 오류 발생: {e}", exc_info=False)

    if not results:
        opt_logger.warning("최적화 실행 결과가 없습니다.")
        return

    # --- 4. 새로운 결과 분석 및 리포팅 로직 ---
    results_df = pd.DataFrame(results)
    
    # 소수점 포맷을 4자리로 변경하여 작은 소수가 잘리지 않도록 함
    pd.options.display.float_format = '{:,.4f}'.format

    results_df.replace([np.inf, -np.inf], 'inf', inplace=True)

    # 분석할 컬럼 순서 정의
    display_columns = param_names + [
        'Final Balance', 'Total PNL %', 'Return/MDD', 'Profit Factor',
        'MDD %', 'Win Rate', 'Total Trades', 'Liquidations'
    ]
    # 결과에 없는 컬럼은 제외
    display_columns = [col for col in display_columns if col in results_df.columns]
    
    # 청산 발생 여부에 따라 데이터프레임 분리
    safe_results = results_df[results_df['Liquidations'] == 0].copy()
    liquidated_results = results_df[results_df['Liquidations'] > 0].copy()

    # --- ✅ 1. 안전한 조합 리포트 ---
    opt_logger.info("\n\n" + "="*80)
    opt_logger.info("✅✅✅ 안전한 파라미터 조합 결과 (청산 0회) ✅✅✅")
    opt_logger.info("="*80)
    
    if not safe_results.empty:
        # 정렬: 1. Return/MDD 내림차순, 2. Profit Factor 내림차순, 3. Total PNL % 내림차순
        safe_results = safe_results.sort_values(
            by=['Return/MDD', 'Profit Factor', 'Total PNL %'],
            ascending=[False, False, False]
        )
        opt_logger.info(f"총 {len(safe_results)}개의 안전한 조합을 찾았습니다. (Return/MDD 기준 정렬)")
        print(safe_results[display_columns].to_string())
    else:
        opt_logger.warning("⚠️ 청산을 피한 안전한 조합을 찾지 못했습니다.")

    # --- 🚨 2. 청산 발생 조합 리포트 ---
    opt_logger.info("\n\n" + "="*80)
    opt_logger.info("🚨🚨🚨 청산 발생 파라미터 조합 결과 (참고용) 🚨🚨🚨")
    opt_logger.info("="*80)

    if not liquidated_results.empty:
        # 정렬: Final Balance 내림차순 (얼마나 버텼는지 참고용)
        liquidated_results = liquidated_results.sort_values(by='Final Balance', ascending=False)
        opt_logger.info(f"총 {len(liquidated_results)}개의 조합에서 청산이 발생했습니다.")
        print(liquidated_results[display_columns].to_string())
    else:
        opt_logger.info("🎉 모든 조합에서 청산이 발생하지 않았습니다!")


if __name__ == "__main__":
    run_optimizer()