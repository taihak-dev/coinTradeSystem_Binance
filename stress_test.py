# stress_test.py
import logging
import pandas as pd
from manager.simulator_ft_dynamic import simulate_futures_dynamic

# --- 로깅 설정 ---
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. 테스트할 폭락 구간 정의 ---
STRESS_PERIODS = [
    {"name": "COVID-19 Crash", "start": "2020-02-15 00:00:00", "end": "2020-04-01 23:59:59"},
    {"name": "May 2021 Crash", "start": "2021-04-10 00:00:00", "end": "2021-07-01 23:59:59"},
    {"name": "Luna/FTX Crash", "start": "2022-04-01 00:00:00", "end": "2022-12-31 23:59:59"},
]

# --- 2. 테스트 시나리오 정의 ---
SCENARIOS = [
    {
        "name": "BTC Mode (No Rebalance)",
        "settings": {
            "market": "BTCUSDT",
            "enable_rebalance": False,
            "take_profit_pct": 0.005,
            "large_flow_pct": 0.17,
            # 공통 설정
            "unit_size": 100,
            "small_flow_units": 2,
            "large_flow_units": 10,
            "small_flow_pct": 0.04,
            "leverage": 5,
            "initial_cash": 3000.0,
            "liquidation_safety_factor": 1.5,
            "profit_reset_pct": 1.0,
            "enable_dynamic_unit": False,
            "save_full_log": False,
            "buy_fee": 0.0004,
            "sell_fee": 0.0004,
            "maintenance_margin_rate": 0.005,
            "slippage_pct": 0.0005,
            "initial_entry_units": 2.0
        }
    },
    {
        "name": "ETH Mode (With Rebalance)",
        "settings": {
            "market": "ETHUSDT",
            "enable_rebalance": True,
            "take_profit_pct": 0.006,
            "large_flow_pct": 0.17,
            # 공통 설정
            "unit_size": 150,
            "small_flow_units": 2,
            "large_flow_units": 10,
            "small_flow_pct": 0.04,
            "leverage": 5,
            "initial_cash": 3000.0,
            "liquidation_safety_factor": 1.5,
            "profit_reset_pct": 1.0,
            "enable_dynamic_unit": False,
            "save_full_log": False,
            "buy_fee": 0.0004,
            "sell_fee": 0.0004,
            "maintenance_margin_rate": 0.005,
            "slippage_pct": 0.0005,
            "initial_entry_units": 2.0
        }
    }
]

def run_stress_test():
    """
    주요 폭락 구간에 대한 스트레스 테스트를 실행하고 결과를 요약합니다.
    """
    # 로거 설정
    logger = logging.getLogger("StressTest")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    results = []
    
    logger.info("🚀 스트레스 테스트 시작: 역사적 폭락장 생존 검증")
    logger.info("=" * 60)

    for scenario in SCENARIOS:
        scenario_name = scenario["name"]
        base_settings = scenario["settings"]
        
        logger.info(f"\n▶ 시나리오: {scenario_name} ({base_settings['market']})")
        
        for period in STRESS_PERIODS:
            period_name = period["name"]
            start_date = period["start"]
            end_date = period["end"]
            
            # 기간 설정 업데이트
            current_settings = base_settings.copy()
            current_settings["start"] = start_date
            current_settings["end"] = end_date
            
            logger.info(f"  - 테스트 구간: {period_name} ({start_date} ~ {end_date})")
            
            try:
                # 시뮬레이션 실행
                sim_result = simulate_futures_dynamic(**current_settings)
                
                # 결과 저장
                result_summary = {
                    "Scenario": scenario_name,
                    "Period": period_name,
                    "Market": base_settings["market"],
                    "Survived": "✅ Yes" if sim_result["Liquidations"] == 0 else "❌ No",
                    "MDD %": sim_result["MDD %"],
                    "Total PNL %": sim_result["Total PNL %"],
                    "Final Balance": sim_result["Final Balance"],
                    "Liquidations": sim_result["Liquidations"]
                }
                results.append(result_summary)
                
            except Exception as e:
                logger.error(f"    ❌ 오류 발생: {e}")

    # --- 결과 리포팅 ---
    logger.info("\n\n" + "=" * 80)
    logger.info("📊 스트레스 테스트 최종 결과 요약")
    logger.info("=" * 80)
    
    if results:
        df_results = pd.DataFrame(results)
        # 보기 좋게 포맷팅
        pd.options.display.float_format = '{:,.2f}'.format
        
        # 컬럼 순서 정렬
        cols = ["Scenario", "Period", "Market", "Survived", "MDD %", "Total PNL %", "Final Balance", "Liquidations"]
        print(df_results[cols].to_string(index=False))
    else:
        logger.warning("결과 데이터가 없습니다.")

if __name__ == "__main__":
    run_stress_test()