import sqlite3
import pandas as pd
import numpy as np
import os
import logging
import itertools
from datetime import datetime, timedelta

# --- 1. 시스템 설정 (Configuration) ---
MARKET = "BTCUSDT"  # 테스트할 마켓 설정

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(f"{MARKET}_Stress_Test") # 로거 이름 동적 변경

# 데이터베이스 경로
DB_PATH = os.path.join(os.path.dirname(__file__), "db", "candle_db.sqlite")

# 고정 리스크 설정 (Fixed Risk Settings)
INITIAL_CASH = 3000.0
STOP_LOSS_THRESHOLD = 0.65  # 초기 자본의 65% 이하 시 손절
PANIC_SELL_PENALTY = 0.02   # 손절 시 2% 추가 슬리피지 패널티
COOLDOWN_MINUTES = 1440     # 손절 후 24시간 매매 중단
FEE_RATE = 0.0004
SLIPPAGE_RATE = 0.0005

# 매매 전략 상수 (기본값)
UNIT_SIZE = 200.0
TAKE_PROFIT_PCT = 0.005
SMALL_FLOW_PCT = 0.04
LARGE_FLOW_PCT = 0.17
INITIAL_UNITS = 2.0
SMALL_FLOW_UNITS = 2.0

# --- 2. 그리드 서치 파라미터 설정 (Grid Search Parameters) ---
GRID_PARAMS = {
    "UNIT_SIZE": [350],
    "TAKE_PROFIT_PCT": [0.006],
    "SMALL_FLOW_PCT": [0.04],
    "LARGE_FLOW_PCT": [0.17],
    "INITIAL_UNITS": [2.0],
    "SMALL_FLOW_UNITS": [2.0],
    "LARGE_FLOW_UNITS": [10.0],
    "LEVERAGE": [10],
    "PROFIT_RESET_TARGET": [1.0],
    "MARGIN_BUFFER": [1.5],
    "SAVE_FULL_LOG": [False]
}

# --- 3. 데이터 로드 함수 ---
def load_candles(market, start, end):
    if not os.path.exists(DB_PATH):
        logger.error(f"❌ DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return pd.DataFrame()

    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = """
                SELECT timestamp, open, high, low, close 
                FROM minute_candles 
                WHERE market = ? AND timestamp BETWEEN ? AND ? 
                ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=[market, start, end])
            
        if df.empty:
            return df

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col])
        return df
    except Exception as e:
        logger.error(f"❌ 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()

# --- 4. 시뮬레이션 엔진 (Core Logic) ---
def run_simulation(df, settings):
    # 설정값 언패킹
    unit_size = settings["UNIT_SIZE"]
    tp_pct = settings["TAKE_PROFIT_PCT"]
    sf_pct = settings["SMALL_FLOW_PCT"]
    lf_pct = settings["LARGE_FLOW_PCT"]
    init_units = settings["INITIAL_UNITS"]
    sf_units = settings["SMALL_FLOW_UNITS"]
    lf_units = settings["LARGE_FLOW_UNITS"]
    leverage = settings["LEVERAGE"]
    profit_reset_target = settings["PROFIT_RESET_TARGET"]
    margin_buffer = settings["MARGIN_BUFFER"]
    save_full_log = settings.get("SAVE_FULL_LOG", False)

    # 상태 변수 초기화
    cash = INITIAL_CASH
    position = {'qty': 0.0, 'avg_price': 0.0}
    
    # 성과 추적 변수
    total_injected = 0.0
    secured_profit = 0.0
    sl_count = 0
    reset_count = 0
    
    # 매매 제어 변수
    cooldown_until = None
    buy_step = 0            # 0:None, 1:Initial, 2:Small, 3:Large
    last_buy_price = 0.0
    hwm = 0.0               # High Water Mark

    # 로그 데이터 저장용 리스트
    log_data = []

    for row in df.itertuples():
        now = row.timestamp
        high = row.high
        low = row.low
        close = row.close
        
        action = "" # 현재 캔들에서의 행동 기록

        # 1. 쿨다운 체크
        if cooldown_until:
            if now < cooldown_until:
                if save_full_log:
                    log_data.append({
                        "Time": now, "Price": close, "Action": "Cooldown", "Cash": cash, 
                        "Equity": cash, "Pos_Qty": 0, "Pos_Avg": 0, "HWM": 0, "Step": 0
                    })
                continue
            else:
                cooldown_until = None

        # 2. HWM 갱신
        if position['qty'] > 0:
            hwm = max(hwm, high)
        else:
            hwm = 0.0

        # 3. 자산 평가 (Equity Calculation)
        if position['qty'] > 0:
            unrealized_pnl = (low - position['avg_price']) * position['qty']
            equity = cash + unrealized_pnl
        else:
            equity = cash

        # 4. 방어 로직 (Stop Loss & Refill)
        if equity <= INITIAL_CASH * STOP_LOSS_THRESHOLD:
            sl_count += 1
            salvaged_equity = equity * (1 - PANIC_SELL_PENALTY)
            
            needed = INITIAL_CASH - salvaged_equity
            if needed > 0:
                total_injected += needed
            
            # 상태 초기화
            cash = INITIAL_CASH
            position = {'qty': 0.0, 'avg_price': 0.0}
            buy_step = 0
            last_buy_price = 0.0
            hwm = 0.0
            cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
            action = "Stop Loss & Refill"
            
            if save_full_log:
                log_data.append({
                    "Time": now, "Price": close, "Action": action, "Cash": cash, 
                    "Equity": equity, "Pos_Qty": 0, "Pos_Avg": 0, "HWM": 0, "Step": 0
                })
            continue

        # 5. 수익 실현 로직 (Profit Reset)
        if profit_reset_target is not None:
            target_equity = INITIAL_CASH * (1 + profit_reset_target)
            current_eval_equity = cash + ((close - position['avg_price']) * position['qty']) if position['qty'] > 0 else cash
            
            if current_eval_equity >= target_equity:
                reset_count += 1
                
                if position['qty'] > 0:
                    exec_price = close * (1 - SLIPPAGE_RATE)
                    revenue = position['qty'] * exec_price
                    cost = position['qty'] * position['avg_price']
                    fee = revenue * FEE_RATE
                    cash += (revenue - cost) - fee
                
                profit = cash - INITIAL_CASH
                if profit > 0:
                    secured_profit += profit
                
                cash = INITIAL_CASH
                position = {'qty': 0.0, 'avg_price': 0.0}
                buy_step = 0
                last_buy_price = 0.0
                hwm = 0.0
                action = "Profit Reset"
                
                if save_full_log:
                    log_data.append({
                        "Time": now, "Price": close, "Action": action, "Cash": cash, 
                        "Equity": current_eval_equity, "Pos_Qty": 0, "Pos_Avg": 0, "HWM": 0, "Step": 0
                    })
                continue

        # 6. 매도(익절) 체크
        if position['qty'] > 0:
            target_price = position['avg_price'] * (1 + tp_pct)
            if high >= target_price:
                exec_price = target_price * (1 - SLIPPAGE_RATE)
                revenue = position['qty'] * exec_price
                cost = position['qty'] * position['avg_price']
                fee = revenue * FEE_RATE
                
                cash += (revenue - cost) - fee
                
                position = {'qty': 0.0, 'avg_price': 0.0}
                buy_step = 0
                last_buy_price = 0.0
                hwm = 0.0
                action = "Take Profit"
                
                if save_full_log:
                    log_data.append({
                        "Time": now, "Price": close, "Action": action, "Cash": cash, 
                        "Equity": cash, "Pos_Qty": 0, "Pos_Avg": 0, "HWM": 0, "Step": 0
                    })
                continue

        # 7. 매수 로직
        # 7-1. 신규 진입
        if position['qty'] == 0:
            buy_amt = unit_size * init_units
            required_margin = (buy_amt / leverage) * margin_buffer
            
            if cash >= required_margin:
                exec_price = close * (1 + SLIPPAGE_RATE)
                qty = buy_amt / exec_price
                fee = buy_amt * FEE_RATE
                cash -= fee
                
                position = {'qty': qty, 'avg_price': exec_price}
                last_buy_price = exec_price
                buy_step = 1
                hwm = exec_price
                action = "Initial Buy"

        # 7-2. 추가 매수
        elif buy_step > 0:
            if buy_step == 1:
                target_base = last_buy_price
                if hwm > last_buy_price * (1 + (sf_pct * 0.5)):
                    target_base = hwm
                
                target_price = target_base * (1 - sf_pct)
                
                if low <= target_price:
                    buy_amt = unit_size * sf_units
                    exec_price = target_price * (1 + SLIPPAGE_RATE)
                    
                    required_margin = (buy_amt / leverage) * margin_buffer
                    
                    if cash >= required_margin:
                        qty = buy_amt / exec_price
                        fee = buy_amt * FEE_RATE
                        cash -= fee
                        
                        new_qty = position['qty'] + qty
                        new_avg = ((position['qty'] * position['avg_price']) + (qty * exec_price)) / new_qty
                        position = {'qty': new_qty, 'avg_price': new_avg}
                        
                        last_buy_price = exec_price
                        buy_step = 2
                        hwm = exec_price
                        action = "Small Flow Buy"

            elif buy_step == 2:
                target_base = last_buy_price
                if hwm > last_buy_price * (1 + (lf_pct * 0.5)):
                    target_base = hwm
                
                target_price = target_base * (1 - lf_pct)
                
                if low <= target_price:
                    buy_amt = unit_size * lf_units
                    exec_price = target_price * (1 + SLIPPAGE_RATE)
                    
                    required_margin = (buy_amt / leverage) * margin_buffer
                    
                    if cash >= required_margin:
                        qty = buy_amt / exec_price
                        fee = buy_amt * FEE_RATE
                        cash -= fee
                        
                        new_qty = position['qty'] + qty
                        new_avg = ((position['qty'] * position['avg_price']) + (qty * exec_price)) / new_qty
                        position = {'qty': new_qty, 'avg_price': new_avg}
                        
                        last_buy_price = exec_price
                        buy_step = 3
                        hwm = exec_price
                        action = "Large Flow Buy"
        
        if save_full_log:
            log_data.append({
                "Time": now, "Price": close, "Action": action, "Cash": cash, 
                "Equity": equity, "Pos_Qty": position['qty'], "Pos_Avg": position['avg_price'], 
                "HWM": hwm, "Step": buy_step
            })

    final_equity = cash
    if position['qty'] > 0:
        final_equity += (df.iloc[-1].close - position['avg_price']) * position['qty']

    # 로그 데이터프레임 생성
    log_df = pd.DataFrame(log_data) if save_full_log else None

    return {
        "sl_count": sl_count,
        "reset_count": reset_count,
        "total_injected": total_injected,
        "secured_profit": secured_profit,
        "final_equity": final_equity,
        "log_df": log_df
    }

# --- 5. 메인 실행 함수 ---
def main():
    scenarios = [
        {"name": "A(Bull)", "start": "2020-01-01 00:00:00", "end": "2021-06-01 23:59:59"},
        {"name": "B(Bear)", "start": "2022-01-01 00:00:00", "end": "2023-12-31 23:59:59"},
        {"name": "C(2025)", "start": "2025-01-01 00:00:00", "end": "2025-12-04 23:59:59"},
        {"name": "D(Full)", "start": "2020-01-01 00:00:00", "end": "2025-12-04 23:59:59"}
    ]

    # 파라미터 조합 생성
    keys = list(GRID_PARAMS.keys())
    values = list(GRID_PARAMS.values())
    combinations = list(itertools.product(*values))
    
    results = []

    print(f"🚀 {MARKET} 순환형 자산 관리 전략 그리드 테스트 시작")
    print(f"💰 초기자본: ${INITIAL_CASH}, 손절선: -35%, 리필: Enabled")
    print(f"🔍 총 {len(combinations)}개의 파라미터 조합 테스트 예정")
    print("=" * 100)

    for scenario in scenarios:
        print(f"\n▶ Scenario {scenario['name']} 데이터 로딩 중...")
        df = load_candles(MARKET, scenario['start'], scenario['end'])
        
        if df.empty:
            print("❌ 데이터가 없습니다.")
            continue

        print(f"  데이터 로드 완료: {len(df)} candles. 시뮬레이션 시작...")
        
        for combo in combinations:
            settings = dict(zip(keys, combo))
            p_target_str = "None" if settings["PROFIT_RESET_TARGET"] is None else f"{settings['PROFIT_RESET_TARGET']*100:.0f}%"
            
            res = run_simulation(df, settings)
            
            net_profit = (res['secured_profit'] + res['final_equity']) - (INITIAL_CASH + res['total_injected'])
            total_invested = INITIAL_CASH + res['total_injected']
            roi = (net_profit / total_invested) * 100 if total_invested > 0 else 0
            
            # 로그 파일 저장
            if settings.get("SAVE_FULL_LOG", False) and res['log_df'] is not None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"StressTest_{scenario['name'].split()[0]}_{MARKET}_Lev{settings['LEVERAGE']}_LF{settings['LARGE_FLOW_UNITS']}_Reset{p_target_str}_{timestamp}.csv"
                filename = filename.replace("(", "").replace(")", "").replace("%", "")
                res['log_df'].to_csv(filename, index=False)
                print(f"  💾 상세 로그 저장 완료: {filename}")

            result_row = {
                "Scenario": scenario['name'],
                "Unit": settings["UNIT_SIZE"],
                "TP": settings["TAKE_PROFIT_PCT"],
                "SF%": settings["SMALL_FLOW_PCT"],
                "LF%": settings["LARGE_FLOW_PCT"],
                "Init U": settings["INITIAL_UNITS"],
                "SF U": settings["SMALL_FLOW_UNITS"],
                "LF U": settings["LARGE_FLOW_UNITS"],
                "Lev": settings["LEVERAGE"],
                "Reset Target": p_target_str,
                "Buffer": settings["MARGIN_BUFFER"],
                "SL": res['sl_count'],
                "Reset": res['reset_count'],
                "Injected": round(res['total_injected'], 2),
                "Secured": round(res['secured_profit'], 2),
                "Final Eq": round(res['final_equity'], 2),
                "Net Profit": round(net_profit, 2),
                "ROI %": round(roi, 2)
            }
            results.append(result_row)

    if results:
        df_res = pd.DataFrame(results)
        print("\n" + "=" * 120)
        print("📊 최종 테스트 결과 요약")
        print("=" * 120)
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 1000)
        print(df_res.to_string(index=False))
        
        # 결과 파일명도 동적으로 변경
        result_filename = f"stress_test_{MARKET.lower()}_final_result.csv"
        df_res.to_csv(result_filename, index=False)
        print(f"\n✅ 결과가 '{result_filename}' 파일로 저장되었습니다.")

if __name__ == "__main__":
    main()