import sqlite3
import pandas as pd
import numpy as np
import os
import logging
import itertools
from datetime import datetime, timedelta

# --- 1. 시스템 설정 (Configuration) ---
MARKET = "BTCUSDT"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(f"{MARKET}_StepUp_Compounding_Test")

# 데이터베이스 경로
DB_PATH = os.path.join(os.path.dirname(__file__), "db", "candle_db.sqlite")

# 기본 리스크 설정
INITIAL_CASH = 3000.0
STOP_LOSS_THRESHOLD = 0.65  # 초기 하드 데크 (3000 * 0.65 = 1950)
PANIC_SELL_PENALTY = 0.02
COOLDOWN_MINUTES = 1440
FEE_RATE = 0.0004
SLIPPAGE_RATE = 0.0005

# 계단식 손절(Step-up Hard Deck) 설정
ENABLE_STEP_UP = True
STEP_1_TRIGGER = 2.0        # 자산 2배 달성 시
STEP_1_LOCK = 1.0           # 원금 보장
STEP_2_TRIGGER = 3.0        # 자산 3배 달성 시
STEP_2_LOCK = 2.0           # 2배 보장

# 매매 전략 파라미터 (그리드 서치)
GRID_PARAMS = {
    "UNIT_RATIO": [0.10, 0.12, 0.15], # [NEW] 자산 대비 유닛 비율 (10%, 12%, 15%)
    "TAKE_PROFIT_PCT": [0.006],
    "SMALL_FLOW_PCT": [0.04],
    "LARGE_FLOW_PCT": [0.17],
    "INITIAL_UNITS": [2.0],
    "SMALL_FLOW_UNITS": [2.0],
    "LARGE_FLOW_UNITS": [10.0],
    "LEVERAGE": [10],
    "MARGIN_BUFFER": [1.5],
    "SAVE_FULL_LOG": [False]
}

# --- 2. 데이터 로드 함수 ---
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
        if df.empty: return df
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col])
        return df
    except Exception as e:
        logger.error(f"❌ 데이터 로드 중 오류: {e}")
        return pd.DataFrame()

# --- 3. 시뮬레이션 엔진 (동적 유닛 + Step-up) ---
def run_simulation(df, settings):
    # 설정값 언패킹
    unit_ratio = settings["UNIT_RATIO"] # [NEW] 비율 사용
    tp_pct = settings["TAKE_PROFIT_PCT"]
    sf_pct = settings["SMALL_FLOW_PCT"]
    lf_pct = settings["LARGE_FLOW_PCT"]
    init_units = settings["INITIAL_UNITS"]
    sf_units = settings["SMALL_FLOW_UNITS"]
    lf_units = settings["LARGE_FLOW_UNITS"]
    leverage = settings["LEVERAGE"]
    margin_buffer = settings["MARGIN_BUFFER"]
    save_full_log = settings.get("SAVE_FULL_LOG", False)

    # 상태 변수 초기화
    cash = INITIAL_CASH
    position = {'qty': 0.0, 'avg_price': 0.0}
    
    # 동적 하드 데크 초기화
    current_hard_deck = INITIAL_CASH * STOP_LOSS_THRESHOLD
    step_level = 0

    total_injected = 0.0
    sl_count = 0
    
    cooldown_until = None
    buy_step = 0
    last_buy_price = 0.0
    hwm = 0.0
    
    # [NEW] 현재 적용 중인 유닛 사이즈 (매 진입 시 갱신)
    current_unit_size = 0.0

    log_data = []

    for row in df.itertuples():
        now = row.timestamp
        high = row.high
        low = row.low
        close = row.close
        action = "" 

        # 1. 쿨다운
        if cooldown_until:
            if now < cooldown_until:
                continue
            else:
                cooldown_until = None

        # 2. HWM 갱신
        if position['qty'] > 0:
            hwm = max(hwm, high)
        else:
            hwm = 0.0

        # 3. 자산 평가 (Equity)
        if position['qty'] > 0:
            unrealized_pnl = (low - position['avg_price']) * position['qty']
            equity = cash + unrealized_pnl
        else:
            equity = cash

        # 4. Step-up Hard Deck 업데이트
        if ENABLE_STEP_UP:
            if step_level < 1 and equity >= INITIAL_CASH * STEP_1_TRIGGER:
                step_level = 1
                current_hard_deck = INITIAL_CASH * STEP_1_LOCK
                action = f"🛡️ Level Up! Hard Deck: ${current_hard_deck:.0f}"
            elif step_level < 2 and equity >= INITIAL_CASH * STEP_2_TRIGGER:
                step_level = 2
                current_hard_deck = INITIAL_CASH * STEP_2_LOCK
                action = f"🛡️ Level Up! Hard Deck: ${current_hard_deck:.0f}"

        # 5. 방어 로직 (Stop Loss)
        if equity <= current_hard_deck:
            sl_count += 1
            salvaged_equity = equity * (1 - PANIC_SELL_PENALTY)
            
            needed = INITIAL_CASH - salvaged_equity
            if needed > 0:
                total_injected += needed
            
            # 초기화 (하드 데크도 초기화)
            cash = INITIAL_CASH
            position = {'qty': 0.0, 'avg_price': 0.0}
            buy_step = 0
            last_buy_price = 0.0
            hwm = 0.0
            current_unit_size = 0.0
            
            current_hard_deck = INITIAL_CASH * STOP_LOSS_THRESHOLD
            step_level = 0
            
            cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
            action = "Stop Loss & Refill"
            
            if save_full_log:
                log_data.append({
                    "Time": now, "Price": close, "Action": action, "Cash": cash, 
                    "Equity": equity, "HardDeck": current_hard_deck, "Level": step_level, "UnitSize": 0
                })
            continue

        # 6. 매도(익절) 로직
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
                current_unit_size = 0.0 # 포지션 종료 시 유닛 사이즈 초기화
                
                action = "Take Profit"
                if save_full_log:
                    log_data.append({
                        "Time": now, "Price": close, "Action": action, "Cash": cash, 
                        "Equity": cash, "HardDeck": current_hard_deck, "Level": step_level, "UnitSize": 0
                    })
                continue

        # 7. 매수 로직
        if position['qty'] == 0:
            # [NEW] 동적 유닛 사이즈 계산 (진입 시점의 Equity 기준)
            current_unit_size = equity * unit_ratio
            
            buy_amt = current_unit_size * init_units
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

        elif buy_step > 0:
            # 추가 매수 시에는 이미 결정된 current_unit_size를 계속 사용 (일관성 유지)
            if buy_step == 1:
                target_base = last_buy_price
                if hwm > last_buy_price * (1 + (sf_pct * 0.5)): target_base = hwm
                target_price = target_base * (1 - sf_pct)
                
                if low <= target_price:
                    buy_amt = current_unit_size * sf_units
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
                if hwm > last_buy_price * (1 + (lf_pct * 0.5)): target_base = hwm
                target_price = target_base * (1 - lf_pct)
                
                if low <= target_price:
                    buy_amt = current_unit_size * lf_units
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
        
        if save_full_log and action:
             log_data.append({
                "Time": now, "Price": close, "Action": action, "Cash": cash, 
                "Equity": equity, "HardDeck": current_hard_deck, "Level": step_level, "UnitSize": current_unit_size
            })

    final_equity = cash
    if position['qty'] > 0:
        final_equity += (df.iloc[-1].close - position['avg_price']) * position['qty']

    log_df = pd.DataFrame(log_data) if save_full_log else None

    return {
        "sl_count": sl_count,
        "total_injected": total_injected,
        "final_equity": final_equity,
        "log_df": log_df
    }

# --- 4. 메인 실행 ---
def main():
    scenarios = [
        {"name": "A (Bull)", "start": "2020-01-01 00:00:00", "end": "2021-06-01 23:59:59"},
        {"name": "B (Bear)", "start": "2022-01-01 00:00:00", "end": "2023-12-31 23:59:59"},
        {"name": "C (2025 10)", "start": "2025-10-01 00:00:00", "end": "2025-10-30 23:59:59"},
        {"name": "D (Full)", "start": "2020-01-01 00:00:00", "end": "2025-12-28 23:59:59"},
        {"name": "E (최근3년)", "start": "2023-01-01 00:00:00", "end": "2025-12-28 23:59:59"}
    ]

    keys = list(GRID_PARAMS.keys())
    values = list(GRID_PARAMS.values())
    combinations = list(itertools.product(*values))
    
    print(f"🛡️ {MARKET} 'Step-up + Compounding' 전략 검증 시작")
    print(f"   - 기본 SL: ${INITIAL_CASH * STOP_LOSS_THRESHOLD:.0f}")
    print(f"   - Level 1: 자산 ${INITIAL_CASH*STEP_1_TRIGGER:.0f} 도달 시 -> SL ${INITIAL_CASH*STEP_1_LOCK:.0f}")
    print("=" * 100)

    for scenario in scenarios:
        print(f"\n▶ Scenario {scenario['name']} 테스트 중...")
        df = load_candles(MARKET, scenario['start'], scenario['end'])
        if df.empty: continue

        for combo in combinations:
            settings = dict(zip(keys, combo))
            res = run_simulation(df, settings)
            
            net_profit = res['final_equity'] - (INITIAL_CASH + res['total_injected'])
            
            if res['log_df'] is not None:
                filename = f"StepUpCompounding_{scenario['name'].split()[0]}_{MARKET}_Ratio{settings['UNIT_RATIO']}.csv"
                res['log_df'].to_csv(filename, index=False)
                print(f"  💾 상세 로그 저장: {filename}")

            print(f"  📊 [Ratio: {settings['UNIT_RATIO']}] SL: {res['sl_count']}회 | 순수익: ${net_profit:,.2f} | 최종자산: ${res['final_equity']:,.2f}")

if __name__ == "__main__":
    main()