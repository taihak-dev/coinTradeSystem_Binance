import sqlite3
import pandas as pd
import numpy as np
import os
import logging
import itertools
from datetime import datetime, timedelta

# --- 1. 시스템 설정 (Configuration) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Compound_Test")

# 데이터베이스 경로
DB_PATH = os.path.join(os.path.dirname(__file__), "db", "candle_db.sqlite")

# 기본 자본 및 리스크 설정
INITIAL_CASH = 3000.0
STOP_LOSS_THRESHOLD = 0.65  # 초기 자본의 65% 이하 시 손절
PANIC_SELL_PENALTY = 0.02   # 손절 시 2% 추가 슬리피지 패널티
COOLDOWN_MINUTES = 1440     # 손절 후 24시간 매매 중단

# 최적 파라미터 (기본값)
MARKET = "BTCUSDT"
UNIT_SIZE = 350.0
TAKE_PROFIT_PCT = 0.006
SMALL_FLOW_PCT = 0.04
LARGE_FLOW_PCT = 0.17
INITIAL_UNITS = 2.0
SMALL_FLOW_UNITS = 2.0
LARGE_FLOW_UNITS = 10.0
LEVERAGE = 10
MARGIN_BUFFER = 1.5
PROFIT_RESET_TARGET = 1.0 # 100% 수익 달성 시 리셋

# 수수료 및 슬리피지
FEE_RATE = 0.0004
SLIPPAGE_RATE = 0.0005

# --- 2. 데이터 로드 함수 ---
def load_candles(market, start, end):
    if not os.path.exists(DB_PATH):
        logger.error(f"❌ DB 파일을 찾을 수 없습니다: {DB_PATH}")
        return pd.DataFrame()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = "SELECT timestamp, open, high, low, close FROM minute_candles WHERE market = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
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

# --- 3. PhoenixBot 클래스 (단일 봇 로직) ---
class PhoenixBot:
    def __init__(self, bot_id, settings):
        self.id = bot_id
        self.settings = settings
        
        # 상태 변수
        self.cash = INITIAL_CASH
        self.position = {'qty': 0.0, 'avg_price': 0.0}
        self.buy_step = 0
        self.last_buy_price = 0.0
        self.hwm = 0.0
        self.cooldown_until = None

    def get_equity(self, price):
        if self.position['qty'] > 0:
            unrealized_pnl = (price - self.position['avg_price']) * self.position['qty']
            return self.cash + unrealized_pnl
        return self.cash

    def run_tick(self, row):
        now, high, low, close = row.timestamp, row.high, row.low, row.close

        if self.cooldown_until and now < self.cooldown_until:
            return "COOLDOWN", 0, 0
        elif self.cooldown_until:
            self.cooldown_until = None

        if self.position['qty'] > 0:
            self.hwm = max(self.hwm, high)
        else:
            self.hwm = 0.0

        # 방어 로직 (Stop Loss)
        equity_at_low = self.get_equity(low)
        if equity_at_low <= INITIAL_CASH * STOP_LOSS_THRESHOLD:
            salvaged_equity = equity_at_low * (1 - PANIC_SELL_PENALTY)
            needed_injection = INITIAL_CASH - salvaged_equity
            
            # 상태 초기화 (부활)
            self.cash = INITIAL_CASH
            self.position = {'qty': 0.0, 'avg_price': 0.0}
            self.buy_step = 0
            self.last_buy_price = 0.0
            self.hwm = 0.0
            self.cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
            return "STOP_LOSS", 0, needed_injection

        # 수익 실현 로직 (Profit Reset)
        if self.settings["PROFIT_RESET_TARGET"] is not None:
            target_equity = INITIAL_CASH * (1 + self.settings["PROFIT_RESET_TARGET"])
            equity_at_close = self.get_equity(close)
            
            if equity_at_close >= target_equity:
                if self.position['qty'] > 0:
                    exec_price = close * (1 - SLIPPAGE_RATE)
                    revenue = self.position['qty'] * exec_price
                    cost = self.position['qty'] * self.position['avg_price']
                    fee = revenue * FEE_RATE
                    self.cash += (revenue - cost) - fee
                
                profit_to_secure = self.cash - INITIAL_CASH
                
                # 상태 초기화
                self.cash = INITIAL_CASH
                self.position = {'qty': 0.0, 'avg_price': 0.0}
                self.buy_step = 0
                self.last_buy_price = 0.0
                self.hwm = 0.0
                return "PROFIT_RESET", profit_to_secure, 0

        # 매도(익절) 로직
        if self.position['qty'] > 0:
            target_price = self.position['avg_price'] * (1 + self.settings["TAKE_PROFIT_PCT"])
            if high >= target_price:
                exec_price = target_price * (1 - SLIPPAGE_RATE)
                revenue = self.position['qty'] * exec_price
                cost = self.position['qty'] * self.position['avg_price']
                fee = revenue * FEE_RATE
                self.cash += (revenue - cost) - fee
                
                self.position = {'qty': 0.0, 'avg_price': 0.0}
                self.buy_step = 0
                self.last_buy_price = 0.0
                self.hwm = 0.0
                return "TAKE_PROFIT", 0, 0

        # 매수 로직
        if self.position['qty'] == 0: # 신규 진입
            buy_amt = self.settings["UNIT_SIZE"] * self.settings["INITIAL_UNITS"]
            required_margin = (buy_amt / self.settings["LEVERAGE"]) * self.settings["MARGIN_BUFFER"]
            if self.cash >= required_margin:
                exec_price = close * (1 + SLIPPAGE_RATE)
                qty = buy_amt / exec_price
                self.cash -= buy_amt * FEE_RATE
                self.position = {'qty': qty, 'avg_price': exec_price}
                self.last_buy_price = exec_price
                self.buy_step = 1
                self.hwm = exec_price
        
        elif self.buy_step > 0: # 추가 매수
            target_base = self.last_buy_price
            if self.buy_step == 1:
                flow_pct, flow_units = self.settings["SMALL_FLOW_PCT"], self.settings["SMALL_FLOW_UNITS"]
            elif self.buy_step == 2:
                flow_pct, flow_units = self.settings["LARGE_FLOW_PCT"], self.settings["LARGE_FLOW_UNITS"]
            else:
                return "ACTIVE", 0, 0

            if self.hwm > self.last_buy_price * (1 + (flow_pct * 0.5)):
                target_base = self.hwm
            
            target_price = target_base * (1 - flow_pct)
            
            if low <= target_price:
                buy_amt = self.settings["UNIT_SIZE"] * flow_units
                required_margin = (buy_amt / self.settings["LEVERAGE"]) * self.settings["MARGIN_BUFFER"]
                if self.cash >= required_margin:
                    exec_price = target_price * (1 + SLIPPAGE_RATE)
                    qty = buy_amt / exec_price
                    self.cash -= buy_amt * FEE_RATE
                    
                    new_qty = self.position['qty'] + qty
                    new_avg = ((self.position['qty'] * self.position['avg_price']) + (qty * exec_price)) / new_qty
                    self.position = {'qty': new_qty, 'avg_price': new_avg}
                    
                    self.last_buy_price = exec_price
                    self.buy_step += 1
                    self.hwm = exec_price

        return "ACTIVE", 0, 0

# --- 5. 시뮬레이터 클래스 (봇 매니저) ---
class CompoundSimulator:
    def __init__(self, df, settings):
        self.df = df
        self.settings = settings
        self.wallet = 0.0
        self.bots = []
        self.total_injected = 0.0
        self.next_bot_id = 1
        self.yearly_log = []

    def spawn_bot(self):
        if self.wallet >= INITIAL_CASH:
            self.wallet -= INITIAL_CASH
            bot = PhoenixBot(self.next_bot_id, self.settings)
            self.bots.append(bot)
            logger.info(f"🌱 Bot Spawned! ID: {self.next_bot_id}, Total Bots: {len(self.bots)}, Wallet: ${self.wallet:,.2f}")
            self.next_bot_id += 1

    def run(self):
        initial_bot = PhoenixBot(self.next_bot_id, self.settings)
        self.bots.append(initial_bot)
        self.next_bot_id += 1
        
        last_year = None

        for row in self.df.itertuples():
            for bot in self.bots:
                status, profit, injection = bot.run_tick(row)
                if status == "PROFIT_RESET":
                    self.wallet += profit
                elif status == "STOP_LOSS":
                    self.total_injected += injection
            
            while self.wallet >= INITIAL_CASH:
                self.spawn_bot()

            current_year = row.timestamp.year
            if last_year != current_year:
                if last_year is not None:
                    self.log_yearly_performance(last_year)
                last_year = current_year
        
        self.log_yearly_performance(last_year)
        self.print_final_report()

    def get_total_equity(self, price):
        total_bot_equity = sum(bot.get_equity(price) for bot in self.bots)
        return total_bot_equity + self.wallet

    def log_yearly_performance(self, year):
        last_day_price = self.df[self.df['timestamp'].dt.year == year].iloc[-1].close
        total_equity = self.get_total_equity(last_day_price)
        
        self.yearly_log.append({
            "Year": year,
            "Bot Count": len(self.bots),
            "Total Equity": total_equity,
            "Secured Wallet": self.wallet,
            "Total Injected": self.total_injected
        })
        logger.info(f"📈 Year-End {year}: Bots: {len(self.bots)}, Total Equity: ${total_equity:,.2f}")

    def print_final_report(self):
        last_price = self.df.iloc[-1].close
        final_total_equity = self.get_total_equity(last_price)
        total_invested = INITIAL_CASH + self.total_injected
        net_profit = final_total_equity - total_invested
        
        num_years = (self.df.iloc[-1].timestamp - self.df.iloc[0].timestamp).days / 365.25
        
        cagr = ((final_total_equity / total_invested) ** (1 / num_years) - 1) * 100 if total_invested > 0 and num_years > 0 else 0
        simple_roi = (net_profit / total_invested) * 100 if total_invested > 0 else 0

        print("\n" + "="*80)
        print("📊 복리 시뮬레이션 최종 결과")
        print("="*80)
        print(f"  - 최종 총 자산 (Total Equity): ${final_total_equity:,.2f}")
        print(f"  - 생성된 총 봇 개수 (Bot Count): {len(self.bots)}")
        print(f"  - 총 추가 투입금 (Total Injected): ${self.total_injected:,.2f}")
        print(f"  - 총 투자 원금 (Total Invested): ${total_invested:,.2f}")
        print(f"  - 순수익 (Net Profit): ${net_profit:,.2f}")
        print("-" * 80)
        print(f"  - 단순 수익률 (Simple ROI): {simple_roi:.2f}%")
        print(f"  - 연 복리 수익률 (CAGR): {cagr:.2f}%")
        print("="*80)
        
        print("\n📜 연도별 상세 로그")
        print("-" * 80)
        if self.yearly_log:
            df_log = pd.DataFrame(self.yearly_log)
            print(df_log.to_string(index=False))
        print("="*80)

# --- 6. 메인 실행 함수 ---
def main():
    scenario = "Full"
    start_date = "2020-01-01 00:00:00"
    end_date = "2025-12-04 23:59:59"
    
    settings = {
        "UNIT_SIZE": UNIT_SIZE,
        "TAKE_PROFIT_PCT": TAKE_PROFIT_PCT,
        "SMALL_FLOW_PCT": SMALL_FLOW_PCT,
        "LARGE_FLOW_PCT": LARGE_FLOW_PCT,
        "INITIAL_UNITS": INITIAL_UNITS,
        "SMALL_FLOW_UNITS": SMALL_FLOW_UNITS,
        "LARGE_FLOW_UNITS": LARGE_FLOW_UNITS,
        "LEVERAGE": LEVERAGE,
        "PROFIT_RESET_TARGET": PROFIT_RESET_TARGET,
        "MARGIN_BUFFER": MARGIN_BUFFER
    }
    
    print(f"🚀 {MARKET} 복리 효과 시뮬레이션 시작")
    print(f"▶ 시나리오: {scenario} ({start_date} ~ {end_date})")
    print(f"▶ 설정: Unit Size={UNIT_SIZE}, Leverage={LEVERAGE}, Reset Target={PROFIT_RESET_TARGET*100 if PROFIT_RESET_TARGET else 'None'}%")
    print("="*80)

    df = load_candles(MARKET, start_date, end_date)
    if not df.empty:
        simulator = CompoundSimulator(df, settings)
        simulator.run()

if __name__ == "__main__":
    main()