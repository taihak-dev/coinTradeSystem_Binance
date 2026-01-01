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
REINVEST_MIN_CASH = 3000.0
STOP_LOSS_THRESHOLD = 0.65
PANIC_SELL_PENALTY = 0.02
COOLDOWN_MINUTES = 1440

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
PROFIT_RESET_TARGET = 1.0

# 수수료 및 슬리피지
FEE_RATE = 0.0004
SLIPPAGE_RATE = 0.0005

# 로그 저장 옵션
SAVE_FULL_LOG = False

# --- 2. 헬퍼 함수 ---
def _format_duration(minutes: float) -> str:
    if minutes is None or np.isnan(minutes) or minutes < 0:
        return "N/A"
    
    minutes = int(minutes)
    days, rem_min = divmod(minutes, 1440)
    hours, mins = divmod(rem_min, 60)
    
    years, days = divmod(days, 365)
    months, days = divmod(days, 30)
    
    parts = []
    if years > 0: parts.append(f"{years}년")
    if months > 0: parts.append(f"{months}개월")
    if days > 0: parts.append(f"{days}일")
    if hours > 0: parts.append(f"{hours}시간")
    if mins > 0: parts.append(f"{mins}분")
    
    if not parts:
        return "0분"
    
    return " ".join(parts[:3]) # 상위 3개 단위만 표시

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
    def __init__(self, bot_id, settings, initial_capital):
        self.id = bot_id
        self.settings = settings
        self.initial_capital = initial_capital 
        self.cash = initial_capital
        self.position = {'qty': 0.0, 'avg_price': 0.0}
        self.buy_step = 0
        self.last_buy_price = 0.0
        self.hwm = 0.0
        self.cooldown_until = None
        self.position_entry_time = None
        
        # 통계용 변수
        # (duration_minutes, start_time, end_time) 튜플을 저장
        self.trade_history = [] 
        self.equity_history = [initial_capital]
        self.sell_count = 0

    def get_equity(self, price):
        if self.position['qty'] > 0:
            unrealized_pnl = (price - self.position['avg_price']) * self.position['qty']
            return self.cash + unrealized_pnl
        return self.cash

    def _record_trade_duration(self, end_time):
        if self.position_entry_time:
            duration = (end_time - self.position_entry_time).total_seconds() / 60
            self.trade_history.append((duration, self.position_entry_time, end_time))
            self.position_entry_time = None

    def run_tick(self, row):
        now, high, low, close = row.timestamp, row.high, row.low, row.close
        action = ""

        current_equity = self.get_equity(close)
        self.equity_history.append(current_equity)

        if self.cooldown_until and now < self.cooldown_until:
            return "COOLDOWN", 0, 0, ""
        elif self.cooldown_until:
            self.cooldown_until = None

        if self.position['qty'] > 0:
            self.hwm = max(self.hwm, high)
        else:
            self.hwm = 0.0

        equity_at_low = self.get_equity(low)
        if equity_at_low <= self.initial_capital * STOP_LOSS_THRESHOLD:
            salvaged_equity = equity_at_low * (1 - PANIC_SELL_PENALTY)
            needed_injection = self.initial_capital - salvaged_equity
            self.cash = self.initial_capital
            self.position = {'qty': 0.0, 'avg_price': 0.0}
            self.buy_step = 0
            self.last_buy_price = 0.0
            self.hwm = 0.0
            self.cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
            
            self._record_trade_duration(now)
            self.sell_count += 1
            return "STOP_LOSS", 0, needed_injection, f"SL (Bot {self.id})"

        if self.settings["PROFIT_RESET_TARGET"] is not None:
            target_equity = self.initial_capital * (1 + self.settings["PROFIT_RESET_TARGET"])
            equity_at_close = self.get_equity(close)
            
            if equity_at_close >= target_equity:
                if self.position['qty'] > 0:
                    exec_price = close * (1 - SLIPPAGE_RATE)
                    revenue = self.position['qty'] * exec_price
                    cost = self.position['qty'] * self.position['avg_price']
                    fee = revenue * FEE_RATE
                    self.cash += (revenue - cost) - fee
                
                profit_to_secure = self.cash - self.initial_capital
                self.cash = self.initial_capital
                self.position = {'qty': 0.0, 'avg_price': 0.0}
                self.buy_step = 0
                self.last_buy_price = 0.0
                self.hwm = 0.0
                
                self._record_trade_duration(now)
                self.sell_count += 1
                return "PROFIT_RESET", profit_to_secure, 0, f"Reset (Bot {self.id})"

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
                
                self._record_trade_duration(now)
                self.sell_count += 1
                return "TAKE_PROFIT", 0, 0, f"TP (Bot {self.id})"

        if self.position['qty'] == 0:
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
                self.position_entry_time = now
                action = f"Initial (Bot {self.id})"
        
        elif self.buy_step > 0:
            target_base = self.last_buy_price
            if self.buy_step == 1:
                flow_pct, flow_units = self.settings["SMALL_FLOW_PCT"], self.settings["SMALL_FLOW_UNITS"]
            elif self.buy_step == 2:
                flow_pct, flow_units = self.settings["LARGE_FLOW_PCT"], self.settings["LARGE_FLOW_UNITS"]
            else:
                return "ACTIVE", 0, 0, ""

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
                    action = f"Flow (Bot {self.id})"

        return "ACTIVE", 0, 0, action

    def get_stats(self):
        if not self.trade_history:
            return {
                "max_duration_str": "N/A",
                "avg_duration_str": "N/A",
                "sell_count": 0,
                "mdd": 0
            }

        # (duration, start, end) 튜플 리스트
        durations = [t[0] for t in self.trade_history]
        
        # 최장 보유 기간 찾기
        max_idx = np.argmax(durations)
        max_duration = durations[max_idx]
        max_start = self.trade_history[max_idx][1]
        max_end = self.trade_history[max_idx][2]
        
        max_duration_str = f"{_format_duration(max_duration)} ({max_start.strftime('%Y-%m-%d %H:%M')} ~ {max_end.strftime('%Y-%m-%d %H:%M')})"
        
        # 평균 보유 기간
        avg_duration = sum(durations) / len(durations)
        avg_duration_str = _format_duration(avg_duration)
        
        equity_series = pd.Series(self.equity_history)
        peak = equity_series.cummax()
        drawdown = (equity_series - peak) / peak
        mdd = drawdown.min() * 100 if not drawdown.empty else 0

        return {
            "max_duration_str": max_duration_str,
            "avg_duration_str": avg_duration_str,
            "sell_count": self.sell_count,
            "mdd": mdd
        }

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
        self.full_log = []
        self.total_equity_history = []

    def spawn_bot(self):
        if self.wallet >= REINVEST_MIN_CASH:
            capital_to_deploy = min(self.wallet, INITIAL_CASH)
            self.wallet -= capital_to_deploy
            bot = PhoenixBot(self.next_bot_id, self.settings, initial_capital=capital_to_deploy)
            self.bots.append(bot)
            logger.info(f"🌱 Bot Spawned! ID: {self.next_bot_id}, Capital: ${capital_to_deploy:,.2f}, Total Bots: {len(self.bots)}, Wallet Rem: ${self.wallet:,.2f}")
            self.next_bot_id += 1

    def run(self):
        initial_bot = PhoenixBot(self.next_bot_id, self.settings, initial_capital=INITIAL_CASH)
        self.bots.append(initial_bot)
        self.next_bot_id += 1
        
        last_year = None

        for row in self.df.itertuples():
            actions_this_tick = []
            current_total_equity = self.wallet
            
            for bot in self.bots:
                status, profit, injection, action = bot.run_tick(row)
                if status == "PROFIT_RESET":
                    self.wallet += profit
                elif status == "STOP_LOSS":
                    self.total_injected += injection
                if action:
                    actions_this_tick.append(action)
                
                current_total_equity += bot.get_equity(row.close)
            
            self.total_equity_history.append(current_total_equity)

            while self.wallet >= REINVEST_MIN_CASH:
                self.spawn_bot()

            current_year = row.timestamp.year
            if last_year != current_year:
                if last_year is not None:
                    self.log_yearly_performance(last_year)
                last_year = current_year
            
            if SAVE_FULL_LOG:
                holding_period_minutes = None
                if self.bots and self.bots[0].position_entry_time:
                    holding_period_minutes = (row.timestamp - self.bots[0].position_entry_time).total_seconds() / 60

                self.full_log.append({
                    "Time": row.timestamp,
                    "Price": row.close,
                    "Action": ", ".join(actions_this_tick),
                    "Total_Equity": current_total_equity,
                    "Bot_Count": len(self.bots),
                    "Wallet": self.wallet,
                    "Secured_Profit": self.wallet,
                    "Total_Injected": self.total_injected,
                    "Holding_Period": _format_duration(holding_period_minutes)
                })
        
        self.log_yearly_performance(last_year)
        self.print_final_report()
        
        if SAVE_FULL_LOG:
            self.save_log_to_excel()

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

        # 전체 시스템 MDD 계산
        equity_series = pd.Series(self.total_equity_history)
        peak = equity_series.cummax()
        drawdown = (equity_series - peak) / peak
        system_mdd = drawdown.min() * 100 if not drawdown.empty else 0

        print("\n" + "="*120)
        print("📊 복리 시뮬레이션 최종 결과")
        print("="*120)
        print(f"  - 최종 총 자산 (Total Equity): ${final_total_equity:,.2f}")
        print(f"  - 생성된 총 봇 개수 (Bot Count): {len(self.bots)}")
        print(f"  - 총 추가 투입금 (Total Injected): ${self.total_injected:,.2f}")
        print(f"  - 총 투자 원금 (Total Invested): ${total_invested:,.2f}")
        print(f"  - 순수익 (Net Profit): ${net_profit:,.2f}")
        print("-" * 120)
        print(f"  - 단순 수익률 (Simple ROI): {simple_roi:.2f}%")
        print(f"  - 연 복리 수익률 (CAGR): {cagr:.2f}%")
        print(f"  - 시스템 최대 낙폭 (System MDD): {system_mdd:.2f}%")
        print("="*120)
        
        print("\n🤖 봇별 상세 통계 (Top 5 & Bottom 5)")
        print("-" * 120)
        # 컬럼 너비 조정
        print(f"{'Bot ID':<8} | {'MDD':<10} | {'Max Duration (Period)':<60} | {'Avg Duration':<15} | {'Sell Count':<10}")
        print("-" * 120)
        
        bot_stats = []
        for bot in self.bots:
            stats = bot.get_stats()
            bot_stats.append({
                "id": bot.id,
                "mdd": stats['mdd'],
                "max_dur": stats['max_duration_str'],
                "avg_dur": stats['avg_duration_str'],
                "sell_cnt": stats['sell_count']
            })
        
        display_bots = bot_stats[:5] + bot_stats[-5:] if len(bot_stats) > 10 else bot_stats
        
        for stat in display_bots:
            print(f"{stat['id']:<8} | {stat['mdd']:>9.2f}% | {stat['max_dur']:<60} | {stat['avg_dur']:<15} | {stat['sell_cnt']:<10}")
        
        if len(bot_stats) > 10:
            print(f"... (Total {len(bot_stats)} bots) ...")
        print("="*120)

        print("\n📜 연도별 상세 로그")
        print("-" * 120)
        if self.yearly_log:
            df_log = pd.DataFrame(self.yearly_log)
            print(df_log.to_string(index=False))
        print("="*120)

    def save_log_to_excel(self):
        if not self.full_log:
            logger.warning("⚠️ 상세 로그 데이터가 없어 파일을 저장하지 않습니다.")
            return
        
        log_df = pd.DataFrame(self.full_log)
        
        start_str = self.df.iloc[0].timestamp.strftime('%Y%m%d')
        end_str = self.df.iloc[-1].timestamp.strftime('%Y%m%d')
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        filename = f"CompoundLog_{MARKET}_{start_str}-{end_str}_{now_str}.xlsx"
        
        try:
            log_df.to_excel(filename, index=False)
            logger.info(f"✅ 상세 로그가 '{filename}' 파일로 저장되었습니다.")
        except Exception as e:
            logger.error(f"❌ 상세 로그 파일 저장 실패: {e}")

# --- 6. 메인 실행 함수 ---
def main():
    scenario = "Full"
    start_date = "2023-01-01 00:00:00"
    end_date = "2025-12-28 23:59:59"
    
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
    print(f"▶ 기간: {start_date} ~ {end_date}")
    print(f"▶ 설정: Unit Size={UNIT_SIZE}, Leverage={LEVERAGE}, Reset Target={PROFIT_RESET_TARGET*100 if PROFIT_RESET_TARGET else 'None'}%")
    print(f"▶ 재투자: Min Cash=${REINVEST_MIN_CASH:,.0f}")
    print("="*80)

    df = load_candles(MARKET, start_date, end_date)
    if not df.empty:
        simulator = CompoundSimulator(df, settings)
        simulator.run()

if __name__ == "__main__":
    main()