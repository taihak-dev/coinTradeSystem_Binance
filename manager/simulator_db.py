# manager/simulator_db.py
import sqlite3
import pandas as pd
from datetime import datetime
from strategy.casino_strategy import generate_buy_orders, generate_sell_orders
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")

INITIAL_CASH = 60_000
BUY_FEE = 0.0005
SELL_FEE = 0.0005


def load_candles_from_db(market: str, start: str, end: str) -> pd.DataFrame:
    # 이 함수는 변경사항 없습니다.
    logging.info(f"📊 {market} 캔들 데이터 DB 로드 시도 중: {start} ~ {end}")
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {os.path.abspath(DB_PATH)}")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM minute_candles WHERE market = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
    try:
        df = pd.read_sql_query(query, conn, params=[market, start, end])
    finally:
        conn.close()
    if df.empty: return df
    df["시간"] = pd.to_datetime(df["timestamp"])
    df["시가"], df["고가"], df["저가"], df["종가"] = df["open"], df["high"], df["low"], df["close"]
    return df[["시간", "시가", "고가", "저가", "종가", "volume"]]


def simulate_with_db(
        market: str, start: str, end: str, unit_size: float,
        small_flow_pct: float, small_flow_units: int,
        large_flow_pct: float, large_flow_units: int,
        take_profit_pct: float, leverage: int = 1
):
    logging.info(f"--- ⏱️ DB 기반 백테스트 시작: {market}, 기간: {start} ~ {end} ---")

    df_candles = load_candles_from_db(market, start, end)
    if df_candles.empty: return

    setting_df = pd.DataFrame([{
        "market": market, "unit_size": unit_size, "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units, "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units, "take_profit_pct": take_profit_pct
    }])

    cash = INITIAL_CASH
    holdings = {}
    buy_log_df = pd.DataFrame(
        columns=["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])
    sell_log_df = pd.DataFrame(columns=["market", "target_price", "sell_amount", "sell_uuid", "filled"])
    realized_pnl, cumulative_fee = 0.0, 0.0
    total_buy_info = {'amount': 0.0, 'volume': 0.0}
    logs = []

    progress_interval = len(df_candles) // 10 or 1

    for i, row in df_candles.iterrows():
        if (i + 1) % progress_interval == 0:
            logging.info(
                f"⏳ 시뮬레이션 진행 중: {row['시간'].strftime('%Y-%m-%d %H:%M:%S')} ({((i + 1) / len(df_candles) * 100):.1f}%)")

        now, current_price = row["시간"], row["종가"]
        events, last_trade_amount, last_trade_fee = [], 0.0, 0.0

        new_buy_orders_df = generate_buy_orders(setting_df, buy_log_df, {market: current_price}, holdings)

        # ❗️ 변경점: buy_log_df가 비어있는 첫 경우를 따로 처리하여 경고 원천 차단
        if not new_buy_orders_df.empty:
            if buy_log_df.empty:
                buy_log_df = new_buy_orders_df.copy()
            else:
                buy_log_df = pd.concat([buy_log_df, new_buy_orders_df], ignore_index=True)

        for idx, r_buy in buy_log_df.iterrows():
            if r_buy["filled"] in ["update", "wait"]:
                price_to_check, amount_to_buy, buy_type = float(r_buy["target_price"]), float(r_buy["buy_amount"]), \
                r_buy["buy_type"]
                is_initial = buy_type == "initial"
                if (is_initial and amount_to_buy > 0) or (not is_initial and current_price <= price_to_check):
                    if cash >= amount_to_buy:
                        final_price = current_price if is_initial else price_to_check
                        fee = amount_to_buy * BUY_FEE
                        volume = (amount_to_buy - fee) / final_price

                        cash -= amount_to_buy
                        cumulative_fee += fee
                        total_buy_info['amount'] += amount_to_buy
                        total_buy_info['volume'] += volume

                        holdings[market] = {'balance': holdings.get(market, {}).get('balance', 0) + volume}
                        buy_log_df.at[idx, "filled"] = "done"
                        last_trade_amount, last_trade_fee = amount_to_buy, fee
                        events.append(f"{buy_type} 매수 체결")

        if market in holdings:
            avg_buy_price = total_buy_info['amount'] / total_buy_info['volume'] if total_buy_info['volume'] > 0 else 0
            holdings[market]['avg_price'] = avg_buy_price

            # 매도 주문 로직은 시뮬레이션 편의상 간단하게 목표가만 계산하여 처리합니다.
            target_sell_price = round(avg_buy_price * (1 + take_profit_pct), 8)

            if current_price >= target_sell_price:
                volume_to_sell = holdings[market]['balance']
                fee = volume_to_sell * current_price * SELL_FEE
                proceeds = volume_to_sell * current_price - fee
                pnl = (current_price - avg_buy_price) * volume_to_sell

                cash += proceeds
                cumulative_fee += fee
                realized_pnl += pnl - fee
                last_trade_amount, last_trade_fee = proceeds, fee
                events.append("매도 체결")

                indices_to_drop = buy_log_df[(buy_log_df['market'] == market) & (buy_log_df['filled'] == 'wait')].index
                buy_log_df.drop(indices_to_drop, inplace=True)

                holdings.pop(market, None)
                sell_log_df = sell_log_df[sell_log_df['market'] != market]
                total_buy_info = {'amount': 0.0, 'volume': 0.0}

        quantity = holdings.get(market, {}).get('balance', 0)
        avg_price = holdings.get(market, {}).get('avg_price', 0)
        portfolio_value = cash + quantity * current_price
        logs.append({
            "시간": now, "종가": current_price, "신호": " / ".join(events) if events else "보유 중",
            "매매금액": round(last_trade_amount, 2), "현재 평단가": round(avg_price, 5),
            "실현 손익": round(realized_pnl, 2), "보유 현금": round(cash, 2),
            "총 누적 수수료": round(cumulative_fee, 2), "총 포트폴리오 값": round(portfolio_value, 2)
        })

    result_df = pd.DataFrame(logs)
    filename = f"DB_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    result_df.to_excel(filename, index=False)
    logging.info(f"✅ 백테스트 결과 파일 저장 완료: {filename}")

    # ... (최종 통계 요약 부분은 기존과 동일) ...