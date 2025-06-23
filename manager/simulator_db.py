# manager/simulator_db.py

import sqlite3
import pandas as pd
from datetime import datetime
from strategy.casino_strategy import generate_buy_orders, generate_sell_orders

INITIAL_CASH = 5_000_000
BUY_FEE = 0.0005
SELL_FEE = 0.0005
DB_PATH = "db/candle_db.sqlite"

def load_candles_from_db(market: str, start: str, end: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT * FROM minute_candles
        WHERE market = ? AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    df = pd.read_sql_query(query, conn, params=[market, start, end])
    conn.close()

    df["시간"] = pd.to_datetime(df["timestamp"])
    df["시가"] = df["open"]
    df["고가"] = df["high"]
    df["저가"] = df["low"]
    df["종가"] = df["close"]

    return df[["시간", "시가", "고가", "저가", "종가", "volume"]]

def simulate_with_db(
    market: str,
    start: str,
    end: str,
    unit_size: float,
    small_flow_pct: float,
    small_flow_units: int,
    large_flow_pct: float,
    large_flow_units: int,
    take_profit_pct: float
):
    print(f"[simulator_db] ⏱️ 블레텍스트 시작 - {market}, {start} ~ {end}")

    df = load_candles_from_db(market, start, end)
    if df.empty:
        print("❌ 선택한 기간에 대한 데이터가 DB에 없습니다.")
        return

    setting_df = pd.DataFrame([{
        "market": market,
        "unit_size": unit_size,
        "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units,
        "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units,
        "take_profit_pct": take_profit_pct
    }])

    cash = INITIAL_CASH
    holdings = {}
    buy_log_df = pd.DataFrame(columns=[
        "time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"
    ])
    sell_log_df = pd.DataFrame(columns=[
        "market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"
    ])

    realized_pnl = 0.0
    total_buy_amount = 0.0
    total_buy_volume = 0.0
    cumulative_fee = 0.0
    last_trade_fee = 0.0
    last_trade_amount = 0.0
    logs = []

    for _, row in df.iterrows():
        now = row["시간"]
        current_price = row["종가"]
        events = []

        current_prices = {market: current_price}
        buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

        for idx, r in buy_log_df.iterrows():
            if r["filled"] in ["update", "wait"] and r["market"] == market:
                price = r["target_price"]
                amount = r["buy_amount"]
                buy_type = r["buy_type"]

                if buy_type == "initial" or current_price <= price:
                    if cash >= amount:
                        fee = amount * BUY_FEE
                        volume = (amount - fee) / price
                        cash -= amount
                        cumulative_fee += fee
                        total_buy_amount += amount
                        total_buy_volume += volume
                        holdings[market] = holdings.get(market, 0) + volume
                        buy_log_df.at[idx, "filled"] = "done"
                        last_trade_amount = amount
                        last_trade_fee = fee

                        events.append(f"{buy_type} 매수")
                    else:
                        buy_log_df.at[idx, "filled"] = "wait"
                else:
                    buy_log_df.at[idx, "filled"] = "wait"

        if market in holdings and holdings[market] > 0:
            balance = holdings[market]
            avg_buy_price = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
            holdings_info = {
                market: {
                    "balance": balance,
                    "locked": 0,
                    "avg_price": avg_buy_price,
                    "current_price": current_price
                }
            }

            sell_log_df = generate_sell_orders(setting_df, holdings_info, sell_log_df)

            for idx, r in sell_log_df.iterrows():
                if r["filled"] == "update" and r["market"] == market:
                    target_price = r["target_sell_price"]
                    if current_price >= target_price:
                        volume = r["quantity"]
                        fee = volume * current_price * SELL_FEE
                        proceeds = volume * current_price - fee
                        pnl = (current_price - avg_buy_price) * volume

                        cash += proceeds
                        cumulative_fee += fee
                        realized_pnl += pnl - fee
                        holdings[market] = 0
                        sell_log_df.at[idx, "filled"] = "done"
                        buy_log_df = buy_log_df[buy_log_df["market"] != market]
                        total_buy_amount = 0.0
                        total_buy_volume = 0.0
                        last_trade_amount = proceeds
                        last_trade_fee = fee
                        events.append("매도")

        quantity = holdings.get(market, 0)
        avg_price = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
        gap_pct = round((current_price - avg_price) / avg_price * 100, 2) if avg_price > 0 else 0
        portfolio_value = cash + quantity * current_price
        signal_str = " / ".join(events) if events else "보유"

        logs.append({
            "시간": now,
            "마켓": market,
            "시가": row["시가"],
            "고가": row["고가"],
            "종가": current_price,
            "신호": signal_str,
            "매매금액": round(last_trade_amount, 2),
            "현재 평단가": round(avg_price, 2),
            "현재 종가와 평단가의 gap(%)": gap_pct,
            "누적 매수금": round(total_buy_amount, 2),
            "실현 손익": round(realized_pnl, 2),
            "보유 현금": round(cash, 2),
            "거래시 수수료": round(last_trade_fee, 2),
            "총 누적 수수료": round(cumulative_fee, 2),
            "총 포트폴리오 값": round(portfolio_value, 2)
        })

    result_df = pd.DataFrame(logs)
    filename = f"전략_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    result_df.to_excel(filename, index=False)
    print(f"\n파일 저장 완료: {filename}")

    if not result_df.empty:
        first, last = result_df.iloc[0], result_df.iloc[-1]
        print("\n통계 요약")
        print(f"▶ 시작: {first['시간']} | 마켓: {first['마켓']}")
        print(f"  - 누적 매수금: {first['누적 매수금']:,}원")
        print(f"  - 실현 손익: {first['실현 손익']:,}원")
        print(f"  - 보유 현금: {first['보유 현금']:,}원")
        print(f"  - 총 포트폴리오 가치: {first['총 포트폴리오 값']:,}원")

        print(f"\n▶ 종료: {last['시간']} | 마켓: {last['마켓']}")
        print(f"  - 누적 매수금: {last['누적 매수금']:,}원")
        print(f"  - 실현 손익: {last['실현 손익']:,}원")
        print(f"  - 보유 현금: {last['보유 현금']:,}원")
        print(f"  - 총 포트폴리오 가치: {last['총 포트폴리오 값']:,}원")
