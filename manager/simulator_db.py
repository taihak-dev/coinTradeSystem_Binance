# manager/simulator_db.py
import sqlite3
import pandas as pd
from datetime import datetime
from strategy.casino_strategy import generate_buy_orders, generate_sell_orders
import os
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


def _format_duration(minutes: int) -> str:
    # (이전 단계에서 추가한 헬퍼 함수 - 변경 없음)
    if minutes < 0: return "N/A"
    days = minutes // (60 * 24)
    hours = (minutes // 60) % 24
    mins = minutes % 60
    if days > 0:
        return f"{days}일 {hours}시간 {mins}분"
    elif hours > 0:
        return f"{hours}시간 {mins}분"
    else:
        return f"{mins}분"


def load_candles_from_db(market: str, start: str, end: str) -> pd.DataFrame:
    # (기존 코드와 동일 - 변경 없음)
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
        take_profit_pct: float, leverage: int = 1,
        # --- 👇👇👇 2. 파라미터 3개 추가 (기본값 설정) 👇👇👇 ---
        initial_cash: float = 60_000.0,
        buy_fee: float = 0.0005,
        sell_fee: float = 0.0005
        # --- 👆👆👆 2. 파라미터 추가 완료 ---
):
    logging.info(f"--- ⏱️ DB 기반 백테스트 시작: {market}, 기간: {start} ~ {end} ---")

    df_candles = load_candles_from_db(market, start, end)
    if df_candles.empty:
        logging.warning("⚠️ 캔들 데이터가 없습니다. 백테스트를 종료합니다.")
        return

    setting_df = pd.DataFrame([{
        "market": market, "unit_size": unit_size, "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units, "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units, "take_profit_pct": take_profit_pct,
        "leverage": leverage
    }])

    # --- 👇 3. 파라미터로 초기화 ---
    cash = initial_cash
    # --- 👆 3. ---

    holdings = {}
    buy_log_df = pd.DataFrame(
        columns=["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"])
    sell_log_df = pd.DataFrame(columns=["market", "target_price", "sell_amount", "sell_uuid", "filled"])
    realized_pnl, cumulative_fee = 0.0, 0.0
    total_buy_info = {'amount': 0.0, 'volume': 0.0}
    logs = []

    current_holding_minutes = 0
    current_units_held = 0.0
    total_sell_trades = 0
    progress_interval = len(df_candles) // 10 or 1

    for i, row in df_candles.iterrows():
        # (중간 로직... 변경 없음)
        if (i + 1) % progress_interval == 0:
            logging.info(
                f"⏳ 시뮬레이션 진행 중: {row['시간'].strftime('%Y-%m-%d %H:%M:%S')} ({((i + 1) / len(df_candles) * 100):.1f}%)")

        now, current_price = row["시간"], row["종가"]
        events, last_trade_amount, last_trade_fee = [], 0.0, 0.0

        if market in holdings:
            current_holding_minutes += 1

        new_buy_orders_df = generate_buy_orders(setting_df, buy_log_df, {market: current_price}, holdings, cash)

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

                        # --- 👇 4. 파라미터 사용 ---
                        fee = amount_to_buy * buy_fee
                        # --- 👆 4. ---

                        volume = (amount_to_buy - fee) / final_price

                        cash -= amount_to_buy
                        cumulative_fee += fee
                        total_buy_info['amount'] += amount_to_buy
                        total_buy_info['volume'] += volume

                        current_units_held += (amount_to_buy / unit_size) if unit_size > 0 else 0

                        holdings[market] = {'balance': holdings.get(market, {}).get('balance', 0) + volume}
                        buy_log_df.at[idx, "filled"] = "done"
                        last_trade_amount, last_trade_fee = amount_to_buy, fee
                        events.append(f"{buy_type} 매수 체결")

        if market in holdings:
            avg_buy_price = total_buy_info['amount'] / total_buy_info['volume'] if total_buy_info['volume'] > 0 else 0
            holdings[market]['avg_price'] = avg_buy_price

            target_sell_price = round(avg_buy_price * (1 + take_profit_pct), 8)

            if current_price >= target_sell_price:
                volume_to_sell = holdings[market]['balance']

                # --- 👇 5. 파라미터 사용 ---
                fee = volume_to_sell * current_price * sell_fee
                # --- 👆 5. ---

                proceeds = volume_to_sell * current_price - fee
                pnl = (current_price - avg_buy_price) * volume_to_sell

                cash += proceeds
                cumulative_fee += fee
                realized_pnl += pnl - fee
                last_trade_amount, last_trade_fee = proceeds, fee
                events.append("매도 체결")

                total_sell_trades += 1
                current_holding_minutes = 0
                current_units_held = 0.0

                indices_to_drop = buy_log_df[(buy_log_df['market'] == market) & (buy_log_df['filled'] == 'wait')].index
                buy_log_df.drop(indices_to_drop, inplace=True)

                holdings.pop(market, None)
                sell_log_df = sell_log_df[sell_log_df['market'] != market]
                total_buy_info = {'amount': 0.0, 'volume': 0.0}

                buy_log_df = buy_log_df[buy_log_df['market'] != market].copy()
                logging.info(f"🧹 {market} 매도 완료. 매수 기록을 초기화합니다.")

        quantity = holdings.get(market, {}).get('balance', 0)
        avg_price = holdings.get(market, {}).get('avg_price', 0)
        portfolio_value = cash + quantity * current_price

        logs.append({
            "시간": now, "종가": current_price, "신호": " / ".join(events) if events else "보유 중",
            "매매금액": round(last_trade_amount, 2), "현재 평단가": round(avg_price, 5),
            "실현 손익": round(realized_pnl, 2), "보유 현금": round(cash, 2),
            "총 누적 수수료": round(cumulative_fee, 2), "총 포트폴리오 값": round(portfolio_value, 2),
            "현재 유닛": current_units_held,
            "연속 보유(분)": current_holding_minutes
        })

    result_df = pd.DataFrame(logs)

    filename = f"DB_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    result_df.to_excel(filename, index=False)
    logging.info(f"✅ 백테스트 결과 파일 저장 완료: {filename}")

    # --- (이전 단계에서 추가한 '결과 요약' 로직 - 변경 없음) ---
    if not result_df.empty:
        # 1. 기본 정보
        final_portfolio_value = result_df['총 포트폴리오 값'].iloc[-1]

        total_roi_pct = ((final_portfolio_value - initial_cash) / initial_cash) * 100 if initial_cash > 0 else 0
        final_realized_pnl = result_df['실현 손익'].iloc[-1]

        # 2. 최장 보유 시간
        max_duration_minutes = result_df['연속 보유(분)'].max()
        max_duration_str = _format_duration(int(max_duration_minutes))

        # 3. 최다 보유 유닛
        max_units = result_df['현재 유닛'].max()

        # 4. 최대 낙폭(MDD) 계산
        peak = result_df['총 포트폴리오 값'].cummax()
        drawdown = (result_df['총 포트폴리오 값'] - peak) / peak
        max_drawdown_pct = drawdown.min() * 100

        try:
            mdd_end_index = drawdown.idxmin()
            mdd_trough_value = result_df.loc[mdd_end_index, '총 포트폴리오 값']  # <--- '최저점' 값
            mdd_peak_value = peak.loc[mdd_end_index]
            mdd_detail_str = f" (Peak {mdd_peak_value:,.2f} USDT -> Trough {mdd_trough_value:,.2f} USDT)"
        except Exception:
            mdd_trough_value = 0  # 예외 발생 시 기본값
            mdd_detail_str = ""

        # --- 👇👇👇 1. 청산 발생 여부 확인 로직 추가 👇👇👇 ---
        # (총 자산 최저점이 0 이하로 내려갔는지 확인)
        liquidation_occurred = "🚨 예 (총 자산 0 이하 도달)" if mdd_trough_value <= 0 else "✅ 아니오"
        # --- 👆👆👆 1. 수정 완료 --- 👆👆👆

        # --- 요약 출력 ---
        print("\n" + "=" * 50)
        print("          📈 백테스트 결과 요약 📈          ")
        print("=" * 50)
        print(f"  - 마켓 (Market):       {market}")
        print(f"  - 기간 (Period):       {start} ~ {end}")
        print(f"  - 초기 자본 (Initial): {initial_cash:,.2f} USDT")
        print("." * 50)
        print("  --- 💰 수익성 (Profitability) ---")
        print(f"  - 최종 포트폴리오 가치:   {final_portfolio_value:,.2f} USDT")
        print(f"  - 총 수익률 (Total ROI): {total_roi_pct:,.2f} %")
        print(f"  - 기간 내 실현 손익:     {final_realized_pnl:,.2f} USDT")
        print(f"  - 총 거래 횟수 (매도):   {total_sell_trades} 회")
        print("." * 50)
        print("  --- 📊 안정성 (Stability & Stats) ---")

        # --- 👇👇👇 2. 청산 여부 출력 라인 추가 👇👇👇 ---
        print(f"  - 청산 발생 여부:      {liquidation_occurred}")
        # --- 👆👆👆 2. 수정 완료 --- 👆👆👆

        print(f"  - 최대 낙폭 (MDD):      {max_drawdown_pct:,.2f} %{mdd_detail_str}")
        print(f"  - 최장기간 보유:         {max_duration_str}")
        print(f"  - 최다보유 유닛:         {max_units:,.2f} units")
        print(f"  - 총 누적 수수료:        {result_df['총 누적 수수료'].iloc[-1]:,.2f} USDT")
        print("=" * 50)
    else:
        logging.warning("⚠️ 백테스트 결과 데이터가 비어있어 요약을 생성할 수 없습니다.")