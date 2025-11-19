# manager/simulator_ft.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from strategy.casino_strategy import generate_buy_orders, generate_sell_orders
import os
import logging
import numpy as np

# --- 기본 설정 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")


# --- 헬퍼 함수 (기존과 동일) ---
def _format_duration(minutes: int) -> str:
    if minutes < 0: return "N/A"
    days, rem = divmod(minutes, 1440)
    hours, mins = divmod(rem, 60)
    if days > 0:
        return f"{days}일 {hours}시간 {mins}분"
    elif hours > 0:
        return f"{hours}시간 {mins}분"
    else:
        return f"{mins}분"


def load_candles_from_db(market: str, start: str, end: str) -> pd.DataFrame:
    logging.info(f"📊 {market} 캔들 데이터 DB 로드 시도 중: {start} ~ {end}")
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {os.path.abspath(DB_PATH)}")
    with sqlite3.connect(DB_PATH) as conn:
        query = "SELECT * FROM minute_candles WHERE market = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
        df = pd.read_sql_query(query, conn, params=[market, start, end])
    if df.empty: return df
    df["시간"] = pd.to_datetime(df["timestamp"])
    df["시가"], df["고가"], df["저가"], df["종가"] = df["open"], df["high"], df["low"], df["close"]
    return df[["시간", "시가", "고가", "저가", "종가", "volume"]]


# --- 👇👇👇 1. 새로운 '요약 리포트 생성기' 헬퍼 함수 👇👇👇 ---
def _generate_segment_summary(
        segment_df: pd.DataFrame,
        segment_start_dt: datetime,
        segment_end_dt: datetime,
        initial_cash_segment: float,
        was_liquidated: bool,
        segment_number: int
):
    """(신규) 각 구간(Segment)의 DataFrame을 받아 요약 리포트를 출력합니다."""

    print("\n" + "=" * 50)
    segment_title = f" 🚨 구간 {segment_number} (청산으로 종료) 🚨 " if was_liquidated else f" ✅ 구간 {segment_number} (테스트 종료) ✅ "
    print(f"{segment_title:^50}")
    print("=" * 50)

    if segment_df.empty:
        print("  - ⚠️ 해당 구간에 데이터가 없습니다.")
        return

    # 1. 기본 정보
    final_equity = segment_df['총 자산(Equity)'].iloc[-1]
    total_roi_pct = ((
                                 final_equity - initial_cash_segment) / initial_cash_segment) * 100 if initial_cash_segment > 0 else 0
    final_realized_pnl = segment_df['실현 손익'].iloc[-1]

    # 총 거래 횟수 (매도 기준)
    total_sell_trades = segment_df['신호'].apply(lambda x: '매도 체결' in x).sum()

    # 2. 최장 보유 시간
    max_duration_minutes = segment_df['연속 보유(분)'].max()
    max_duration_str = _format_duration(int(max_duration_minutes))

    # 3. 최다 보유 유닛
    max_units = segment_df['현재 유닛'].max()

    # 4. 최대 낙폭(MDD) 계산
    peak = segment_df['총 자산(Equity)'].cummax()
    drawdown = (segment_df['총 자산(Equity)'] - peak) / peak
    max_drawdown_pct = drawdown.min() * 100

    try:
        mdd_end_index = drawdown.idxmin()
        mdd_trough_value = segment_df.loc[mdd_end_index, '총 자산(Equity)']
        mdd_peak_value = peak.loc[mdd_end_index]
        mdd_detail_str = f" (Peak {mdd_peak_value:,.2f} USDT -> Trough {mdd_trough_value:,.2f} USDT)"
    except Exception:
        mdd_detail_str = ""

    liquidation_status = "🚨 예 (구간 종료)" if was_liquidated else "✅ 아니오"

    # --- 요약 출력 ---
    print(
        f"  - 구간 기간:         {segment_start_dt.strftime('%Y-%m-%d %H:%M:%S')} ~ {segment_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  - 초기 자본 (Initial): {initial_cash_segment:,.2f} USDT")
    print("." * 50)
    print("  --- 💰 수익성 (Profitability) ---")
    print(f"  - 최종 총 자산 (Equity): {final_equity:,.2f} USDT")
    print(f"  - 총 수익률 (Total ROI): {total_roi_pct:,.2f} %")
    print(f"  - 기간 내 실현 손익:     {final_realized_pnl:,.2f} USDT")
    print(f"  - 총 거래 횟수 (매도):   {total_sell_trades} 회")
    print("." * 50)
    print("  --- 📊 안정성 (Stability & Stats) ---")
    print(f"  - 청산 발생 여부:      {liquidation_status}")
    print(f"  - 최대 낙폭 (MDD):      {max_drawdown_pct:,.2f} %{mdd_detail_str}")
    print(f"  - 최장기간 보유:         {max_duration_str}")
    print(f"  - 최다보유 유닛:         {max_units:,.2f} units")


# --- 🚀 새로운 선물 백테스팅 엔진 🚀 ---
def simulate_futures_with_db(
        market: str, start: str, end: str, unit_size: float,
        small_flow_pct: float, small_flow_units: int,
        large_flow_pct: float, large_flow_units: int,
        take_profit_pct: float,
        leverage: int,
        save_full_log: bool = False,
        initial_cash: float = 10_000.0,
        buy_fee: float = 0.0004,
        sell_fee: float = 0.0004,
        maintenance_margin_rate: float = 0.005
):
    logging.info(f"--- ⏱️ 선물(Futures) 백테스트 시작: {market}, 기간: {start} ~ {end} ---")
    logging.info(f"--- 레버리지: {leverage}x, 초기 자본: {initial_cash:,.2f} USDT ---")

    df_candles = load_candles_from_db(market, start, end)
    if df_candles.empty:
        logging.warning("⚠️ 캔들 데이터가 없습니다. 백테스트를 종료합니다.")
        return

    # 전략 모듈에 전달할 가상 setting_df (레버리지 포함)
    setting_df = pd.DataFrame([{
        "market": market, "unit_size": unit_size, "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units, "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units, "take_profit_pct": take_profit_pct,
        "leverage": leverage
    }])

    # --- 👇👇👇 2. '구간별' 추적 변수로 수정 👇👇👇 ---

    # 마스터 리포트 (모든 구간의 결과 DF를 저장)
    master_report_segments = []
    liquidation_events = []  # 청산 발생 시점 저장

    # --- 1구간 시작 변수 설정 ---
    segment_logs = []  # 1구간의 로그
    segment_start_dt = pd.to_datetime(start)  # 1구간의 시작 시간

    realized_pnl = 0.0  # 1구간의 실현 손익
    unrealized_pnl = 0.0
    used_margin = 0.0
    total_equity = initial_cash
    available_margin = initial_cash

    position = {}
    buy_log_df = pd.DataFrame(
        columns=["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "filled"])

    current_holding_minutes = 0
    current_units_held = 0.0
    # total_sell_trades는 이제 요약 함수에서 계산함
    # --- 👆👆👆 2. 수정 완료 --- 👆👆👆

    progress_interval = len(df_candles) // 10 or 1

    # --- 🔄 메인 시뮬레이션 루프 ---
    for i, row in df_candles.iterrows():
        now, current_price = row["시간"], row["종가"]
        events, last_trade_amount, last_trade_fee = [], 0.0, 0.0

        # --- 👇👇👇 3. 청산 검사 로직 수정 (break -> reset) 👇👇👇 ---
        if market in position:
            pos_data = position[market]
            pos_value = pos_data.get('quantity', 0.0) * current_price
            maintenance_margin_needed = pos_value * maintenance_margin_rate

            if available_margin < maintenance_margin_needed:
                # --- 🚨 청산 발생! 🚨 ---
                logging.error(f"🚨🚨🚨 청산 발생! 🚨🚨🚨 시간: {now}")
                logging.error(f"    사용 가능 증거금: {available_margin:,.2f} < 필요 유지 증거금: {maintenance_margin_needed:,.2f}")
                events.append("!!! 강제 청산 !!!")

                # (1) 현재까지의 로그를 DataFrame으로 만듦
                result_df_segment = pd.DataFrame(segment_logs)
                # (2) 이 구간의 리포트를 마스터 리스트에 추가 (청산 플래그=True)
                master_report_segments.append(
                    (result_df_segment, segment_start_dt, now, initial_cash, True)
                )
                # (3) 청산 이벤트 기록
                liquidation_events.append(now)

                # (4) 모든 계좌/전략 변수 '초기화'
                logging.warning(f"--- 🔄 계좌 초기화. {now + timedelta(minutes=1)} 부터 테스트 재시작 ---")
                realized_pnl = 0.0
                unrealized_pnl = 0.0
                used_margin = 0.0
                total_equity = initial_cash
                available_margin = initial_cash
                position = {}
                buy_log_df = pd.DataFrame(
                    columns=["time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "filled"])
                current_holding_minutes = 0
                current_units_held = 0.0

                # (5) 다음 구간 추적 변수 설정
                segment_logs = []  # 새 로그 리스트
                segment_start_dt = now + timedelta(minutes=1)  # 다음 1분부터 새 구간 시작

                continue  # 이번 1분봉은 여기서 종료 (청산 처리)
        # --- 👆👆👆 3. 청산 로직 수정 완료 --- 👆👆👆

        # --- 2. 계좌 상태 업데이트 (매 분마다) ---
        if market in position:
            current_holding_minutes += 1
            pos_data = position[market]
            unrealized_pnl = (current_price - pos_data.get('avg_price', 0.0)) * pos_data.get('quantity', 0.0)
        else:
            unrealized_pnl = 0.0

        total_equity = initial_cash + realized_pnl + unrealized_pnl
        available_margin = total_equity - used_margin

        # --- 3. 매수 전략 실행 ---
        sim_holdings = {market: {"balance": position.get(market, {}).get('quantity', 0),
                                 "avg_price": position.get(market, {}).get('avg_price',
                                                                           0)}} if market in position else {}
        new_buy_orders_df = generate_buy_orders(setting_df, buy_log_df, {market: current_price}, sim_holdings,
                                                available_margin)
        if not new_buy_orders_df.empty:
            buy_log_df = pd.concat([buy_log_df, new_buy_orders_df], ignore_index=True)

        # --- 4. 매수 주문 체결 로직 (선물용) ---
        for idx, r_buy in buy_log_df.iterrows():
            if r_buy["filled"] == "update":
                # ... (이하 매수 로직은 기존과 동일) ...
                price_to_check, amount_to_buy, buy_type = float(r_buy["target_price"]), float(r_buy["buy_amount"]), \
                    r_buy["buy_type"]
                final_price = current_price if buy_type == "initial" else price_to_check
                fee = amount_to_buy * buy_fee
                volume = (amount_to_buy - fee) / final_price
                realized_pnl -= fee
                old_quantity = position.get(market, {}).get('quantity', 0.0)
                old_avg_price = position.get(market, {}).get('avg_price', 0.0)
                new_quantity = old_quantity + volume
                new_avg_price = ((old_avg_price * old_quantity) + (final_price * volume)) / new_quantity
                cost_basis = new_avg_price * new_quantity
                used_margin = cost_basis / leverage
                position[market] = {'quantity': new_quantity, 'avg_price': new_avg_price, 'cost_basis': cost_basis}
                current_units_held += (amount_to_buy / unit_size) if unit_size > 0 else 0
                buy_log_df.at[idx, "filled"] = "done"
                last_trade_amount, last_trade_fee = amount_to_buy, fee
                events.append(f"{buy_type} 매수")

        # --- 5. 매도 주문 체결 로직 (선물용) ---
        if market in position:
            avg_buy_price = position[market]['avg_price']
            target_sell_price = round(avg_buy_price * (1 + take_profit_pct), 8)
            if current_price >= target_sell_price:
                # ... (이하 매도 로직은 기존과 동일) ...
                volume_to_sell = position[market]['quantity']
                proceeds = volume_to_sell * current_price
                fee = proceeds * sell_fee
                actual_proceeds = proceeds - fee
                cost_basis = position[market]['cost_basis']
                profit = actual_proceeds - cost_basis
                realized_pnl += profit
                position.pop(market, None)
                used_margin = 0.0
                unrealized_pnl = 0.0
                # total_sell_trades += 1 (요약 함수에서 계산하므로 삭제)
                current_holding_minutes = 0
                current_units_held = 0.0
                buy_log_df = buy_log_df[buy_log_df['market'] != market].copy()
                last_trade_amount, last_trade_fee = proceeds, fee
                events.append("매도 체결")
                logging.info(f"🧹 {market} 매도 완료. (실현 손익: {profit:,.2f} USDT)")

        # --- 6. 매 분봉 로그 기록 ---
        total_equity = initial_cash + realized_pnl + unrealized_pnl
        available_margin = total_equity - used_margin

        # 'logs'가 아닌 'segment_logs'에 기록
        segment_logs.append({
            "시간": now, "종가": current_price, "신호": " / ".join(events) if events else "보유 중",
            "총 자산(Equity)": round(total_equity, 2),
            "사용 증거금": round(used_margin, 2),
            "사용 가능 증거금": round(available_margin, 2),
            "미실현 손익": round(unrealized_pnl, 2),
            "실현 손익": round(realized_pnl, 2),
            "현재 유닛": current_units_held,
            "연속 보유(분)": current_holding_minutes
        })
    # --- 🔄 메인 루프 종료 🔄 ---

    # --- 👇👇👇 4. 새로운 최종 리포트 생성 로직 👇👇👇 ---

    # (1) 마지막 구간(청산 없이 종료된)의 리포트를 마스터 리스트에 추가
    if segment_logs:  # 마지막 구간에 로그가 있다면
        result_df_segment = pd.DataFrame(segment_logs)
        segment_end_dt = df_candles.iloc[-1]["시간"]
        master_report_segments.append(
            (result_df_segment, segment_start_dt, segment_end_dt, initial_cash, False)
        )

    # (2) 마스터 요약 출력
    print("\n" + "=" * 50)
    print("     📊 선물(Futures) 백테스트 마스터 요약 📊     ")
    print("=" * 50)
    print(f"  - 마켓 (Market):       {market} (Leverage: {leverage}x)")
    print(f"  - 전체 기간:         {start} ~ {end}")
    print(f"  - 초기 자본 (Initial): {initial_cash:,.2f} USDT")
    print("-" * 50)
    print(f"  - 🚨 총 청산 발생 횟수: {len(liquidation_events)} 회")
    if liquidation_events:
        for i, liq_time in enumerate(liquidation_events):
            print(f"    - {i + 1}차 청산 시점: {liq_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # (3) 각 구간별 상세 리포트 출력
    if not master_report_segments:
        logging.warning("⚠️ 백테스트 결과 데이터가 비어있어 요약을 생성할 수 없습니다.")
        return

    for i, (segment_df, start_dt, end_dt, seg_cash, was_liq) in enumerate(master_report_segments):
        _generate_segment_summary(
            segment_df=segment_df,
            segment_start_dt=start_dt,
            segment_end_dt=end_dt,
            initial_cash_segment=seg_cash,
            was_liquidated=was_liq,
            segment_number=i + 1
        )

    # (4) 파일 저장 로직 (스위치가 켜져 있을 때만)
    if save_full_log:
        logging.info(f"ℹ️ 전체 로그 파일 저장 시도 중...")
        try:
            # (주의: 모든 구간의 로그를 합쳐서 저장하지 않고, 마지막 구간만 저장합니다.)
            # (모든 구간을 합치려면 `pd.concat`이 필요합니다.)
            final_segment_df = master_report_segments[-1][0]  # 마지막 구간 DF
            filename = f"FT_시뮬_{market}_{leverage}x_LAST_SEGMENT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            final_segment_df.to_csv(filename, index=False, encoding='utf-8-sig')
            logging.info(f"✅ 백테스트 마지막 구간의 로그 파일 저장 완료: {filename}")
        except Exception as e:
            logging.error(f"❌ 백테스트 결과 파일 저장 실패: {e}")
    else:
        logging.info("ℹ️ 전체 로그 파일 저장을 건너뛰었습니다 (설정).")
    # --- 👆👆👆 4. 리포트 로직 수정 완료 --- 👆👆👆