# manager/simulator_ft_dynamic.py

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


def _generate_segment_summary(
        segment_df: pd.DataFrame,
        segment_start_dt: datetime,
        segment_end_dt: datetime,
        initial_cash_segment: float,
        was_liquidated: bool,
        segment_number: int
):
    print("\n" + "=" * 50)
    segment_title = f" 🚨 구간 {segment_number} (청산으로 종료) 🚨 " if was_liquidated else f" ✅ 구간 {segment_number} (테스트 종료) ✅ "
    print(f"{segment_title:^50}")
    print("=" * 50)
    if segment_df.empty:
        print("  - ⚠️ 해당 구간에 데이터가 없습니다.")
        return
    final_equity = segment_df['총 자산(Equity)'].iloc[-1]
    total_roi_pct = ((
                                 final_equity - initial_cash_segment) / initial_cash_segment) * 100 if initial_cash_segment > 0 else 0
    final_realized_pnl = segment_df['실현 손익'].iloc[-1]
    total_sell_trades = segment_df['신호'].apply(lambda x: '매도 체결' in x).sum()
    max_duration_minutes = segment_df['연속 보유(분)'].max()
    max_duration_str = _format_duration(int(max_duration_minutes))
    max_units = segment_df['현재 유닛'].max()
    peak = segment_df['총 자산(Equity)'].cummax()
    drawdown = (segment_df['총 자산(Equity)'] - peak) / peak
    max_drawdown_pct = drawdown.min() * 100
    try:
        mdd_end_index = drawdown.idxmin()
        mdd_start_df = segment_df.loc[:mdd_end_index]
        mdd_start_index = mdd_start_df['총 자산(Equity)'].idxmax()

        peak_value = segment_df.loc[mdd_start_index, '총 자산(Equity)']
        trough_value = segment_df.loc[mdd_end_index, '총 자산(Equity)']
        peak_time = segment_df.loc[mdd_start_index, '시간']
        trough_time = segment_df.loc[mdd_end_index, '시간']

        mdd_detail_str = f" (Peak: {peak_value:,.2f} USDT at {peak_time.strftime('%m-%d %H:%M')} -> Trough: {trough_value:,.2f} USDT at {trough_time.strftime('%m-%d %H:%M')})"
    except Exception:
        mdd_detail_str = ""
    liquidation_status = "🚨 예 (구간 종료)" if was_liquidated else "✅ 아니오"
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


# --- 🚀 V2 선물 백테스팅 엔진 (OHLC + Slippage) 🚀 ---
def simulate_futures_dynamic( # 함수 이름 변경
        market: str, start: str, end: str, unit_size: float,
        small_flow_pct: float, small_flow_units: int,
        large_flow_pct: float, large_flow_units: int,
        take_profit_pct: float,
        leverage: int,
        save_full_log: bool = False,
        initial_cash: float = 10_000.0,
        buy_fee: float = 0.0004,
        sell_fee: float = 0.0004,
        maintenance_margin_rate: float = 0.005,
        slippage_pct: float = 0.0005,
        liquidation_safety_factor: float = 1.0,
        # --- 👇👇👇 동적 유닛 관련 파라미터 추가 👇👇👇 ---
        enable_dynamic_unit: bool = False,
        profit_reset_pct: float = 0.0 # 0% 수익 시 리셋 (기본값: 비활성화)
        # --- 👆👆👆 추가 완료 --- 👆👆👆
):
    logging.info(f"--- ⏱️ V2 선물(OHLC) 백테스트 시작: {market}, 기간: {start} ~ {end} ---")
    logging.info(f"--- 레버리지: {leverage}x, 초기 자본: {initial_cash:,.2f} USDT, 슬리피지: {slippage_pct * 100:.3f}%, 청산 안전 계수: {liquidation_safety_factor} ---")
    # --- 👇👇👇 동적 유닛/리셋 설정 로그 추가 👇👇👇 ---
    if enable_dynamic_unit:
        logging.info(f"--- 📈 동적 유닛 활성화. 수익 {profit_reset_pct * 100:.0f}% 달성 시 자본 리셋 ---")
    else:
        logging.info("--- 📉 동적 유닛 비활성화. 고정 유닛 사용 ---")
    # --- 👆👆👆 추가 완료 --- 👆👆👆

    df_candles = load_candles_from_db(market, start, end)
    if df_candles.empty:
        logging.warning("⚠️ 캔들 데이터가 없습니다. 백테스트를 종료합니다.")
        return {
            'Final Balance': initial_cash, 'Total PNL %': 0, 'Win Rate': 0,
            'MDD %': 0, 'Total Trades': 0, 'Liquidations': 0,
            'Profit Factor': 0, 'Return/MDD': 0, 'Reset Count': 0
        }

    # setting_df는 이제 동적 유닛 계산을 위해 매번 업데이트될 수 있으므로, 초기값만 설정
    initial_setting_df_values = {
        "market": market, "unit_size": unit_size, "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units, "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units, "take_profit_pct": take_profit_pct,
        "leverage": leverage
    }

    master_report_segments = []
    liquidation_events = []
    segment_logs = []
    segment_start_dt = pd.to_datetime(start)

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
    
    # --- 👇👇👇 동적 유닛/리셋 관련 변수 추가 👇👇👇 ---
    accumulated_profit = 0.0 # 리셋된 수익을 누적할 변수
    original_initial_cash = initial_cash # 초기 자본을 기억 (리셋 시 사용)
    reset_count = 0 # 리셋 횟수 변수 추가
    # --- 👆👆👆 추가 완료 --- 👆👆👆

    for i, row in df_candles.iterrows():
        price_open, price_high, price_low, price_close, now = row["시가"], row["고가"], row["저가"], row["종가"], row["시간"]
        events, last_trade_amount, last_trade_fee = [], 0.0, 0.0

        # --- 👇👇👇 동적 유닛 계산 👇👇👇 ---
        current_unit_size = unit_size
        if enable_dynamic_unit and total_equity > original_initial_cash:
            current_unit_size = unit_size * (total_equity / original_initial_cash)
        
        # setting_df 업데이트 (generate_buy_orders에 전달)
        current_setting_df_values = initial_setting_df_values.copy()
        current_setting_df_values["unit_size"] = current_unit_size
        setting_df = pd.DataFrame([current_setting_df_values])
        # --- 👆👆👆 동적 유닛 계산 완료 👆👆👆

        if market in position:
            pos_data = position[market]
            pos_value = pos_data.get('quantity', 0.0) * price_low
            maintenance_margin_needed = pos_value * maintenance_margin_rate

            if available_margin < (maintenance_margin_needed * liquidation_safety_factor):
                events.append("!!! 강제 청산 !!!")
                if segment_logs:
                    result_df_segment = pd.DataFrame(segment_logs)
                    master_report_segments.append(
                        (result_df_segment, segment_start_dt, now, initial_cash, True)
                    )
                liquidation_events.append(now)
                realized_pnl, unrealized_pnl, used_margin = 0.0, 0.0, 0.0
                total_equity, available_margin = initial_cash, initial_cash
                position = {}
                buy_log_df = pd.DataFrame(columns=buy_log_df.columns)
                current_holding_minutes, current_units_held = 0, 0.0
                segment_logs = []
                segment_start_dt = now + timedelta(minutes=1)
                continue

        if market in position:
            avg_buy_price = position[market]['avg_price']
            target_sell_price = round(avg_buy_price * (1 + take_profit_pct), 8)
            if price_high >= target_sell_price:
                final_sell_price = target_sell_price * (1 - slippage_pct)
                volume_to_sell = position[market]['quantity']
                proceeds = volume_to_sell * final_sell_price
                fee = proceeds * sell_fee
                actual_proceeds = proceeds - fee
                cost_basis = position[market]['cost_basis']
                profit = actual_proceeds - cost_basis
                realized_pnl += profit
                position.pop(market, None)
                used_margin, unrealized_pnl = 0.0, 0.0
                current_holding_minutes, current_units_held = 0, 0.0
                buy_log_df = buy_log_df[buy_log_df['market'] != market].copy()
                last_trade_amount, last_trade_fee = proceeds, fee
                events.append("매도 체결")
                logging.info(f"🧹 {market} 매도 완료. (실현 손익: {profit:,.2f} USDT)")
                # continue # 매도 후 즉시 continue 제거 -> 같은 캔들에서 매수도 가능하도록

        sim_holdings = {market: {"balance": position.get(market, {}).get('quantity', 0),
                                 "avg_price": position.get(market, {}).get('avg_price', 0)}} if market in position else {}
        new_buy_orders_df = generate_buy_orders(setting_df, buy_log_df, {market: price_low}, sim_holdings, available_margin)
        if not new_buy_orders_df.empty:
            buy_log_df = pd.concat([buy_log_df, new_buy_orders_df], ignore_index=True)

        for idx, r_buy in buy_log_df.iterrows():
            if r_buy["filled"] == "update":
                price_to_check, amount_to_buy, buy_type = float(r_buy["target_price"]), float(r_buy["buy_amount"]), r_buy["buy_type"]
                final_price = 0.0
                if buy_type == "initial":
                    final_price = price_close * (1 + slippage_pct)
                elif price_low <= price_to_check:
                    final_price = price_to_check * (1 + slippage_pct)
                else:
                    continue
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

        if market in position:
            current_holding_minutes += 1
            pos_data = position[market]
            unrealized_pnl = (price_close - pos_data.get('avg_price', 0.0)) * pos_data.get('quantity', 0.0)
        else:
            unrealized_pnl = 0.0

        total_equity = initial_cash + realized_pnl + unrealized_pnl
        available_margin = total_equity - used_margin

        segment_logs.append({
            "시간": now, "종가": price_close, "신호": " / ".join(events) if events else "보유 중",
            "총 자산(Equity)": round(total_equity, 2), "사용 증거금": round(used_margin, 2),
            "사용 가능 증거금": round(available_margin, 2), "미실현 손익": round(unrealized_pnl, 2),
            "실현 손익": round(realized_pnl, 2), "현재 유닛": current_units_held,
            "연속 보유(분)": current_holding_minutes
        })

        # --- 👇👇👇 수익 실현 및 리셋 로직 수정 👇👇👇 ---
        if enable_dynamic_unit and profit_reset_pct > 0 and total_equity >= original_initial_cash * (1 + profit_reset_pct):
            events.append("💰 수익 실현 및 계좌 리셋!")
            
            if market in position:
                pos_data = position[market]
                realized_pnl += (price_close - pos_data.get('avg_price', 0.0)) * pos_data.get('quantity', 0.0)
                position.pop(market, None)
                used_margin = 0.0
                unrealized_pnl = 0.0
                buy_log_df = pd.DataFrame(columns=buy_log_df.columns)
            
            accumulated_profit += (total_equity - original_initial_cash)
            
            # 계좌 초기화 (realized_pnl 초기화 추가)
            total_equity = original_initial_cash
            available_margin = original_initial_cash
            initial_cash = original_initial_cash
            realized_pnl = 0.0 # "유령 이익" 버그 수정
            reset_count += 1
            
            logging.info(f"💰 수익 실현 및 계좌 리셋! (누적 수익: {accumulated_profit:,.2f} USDT, 현재 자본: {total_equity:,.2f} USDT, 리셋 횟수: {reset_count})")
            
            if segment_logs:
                result_df_segment = pd.DataFrame(segment_logs)
                master_report_segments.append(
                    (result_df_segment, segment_start_dt, now, initial_cash, False)
                )
            segment_logs = []
            segment_start_dt = now + timedelta(minutes=1)
            continue # 현실성을 위해 리셋 후 즉시 다음 캔들로 이동
        # --- 👆👆👆 수정 완료 --- 👆👆👆

    if segment_logs:
        result_df_segment = pd.DataFrame(segment_logs)
        segment_end_dt = df_candles.iloc[-1]["시간"]
        master_report_segments.append(
            (result_df_segment, segment_start_dt, segment_end_dt, initial_cash, False)
        )

    # --- 최종 리포트 ---
    print("\n" + "=" * 50)
    print("     📊 선물(Futures) 백테스트 마스터 요약 📊     ")
    print("=" * 50)
    print(f"  - 마켓 (Market):       {market} (Leverage: {leverage}x)")
    print(f"  - 전체 기간:         {start} ~ {end}")
    print(f"  - 초기 자본 (Initial): {original_initial_cash:,.2f} USDT") # 원본 초기 자본 출력
    print("-" * 50)
    # --- 👇👇👇 설정 정보 출력 추가 👇👇👇 ---
    print("  --- ⚙️ 백테스트 설정 (Settings) ⚙️ ---")
    print(f"  - 전략 (Strategy):")
    print(f"    - unit_size: {unit_size}, take_profit_pct: {take_profit_pct}")
    print(f"    - small_flow: {small_flow_pct * 100:.2f}% (x{small_flow_units})")
    print(f"    - large_flow: {large_flow_pct * 100:.2f}% (x{large_flow_units})")
    print(f"  - 수수료 및 슬리피지 (Fees & Slippage):")
    print(f"    - buy_fee: {buy_fee}, sell_fee: {sell_fee}, slippage_pct: {slippage_pct}")
    print(f"  - 리스크 관리 (Risk Management):")
    print(f"    - maintenance_margin_rate: {maintenance_margin_rate}")
    print(f"    - liquidation_safety_factor: {liquidation_safety_factor}")
    print(f"  - 동적 유닛 (Dynamic Unit):")
    print(f"    - enable_dynamic_unit: {enable_dynamic_unit}")
    print(f"    - profit_reset_pct: {profit_reset_pct * 100:.0f}%")
    print("-" * 50)
    # --- 👆👆👆 추가 완료 --- 👆👆👆
    print(f"  - 🚨 총 청산 발생 횟수: {len(liquidation_events)} 회")
    if liquidation_events:
        for i, liq_time in enumerate(liquidation_events):
            print(f"    - {i + 1}차 청산 시점: {liq_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    if not master_report_segments:
        logging.warning("⚠️ 백테스트 결과 데이터가 비어있어 요약을 생성할 수 없습니다.")
    else:
        for i, (segment_df, start_dt, end_dt, seg_cash, was_liq) in enumerate(master_report_segments):
            _generate_segment_summary(
                segment_df=segment_df,
                segment_start_dt=start_dt,
                segment_end_dt=end_dt,
                initial_cash_segment=seg_cash,
                was_liquidated=was_liq,
                segment_number=i + 1
            )

    # --- 👇👇👇 파일 저장 로직 복원 👇👇👇 ---
    if save_full_log:
        logging.info(f"ℹ️ 전체 로그 파일 저장 시도 중...")
        try:
            if master_report_segments:
                final_segment_df = master_report_segments[-1][0]
                if not final_segment_df.empty:
                    filename = f"FT_V2_시뮬_{market}_{leverage}x_LAST_SEGMENT_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    final_segment_df.to_excel(filename, index=False)
                    logging.info(f"✅ 백테스트 마지막 구간의 로그 파일 저장 완료: {filename}")
                else:
                    logging.warning("⚠️ 마지막 세그먼트가 비어 있어 로그 파일을 저장하지 않습니다.")
            else:
                logging.warning("⚠️ 결과 세그먼트가 없어 로그 파일을 저장하지 않습니다.")
        except Exception as e:
            logging.error(f"❌ 백테스트 결과 파일 저장 실패: {e}")
    else:
        logging.info("ℹ️ 전체 로그 파일 저장을 건너뛰었습니다 (설정).")
    # --- 👆👆👆 복원 완료 --- 👆👆👆

    # --- 최종 결과 딕셔너리 반환 ---
    final_stats = {}
    if master_report_segments:
        last_segment_df, _, _, seg_cash, was_liquidated_in_last_segment = master_report_segments[-1]
        if not last_segment_df.empty:
            final_balance = last_segment_df['총 자산(Equity)'].iloc[-1]
            total_pnl_pct = ((final_balance - original_initial_cash) / original_initial_cash) * 100 # 원본 초기 자본 기준
            peak = last_segment_df['총 자산(Equity)'].cummax()
            drawdown = (last_segment_df['총 자산(Equity)'] - peak) / peak
            mdd_pct = drawdown.min() * 100
            
            sell_signals = last_segment_df[last_segment_df['신호'].str.contains('매도 체결')]
            total_trades = len(sell_signals)
            
            win_rate, profit_factor, return_mdd_ratio = 0, 0, 0
            
            if total_trades > 0:
                trade_pnls = []
                previous_realized_pnl = 0
                if len(master_report_segments) > 1:
                    previous_segment_df, _, _, _, was_liquidated = master_report_segments[-2]
                    if was_liquidated and not previous_segment_df.empty:
                         previous_realized_pnl = previous_segment_df['실현 손익'].iloc[-1]

                for index, trade in sell_signals.iterrows():
                    current_realized_pnl = trade['실현 손익']
                    pnl = current_realized_pnl - previous_realized_pnl
                    trade_pnls.append(pnl)
                    previous_realized_pnl = current_realized_pnl
                
                pnl_per_trade = pd.Series(trade_pnls)

                winning_trades = (pnl_per_trade > 0).sum()
                win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
                
                total_profit = pnl_per_trade[pnl_per_trade > 0].sum()
                total_loss = abs(pnl_per_trade[pnl_per_trade < 0].sum())
                
                profit_factor = total_profit / total_loss if total_loss > 0 else np.inf
            
            return_mdd_ratio = total_pnl_pct / abs(mdd_pct) if mdd_pct < 0 else np.inf

            final_stats = {
                'Final Balance': final_balance + accumulated_profit, # 누적 수익 합산
                'Total PNL %': ((final_balance + accumulated_profit - original_initial_cash) / original_initial_cash) * 100, # 누적 수익 합산
                'Win Rate': win_rate,
                'MDD %': mdd_pct,
                'Total Trades': total_trades,
                'Liquidations': len(liquidation_events),
                'Profit Factor': profit_factor,
                'Return/MDD': return_mdd_ratio,
                'Accumulated Profit': accumulated_profit, # 누적 수익 별도 추가
                'Reset Count': reset_count # 리셋 횟수 추가
            }
        else:
             final_stats = {
                'Final Balance': seg_cash + accumulated_profit, # 누적 수익 합산
                'Total PNL %': ((seg_cash + accumulated_profit - original_initial_cash) / original_initial_cash) * 100, # 누적 수익 합산
                'Win Rate': 0, 'MDD %': 0,
                'Total Trades': 0, 'Liquidations': len(liquidation_events),
                'Profit Factor': 0, 'Return/MDD': 0,
                'Accumulated Profit': accumulated_profit,
                'Reset Count': reset_count
            }
    else:
        final_stats = {
            'Final Balance': initial_cash + accumulated_profit, # 누적 수익 합산
            'Total PNL %': ((initial_cash + accumulated_profit - original_initial_cash) / original_initial_cash) * 100, # 누적 수익 합산
            'Win Rate': 0, 'MDD %': 0,
            'Total Trades': 0, 'Liquidations': len(liquidation_events),
            'Profit Factor': 0, 'Return/MDD': 0,
            'Accumulated Profit': accumulated_profit,
            'Reset Count': reset_count
        }
        
    return final_stats