# manager/simulator_db.py

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from strategy.casino_strategy import generate_buy_orders, generate_sell_orders
import os
import logging # 로깅 모듈 임포트

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 현재 파일의 위치를 기준으로 프로젝트 루트 디렉토리의 절대 경로를 계산
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "candle_db.sqlite")

# 시뮬레이션 초기 설정 (실거래와 분리하여 관리)
INITIAL_CASH = 60_000 # 초기 현금 (USDT)
BUY_FEE = 0.0005 # 매수 수수료율 (예: 0.05%)
SELL_FEE = 0.0005 # 매도 수수료율 (예: 0.05%)


def load_candles_from_db(market: str, start: str, end: str) -> pd.DataFrame:
    """
    SQLite 데이터베이스에서 지정된 마켓과 기간의 분봉 캔들 데이터를 로드합니다.

    :param market: 마켓 심볼 (예: BTCUSDT)
    :param start: 시작 일시 (YYYY-MM-DD HH:MM:SS)
    :param end: 종료 일시 (YYYY-MM-DD HH:MM:SS)
    :return: 캔들 데이터 DataFrame
    :raises FileNotFoundError: 데이터베이스 파일을 찾을 수 없을 때
    """
    logging.info(f"📊 {market} 캔들 데이터 DB 로드 시도 중: {start} ~ {end}")

    if not os.path.exists(DB_PATH):
        logging.error(f"❌ 데이터베이스 파일을 찾을 수 없습니다. 경로를 확인하세요: {os.path.abspath(DB_PATH)}")
        raise FileNotFoundError(f"데이터베이스 파일을 찾을 수 없습니다. 경로를 확인하세요: {os.path.abspath(DB_PATH)}")

    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT * FROM minute_candles
        WHERE market = ? AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    try:
        df = pd.read_sql_query(query, conn, params=[market, start, end])
        if df.empty:
            logging.warning(f"⚠️ 선택한 기간 ({start} ~ {end})에 대한 {market} 캔들 데이터가 DB에 없습니다.")
        else:
            logging.info(f"✅ {market} 캔들 데이터 {len(df)}개 로드 완료.")
    except Exception as e:
        logging.error(f"❌ DB에서 캔들 데이터 로드 중 오류 발생: {e}", exc_info=True)
        raise e
    finally:
        conn.close()

    # Pandas DataFrame 컬럼명 변경 (기존 Upbit 백테스터와 호환성 유지)
    df["시간"] = pd.to_datetime(df["timestamp"])
    df["시가"] = df["open"]
    df["고가"] = df["high"]
    df["저가"] = df["low"]
    df["종가"] = df["close"]

    # 필요한 컬럼만 반환
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
    take_profit_pct: float,
    leverage: int = 1 # 레버리지 파라미터 추가 (기본값 1)
):
    """
    데이터베이스에 저장된 캔들 데이터를 사용하여 매매 전략을 시뮬레이션합니다.
    API 호출 없이 빠르게 과거 데이터를 기반으로 백테스트를 수행합니다.

    :param market: 시뮬레이션할 마켓 심볼 (예: XRPUSDT)
    :param start: 백테스트 시작 일시 (YYYY-MM-DD HH:MM:SS)
    :param end: 백테스트 종료 일시 (YYYY-MM-DD HH:MM:SS)
    :param unit_size: 단위 투자 금액 (레버리지 적용 전)
    :param small_flow_pct: 소액 분할 매수 하락률 (%)
    :param small_flow_units: 소액 분할 매수 단계 수
    :param large_flow_pct: 대액 분할 매수 하락률 (%)
    :param large_flow_units: 대액 분할 매수 단계 수
    :param take_profit_pct: 익절 목표 수익률 (%)
    :param leverage: 적용할 레버리지 배수 (시뮬레이션 투자금에 영향)
    """
    logging.info(f"--- ⏱️ DB 기반 백테스트 시작: {market}, 기간: {start} ~ {end} ---")
    logging.info(f"🔬 레버리지 적용: {leverage}x (기본 투자금: {unit_size}USDT -> 실제 투자금: {unit_size * leverage}USDT)")

    df_candles = load_candles_from_db(market, start, end)
    if df_candles.empty:
        logging.error("❌ 선택한 기간에 대한 캔들 데이터가 DB에 없어 백테스트를 진행할 수 없습니다.")
        return

    # 전략 설정 DataFrame 생성 (레버리지가 unit_size에 곱해져 실제 투자금으로 반영)
    setting_df = pd.DataFrame([{
        "market": market,
        "unit_size": unit_size * leverage, # 레버리지를 곱한 값을 실제 투자금으로 사용
        "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units,
        "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units,
        "take_profit_pct": take_profit_pct
    }])

    # 시뮬레이션 변수 초기화 (API 시뮬레이터와 동일)
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

    progress_interval = len(df_candles) / 10 # 10% 진행마다 로그 출력
    next_progress_log_step = progress_interval

    # 캔들 데이터를 순회하며 매매 시뮬레이션
    for i, row in df_candles.iterrows():
        if i >= next_progress_log_step:
            logging.info(f"⏳ 시뮬레이션 진행 중: {row['시간'].strftime('%Y-%m-%d %H:%M:%S')} ({((i+1)/len(df_candles)*100):.1f}%)")
            next_progress_log_step += progress_interval

        now = row["시간"]
        current_price = row["종가"]
        events = []

        current_prices = {market: current_price}
        # 매수 주문 생성 및 처리 로직 (API 시뮬레이터와 동일)
        buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)

        for idx_buy, r_buy in buy_log_df.iterrows():
            if r_buy["filled"] in ["update", "wait"] and r_buy["market"] == market:
                price_to_check = float(r_buy["target_price"])
                amount_to_buy = float(r_buy["buy_amount"])
                buy_type = r_buy["buy_type"]

                if buy_type == "initial" or current_price <= price_to_check:
                    if cash >= amount_to_buy:
                        fee = amount_to_buy * BUY_FEE
                        volume = (amount_to_buy - fee) / price_to_check
                        cash -= amount_to_buy
                        cumulative_fee += fee
                        total_buy_amount += amount_to_buy
                        total_buy_volume += volume
                        holdings[market] = holdings.get(market, 0) + volume
                        buy_log_df.at[idx_buy, "filled"] = "done"
                        last_trade_amount = amount_to_buy
                        last_trade_fee = fee
                        events.append(f"{buy_type} 매수 체결 ({amount_to_buy:.2f}USDT)")
                        logging.debug(f"📈 {now.strftime('%H:%M')} | {market} {buy_type} 매수 체결: 가격={price_to_check:.8f}, 수량={volume:.4f}, 현금잔고={cash:.2f}")
                    else:
                        buy_log_df.at[idx_buy, "filled"] = "wait"
                        logging.debug(f"현금 부족으로 {market} {buy_type} 매수 대기: 필요={amount_to_buy:.2f}, 보유={cash:.2f}")
                else:
                    buy_log_df.at[idx_buy, "filled"] = "wait"
                    logging.debug(f"조건 미달로 {market} {buy_type} 매수 대기: 현재가={current_price:.8f}, 목표가={price_to_check:.8f}")

        # 보유 코인이 있을 경우 매도 주문 생성 및 처리
        if market in holdings and holdings[market] > 0:
            balance = holdings[market]
            avg_buy_price = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
            holdings_info = {
                market: {
                    "balance": balance, "locked": 0, "avg_price": avg_buy_price, "current_price": current_price
                }
            }
            sell_log_df = generate_sell_orders(setting_df, holdings_info, sell_log_df)

            for idx_sell, r_sell in sell_log_df.iterrows():
                if r_sell["filled"] == "update" and r_sell["market"] == market:
                    target_sell_price = float(r_sell["target_sell_price"])
                    if current_price >= target_sell_price:
                        volume_to_sell = float(r_sell["quantity"])
                        if holdings[market] >= volume_to_sell:
                            fee = volume_to_sell * current_price * SELL_FEE
                            proceeds = volume_to_sell * current_price - fee
                            pnl = (current_price - avg_buy_price) * volume_to_sell

                            cash += proceeds
                            cumulative_fee += fee
                            realized_pnl += pnl - fee
                            holdings[market] = 0
                            sell_log_df.at[idx_sell, "filled"] = "done"
                            buy_log_df = buy_log_df[buy_log_df["market"] != market]
                            total_buy_amount = 0.0
                            total_buy_volume = 0.0
                            last_trade_amount = proceeds
                            last_trade_fee = fee
                            events.append(f"매도 체결 ({volume_to_sell:.4f}개)")
                            logging.debug(f"📉 {now.strftime('%H:%M')} | {market} 매도 체결: 가격={current_price:.8f}, 수량={volume_to_sell:.4f}, 현금잔고={cash:.2f}")
                        else:
                            logging.warning(f"⚠️ {market} 매도 시도 수량({volume_to_sell:.4f})이 보유 수량({holdings[market]:.4f})보다 많습니다. 매도 불가.")
                    else:
                        logging.debug(f"조건 미달로 {market} 매도 대기: 현재가={current_price:.8f}, 목표가={target_sell_price:.8f}")

        # 시뮬레이션 로그 기록
        quantity = holdings.get(market, 0)
        avg_price_for_display = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
        gap_pct = round(
            (current_price - avg_price_for_display) / avg_price_for_display * 100, 2
        ) if avg_price_for_display > 0 else 0

        portfolio_value = cash + quantity * current_price
        signal_str = " / ".join(events) if events else "보유 중"

        logs.append({
            "시간": now,
            "마켓": market,
            "시가": row["시가"],
            "고가": row["고가"],
            "저가": row["저가"],
            "종가": current_price,
            "신호": signal_str,
            "매매금액": round(last_trade_amount, 2),
            "현재 평단가": round(avg_price_for_display, 5),
            "현재 종가와 평단가의 gap(%)": gap_pct,
            "누적 매수금": round(total_buy_amount, 2),
            "실현 손익": round(realized_pnl, 2),
            "보유 현금": round(cash, 2),
            "거래시 수수료": round(last_trade_fee, 2),
            "총 누적 수수료": round(cumulative_fee, 2),
            "총 포트폴리오 값": round(portfolio_value, 2)
        })

    result_df = pd.DataFrame(logs)
    # 결과 엑셀 파일 저장
    filename = f"DB_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    try:
        result_df.to_excel(filename, index=False)
        logging.info(f"✅ 백테스트 결과 파일 저장 완료: {filename}")
    except Exception as e:
        logging.error(f"❌ 백테스트 결과 파일 저장 실패: {e}", exc_info=True)

    # 최종 통계 요약 출력 (API 시뮬레이터와 동일)
    if not result_df.empty:
        first_row = result_df.iloc[0]
        last_row = result_df.iloc[-1]
        logging.info("\n--- 📈 백테스트 통계 요약 ---")
        logging.info(f"▶ 시작: {first_row['시간']} | 마켓: {first_row['마켓']}")
        logging.info(f"  - 시작 현금: {INITIAL_CASH:,}USDT")
        logging.info(f"  - 시작 포트폴리오 가치: {first_row['총 포트폴리오 값']:,}USDT")

        logging.info(f"\n▶ 종료: {last_row['시간']} | 마켓: {last_row['마켓']}")
        logging.info(f"  - 최종 보유 현금: {last_row['보유 현금']:,}USDT")
        logging.info(f"  - 최종 실현 손익: {last_row['실현 손익']:,}USDT")
        logging.info(f"  - 총 누적 수수료: {last_row['총 누적 수수료']:,}USDT")
        logging.info(f"  - 최종 포트폴리오 가치: {last_row['총 포트폴리오 값']:,}USDT")

        # 최종 수익률 계산
        final_pnl = last_row['총 포트폴리오 값'] - INITIAL_CASH
        pnl_rate = (final_pnl / INITIAL_CASH) * 100 if INITIAL_CASH > 0 else 0
        logging.info(f"  - 최종 총 손익: {final_pnl:,}USDT ({pnl_rate:.2f}%)")
    else:
        logging.warning("⚠️ 백테스트 결과 데이터가 비어있습니다. 거래가 발생하지 않았을 수 있습니다.")

    logging.info("--- ⏱️ DB 기반 백테스트 완료 ---")