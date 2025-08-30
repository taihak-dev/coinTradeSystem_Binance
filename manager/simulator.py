# manager/simulator.py

import pandas as pd
from datetime import datetime, timedelta
import time
import config
import logging # 로깅 모듈 임포트

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from strategy.casino_strategy import generate_buy_orders, generate_sell_orders

# config 설정에 따라 다른 API 모듈을 가져옴
if config.EXCHANGE == 'binance':
    from services.exchange_service import get_minute_candles
    logging.info("[SYSTEM] API 시뮬레이터: 바이낸스 모드로 실행합니다.")
else:
    from api.upbit.price import get_minute_candles
    logging.info("[SYSTEM] API 시뮬레이터: 업비트 모드로 실행합니다.")

# 시뮬레이션 초기 설정 (실거래와 분리하여 관리)
INITIAL_CASH = 60_000 # 초기 현금 (USDT)
BUY_FEE = 0.0005 # 매수 수수료율 (예: 0.05%)
SELL_FEE = 0.0005 # 매도 수수료율 (예: 0.05%)


def simulate_with_api(
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
    API를 통해 실시간으로 캔들 데이터를 가져오면서 매매 전략을 시뮬레이션합니다.
    이는 실제 자동 매매와 유사한 방식으로 백테스트를 수행합니다.

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
    logging.info(f"--- ⏱️ API 기반 백테스트 시작: {market}, 기간: {start} ~ {end} ---")
    logging.info(f"🔬 레버리지 적용: {leverage}x (기본 투자금: {unit_size}USDT -> 실제 투자금: {unit_size * leverage}USDT)")

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

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

    # 시뮬레이션 변수 초기화
    cash = INITIAL_CASH # 초기 현금 보유액
    holdings = {} # {market: 수량} 형태의 보유 코인 정보
    # 매수/매도 로그 DataFrame 초기화 (실제 주문 기록과 유사)
    buy_log_df = pd.DataFrame(columns=[
        "time", "market", "target_price", "buy_amount", "buy_units", "buy_type", "buy_uuid", "filled"
    ])
    sell_log_df = pd.DataFrame(columns=[
        "market", "avg_buy_price", "quantity", "target_sell_price", "sell_uuid", "filled"
    ])

    realized_pnl = 0.0 # 실현 손익
    total_buy_amount = 0.0 # 누적 매수 금액 (레버리지 적용 후)
    total_buy_volume = 0.0 # 누적 매수 수량
    cumulative_fee = 0.0 # 누적 거래 수수료
    last_trade_fee = 0.0 # 직전 거래 수수료
    last_trade_amount = 0.0 # 직전 거래 금액 (매수/매도)
    logs = [] # 매 시각별 시뮬레이션 결과 기록

    current_time = start_dt # 시뮬레이션 시작 시간
    progress_interval = (end_dt - start_dt).total_seconds() / 10 # 10% 진행마다 로그 출력
    next_progress_log_time = start_dt + timedelta(seconds=progress_interval)

    while current_time <= end_dt:
        # 진행 상황 로그 출력
        if current_time >= next_progress_log_time:
            logging.info(f"⏳ 시뮬레이션 진행 중: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            next_progress_log_time += timedelta(seconds=progress_interval)

        try:
            # API를 통해 현재 시점의 캔들 1개를 가져옴
            # Upbit의 경우 to 파라미터가 KST 기준이므로 KST로 변환
            # Binance의 get_minute_candles는 내부적으로 처리
            candle = get_minute_candles(market, to=current_time.strftime("%Y-%m-%d %H:%M:%S"), count=1)
            if not candle:
                logging.warning(f"⚠️ {current_time}에 대한 캔들 데이터를 찾을 수 없습니다. 다음 시간으로 건너뜁니다.")
                current_time += timedelta(minutes=1)
                time.sleep(0.1) # API 요청 간 최소 딜레이
                continue

            candle = candle[0] # 첫 번째 (가장 최신) 캔들 사용
            now = pd.to_datetime(candle["candle_date_time_kst"]) # 캔들 시작 시간 (KST)
            current_price = candle["trade_price"] # 캔들 종가
            events = [] # 해당 분에 발생한 이벤트 목록

            current_prices = {market: current_price}
            # 1. 매수 주문 생성 (전략에 따라 buy_log_df 업데이트)
            buy_log_df = generate_buy_orders(setting_df, buy_log_df, current_prices)
            # logging.debug(f"매수 주문 생성 후 buy_log_df:\n{buy_log_df}")

            # 2. 생성된 매수 주문 처리 (실제 매매 시뮬레이션)
            for idx, r in buy_log_df.iterrows():
                # 'update' 또는 'wait' 상태의 주문만 고려 (이미 'done'은 스킵)
                if r["filled"] in ["update", "wait"] and r["market"] == market:
                    price_to_check = float(r["target_price"])
                    amount_to_buy = float(r["buy_amount"]) # 레버리지 적용된 실제 투자금
                    buy_type = r["buy_type"]

                    # 매수 조건 확인: 현재 가격이 목표 가격 이하이거나, 최초 주문인 경우
                    if buy_type == "initial" or current_price <= price_to_check:
                        if cash >= amount_to_buy:
                            # 매수 체결 시뮬레이션
                            fee = amount_to_buy * BUY_FEE # 매수 수수료
                            volume = (amount_to_buy - fee) / price_to_check # 수수료 제외한 실제 매수 수량
                            cash -= amount_to_buy # 현금 감소
                            cumulative_fee += fee # 누적 수수료
                            total_buy_amount += amount_to_buy # 누적 매수 금액
                            total_buy_volume += volume # 누적 매수 수량
                            holdings[market] = holdings.get(market, 0) + volume # 보유 수량 증가
                            buy_log_df.at[idx, "filled"] = "done" # 주문 상태 'done'으로 변경
                            last_trade_amount = amount_to_buy # 마지막 거래 금액
                            last_trade_fee = fee # 마지막 거래 수수료
                            events.append(f"{buy_type} 매수 체결 ({amount_to_buy:.2f}USDT)") # 이벤트 기록
                            logging.info(f"📈 {now.strftime('%H:%M')} | {market} {buy_type} 매수 체결: 가격={price_to_check:.8f}, 수량={volume:.4f}, 현금잔고={cash:.2f}")
                        else:
                            buy_log_df.at[idx, "filled"] = "wait" # 현금 부족으로 대기
                            logging.debug(f"현금 부족으로 {market} {buy_type} 매수 대기: 필요={amount_to_buy:.2f}, 보유={cash:.2f}")
                    else:
                        buy_log_df.at[idx, "filled"] = "wait" # 조건 미달로 대기
                        logging.debug(f"조건 미달로 {market} {buy_type} 매수 대기: 현재가={current_price:.8f}, 목표가={price_to_check:.8f}")

            # 보유 코인이 있을 경우 매도 주문 생성 및 처리
            if market in holdings and holdings[market] > 0:
                balance = holdings[market] # 현재 보유 수량
                # 평균 매수 가격 계산 (0으로 나누는 것 방지)
                avg_buy_price = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
                holdings_info = {
                    market: {
                        "balance": balance,
                        "locked": 0, # 시뮬레이션에서는 locked 개념은 0으로 처리
                        "avg_price": avg_buy_price,
                        "current_price": current_price # 현재가를 매도 전략에 전달 (선택 사항)
                    }
                }
                # 매도 주문 생성 (전략에 따라 sell_log_df 업데이트)
                sell_log_df = generate_sell_orders(setting_df, holdings_info, sell_log_df)
                # logging.debug(f"매도 주문 생성 후 sell_log_df:\n{sell_log_df}")

                # 3. 생성된 매도 주문 처리 (실제 매매 시뮬레이션)
                for idx, r in sell_log_df.iterrows():
                    # 'update' 상태의 주문만 고려
                    if r["filled"] == "update" and r["market"] == market:
                        target_sell_price = float(r["target_sell_price"])
                        if current_price >= target_sell_price:
                            # 매도 체결 시뮬레이션
                            volume_to_sell = float(r["quantity"])
                            if holdings[market] >= volume_to_sell: # 보유 수량 확인
                                fee = volume_to_sell * current_price * SELL_FEE # 매도 수수료
                                proceeds = volume_to_sell * current_price - fee # 매도 수익
                                pnl = (current_price - avg_buy_price) * volume_to_sell # 순수 가격 차이로 인한 손익

                                cash += proceeds # 현금 증가
                                cumulative_fee += fee # 누적 수수료
                                realized_pnl += pnl - fee # 실현 손익 (수수료 제외)
                                holdings[market] = 0 # 보유 수량 0으로 초기화
                                sell_log_df.at[idx, "filled"] = "done" # 매도 주문 상태 'done'으로 변경
                                # 매도 완료 시 해당 마켓의 모든 미체결 매수 주문 제거
                                buy_log_df = buy_log_df[buy_log_df["market"] != market]
                                total_buy_amount = 0.0 # 매도 완료시 누적 매수금, 수량 초기화
                                total_buy_volume = 0.0
                                last_trade_amount = proceeds # 마지막 거래 금액
                                last_trade_fee = fee # 마지막 거래 수수료
                                events.append(f"매도 체결 ({volume_to_sell:.4f}개)") # 이벤트 기록
                                logging.info(f"📉 {now.strftime('%H:%M')} | {market} 매도 체결: 가격={current_price:.8f}, 수량={volume_to_sell:.4f}, 현금잔고={cash:.2f}")
                            else:
                                logging.warning(f"⚠️ {market} 매도 시도 수량({volume_to_sell:.4f})이 보유 수량({holdings[market]:.4f})보다 많습니다. 매도 불가.")
                        else:
                            logging.debug(f"조건 미달로 {market} 매도 대기: 현재가={current_price:.8f}, 목표가={target_sell_price:.8f}")

            # 시뮬레이션 로그 기록
            quantity = holdings.get(market, 0) # 현재 보유 수량
            # gap_pct 계산 (평단가 0 방지)
            avg_price_for_display = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
            gap_pct = round(
                (current_price - avg_price_for_display) / avg_price_for_display * 100, 2
            ) if avg_price_for_display > 0 else 0

            portfolio_value = cash + quantity * current_price # 총 포트폴리오 가치
            signal_str = " / ".join(events) if events else "보유 중" # 발생한 이벤트 요약

            logs.append({
                "시간": now,
                "마켓": market,
                "시가": candle["opening_price"],
                "고가": candle["high_price"],
                "저가": candle["low_price"],
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

        except Exception as e:
            logging.error(f"❌ {current_time} 시뮬레이션 중 오류 발생: {e}", exc_info=True)
            # 오류 발생 시 해당 시간 스킵하고 다음으로 진행
        finally:
            current_time += timedelta(minutes=1) # 다음 1분으로 진행
            # API 요청 간 딜레이는 캔들 로딩 부분에서 이미 적용됨 (time.sleep(0.1))

    result_df = pd.DataFrame(logs)
    # 결과 엑셀 파일 저장
    filename = f"API_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    try:
        result_df.to_excel(filename, index=False)
        logging.info(f"✅ 백테스트 결과 파일 저장 완료: {filename}")
    except Exception as e:
        logging.error(f"❌ 백테스트 결과 파일 저장 실패: {e}", exc_info=True)

    # 최종 통계 요약 출력
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

    logging.info("--- ⏱️ API 기반 백테스트 완료 ---")