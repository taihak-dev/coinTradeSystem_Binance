# simulator.py

import pandas as pd
from datetime import datetime, timedelta
import time
import config  # 추가

from strategy.casino_strategy import generate_buy_orders, generate_sell_orders

# 추가: config 설정에 따라 다른 API 모듈을 가져옴
if config.EXCHANGE == 'binance':
    from api.binance.price import get_minute_candles
    print("[SYSTEM] API 시뮬레이터: 바이낸스 모드로 실행합니다.")
else:
    from api.upbit.price import get_minute_candles
    print("[SYSTEM] API 시뮬레이터: 업비트 모드로 실행합니다.")

INITIAL_CASH = 60_000
BUY_FEE = 0.0005
SELL_FEE = 0.0005

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
    leverage: int = 1  # 추가: 레버리지 파라미터
):
    print(f"[simulator.py] ⏱️ API 기반 백테스트 시작 - {market}, {start} ~ {end}")

    # 추가: 레버리지 적용 로그
    print(f"[simulator.py] 🔬 레버리지 적용: {leverage}x (기본 투자금: {unit_size} -> 실제 투자금: {unit_size * leverage})")

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)

    setting_df = pd.DataFrame([{
        "market": market,
        "unit_size": unit_size * leverage,  # 변경: 레버리지를 곱한 값을 실제 투자금으로 사용
        "small_flow_pct": small_flow_pct,
        "small_flow_units": small_flow_units,
        "large_flow_pct": large_flow_pct,
        "large_flow_units": large_flow_units,
        "take_profit_pct": take_profit_pct
    }])

    # 이하 로직은 DB 시뮬레이터와 거의 동일하며, 데이터 로딩 방식만 다릅니다.
    # 대부분의 코드는 수정할 필요가 없습니다.
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

    current_time = start_dt
    while current_time <= end_dt:
        try:
            # API를 통해 현재 시점의 캔들 1개를 가져옴
            candle = get_minute_candles(market, to=current_time.strftime("%Y-%m-%d %H:%M:%S"), count=1)
            if not candle:
                current_time += timedelta(minutes=1)
                continue

            candle = candle[0]
            now = pd.to_datetime(candle["candle_date_time_kst"])
            current_price = candle["trade_price"]
            events = []

            # --- 이하 매매 로직은 DB 시뮬레이터와 동일 ---
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

            if market in holdings and holdings[market] > 0:
                balance = holdings[market]
                avg_buy_price = total_buy_amount / total_buy_volume if total_buy_volume > 0 else 0
                holdings_info = {
                    market: {
                        "balance": balance, "locked": 0, "avg_price": avg_buy_price
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
            portfolio_value = cash + quantity * current_price
            signal_str = " / ".join(events) if events else "보유"

            logs.append({
                "시간": now, "종가": current_price, "신호": signal_str,
                "매매금액": round(last_trade_amount, 2), "현재 평단가": round(avg_price, 5),
                "누적 매수금": round(total_buy_amount, 2), "실현 손익": round(realized_pnl, 2),
                "보유 현금": round(cash, 2), "총 누적 수수료": round(cumulative_fee, 2),
                "총 포트폴리오 값": round(portfolio_value, 2)
            })

            time.sleep(0.2) # API 요청 제한 방지
        except Exception as e:
            print(f"Error during simulation at {current_time}: {e}")

        current_time += timedelta(minutes=1)
        if current_time.minute == 0:
            print(f"Simulating... {current_time}")

    result_df = pd.DataFrame(logs)
    filename = f"API_시뮬_{market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    result_df.to_excel(filename, index=False)
    print(f"\n파일 저장 완료: {filename}")

    if not result_df.empty:
        first, last = result_df.iloc[0], result_df.iloc[-1]
        print("\n통계 요약")
        print(f"▶ 시작: {first['시간']} | 마켓: {first['마켓']}")
        print(f"  - 누적 매수금: {first['누적 매수금']:,}USDT")
        print(f"  - 실현 손익: {first['실현 손익']:,}USDT")
        print(f"  - 보유 현금: {first['보유 현금']:,}USDT")
        print(f"  - 총 포트폴리오 가치: {first['총 포트폴리오 값']:,}USDT")

        print(f"\n▶ 종료: {last['시간']} | 마켓: {last['마켓']}")
        print(f"  - 누적 매수금: {last['누적 매수금']:,}USDT")
        print(f"  - 실현 손익: {last['실현 손익']:,}USDT")
        print(f"  - 보유 현금: {last['보유 현금']:,}USDT")
        print(f"  - 총 포트폴리오 가치: {last['총 포트폴리오 값']:,}USDT")