# strategy/sell_entry.py
# 매도 엔트리: 서비스 레이어를 경유해 주문 상태를 조회하고,
#              상태 문자열 소문자 처리 및 체결 값 키 다양성/안전 캐스팅을 적용

import sys
import logging
import math
from datetime import datetime
import pandas as pd

import config
from utils.telegram_notifier import notify_order_event, notify_error
from utils.common_utils import get_current_holdings
from manager.order_executor import execute_sell_orders
from strategy.casino_strategy import generate_sell_orders

# ──────────────────────────────────────────────────────────────────────────────
# 거래소별 주문 상태 조회 함수 임포트 (바이낸스는 서비스 레이어 경유)
# ──────────────────────────────────────────────────────────────────────────────
if config.EXCHANGE == 'binance':
    logging.info("[SYSTEM] 바이낸스 모드로 매도 로직을 설정합니다.")
    from services.exchange_service import get_order_result  # (order_id, market)
elif config.EXCHANGE == 'upbit':
    logging.info("[SYSTEM] 업비트 모드로 매도 로직을 설정합니다.")
    # 필요 시 업비트 주문 상태 조회를 연결
    # from api.upbit.order import get_order_results_by_uuids as get_order_result
else:
    logging.warning(f"[SYSTEM] 알 수 없는 EXCHANGE 값: {config.EXCHANGE}. 기본값(바이낸스)로 취급합니다.")
    from services.exchange_service import get_order_result


# ──────────────────────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────────────────────
def _load_csv(path: str, columns_if_new: list[str]) -> pd.DataFrame:
    """
    CSV를 안전하게 로드. 없으면 지정 컬럼으로 빈 DF 생성.
    sell_uuid는 문자열로 다루기 위해 dtype 지정.
    """
    try:
        return pd.read_csv(path, dtype={'sell_uuid': str})
    except FileNotFoundError:
        logging.info(f"[sell_entry] '{path}' 파일이 없어 새로 생성합니다.")
        return pd.DataFrame(columns=columns_if_new)
    except Exception as e:
        logging.error(f"[sell_entry] '{path}' 로드 중 오류: {e}", exc_info=True)
        raise


def _safe_float(value, default: float = 0.0) -> float:
    """값을 float로 안전 캐스팅. None/빈문자/NaN/변환실패 시 default."""
    try:
        if value is None:
            return float(default)
        if isinstance(value, str) and value.strip() == "":
            return float(default)
        f = float(value)
        if math.isnan(f):
            return float(default)
        return f
    except Exception:
        return float(default)


# 체결 응답에서 필요한 값들을 안전하게 추출
def _extract_done_fields(res: dict, avg_buy_price: float) -> tuple[float, float, float, float]:
    """
    가격/수량/누적 체결 금액을 안전 추출.
    - 키 다양성 대응:
      price:        avg_price | price
      executed_qty: executed_qty | executedQty
      cum_quote:    cum_quote | cummulativeQuoteQty
    - PNL = (sell_price - avg_buy_price) * sold_quantity
    """
    sell_price = _safe_float(res.get('avg_price') or res.get('price') or 0.0, 0.0)
    sold_quantity = _safe_float(res.get('executed_qty') or res.get('executedQty') or 0.0, 0.0)
    total_amount = _safe_float(res.get('cum_quote') or res.get('cummulativeQuoteQty') or 0.0, 0.0)
    pnl = (sell_price - avg_buy_price) * sold_quantity if avg_buy_price > 0 else 0.0
    return sell_price, sold_quantity, total_amount, pnl


# ──────────────────────────────────────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────────────────────────────────────
def run():
    logging.info("[sell_entry] 매도 전략 시작")

    # 1) 설정/보유 로드
    setting_df = _load_csv("setting.csv", columns_if_new=["market", "weight", "enable"])
    holdings = get_current_holdings()  # 현재 보유 수량/평단 등 (공통 util)

    # 2) 기존 매도 로그 로드 (없으면 스키마대로 생성)
    sell_log_df = _load_csv(
        "sell_log.csv",
        columns_if_new=[
            "market",
            "avg_buy_price",
            "quantity",
            "target_sell_price",
            "sell_uuid",
            "filled",
            "time",
        ],
    )

    # 3) 전략으로부터 신규/정정 매도 주문 후보 생성
    try:
        candidate_sell_df = generate_sell_orders(setting_df, holdings, sell_log_df)
        if candidate_sell_df is None:
            candidate_sell_df = pd.DataFrame(columns=sell_log_df.columns)
        if not isinstance(candidate_sell_df, pd.DataFrame):
            raise ValueError("generate_sell_orders()는 DataFrame을 반환해야 합니다.")
    except Exception as e:
        logging.error(f"[sell_entry] 매도 후보 생성 실패: {e}", exc_info=True)
        notify_error("Sell Strategy", f"매도 후보 생성 실패: {e}")
        candidate_sell_df = pd.DataFrame(columns=sell_log_df.columns)

    # 4) 기존 로그와 병합(정책에 맞게 조정 가능)
    #    여기서는 sell_uuid 기준으로 최신 정보를 우선
    if not sell_log_df.empty and "sell_uuid" in sell_log_df.columns:
        merge_on = ["market", "sell_uuid"]
        combined_sell_log_df = pd.concat([sell_log_df, candidate_sell_df], ignore_index=True)
        combined_sell_log_df.drop_duplicates(subset=merge_on, keep="last", inplace=True)
    else:
        combined_sell_log_df = candidate_sell_df.copy()

    # 5) 'wait' 상태 주문의 체결/취소 여부 갱신
    if not combined_sell_log_df.empty:
        # 결측 기본값 보정
        for col, default in [("filled", ""), ("avg_buy_price", 0.0)]:
            if col not in combined_sell_log_df.columns:
                combined_sell_log_df[col] = default

        for idx, row in combined_sell_log_df.iterrows():
            market = str(row.get("market", "")).strip()  # ← 먼저 기본값으로 확보
            try:
                order_id = str(row.get("sell_uuid", "")).strip()
                filled = str(row.get("filled", "")).lower().strip()

                if not market or not order_id:
                    continue  # 필수 정보 없으면 패스

                # 'wait' 상태만 조회
                if filled != "wait":
                    continue

                # ── 주문 상태 조회 ───────────────────────────────────────────
                res_raw = get_order_result(order_id, market)
                res = res_raw or {}
                if not isinstance(res, dict):
                    raise ValueError(f"invalid order result type: {type(res_raw)}")

                state = str(res.get("state", "")).lower()
                # ────────────────────────────────────────────────────────────

                if state == "done":
                    # 체결 처리
                    combined_sell_log_df.at[idx, "filled"] = "done"
                    combined_sell_log_df.at[idx, "time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    avg_buy_price = _safe_float(row.get("avg_buy_price", 0.0), 0.0)
                    sell_price, sold_qty, total_amount, pnl = _extract_done_fields(res, avg_buy_price)

                    logging.info(f"🎉 [{market}] 매도 체결! price={sell_price}, qty={sold_qty}, pnl={pnl:.6f}")
                    notify_order_event(
                        "체결",
                        market,
                        {
                            "filled_qty": sold_qty,
                            "price": sell_price,
                            "total_amount": total_amount,
                            "fee": 0.0,   # 필요 시 거래소 응답의 수수료 키로 대체
                            "pnl": pnl,
                        },
                    )

                elif state == "cancel":
                    # (옵션) 취소 상태 반영
                    combined_sell_log_df.at[idx, "filled"] = "cancel"
                    combined_sell_log_df.at[idx, "time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f"ℹ️ [{market}] 매도 주문이 취소로 확인되어 로그를 갱신합니다.")

                else:
                    # 여전히 대기(wait) or 기타 상태
                    logging.debug(f"[{market}] 주문 상태 유지: state={state}")

            except (KeyError, ValueError, TypeError) as e:
                logging.error(f"[sell_entry] 데이터 처리 오류({market}): {e}", exc_info=True)
                notify_error("Sell Update", f"{market} 데이터 처리 오류: {e}")
            except Exception as e:
                logging.error(f"[sell_entry] 알 수 없는 오류({market}): {e}", exc_info=True)
                notify_error("Sell Update", f"{market} 알 수 없는 오류: {e}")

    # 6) 실제 주문 실행(전략이 새 주문을 만든 경우) 및 저장
    try:
        final_sell_log_df = execute_sell_orders(sell_log_df=combined_sell_log_df)

        # (선택) 컬럼 순서 고정 저장 — 사람이 보기 편하고 안정적
        final_cols = [
            "market",
            "avg_buy_price",
            "quantity",
            "target_sell_price",
            "sell_uuid",
            "filled",
            "time",
        ]
        for c in final_cols:
            if c not in final_sell_log_df.columns:
                final_sell_log_df[c] = ""

        final_sell_log_df[final_cols].to_csv("sell_log.csv", index=False)
        logging.info("[sell_entry] 모든 주문 처리 완료 → sell_log.csv 저장 완료")
    except Exception as e:
        logging.error(f"🚨 매도 주문 실행 중 치명적 오류: {e}", exc_info=True)
        notify_error("Sell Execution", f"매도 주문 실행 중 오류: {e}")
        sys.exit(1)

    logging.info("[sell_entry] 매도 전략 흐름 종료")


def run_sell_entry_flow() -> None:
    """엔트리 호환 래퍼: 기존 entry.py가 기대하는 이름 유지"""
    return run()