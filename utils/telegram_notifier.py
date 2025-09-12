# utils/telegram_notifier.py

import requests
import logging
import config

# 로깅 설정 (다른 모듈과 동일하게)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_BOT_TOKEN = config.TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID = config.TELEGRAM_CHAT_ID


def send_telegram_message(message: str):
    """
    텔레그램 봇을 통해 메시지를 전송합니다.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("⚠️ 텔레그램 봇 토큰 또는 Chat ID가 설정되지 않아 알림을 보낼 수 없습니다.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'  # 메시지를 Markdown 형식으로 파싱 (볼드, 이탤릭 등 사용 가능)
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
        logging.info(f"✅ 텔레그램 메시지 전송 성공: {message[:50]}...")  # 메시지 길면 잘라서 로깅
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ 텔레그램 메시지 전송 실패: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"❌ 알 수 없는 텔레그램 전송 오류: {e}", exc_info=True)


# --- 알림 유형별 헬퍼 함수 (편의를 위해) ---
def notify_bot_status(status: str, detail: str = ""):
    """봇 시작, 종료, 정상 동작 알림"""
    icon = "✅" if "시작" in status or "정상" in status else "⚠️"
    send_telegram_message(f"{icon} *[봇 상태]* {status}\n{detail}")


def notify_error(module: str, message: str):
    """오류 발생 알림"""
    send_telegram_message(f"🚨 *[오류 발생]* `[{module}]`\n`{message}`")


def notify_order_event(event_type: str, market: str, details: dict):
    """주문 제출, 체결, 취소, 실패 알림"""
    icon_map = {
        "제출": "📝", "체결": "✅", "부분 체결": "✅",
        "취소": "🚫", "실패": "❌"
    }
    icon = icon_map.get(event_type, "ℹ️")

    msg = f"{icon} *[주문 {event_type}]* `{market}`\n"
    if event_type == "제출":
        msg += f"수량: `{details.get('quantity'):.6f}`개, 가격: `{details.get('price'):.8f}` USDT\n"
        msg += f"타입: `{details.get('type')}`, 레버리지: `{details.get('leverage')}`x\n"
    elif event_type == "체결" or event_type == "부분 체결":
        msg += f"체결 수량: `{details.get('filled_qty'):.6f}`개, 체결가: `{details.get('price'):.8f}` USDT\n"
        msg += f"총 금액: `{details.get('total_amount'):.2f}` USDT, 수수료: `{details.get('fee'):.2f}` USDT\n"
        if details.get('pnl') is not None:
            pnl_val = details.get('pnl', 0)
            pnl_icon = "🟢" if pnl_val >= 0 else "🔴"
            msg += f"실현 손익: {pnl_icon}`{pnl_val:.2f}` USDT"
    elif event_type == "취소" or event_type == "실패":
        msg += f"사유: `{details.get('reason', '알 수 없음')}`\n"

    send_telegram_message(msg)


def notify_position_summary(summary: dict):
    """주기적인 포지션 및 계좌 요약 알림"""
    msg = "*[📊 포지션/계좌 현황 요약]*\n\n"

    # --- 👇👇👇 여기가 수정된 부분입니다 👇👇👇 ---
    # 'total_portfolio_value' -> 'total_wallet_balance' 로 키 이름 변경
    total_balance = summary.get('total_wallet_balance')
    if total_balance is not None:
        msg += f"💰 **총 자산 가치:** `{total_balance:.2f}` USDT\n"
    # --- 👆👆👆 여기까지 수정 완료 --- 👆👆👆

    msg += f"💵 **사용 가능 USDT:** `{summary.get('usdt_balance'):.2f}` USDT\n"
    msg += f"📈 **총 미실현 손익:** `{summary.get('total_unrealized_pnl'):.2f}` USDT\n"

    if summary.get('open_positions'):
        msg += "\n--- *보유 포지션 상세* ---\n"
        sorted_positions = sorted(summary['open_positions'], key=lambda x: x.get('unRealizedProfit', 0), reverse=True)

        for pos_info in sorted_positions:
            pnl_val = pos_info.get('unRealizedProfit', 0)
            pnl_icon = "🟢" if pnl_val >= 0 else "🔴"
            roe_val = pos_info.get('roe', 0.0)

            msg += f"\n*{pos_info.get('symbol')}* ({pos_info.get('leverage')}x)\n"
            msg += f"  - **수량:** `{pos_info.get('positionAmt', 0):.6f}` 개\n"
            msg += f"  - **평단가:** `{pos_info.get('entryPrice', 0):.8f}`\n"
            msg += f"  - **현재가:** `{pos_info.get('markPrice', 0):.8f}`\n"
            msg += f"  - **미실현 손익(수익률):** {pnl_icon}`{pnl_val:.2f}` USDT (`{roe_val:.2f}`%)\n"
            msg += f"  - **청산가:** `{pos_info.get('liquidationPrice', 0):.8f}`\n"
        msg += "--------------------------\n"
    else:
        msg += "\n현재 열려있는 포지션이 없습니다.\n"

    send_telegram_message(msg)


def notify_liquidation_warning(market, current_price, liquidation_price, entry_price, roe, warning_level):
    """청산 위험 경고 알림"""
    icon = "⚠️" if warning_level == 1 else "🚨🚨"
    title = "청산 위험 경고" if warning_level == 1 else "긴급 청산 경고!"

    msg = f"{icon} *[{title}]* `{market}`\n"
    msg += f"  현재가: `{current_price:.8f}`\n"
    msg += f"  청산가: `{liquidation_price:.8f}`\n"
    msg += f"  진입가: `{entry_price:.8f}`\n"
    msg += f"  현재 손실률: `{roe:.2f}`%"

    send_telegram_message(msg)