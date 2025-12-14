# manager/hwm_manager.py
import json
import os
import logging
from utils.telegram_notifier import notify_hwm_event

HWM_FILE = "hwm_data.json"

class HighWaterMarkManager:
    def __init__(self):
        self.hwm_data = self._load_hwm()

    def _load_hwm(self):
        if not os.path.exists(HWM_FILE):
            return {}
        try:
            with open(HWM_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"HWM 파일 로드 실패: {e}")
            return {}

    def _save_hwm(self):
        try:
            with open(HWM_FILE, 'w') as f:
                json.dump(self.hwm_data, f, indent=4)
        except Exception as e:
            logging.error(f"HWM 파일 저장 실패: {e}")

    def update_hwm(self, market: str, price: float):
        """
        현재 가격이 기존 HWM보다 높으면 갱신합니다.
        """
        old_hwm = self.hwm_data.get(market, 0.0)
        if price > old_hwm:
            self.hwm_data[market] = price
            self._save_hwm()
            logging.info(f"[{market}] HWM 갱신: {old_hwm} -> {price}")
            # notify_hwm_event("갱신", market, price, old_hwm) # 너무 잦은 알림을 방지하기 위해 주석 처리

    def get_hwm(self, market: str) -> float:
        return self.hwm_data.get(market, 0.0)

    def reset_hwm(self, market: str, new_price: float = 0.0):
        """
        HWM을 특정 가격(체결가) 또는 0으로 리셋합니다.
        """
        old_hwm = self.hwm_data.get(market, 0.0)
        self.hwm_data[market] = new_price
        self._save_hwm()
        logging.info(f"[{market}] HWM 리셋: {old_hwm} -> {new_price}")
        notify_hwm_event("리셋", market, new_price)

# 싱글톤 인스턴스 생성
hwm_manager = HighWaterMarkManager()