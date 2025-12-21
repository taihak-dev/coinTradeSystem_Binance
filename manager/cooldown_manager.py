# manager/cooldown_manager.py
import json
import os
import logging
from datetime import datetime, timedelta
import config

COOLDOWN_FILE = "cooldown_status.json"

class CooldownManager:
    def __init__(self):
        self.status = self._load_status()

    def _load_status(self):
        if not os.path.exists(COOLDOWN_FILE):
            return {"is_active": False, "start_time": None, "end_time": None}
        try:
            with open(COOLDOWN_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"쿨다운 상태 파일 로드 실패: {e}")
            return {"is_active": False, "start_time": None, "end_time": None}

    def _save_status(self):
        try:
            with open(COOLDOWN_FILE, 'w') as f:
                json.dump(self.status, f, indent=4)
        except Exception as e:
            logging.error(f"쿨다운 상태 파일 저장 실패: {e}")

    def start_cooldown(self):
        """쿨다운 시작"""
        now = datetime.now()
        end_time = now + timedelta(minutes=config.COOLDOWN_MINUTES)
        self.status = {
            "is_active": True,
            "start_time": now.isoformat(),
            "end_time": end_time.isoformat()
        }
        self._save_status()
        logging.info(f"❄️ 쿨다운 시작! 종료 예정 시간: {end_time}")

    def end_cooldown(self):
        """쿨다운 종료"""
        self.status = {
            "is_active": False,
            "start_time": None,
            "end_time": None
        }
        self._save_status()
        logging.info("🔥 쿨다운 종료! 매매를 재개합니다.")

    def is_cooldown_active(self) -> bool:
        """현재 쿨다운 중인지 확인"""
        if not self.status["is_active"]:
            return False
        
        # 쿨다운 시간이 지났는지 확인
        if self.status["end_time"]:
            end_time = datetime.fromisoformat(self.status["end_time"])
            if datetime.now() >= end_time:
                # 시간이 지났지만 아직 잔고 체크 등을 위해 상태는 True로 유지할 수 있음
                # 하지만 여기서는 시간만 체크하고, 잔고 체크는 외부에서 수행하도록 함
                return True 
        return True

    def get_end_time(self):
        if self.status["end_time"]:
            return datetime.fromisoformat(self.status["end_time"])
        return None

# 싱글톤 인스턴스
cooldown_manager = CooldownManager()