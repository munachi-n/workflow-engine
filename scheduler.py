from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import os


class Schedule:
    def __init__(self, dag_id: str, schedule_type: str, interval: int = None):
        self.dag_id = dag_id
        self.schedule_type = schedule_type
        self.interval = interval
        self.enabled = True
        self.last_run = None
        self.next_run = None

    def calculate_next_run(self) -> datetime:
        if self.schedule_type == "daily":
            self.next_run = datetime.now() + timedelta(days=1)
        elif self.schedule_type == "hourly":
            self.next_run = datetime.now() + timedelta(hours=1)
        elif self.schedule_type == "interval":
            if self.interval:
                self.next_run = datetime.now() + timedelta(seconds=self.interval)
        return self.next_run

    def should_run(self) -> bool:
        if not self.enabled:
            return False
        if self.next_run is None:
            return True
        return datetime.now() >= self.next_run

    def to_dict(self) -> Dict:
        return {
            "dag_id": self.dag_id,
            "schedule_type": self.schedule_type,
            "interval": self.interval,
            "enabled": self.enabled,
            "last_run": self.last_run,
            "next_run": self.next_run.isoformat() if self.next_run else None,
        }


class Scheduler:
    def __init__(self, storage_path="schedules"):
        self.storage_path = storage_path
        self.schedules: Dict[str, Schedule] = {}
        os.makedirs(storage_path, exist_ok=True)
        self._load_schedules()

    def _load_schedules(self):
        if os.path.exists(os.path.join(self.storage_path, "schedules.json")):
            with open(os.path.join(self.storage_path, "schedules.json"), "r") as f:
                data = json.load(f)
                for dag_id, sched_data in data.items():
                    sched = Schedule(
                        dag_id, sched_data["schedule_type"], sched_data.get("interval")
                    )
                    sched.enabled = sched_data.get("enabled", True)
                    self.schedules[dag_id] = sched

    def _save_schedules(self):
        data = {dag_id: sched.to_dict() for dag_id, sched in self.schedules.items()}
        with open(os.path.join(self.storage_path, "schedules.json"), "w") as f:
            json.dump(data, f, indent=2)

    def add_schedule(self, schedule: Schedule):
        self.schedules[schedule.dag_id] = schedule
        schedule.calculate_next_run()
        self._save_schedules()

    def remove_schedule(self, dag_id: str):
        if dag_id in self.schedules:
            del self.schedules[dag_id]
            self._save_schedules()

    def enable_schedule(self, dag_id: str):
        if dag_id in self.schedules:
            self.schedules[dag_id].enabled = True
            self._save_schedules()

    def disable_schedule(self, dag_id: str):
        if dag_id in self.schedules:
            self.schedules[dag_id].enabled = False
            self._save_schedules()

    def get_pending_runs(self) -> List[str]:
        pending = []
        for dag_id, schedule in self.schedules.items():
            if schedule.should_run():
                pending.append(dag_id)
        return pending

    def mark_run(self, dag_id: str):
        if dag_id in self.schedules:
            self.schedules[dag_id].last_run = datetime.now().isoformat()
            self.schedules[dag_id].calculate_next_run()
            self._save_schedules()

    def list_schedules(self) -> List[Dict]:
        return [sched.to_dict() for sched in self.schedules.values()]


class CronParser:
    @staticmethod
    def parse(cron_expr: str) -> Schedule:
        parts = cron_expr.split()
        if len(parts) < 5:
            raise ValueError("Invalid cron expression")

        return Schedule(dag_id="default", schedule_type="custom", interval=60)

    @staticmethod
    def validate(cron_expr: str) -> bool:
        try:
            CronParser.parse(cron_expr)
            return True
        except:
            return False


if __name__ == "__main__":
    scheduler = Scheduler()

    sched = Schedule("my_dag", "daily")
    scheduler.add_schedule(sched)

    print(scheduler.list_schedules())
    print(scheduler.get_pending_runs())
