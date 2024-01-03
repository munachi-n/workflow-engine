from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
import json
import os


class TriggerType(Enum):
    MANUAL = "manual"
    WEBHOOK = "webhook"
    SCHEDULED = "scheduled"
    DEPENDENCY = "dependency"
    SENSOR = "sensor"


class Trigger:
    def __init__(self, trigger_id: str, trigger_type: TriggerType, config: Dict = None):
        self.trigger_id = trigger_id
        self.trigger_type = trigger_type
        self.config = config or {}
        self.created_at = datetime.now().isoformat()
        self.last_triggered = None

    def fire(self, payload: Dict = None) -> Dict:
        self.last_triggered = datetime.now().isoformat()
        return {
            "trigger_id": self.trigger_id,
            "trigger_type": self.trigger_type.value,
            "fired_at": self.last_triggered,
            "payload": payload or {},
        }

    def to_dict(self) -> Dict:
        return {
            "trigger_id": self.trigger_id,
            "trigger_type": self.trigger_type.value,
            "config": self.config,
            "created_at": self.created_at,
            "last_triggered": self.last_triggered,
        }


class TriggerManager:
    def __init__(self, storage_path="triggers"):
        self.storage_path = storage_path
        self.triggers: Dict[str, Trigger] = {}
        self.listeners: Dict[str, List[str]] = {}
        os.makedirs(storage_path, exist_ok=True)

    def register_trigger(self, trigger: Trigger):
        self.triggers[trigger.trigger_id] = trigger

    def add_listener(self, trigger_id: str, dag_id: str):
        if trigger_id not in self.listeners:
            self.listeners[trigger_id] = []
        if dag_id not in self.listeners[trigger_id]:
            self.listeners[trigger_id].append(dag_id)

    def remove_listener(self, trigger_id: str, dag_id: str):
        if trigger_id in self.listeners:
            self.listeners[trigger_id] = [
                d for d in self.listeners[trigger_id] if d != dag_id
            ]

    def fire_trigger(self, trigger_id: str, payload: Dict = None) -> Optional[Dict]:
        if trigger_id not in self.triggers:
            return None

        trigger = self.triggers[trigger_id]
        result = trigger.fire(payload)

        triggered_dags = self.listeners.get(trigger_id, [])
        result["triggered_dags"] = triggered_dags

        return result

    def list_triggers(self) -> List[Dict]:
        return [t.to_dict() for t in self.triggers.values()]

    def get_trigger(self, trigger_id: str) -> Optional[Dict]:
        if trigger_id in self.triggers:
            return self.triggers[trigger_id].to_dict()
        return None


class WebhookTrigger(Trigger):
    def __init__(self, trigger_id: str, secret: str = None):
        super().__init__(trigger_id, TriggerType.WEBHOOK, {"secret": secret})
        self.received_events = []

    def verify_signature(self, payload: str, signature: str) -> bool:
        if not self.config.get("secret"):
            return True
        import hashlib
        import hmac

        expected = hmac.new(
            self.config["secret"].encode(), payload.encode(), hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def receive(self, payload: Dict) -> Dict:
        self.received_events.append(
            {"timestamp": datetime.now().isoformat(), "payload": payload}
        )
        return self.fire(payload)


class SensorTrigger(Trigger):
    def __init__(self, trigger_id: str, sensor_func, threshold: Any = None):
        super().__init__(trigger_id, TriggerType.SENSOR, {"threshold": threshold})
        self.sensor_func = sensor_func
        self.last_value = None

    def check(self) -> bool:
        value = self.sensor_func()
        self.last_value = value

        if self.config.get("threshold") is None:
            return True

        return value >= self.config["threshold"]


if __name__ == "__main__":
    tm = TriggerManager()

    trigger = Trigger("manual_1", TriggerType.MANUAL)
    tm.register_trigger(trigger)

    tm.add_listener("manual_1", "my_dag")

    result = tm.fire_trigger("manual_1", {"user": "admin"})
    print(result)
