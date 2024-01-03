import os
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Callable, Optional
import hashlib
import time
import traceback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Task:
    def __init__(
        self,
        task_id: str,
        func: Callable,
        params: Dict = None,
        depends_on: List[str] = None,
    ):
        self.task_id = task_id
        self.func = func
        self.params = params or {}
        self.depends_on = depends_on or []
        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None
        self.started_at = None
        self.completed_at = None
        self.try_count = 0

    def execute(self) -> Any:
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.now().isoformat()
        self.try_count += 1

        try:
            logger.info(f"Executing task: {self.task_id}")
            self.result = self.func(**self.params)
            self.status = TaskStatus.SUCCESS
            logger.info(f"Task {self.task_id} completed successfully")
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.error = str(e)
            logger.error(f"Task {self.task_id} failed: {e}")
            raise
        finally:
            self.completed_at = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": str(self.result) if self.result else None,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "try_count": self.try_count,
        }


class DAG:
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: Dict[str, Task] = {}
        self.execution_order: List[str] = []

    def add_task(self, task: Task):
        self.tasks[task.task_id] = task

    def topological_sort(self) -> List[str]:
        in_degree = {tid: 0 for tid in self.tasks}
        for task in self.tasks.values():
            for dep in task.depends_on:
                if dep in in_degree:
                    in_degree[task.task_id] += 1

        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        self.execution_order = []

        while queue:
            current = queue.pop(0)
            self.execution_order.append(current)

            for task in self.tasks.values():
                if current in task.depends_on:
                    in_degree[task.task_id] -= 1
                    if in_degree[task.task_id] == 0:
                        queue.append(task.task_id)

        return self.execution_order

    def get_ready_tasks(self, completed: List[str]) -> List[Task]:
        ready = []
        for task in self.tasks.values():
            if task.status == TaskStatus.PENDING:
                if all(dep in completed for dep in task.depends_on):
                    ready.append(task)
        return ready

    def to_dict(self) -> Dict:
        return {
            "dag_id": self.dag_id,
            "tasks": {tid: t.to_dict() for tid, t in self.tasks.items()},
            "execution_order": self.execution_order,
        }


class WorkflowEngine:
    def __init__(self, storage_path="workflows"):
        self.storage_path = storage_path
        self.dags: Dict[str, DAG] = {}
        self.runs: Dict[str, Dict] = {}
        os.makedirs(storage_path, exist_ok=True)

    def register_dag(self, dag: DAG):
        self.dags[dag.dag_id] = dag
        logger.info(f"Registered DAG: {dag.dag_id}")

    def run_dag(self, dag_id: str, max_parallel: int = 4) -> str:
        if dag_id not in self.dags:
            raise ValueError(f"DAG {dag_id} not found")

        dag = self.dags[dag_id]
        dag.topological_sort()

        run_id = hashlib.md5(f"{dag_id}{datetime.now()}".encode()).hexdigest()[:12]

        run = {
            "run_id": run_id,
            "dag_id": dag_id,
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "tasks_completed": [],
            "tasks_failed": [],
        }

        self.runs[run_id] = run
        logger.info(f"Starting run {run_id} for DAG {dag_id}")

        completed = []
        failed = []

        while len(completed) + len(failed) < len(dag.tasks):
            ready_tasks = dag.get_ready_tasks(completed)

            for task in ready_tasks[:max_parallel]:
                try:
                    task.execute()
                    completed.append(task.task_id)
                    run["tasks_completed"].append(task.task_id)
                except Exception as e:
                    failed.append(task.task_id)
                    run["tasks_failed"].append(task.task_id)

            time.sleep(0.1)

        run["status"] = "completed" if not failed else "failed"
        run["completed_at"] = datetime.now().isoformat()

        self._save_run(run)
        logger.info(f"Run {run_id} {run['status']}")

        return run_id

    def _save_run(self, run: Dict):
        filepath = os.path.join(self.storage_path, f"{run['run_id']}.json")
        with open(filepath, "w") as f:
            json.dump(run, f, indent=2)

    def get_run(self, run_id: str) -> Optional[Dict]:
        filepath = os.path.join(self.storage_path, f"{run_id}.json")
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return json.load(f)
        return None

    def list_runs(self, dag_id: str = None) -> List[Dict]:
        runs = []
        for run in self.runs.values():
            if dag_id is None or run["dag_id"] == dag_id:
                runs.append(run)
        return runs

    def get_dag_status(self, dag_id: str) -> Dict:
        if dag_id not in self.dags:
            return {"error": "DAG not found"}

        dag = self.dags[dag_id]
        return {
            "dag_id": dag_id,
            "total_tasks": len(dag.tasks),
            "status": "registered",
            "tasks": {tid: t.status.value for tid, t in dag.tasks.items()},
        }


def sample_task_1(**kwargs):
    logger.info(f"Task 1 running with {kwargs}")
    return "task_1_result"


def sample_task_2(**kwargs):
    logger.info(f"Task 2 running with {kwargs}")
    return "task_2_result"


def sample_task_3(**kwargs):
    logger.info(f"Task 3 running with {kwargs}")
    return "task_3_result"


if __name__ == "__main__":
    engine = WorkflowEngine()

    dag = DAG("sample_dag")
    dag.add_task(Task("task_1", sample_task_1))
    dag.add_task(Task("task_2", sample_task_2, depends_on=["task_1"]))
    dag.add_task(Task("task_3", sample_task_3, depends_on=["task_1"]))

    engine.register_dag(dag)
    run_id = engine.run_dag("sample_dag")
    print(f"Run completed: {run_id}")
