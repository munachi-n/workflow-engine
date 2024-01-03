import unittest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from engine import WorkflowEngine, Task, TaskStatus, DAG
from scheduler import Scheduler, Schedule
from triggers import TriggerManager, Trigger, TriggerType


class TestWorkflowEngine(unittest.TestCase):
    def setUp(self):
        self.engine = WorkflowEngine(storage_path="test_workflows")

    def test_create_task(self):
        task = Task("task_1", lambda: None)
        self.assertEqual(task.task_id, "task_1")
        self.assertEqual(task.status, TaskStatus.PENDING)

    def test_task_execution(self):
        def sample_func(x, y):
            return x + y

        task = Task("task_1", sample_func, {"x": 2, "y": 3})
        task.execute()
        self.assertEqual(task.result, 5)
        self.assertEqual(task.status, TaskStatus.SUCCESS)

    def test_dag_creation(self):
        dag = DAG("test_dag")
        dag.add_task(Task("task_1", lambda: None))
        dag.add_task(Task("task_2", lambda: None, depends_on=["task_1"]))
        self.assertEqual(len(dag.tasks), 2)

    def test_topological_sort(self):
        dag = DAG("test_dag")
        dag.add_task(Task("task_1", lambda: None))
        dag.add_task(Task("task_2", lambda: None, depends_on=["task_1"]))
        dag.add_task(Task("task_3", lambda: None, depends_on=["task_1"]))

        order = dag.topological_sort()
        self.assertEqual(order[0], "task_1")

    def test_run_dag(self):
        def add_numbers(x, y):
            return x + y

        dag = DAG("test_dag")
        dag.add_task(Task("task_1", add_numbers, {"x": 1, "y": 2}))

        self.engine.register_dag(dag)
        run_id = self.engine.run_dag("test_dag")

        self.assertIsNotNone(run_id)
        run = self.engine.get_run(run_id)
        self.assertEqual(run["status"], "completed")


class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler(storage_path="test_schedules")

    def test_create_schedule(self):
        sched = Schedule("test_dag", "daily")
        self.scheduler.add_schedule(sched)
        self.assertIn(
            "test_dag", [s["dag_id"] for s in self.scheduler.list_schedules()]
        )

    def test_schedule_next_run(self):
        sched = Schedule("test_dag", "daily")
        sched.calculate_next_run()
        self.assertIsNotNone(sched.next_run)

    def test_enable_disable(self):
        sched = Schedule("test_dag", "daily")
        self.scheduler.add_schedule(sched)

        self.scheduler.disable_schedule("test_dag")
        self.assertFalse(self.scheduler.schedules["test_dag"].enabled)

        self.scheduler.enable_schedule("test_dag")
        self.assertTrue(self.scheduler.schedules["test_dag"].enabled)


class TestTriggers(unittest.TestCase):
    def setUp(self):
        self.tm = TriggerManager()

    def test_create_trigger(self):
        trigger = Trigger("trigger_1", TriggerType.MANUAL)
        self.tm.register_trigger(trigger)

        triggers = self.tm.list_triggers()
        self.assertEqual(len(triggers), 1)

    def test_fire_trigger(self):
        trigger = Trigger("trigger_1", TriggerType.MANUAL)
        self.tm.register_trigger(trigger)
        self.tm.add_listener("trigger_1", "dag_1")

        result = self.tm.fire_trigger("trigger_1", {"data": "test"})

        self.assertIsNotNone(result)
        self.assertEqual(result["trigger_id"], "trigger_1")
        self.assertIn("dag_1", result["triggered_dags"])


if __name__ == "__main__":
    unittest.main()
