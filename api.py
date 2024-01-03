from flask import Flask, request, jsonify
import json
import logging
from datetime import datetime

from engine import WorkflowEngine, Task, TaskStatus, DAG
from scheduler import Scheduler, Schedule
from triggers import TriggerManager, Trigger, TriggerType

app = Flask(__name__)

engine = WorkflowEngine()
scheduler = Scheduler()
trigger_manager = TriggerManager()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


@app.route("/dags", methods=["GET"])
def list_dags():
    return jsonify({"dags": list(engine.dags.keys())})


@app.route("/dags", methods=["POST"])
def create_dag():
    data = request.json
    dag_id = data.get("dag_id")

    if not dag_id:
        return jsonify({"error": "dag_id required"}), 400

    if dag_id in engine.dags:
        return jsonify({"error": "DAG already exists"}), 400

    dag = DAG(dag_id)

    for task_data in data.get("tasks", []):
        task = Task(
            task_id=task_data["task_id"],
            func=lambda **kwargs: kwargs,
            params=task_data.get("params", {}),
            depends_on=task_data.get("depends_on", []),
        )
        dag.add_task(task)

    engine.register_dag(dag)
    return jsonify({"dag_id": dag_id, "status": "created"})


@app.route("/dags/<dag_id>", methods=["GET"])
def get_dag(dag_id):
    if dag_id not in engine.dags:
        return jsonify({"error": "DAG not found"}), 404

    return jsonify(engine.dags[dag_id].to_dict())


@app.route("/dags/<dag_id>/run", methods=["POST"])
def run_dag(dag_id):
    try:
        run_id = engine.run_dag(dag_id)
        return jsonify({"run_id": run_id, "status": "started"})
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


@app.route("/runs", methods=["GET"])
def list_runs():
    dag_id = request.args.get("dag_id")
    runs = engine.list_runs(dag_id)
    return jsonify({"runs": runs})


@app.route("/runs/<run_id>", methods=["GET"])
def get_run(run_id):
    run = engine.get_run(run_id)
    if run is None:
        return jsonify({"error": "Run not found"}), 404
    return jsonify(run)


@app.route("/schedules", methods=["GET"])
def list_schedules():
    return jsonify({"schedules": scheduler.list_schedules()})


@app.route("/schedules", methods=["POST"])
def create_schedule():
    data = request.json
    dag_id = data.get("dag_id")
    schedule_type = data.get("schedule_type", "daily")
    interval = data.get("interval")

    if not dag_id:
        return jsonify({"error": "dag_id required"}), 400

    sched = Schedule(dag_id, schedule_type, interval)
    scheduler.add_schedule(sched)

    return jsonify({"dag_id": dag_id, "status": "scheduled"})


@app.route("/schedules/<dag_id>", methods=["DELETE"])
def delete_schedule(dag_id):
    scheduler.remove_schedule(dag_id)
    return jsonify({"status": "deleted"})


@app.route("/triggers", methods=["GET"])
def list_triggers():
    return jsonify({"triggers": trigger_manager.list_triggers()})


@app.route("/triggers", methods=["POST"])
def create_trigger():
    data = request.json
    trigger_id = data.get("trigger_id")
    trigger_type = data.get("trigger_type", "manual")

    if not trigger_id:
        return jsonify({"error": "trigger_id required"}), 400

    trigger = Trigger(trigger_id, TriggerType(trigger_type))
    trigger_manager.register_trigger(trigger)

    return jsonify({"trigger_id": trigger_id, "status": "created"})


@app.route("/triggers/<trigger_id>/fire", methods=["POST"])
def fire_trigger(trigger_id):
    payload = request.json or {}
    result = trigger_manager.fire_trigger(trigger_id, payload)

    if result is None:
        return jsonify({"error": "Trigger not found"}), 404

    return jsonify(result)


# if __name__ == "__main__":
#     app.run(debug=True, port=5000)
