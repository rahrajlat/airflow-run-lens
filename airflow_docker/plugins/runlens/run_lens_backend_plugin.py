"""RunLens backend API plugin for Airflow 3."""

from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles

try:
    from airflow.api_fastapi.core_api.security import get_user
except ImportError:
    get_user = None


def get_db() -> Generator:
    session = Session()
    try:
        yield session
    finally:
        session.close()


dependencies = [Depends(get_user)] if get_user is not None else []
app = FastAPI(title="RunLens API", dependencies=dependencies)
static_dir = Path(__file__).parent
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="runlens_static")


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def seconds_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return max((end - start).total_seconds(), 0.0)


def task_duration_seconds(task_instance: TaskInstance) -> float | None:
    duration = getattr(task_instance, "duration", None)
    if duration is not None:
        return float(duration)
    return seconds_between(task_instance.start_date, task_instance.end_date)


def dag_run_duration_seconds(dag_run: DagRun) -> float | None:
    duration = getattr(dag_run, "duration", None)
    if duration is not None:
        return float(duration)
    return seconds_between(dag_run.start_date, dag_run.end_date)


def serialize_dag(dag_model: DagModel) -> dict[str, Any]:
    return {
        "dag_id": dag_model.dag_id,
        "is_paused": dag_model.is_paused,
        "is_active": dag_model.is_active,
        "last_parsed_time": iso_or_none(getattr(dag_model, "last_parsed_time", None)),
        "last_expired": iso_or_none(getattr(dag_model, "last_expired", None)),
    }


def serialize_run(dag_run: DagRun) -> dict[str, Any]:
    return {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "state": str(dag_run.state) if dag_run.state is not None else None,
        "run_type": str(getattr(dag_run, "run_type", "")) or None,
        "logical_date": iso_or_none(getattr(dag_run, "logical_date", None)),
        "start_date": iso_or_none(dag_run.start_date),
        "end_date": iso_or_none(dag_run.end_date),
        "duration_seconds": dag_run_duration_seconds(dag_run),
    }


def serialize_task_instance(task_instance: TaskInstance) -> dict[str, Any]:
    return {
        "dag_id": task_instance.dag_id,
        "run_id": task_instance.run_id,
        "task_id": task_instance.task_id,
        "map_index": task_instance.map_index,
        "state": str(task_instance.state) if task_instance.state is not None else None,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "operator": getattr(task_instance, "operator", None),
        "start_date": iso_or_none(task_instance.start_date),
        "end_date": iso_or_none(task_instance.end_date),
        "duration_seconds": task_duration_seconds(task_instance),
    }


def get_run_or_404(session, dag_id: str, run_id: str) -> DagRun:
    dag_run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
        .one_or_none()
    )
    if dag_run is None:
        raise HTTPException(
            status_code=404,
            detail=f"DAG run not found for dag_id={dag_id!r}, run_id={run_id!r}",
        )
    return dag_run


def list_task_instances(session, dag_id: str, run_id: str) -> list[TaskInstance]:
    return (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id)
        .order_by(TaskInstance.start_date.asc().nullslast(), TaskInstance.task_id.asc())
        .all()
    )


def percent_delta(base: float | None, compare: float | None) -> float | None:
    if base in (None, 0) or compare is None:
        return None
    return ((compare - base) / base) * 100


def count_by_state(tasks: list[dict[str, Any]], state: str) -> int:
    return sum(1 for task in tasks if task["state"] == state)


def retry_total(tasks: list[dict[str, Any]]) -> int:
    return sum(max((task["try_number"] or 1) - 1, 0) for task in tasks)


def median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[midpoint]
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/dags")
def dags(
    include_inactive: bool = False,
    limit: int = Query(default=100, ge=1, le=500),
    session=Depends(get_db),
) -> dict[str, Any]:
    query = session.query(DagModel)
    if not include_inactive:
        query = query.filter(DagModel.is_active.is_(True))

    dag_models = query.order_by(DagModel.dag_id.asc()).limit(limit).all()
    return {"dags": [serialize_dag(dag_model) for dag_model in dag_models]}


@app.get("/dags/{dag_id}/runs")
def dag_runs(
    dag_id: str,
    limit: int = Query(default=25, ge=1, le=200),
    session=Depends(get_db),
) -> dict[str, Any]:
    runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .order_by(DagRun.start_date.desc().nullslast(), DagRun.run_id.desc())
        .limit(limit)
        .all()
    )
    return {"dag_id": dag_id, "runs": [serialize_run(run) for run in runs]}


@app.get("/dags/{dag_id}/runs/{run_id}/tasks")
def run_tasks(dag_id: str, run_id: str, session=Depends(get_db)) -> dict[str, Any]:
    dag_run = get_run_or_404(session, dag_id, run_id)
    task_instances = list_task_instances(session, dag_id, run_id)
    return {
        "dag_id": dag_id,
        "run": serialize_run(dag_run),
        "tasks": [serialize_task_instance(task) for task in task_instances],
    }


@app.get("/dags/{dag_id}/history")
def dag_history(
    dag_id: str,
    days: int = Query(default=10, ge=1, le=365),
    limit: int = Query(default=100, ge=1, le=500),
    session=Depends(get_db),
) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .filter(DagRun.start_date >= since)
        .order_by(DagRun.start_date.asc().nullslast(), DagRun.run_id.asc())
        .limit(limit)
        .all()
    )
    run_ids = [run.run_id for run in runs]
    if not run_ids:
        return {"dag_id": dag_id, "days": days, "runs": [], "tasks": [], "summary": {}}

    task_instances = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id.in_(run_ids))
        .order_by(TaskInstance.run_id.asc(), TaskInstance.task_id.asc())
        .all()
    )
    serialized_tasks = [serialize_task_instance(task) for task in task_instances]
    latest_run_id = run_ids[-1]
    by_task_id: dict[str, list[dict[str, Any]]] = {}
    for task in serialized_tasks:
        by_task_id.setdefault(task["task_id"], []).append(task)

    task_summaries = []
    for task_id, task_history in sorted(by_task_id.items()):
        previous_durations = [
            task["duration_seconds"]
            for task in task_history
            if task["run_id"] != latest_run_id and task["duration_seconds"] is not None
        ]
        latest_task = next(
            (task for task in task_history if task["run_id"] == latest_run_id),
            None,
        )
        latest_duration = latest_task["duration_seconds"] if latest_task else None
        median_duration = median(previous_durations)
        task_summaries.append(
            {
                "task_id": task_id,
                "latest": latest_task,
                "median_seconds": median_duration,
                "latest_delta_seconds": (
                    latest_duration - median_duration
                    if latest_duration is not None and median_duration is not None
                    else None
                ),
                "latest_delta_percent": percent_delta(median_duration, latest_duration),
                "history": task_history,
            }
        )

    return {
        "dag_id": dag_id,
        "days": days,
        "runs": [serialize_run(run) for run in runs],
        "tasks": task_summaries,
        "summary": {
            "run_count": len(runs),
            "success_count": sum(1 for run in runs if str(run.state) == "success"),
            "failed_count": sum(1 for run in runs if str(run.state) == "failed"),
            "latest_run_id": latest_run_id,
        },
    }


@app.get("/dags/{dag_id}/compare")
def compare_runs(
    dag_id: str,
    base_run_id: str,
    compare_run_id: str,
    session=Depends(get_db),
) -> dict[str, Any]:
    base_run = get_run_or_404(session, dag_id, base_run_id)
    compare_run = get_run_or_404(session, dag_id, compare_run_id)
    base_tasks = [
        serialize_task_instance(task)
        for task in list_task_instances(session, dag_id, base_run_id)
    ]
    compare_tasks = [
        serialize_task_instance(task)
        for task in list_task_instances(session, dag_id, compare_run_id)
    ]

    base_by_task_id = {task["task_id"]: task for task in base_tasks}
    compare_by_task_id = {task["task_id"]: task for task in compare_tasks}
    task_ids = sorted(set(base_by_task_id) | set(compare_by_task_id))

    task_comparison = []
    for task_id in task_ids:
        base_task = base_by_task_id.get(task_id)
        compare_task = compare_by_task_id.get(task_id)
        base_duration = base_task["duration_seconds"] if base_task else None
        compare_duration = compare_task["duration_seconds"] if compare_task else None

        task_comparison.append(
            {
                "task_id": task_id,
                "base": base_task,
                "compare": compare_task,
                "duration_delta_seconds": (
                    compare_duration - base_duration
                    if base_duration is not None and compare_duration is not None
                    else None
                ),
                "duration_delta_percent": percent_delta(base_duration, compare_duration),
                "retry_delta": (
                    max((compare_task["try_number"] or 1) - 1, 0)
                    - max((base_task["try_number"] or 1) - 1, 0)
                    if base_task and compare_task
                    else None
                ),
            }
        )

    base_duration = dag_run_duration_seconds(base_run)
    compare_duration = dag_run_duration_seconds(compare_run)

    return {
        "dag_id": dag_id,
        "base_run": serialize_run(base_run),
        "compare_run": serialize_run(compare_run),
        "summary": {
            "total_duration": {
                "base_seconds": base_duration,
                "compare_seconds": compare_duration,
                "delta_seconds": (
                    compare_duration - base_duration
                    if base_duration is not None and compare_duration is not None
                    else None
                ),
                "delta_percent": percent_delta(base_duration, compare_duration),
            },
            "succeeded_tasks": {
                "base": count_by_state(base_tasks, "success"),
                "compare": count_by_state(compare_tasks, "success"),
            },
            "failed_tasks": {
                "base": count_by_state(base_tasks, "failed"),
                "compare": count_by_state(compare_tasks, "failed"),
            },
            "total_retries": {
                "base": retry_total(base_tasks),
                "compare": retry_total(compare_tasks),
            },
        },
        "tasks": task_comparison,
    }


app_with_metadata = {
    "app": app,
    "url_prefix": "/runlens/api",
    "name": "RunLens API",
}

react_app_with_metadata = {
    "name": "RunLens",
    "bundle_url": "/runlens/api/static/runlens.js",
    "destination": "dag",
    "url_route": "runlens",
}


class RunLensBackendPlugin(AirflowPlugin):
    name = "run_lens_backend"
    fastapi_apps = [app_with_metadata]
    react_apps = [react_app_with_metadata]
