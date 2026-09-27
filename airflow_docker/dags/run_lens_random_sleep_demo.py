"""Demo DAG for generating varied task runtimes for RunLens comparisons."""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


def random_sleep(task_label: str, min_seconds: int, max_seconds: int, **context) -> None:
    """Sleep for a random duration and log enough context to inspect the run."""
    duration = random.randint(min_seconds, max_seconds)
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown"

    print(
        f"{task_label} sleeping for {duration}s "
        f"(range={min_seconds}-{max_seconds}s, run_id={run_id})"
    )
    time.sleep(duration)
    print(f"{task_label} finished after {duration}s")


default_args = {
    "owner": "run_lens",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="run_lens_random_sleep_demo",
    description="Random sleep tasks for exercising RunLens duration comparisons.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    tags=["runlens", "demo", "random-sleep"],
) as dag:
    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Extract customers",
            "min_seconds": 20,
            "max_seconds": 70,
        },
    )

    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Validate schema",
            "min_seconds": 8,
            "max_seconds": 30,
        },
    )

    transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Transform customers",
            "min_seconds": 45,
            "max_seconds": 180,
        },
    )

    enrich_segments = PythonOperator(
        task_id="enrich_segments",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Enrich segments",
            "min_seconds": 25,
            "max_seconds": 100,
        },
    )

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Load to warehouse",
            "min_seconds": 20,
            "max_seconds": 80,
        },
    )

    generate_metrics = PythonOperator(
        task_id="generate_metrics",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Generate metrics",
            "min_seconds": 10,
            "max_seconds": 45,
        },
    )

    send_notifications = PythonOperator(
        task_id="send_notifications",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Send notifications",
            "min_seconds": 8,
            "max_seconds": 35,
        },
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=random_sleep,
        op_kwargs={
            "task_label": "Cleanup",
            "min_seconds": 5,
            "max_seconds": 25,
        },
    )

    extract_customers >> validate_schema >> transform_customers
    transform_customers >> [enrich_segments, generate_metrics]
    enrich_segments >> load_to_warehouse
    [load_to_warehouse, generate_metrics] >> send_notifications >> cleanup
