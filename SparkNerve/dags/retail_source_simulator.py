"""Keeps the SQL Server source alive in the Docker stack: every few minutes a
burst of realistic changes, with an occasional defective row for the DQ engine."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="retail_source_simulator",
    description="Simulated OLTP traffic on RetailDB (SQL Server)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sparknerve", "simulator"],
) as dag:
    BashOperator(task_id="tick", bash_command="python -m simulator tick --changes 40 --bad 1")
