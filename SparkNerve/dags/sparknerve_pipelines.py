"""Airflow DAG factory: one DAG per pipeline metadata file.

Nothing here is specific to a table. Adding a table, a rule or a foreign key is a
JSON change: the next DAG parse regenerates the tasks and their dependencies
from sparknerve.planner, the same plan the live demo executes.

    <table>.extract   SQL Server -> bronze      (no upstream: all extracts run in parallel)
    <table>.validate  bronze -> DQ -> silver    (after its extract and after every table it
                                                 references through a foreign_key rule)
    run_report        audit summary + metrics   (trigger_rule=all_done; fails the DAG run
                                                 when any table is unhealthy)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from sparknerve.metadata import Pipeline, load_pipelines
from sparknerve.planner import REPORT, build_plan, cli_command

RETRIES = int(os.getenv("SPARKNERVE_TASK_RETRIES", "1"))


def build_dag(pipeline: Pipeline) -> DAG:
    plan = build_plan(pipeline)
    dag = DAG(
        dag_id=f"sparknerve__{pipeline.name}",
        description=pipeline.description,
        schedule=pipeline.schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        # Stages keep checkpoints per table: two concurrent runs of one pipeline
        # would race on them, so runs of the same DAG are serialised.
        max_active_runs=1,
        default_args={
            "owner": pipeline.owner or "sparknerve",
            "retries": RETRIES,
            "retry_delay": timedelta(minutes=1),
            "execution_timeout": timedelta(minutes=30),
        },
        tags=["sparknerve", "metadata-driven", pipeline.name],
        doc_md=f"Generated from `{os.path.basename(pipeline.source_path or '')}`.\n\n{pipeline.description}",
    )
    with dag:
        groups = {t.name: TaskGroup(group_id=t.name, tooltip=t.description or t.source_table) for t in pipeline.tables}
        tasks = {}
        for task in plan.tasks:
            command = cli_command(pipeline.name, task, "{{ run_id }}")
            if task.stage == REPORT:
                tasks[task.task_id] = BashOperator(task_id=task.task_id, bash_command=command,
                                                   trigger_rule=TriggerRule.ALL_DONE, retries=0)
            else:
                tasks[task.task_id] = BashOperator(task_id=task.stage, task_group=groups[task.table],
                                                   bash_command=command)
        for upstream, downstream in plan.edges():
            tasks[upstream] >> tasks[downstream]
    return dag


for _pipeline in load_pipelines().values():
    globals()[f"sparknerve__{_pipeline.name}"] = build_dag(_pipeline)
