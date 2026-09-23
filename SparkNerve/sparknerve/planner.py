"""Metadata -> task graph. The Airflow DAG factory and the live demo both run
this plan, so the demo executes exactly the DAG that Airflow would.

For every table: `<table>.extract` (source -> bronze) and `<table>.validate`
(bronze -> DQ -> silver + quarantine). A validate task also waits for the
validate task of every table its foreign-key rules reference, so orders are
checked against customers that have already landed in silver. Extracts have no
upstream and run in parallel. `run_report` runs last, whatever happened, and
fails the run if any stage failed.
"""

from __future__ import annotations

from dataclasses import dataclass

from sparknerve.metadata import Pipeline

EXTRACT = "extract"
VALIDATE = "validate"
REPORT = "report"
REPORT_TASK_ID = "run_report"


@dataclass(frozen=True)
class Task:
    task_id: str
    stage: str
    table: str | None
    upstream: tuple[str, ...]


@dataclass(frozen=True)
class Plan:
    pipeline: str
    tasks: tuple[Task, ...]

    def task(self, task_id: str) -> Task:
        for task in self.tasks:
            if task.task_id == task_id:
                return task
        raise KeyError(task_id)

    def downstream(self, task_id: str) -> list[str]:
        return [t.task_id for t in self.tasks if task_id in t.upstream]

    def topological(self) -> list[Task]:
        """Kahn's algorithm; ties keep metadata order, so runs are deterministic."""
        done: set[str] = set()
        ordered: list[Task] = []
        pending = list(self.tasks)
        while pending:
            ready = [t for t in pending if set(t.upstream) <= done]
            if not ready:
                raise ValueError(f"Plan for {self.pipeline} has a cycle among {[t.task_id for t in pending]}")
            for task in ready:
                ordered.append(task)
                done.add(task.task_id)
            pending = [t for t in pending if t.task_id not in done]
        return ordered

    def edges(self) -> list[tuple[str, str]]:
        return [(up, t.task_id) for t in self.tasks for up in t.upstream]


def task_id(table: str, stage: str) -> str:
    return f"{table}.{stage}"


def build_plan(pipeline: Pipeline) -> Plan:
    tasks = [Task(task_id(t.name, EXTRACT), EXTRACT, t.name, ()) for t in pipeline.tables]
    for table in pipeline.tables:
        upstream = (task_id(table.name, EXTRACT),) + tuple(task_id(dep, VALIDATE) for dep in table.depends_on)
        tasks.append(Task(task_id(table.name, VALIDATE), VALIDATE, table.name, upstream))
    tasks.append(Task(REPORT_TASK_ID, REPORT, None, tuple(task_id(t.name, VALIDATE) for t in pipeline.tables)))
    plan = Plan(pipeline.name, tuple(tasks))
    plan.topological()
    return plan


def cli_command(pipeline: str, task: Task, run_id: str) -> str:
    """The shell command that executes one task (what the Airflow operator runs)."""
    if task.stage == REPORT:
        return f"python -m sparknerve report --pipeline {pipeline} --run-id '{run_id}'"
    return (f"python -m sparknerve run --pipeline {pipeline} --table {task.table} --stage {task.stage} "
            f"--run-id '{run_id}'")
