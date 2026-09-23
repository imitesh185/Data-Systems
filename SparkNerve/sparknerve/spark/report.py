"""Report stage: summarise one pipeline run from the audit table, push
pipeline-level metrics, and fail when any table is unhealthy (the report is the
DAG's leaf task, so its state is the DAG run's state)."""

from __future__ import annotations

import json
from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sparknerve.audit import FAILED, NO_DATA, SUCCEEDED, StageRun
from sparknerve.metadata import Pipeline
from sparknerve.observability import pipeline_registry, push
from sparknerve.planner import EXTRACT, REPORT, VALIDATE
from sparknerve.settings import Settings
from sparknerve.spark.delta_io import finish, is_delta


def table_outcomes(pipeline: Pipeline, rows: list[dict]) -> dict[str, str]:
    """Latest status per (table, stage) -> succeeded | no_data | failed | not_run."""
    latest: dict[tuple[str, str], str] = {}
    for row in sorted(rows, key=lambda r: r["finished_at"]):
        latest[(row["table_name"], row["stage"])] = row["status"]
    outcomes = {}
    for table in pipeline.table_names:
        statuses = [latest.get((table, stage)) for stage in (EXTRACT, VALIDATE)]
        if FAILED in statuses:
            outcomes[table] = "failed"
        elif None in statuses:
            outcomes[table] = "not_run"
        elif all(s == NO_DATA for s in statuses):
            outcomes[table] = "no_data"
        else:
            outcomes[table] = "succeeded"
    return outcomes


def run_report(spark: SparkSession, pipeline: Pipeline, run_id: str, settings: Settings) -> StageRun:
    audit = settings.lake.audit
    rows = []
    if is_delta(spark, audit):
        rows = [r.asDict() for r in spark.read.format("delta").load(audit).where(F.col("run_id") == run_id).collect()]
    outcomes = table_outcomes(pipeline, [r for r in rows if r["stage"] != REPORT])
    ok = all(o in ("succeeded", "no_data") for o in outcomes.values())
    run = StageRun(run_id, pipeline.name, "_all", REPORT, note=json.dumps(outcomes))
    unhealthy = ", ".join(f"{t}={o}" for t, o in outcomes.items() if o not in ("succeeded", "no_data"))
    finish(spark, settings, run, SUCCEEDED if ok else FAILED, None if ok else f"Tables not healthy: {unhealthy}")
    registry = pipeline_registry(pipeline.name, ok, Counter(outcomes.values()), run.finished_at.timestamp())
    push(registry, settings.pushgateway, {"pipeline": pipeline.name, "table": "_all", "stage": REPORT})
    return run
