"""Delta helpers shared by the Spark stages: existence, schema, versions, the
watermark control table, the audit table and stage bookkeeping."""

from __future__ import annotations

import logging
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from sparknerve.audit import AUDIT_SCHEMA, StageRun, describe_error
from sparknerve.checkpoint import WATERMARK_SCHEMA, WatermarkState
from sparknerve.observability import push_stage
from sparknerve.settings import Settings

log = logging.getLogger("sparknerve")


def ddl(schema: tuple[tuple[str, str], ...]) -> str:
    return ", ".join(f"`{name}` {kind.upper()}" for name, kind in schema)


def is_delta(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def schema_of(spark: SparkSession, path: str) -> dict[str, str] | None:
    if not is_delta(spark, path):
        return None
    return {f.name: f.dataType.simpleString() for f in spark.read.format("delta").load(path).schema.fields}


def version_of(spark: SparkSession, path: str) -> int | None:
    if not is_delta(spark, path):
        return None
    return int(DeltaTable.forPath(spark, path).history(1).select("version").first()[0])


def last_commit(spark: SparkSession, path: str) -> dict:
    row = DeltaTable.forPath(spark, path).history(1).select("version", "operation", "operationMetrics").first()
    return {"version": int(row["version"]), "operation": row["operation"],
            "metrics": dict(row["operationMetrics"] or {})}


def read_watermark(spark: SparkSession, path: str) -> WatermarkState:
    if not is_delta(spark, path):
        return WatermarkState()
    rows = spark.read.format("delta").load(path).collect()
    return WatermarkState.from_record(rows[0].asDict()) if rows else WatermarkState()


def write_watermark(spark: SparkSession, path: str, pipeline: str, table: str, state: WatermarkState) -> None:
    """Overwrite the one-row control table (atomic: one Delta commit)."""
    frame = spark.createDataFrame([state.to_record(pipeline, table)], ddl(WATERMARK_SCHEMA))
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def append_audit(spark: SparkSession, path: str, run: StageRun, attempts: int = 4) -> None:
    """Blind appends never conflict in Delta, but parallel tasks can race to create
    the table; the loser retries and appends to the table the winner created."""
    frame = spark.createDataFrame([run.to_record()], ddl(AUDIT_SCHEMA))
    for attempt in range(1, attempts + 1):
        try:
            frame.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
            return
        except Exception:  # noqa: BLE001 - retried, then re-raised
            if attempt == attempts:
                raise
            time.sleep(0.5 * attempt)


def finish(spark: SparkSession, settings: Settings, run: StageRun, status: str, error=None,
           severities=None, layers=None) -> StageRun:
    """Close a stage: audit row (always), Prometheus push (best effort), log line."""
    run.finish(status, describe_error(error))
    append_audit(spark, settings.lake.audit, run)
    push_stage(run, settings.pushgateway, severities, layers)
    (log.error if error is not None else log.info)(run.summary())
    return run
