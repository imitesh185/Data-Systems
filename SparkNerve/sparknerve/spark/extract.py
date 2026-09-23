"""Extract stage: SQL Server -> bronze (raw, append-only, schema-evolving).

    1. window   next_window(): replay a planned-but-uncommitted window, or plan
                (committed watermark, MAX(watermark column)] fresh
    2. schema   diff the JDBC schema against the last committed source schema
                and apply the table's schema_evolution policy (before any write)
    3. plan     persist (seq, high) to the watermark table            ~ offsets log
    4. read     JDBC query with the window pushed down to SQL Server; parallel
                reads split on the primary key when read_partitions > 1
    5. append   bronze append with txnAppId/txnVersion = seq (Delta skips it if
                this seq already landed), mergeSchema for new columns
    6. commit   mark seq committed (+ source schema)                   ~ commit log
"""

from __future__ import annotations

import json
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sparknerve.audit import FAILED, NO_DATA, SUCCEEDED, StageRun
from sparknerve.checkpoint import ExtractWindow, next_window
from sparknerve.evolution import business_columns, diff_schema, enforce_policy
from sparknerve.metadata import Pipeline, TableSpec
from sparknerve.planner import EXTRACT
from sparknerve.settings import JdbcConnection, Settings, app_id
from sparknerve.spark.delta_io import finish, last_commit, read_watermark, schema_of, version_of, write_watermark

WATERMARK_TEXT = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,7})?$")


def tsql_name(name: str) -> str:
    return ".".join("[" + part.replace("]", "]]") + "]" for part in name.split("."))


def tsql_timestamp(watermark: str) -> str:
    # Watermarks come from SQL Server itself (CONVERT style 121); still refuse
    # anything else before it is embedded in SQL.
    if not WATERMARK_TEXT.match(watermark):
        raise ValueError(f"Refusing malformed watermark {watermark!r}")
    return f"CAST('{watermark}' AS DATETIME2(7))"


def window_predicate(spec: TableSpec, window: ExtractWindow) -> str:
    column = tsql_name(spec.watermark_column)
    predicate = f"{column} <= {tsql_timestamp(window.high)}"
    if window.low is not None:
        predicate = f"{column} > {tsql_timestamp(window.low)} AND {predicate}"
    return predicate


def jdbc(spark: SparkSession, conn: JdbcConnection, fetch_size: int):
    return (
        spark.read.format("jdbc")
        .option("url", conn.url)
        .option("user", conn.user)
        .option("password", conn.password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", str(fetch_size))
    )


def source_high(spark: SparkSession, conn: JdbcConnection, spec: TableSpec) -> str | None:
    """MAX(watermark) as exact text (style 121): no precision lost to a driver or float."""
    query = (f"SELECT CONVERT(VARCHAR(27), MAX({tsql_name(spec.watermark_column)}), 121) AS hwm "
             f"FROM {tsql_name(spec.source_table)}")
    row = jdbc(spark, conn, 1).option("query", query).load().first()
    return row["hwm"] if row else None


def read_window(spark: SparkSession, conn: JdbcConnection, spec: TableSpec, window: ExtractWindow,
                fetch_size: int) -> DataFrame:
    where = window_predicate(spec, window)
    table = tsql_name(spec.source_table)
    reader = jdbc(spark, conn, fetch_size).option("dbtable", f"(SELECT * FROM {table} WHERE {where}) AS src")
    if spec.read_partitions > 1:
        key = spec.primary_key[0]
        bounds = (jdbc(spark, conn, 1)
                  .option("query", f"SELECT MIN({tsql_name(key)}) AS lo, MAX({tsql_name(key)}) AS hi "
                                   f"FROM {table} WHERE {where}")
                  .load().first())
        lo, hi = (bounds["lo"], bounds["hi"]) if bounds else (None, None)
        if isinstance(lo, int) and isinstance(hi, int) and hi > lo:
            reader = (reader.option("partitionColumn", key).option("lowerBound", str(lo))
                      .option("upperBound", str(hi + 1)).option("numPartitions", str(spec.read_partitions)))
    return reader.load()


def run_extract(spark: SparkSession, pipeline: Pipeline, spec: TableSpec, run_id: str, settings: Settings,
                conn: JdbcConnection | None = None) -> StageRun:
    p, t = pipeline.name, spec.name
    run = StageRun(run_id, p, t, EXTRACT)
    bronze = settings.lake.bronze(p, t)
    control = settings.lake.watermark(p, t)
    layers = {}
    try:
        conn = conn or JdbcConnection.from_env(pipeline.connection)
        state = read_watermark(spark, control)
        window = next_window(state, lambda: source_high(spark, conn, spec))
        if window is None:
            run.watermark_from = run.watermark_to = state.committed_watermark
            run.delta_version = layers["bronze"] = version_of(spark, bronze)
            run.note = "no source rows newer than the watermark"
            return finish(spark, settings, run, NO_DATA, layers=layers)
        run.batch_id, run.recovered = window.seq, window.recovered
        run.watermark_from, run.watermark_to = window.low, window.high

        source = read_window(spark, conn, spec, window, pipeline.fetch_size)
        incoming = {f.name: f.dataType.simpleString() for f in source.schema.fields}
        previous = state.last_source_schema or business_columns(schema_of(spark, bronze) or {}) or None
        changes = enforce_policy(spec, diff_schema(previous, incoming))
        run.schema_changes = [c.describe() for c in changes]

        if not window.recovered:
            state = state.plan(window.seq, window.high)
            write_watermark(spark, control, p, t, state)

        before = version_of(spark, bronze)
        (
            source.withColumn("_run_id", F.lit(run_id))
            .withColumn("_extract_seq", F.lit(window.seq).cast("bigint"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.to_date(F.col("_ingested_at")))
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("txnAppId", app_id(p, t, "bronze"))
            .option("txnVersion", str(window.seq))
            .option("userMetadata", json.dumps({"sparknerve.run_id": run_id, "seq": window.seq}))
            .partitionBy("_ingest_date")
            .save(bronze)
        )
        after = version_of(spark, bronze)
        run.delta_version = layers["bronze"] = after
        if after is not None and after != before:
            run.rows_read = run.rows_written = int(last_commit(spark, bronze)["metrics"].get("numOutputRows", 0))
        else:
            run.note = f"bronze already holds seq {window.seq}: append skipped (idempotent txn)"

        write_watermark(spark, control, p, t, state.commit(incoming))
        return finish(spark, settings, run, SUCCEEDED, layers=layers)
    except Exception as exc:  # noqa: BLE001 - every failure is audited, then the task fails
        return finish(spark, settings, run, FAILED, exc, layers=layers)
