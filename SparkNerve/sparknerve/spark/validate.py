"""Validate stage: bronze -> data quality engine -> quarantine + silver.

Bronze is read with Structured Streaming (trigger availableNow) and a
checkpoint, so each run processes exactly the bronze commits that arrived since
the last successful run, then stops. Per micro-batch (foreachBatch):

    rules      compiled JSON rules (Spark SQL); foreign keys are left joins on
               the distinct keys of the referenced silver table
    quarantine rows failing an error rule -> append, txnAppId/txnVersion = batch
               id, so a replayed batch is not quarantined twice
    gate       circuit breaker: too many invalid rows -> raise; silver untouched
               and the checkpoint stays uncommitted, so the batch is retried
    silver     latest valid version per key -> MERGE pruned to the partitions in
               the batch; update only if the source watermark is newer, so a
               replayed batch changes nothing; new columns via autoMerge

A crash anywhere before Spark commits the batch replays it with the same batch
id and the same data: quarantine skips, MERGE is a no-op, nothing duplicates.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from sparknerve.audit import FAILED, NO_DATA, SUCCEEDED, StageRun, utcnow
from sparknerve.evolution import SILVER_COLUMNS, TECHNICAL_COLUMNS, diff_schema
from sparknerve.metadata import Pipeline, TableSpec
from sparknerve.planner import VALIDATE
from sparknerve.quality import check_gate
from sparknerve.rules import CompiledRule, compile_rules, failure_columns, sql_literal
from sparknerve.settings import LakeLayout, Settings, app_id
from sparknerve.spark.delta_io import finish, is_delta, last_commit, schema_of, version_of

MAX_PRUNING_VALUES = 1000
log = logging.getLogger("sparknerve")


def batch_ids(spark: SparkSession, directory: str) -> set[int]:
    """Batch ids in a Structured Streaming checkpoint's offsets/ or commits/ log."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(directory)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return set()
    return {int(s.getPath().getName()) for s in fs.listStatus(path) if s.getPath().getName().isdigit()}


def pending_batch(spark: SparkSession, checkpoint: str) -> int | None:
    offsets, commits = batch_ids(spark, f"{checkpoint}/offsets"), batch_ids(spark, f"{checkpoint}/commits")
    return max(offsets) if offsets and max(offsets) not in commits else None


def reference_keys(spark: SparkSession, lake: LakeLayout, pipeline: str, rule: CompiledRule, local_type) -> DataFrame:
    lk = rule.lookup
    path = lake.silver(pipeline, lk.table)
    if is_delta(spark, path):
        keys = spark.read.format("delta").load(path).select(F.col(lk.column).alias(lk.key_alias)).distinct()
    else:
        keys = spark.createDataFrame([], StructType([StructField(lk.key_alias, local_type)]))
    return keys.withColumn(lk.flag_alias, F.lit(True))


def rule_names_array(compiled: list[CompiledRule], flags: dict[str, str], severity: str):
    names = [F.when(F.col(flags[c.name]), F.lit(c.name)) for c in compiled if c.severity == severity]
    if not names:
        return F.array().cast("array<string>")
    return F.filter(F.array(*names), lambda name: name.isNotNull())


def apply_rules(spark: SparkSession, frame: DataFrame, compiled: list[CompiledRule], lake: LakeLayout,
                pipeline: str) -> tuple[DataFrame, dict[str, str]]:
    """Adds one boolean column per rule plus _dq_errors / _dq_warnings (rule names)."""
    flags = failure_columns(compiled)
    for rule in compiled:
        if rule.lookup:
            lk = rule.lookup
            keys = reference_keys(spark, lake, pipeline, rule, frame.schema[lk.local_column].dataType)
            frame = frame.join(keys, frame[lk.local_column] == keys[lk.key_alias], "left").drop(lk.key_alias)
    for rule in compiled:
        frame = frame.withColumn(flags[rule.name], F.expr(rule.failed_sql))
    frame = (frame.withColumn("_dq_errors", rule_names_array(compiled, flags, "error"))
             .withColumn("_dq_warnings", rule_names_array(compiled, flags, "warn")))
    return frame.drop(*[r.lookup.flag_alias for r in compiled if r.lookup]), flags


def latest_per_key(frame: DataFrame, spec: TableSpec) -> DataFrame:
    newest = Window.partitionBy(*spec.primary_key).orderBy(F.col(spec.watermark_column).desc(),
                                                           F.col("_extract_seq").desc())
    return frame.withColumn("__rn", F.row_number().over(newest)).where(F.col("__rn") == 1).drop("__rn")


def merge_silver(spark: SparkSession, rows: DataFrame, spec: TableSpec, path: str, run_id: str, run: StageRun) -> None:
    part = spec.partition_by
    values = [r[0] for r in rows.select(part).distinct().collect()] if part else []
    run.partitions_touched = sorted(str(v) for v in values)
    metadata = json.dumps({"sparknerve.run_id": run_id, "batch_id": run.batch_id})
    before = version_of(spark, path)
    if before is None:
        writer = rows.write.format("delta").mode("append").option("userMetadata", metadata)
        (writer.partitionBy(part) if part else writer).save(path)
        run.rows_written = int(last_commit(spark, path)["metrics"].get("numOutputRows", 0))
        run.delta_version = version_of(spark, path)
        return

    incoming = {f.name: f.dataType.simpleString() for f in rows.schema.fields}
    run.schema_changes = [c.describe() for c in diff_schema(schema_of(spark, path), incoming) if c.kind == "added"]
    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec.primary_key)
    if part and values and len(values) <= MAX_PRUNING_VALUES:
        # Partition pruning: MERGE scans and rewrites only the partitions present in the batch.
        condition += f" AND t.`{part}` IN ({', '.join(sql_literal(v, 'spark') for v in sorted(values))})"
    elif part:
        run.partitions_touched = []
    guard = f"s.`{spec.watermark_column}` > t.`{spec.watermark_column}`"
    spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", metadata)
    try:
        (DeltaTable.forPath(spark, path).alias("t").merge(rows.alias("s"), condition)
         .whenMatchedUpdateAll(condition=guard)
         .whenNotMatchedInsertAll()
         .execute())
    finally:
        spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
    commit = last_commit(spark, path)
    if commit["version"] != before:
        m = commit["metrics"]
        run.rows_written = int(m.get("numTargetRowsInserted", 0)) + int(m.get("numTargetRowsUpdated", 0))
        run.files_rewritten = int(m.get("numTargetFilesRemoved", 0))
        run.note = (f"MERGE +{m.get('numTargetRowsInserted', 0)} ~{m.get('numTargetRowsUpdated', 0)}, "
                    f"{run.files_rewritten} file(s) rewritten in {len(run.partitions_touched) or 'all'} partition(s)")
    run.delta_version = commit["version"]


def process_batch(spark: SparkSession, batch: DataFrame, run: StageRun, spec: TableSpec, compiled: list[CompiledRule],
                  settings: Settings, pipeline: str, threshold: float | None) -> None:
    lake, t, batch_id = settings.lake, spec.name, run.batch_id
    run.rows_read = batch.count()
    if run.rows_read == 0:
        run.note = "empty micro-batch"
        return
    checked, flags = apply_rules(spark, batch, compiled, lake, pipeline)
    checked = checked.persist()
    try:
        has_errors = F.size("_dq_errors") > 0
        totals = checked.agg(
            *[F.sum(F.col(flags[c.name]).cast("long")).alias(f"f{i}") for i, c in enumerate(compiled)],
            F.sum(has_errors.cast("long")).alias("quarantined"),
            F.sum(((~has_errors) & (F.size("_dq_warnings") > 0)).cast("long")).alias("warned"),
        ).first()
        run.rule_failures = {c.name: int(totals[f"f{i}"] or 0) for i, c in enumerate(compiled) if totals[f"f{i}"]}
        run.rows_quarantined = int(totals["quarantined"] or 0)
        run.rows_warned = int(totals["warned"] or 0)
        checked = checked.drop(*flags.values())
        business = [c for c in batch.columns if c not in TECHNICAL_COLUMNS]

        quarantine = lake.quarantine(pipeline, t)
        if run.rows_quarantined:
            before = version_of(spark, quarantine)
            (checked.where(has_errors)
             .select(*business, "_run_id", "_extract_seq", "_ingested_at", "_dq_errors", "_dq_warnings",
                     F.lit(run.run_id).alias("_validate_run_id"), F.lit(batch_id).cast("bigint").alias("_batch_id"),
                     F.current_timestamp().alias("_quarantined_at"))
             .write.format("delta").mode("append")
             .option("mergeSchema", "true")
             .option("txnAppId", app_id(pipeline, t, "quarantine"))
             .option("txnVersion", str(batch_id))
             .option("userMetadata", json.dumps({"sparknerve.run_id": run.run_id, "batch_id": batch_id}))
             .save(quarantine))
            run.quarantine_version = version_of(spark, quarantine)
            if run.quarantine_version == before:
                run.note = f"quarantine already holds batch {batch_id}: append skipped (idempotent txn)"
        else:
            run.quarantine_version = version_of(spark, quarantine)

        check_gate(spec, run.rows_read, run.rows_quarantined, threshold)

        valid = latest_per_key(checked.where(~has_errors), spec).select(*business, *SILVER_COLUMNS)
        silver = lake.silver(pipeline, t)
        if run.rows_read > run.rows_quarantined:
            merge_silver(spark, valid, spec, silver, run.run_id, run)
        else:
            run.delta_version = version_of(spark, silver)
    finally:
        checked.unpersist()


def run_validate(spark: SparkSession, pipeline: Pipeline, spec: TableSpec, run_id: str, settings: Settings,
                 as_of: datetime | None = None, threshold: float | None = None) -> list[StageRun]:
    p, t = pipeline.name, spec.name
    bronze = settings.lake.bronze(p, t)
    checkpoint = settings.lake.checkpoint(p, t, VALIDATE)
    if not is_delta(spark, bronze):
        run = StageRun(run_id, p, t, VALIDATE, note="bronze table does not exist yet")
        return [finish(spark, settings, run, NO_DATA)]

    compiled = compile_rules(spec.rules, "spark", as_of or utcnow())
    replay = pending_batch(spark, checkpoint)
    runs: list[StageRun] = []
    errors: list[BaseException] = []

    def process(batch: DataFrame, batch_id: int) -> None:
        session = batch.sparkSession
        run = StageRun(run_id, p, t, VALIDATE, batch_id=int(batch_id), recovered=batch_id == replay)
        runs.append(run)
        frame = batch.persist()
        try:
            process_batch(session, frame, run, spec, compiled, settings, p, threshold)
        except Exception as exc:
            errors.append(exc)
            finish(session, settings, run, FAILED, exc, spec.severities,
                   {"silver": version_of(session, settings.lake.silver(p, t)), "quarantine": run.quarantine_version})
            raise
        finally:
            frame.unpersist()
        finish(session, settings, run, SUCCEEDED, None, spec.severities,
               {"silver": run.delta_version, "quarantine": run.quarantine_version})

    query = (
        spark.readStream.format("delta").load(bronze)
        .writeStream.queryName(f"sparknerve.{p}.{t}.validate")
        .foreachBatch(process)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
    except Exception as exc:  # noqa: BLE001 - the batch failure is already audited
        if not errors:
            run = StageRun(run_id, p, t, VALIDATE)
            runs.append(finish(spark, settings, run, FAILED, exc))
    if not runs:
        run = StageRun(run_id, p, t, VALIDATE, note="no new bronze commits")
        run.delta_version = version_of(spark, settings.lake.silver(p, t))
        runs.append(finish(spark, settings, run, NO_DATA))
    return runs
