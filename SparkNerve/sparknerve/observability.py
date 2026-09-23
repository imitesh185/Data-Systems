"""Prometheus metrics. Stages are short-lived batch jobs, so each stage builds a
fresh registry and pushes it to the Pushgateway (grouping key: pipeline, table,
stage), which Prometheus scrapes. `pushadd` replaces only the metric names that
are pushed, so a failed run keeps the last *success* timestamp that the
staleness alert relies on.

Observability must never break the data path: push failures are logged, not
raised.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Gauge, generate_latest, pushadd_to_gateway

from sparknerve.audit import StageRun
from sparknerve.checkpoint import watermark_epoch

JOB = "sparknerve"
STAGE_LABELS = ["pipeline", "table", "stage"]
log = logging.getLogger(__name__)


def stage_registry(
    run: StageRun,
    severities: Mapping[str, str] | None = None,
    layer_versions: Mapping[str, int | None] | None = None,
) -> CollectorRegistry:
    registry = CollectorRegistry()
    base = {"pipeline": run.pipeline, "table": run.table, "stage": run.stage}

    def gauge(name: str, doc: str, extra: list[str] | None = None) -> Gauge:
        return Gauge(name, doc, STAGE_LABELS + (extra or []), registry=registry)

    rows = gauge("sparknerve_stage_rows", "Rows handled by the last run of the stage", ["kind"])
    for kind, value in (("read", run.rows_read), ("written", run.rows_written),
                        ("quarantined", run.rows_quarantined), ("warned", run.rows_warned)):
        rows.labels(**base, kind=kind).set(value)

    gauge("sparknerve_stage_duration_seconds", "Duration of the last run").labels(**base).set(run.duration_ms / 1000)
    gauge("sparknerve_stage_success", "1 if the last run succeeded, 0 if it failed").labels(**base).set(
        1 if run.succeeded else 0)
    finished = (run.finished_at or run.started_at).timestamp()
    gauge("sparknerve_stage_last_run_timestamp_seconds", "End time of the last run").labels(**base).set(finished)
    if run.succeeded:
        gauge("sparknerve_stage_last_success_timestamp_seconds", "End time of the last successful run").labels(
            **base).set(finished)
    gauge("sparknerve_schema_changes", "Schema changes applied in the last run").labels(**base).set(
        len(run.schema_changes))

    if run.stage == "extract":
        epoch = watermark_epoch(run.watermark_to)
        if epoch is not None:
            gauge("sparknerve_source_watermark_timestamp_seconds",
                  "Watermark (modified time) of the newest source row loaded").labels(**base).set(epoch)

    if run.stage == "validate":
        failures = gauge("sparknerve_dq_rule_failures", "Rows failing each rule in the last batch",
                         ["rule", "severity"])
        for rule, severity in (severities or {}).items():
            failures.labels(**base, rule=rule, severity=severity).set(run.rule_failures.get(rule, 0))
        gauge("sparknerve_quarantine_ratio", "Share of rows quarantined in the last batch").labels(**base).set(
            run.quarantine_ratio)
        gauge("sparknerve_merge_files_rewritten", "Silver files rewritten by the last MERGE").labels(**base).set(
            run.files_rewritten)
        gauge("sparknerve_partitions_touched", "Silver partitions the last MERGE was pruned to").labels(**base).set(
            len(run.partitions_touched))

    versions = gauge("sparknerve_delta_table_version", "Delta table version after the last run", ["layer"])
    for layer, version in (layer_versions or {}).items():
        if version is not None:
            versions.labels(**base, layer=layer).set(version)
    return registry


def pipeline_registry(pipeline: str, succeeded: bool, table_status: Mapping[str, int],
                      finished_at: float) -> CollectorRegistry:
    registry = CollectorRegistry()
    Gauge("sparknerve_pipeline_run_success", "1 if every stage of the last run succeeded", ["pipeline"],
          registry=registry).labels(pipeline=pipeline).set(1 if succeeded else 0)
    Gauge("sparknerve_pipeline_last_run_timestamp_seconds", "End time of the last pipeline run", ["pipeline"],
          registry=registry).labels(pipeline=pipeline).set(finished_at)
    tables = Gauge("sparknerve_pipeline_tables", "Tables by outcome in the last run", ["pipeline", "status"],
                   registry=registry)
    for status in ("succeeded", "no_data", "failed", "not_run"):
        tables.labels(pipeline=pipeline, status=status).set(table_status.get(status, 0))
    return registry


def exposition(registry: CollectorRegistry) -> str:
    """The text Prometheus would scrape (shown in the live demo)."""
    return generate_latest(registry).decode("utf-8")


def push(registry: CollectorRegistry, gateway: str | None, grouping_key: Mapping[str, str]) -> bool:
    if not gateway:
        return False
    try:
        pushadd_to_gateway(gateway, job=JOB, registry=registry, grouping_key=dict(grouping_key), timeout=10)
        return True
    except Exception as exc:  # noqa: BLE001 - metrics must not fail the pipeline
        log.warning("Could not push metrics to %s: %s", gateway, exc)
        return False


def push_stage(run: StageRun, gateway: str | None, severities=None, layer_versions=None) -> CollectorRegistry:
    registry = stage_registry(run, severities, layer_versions)
    push(registry, gateway, {"pipeline": run.pipeline, "table": run.table, "stage": run.stage})
    return registry
