"""The audit record every stage writes, success or failure, to the Delta table
`<lake>/audit/stage_runs`. One schema for both engines; collections are stored
as JSON strings so the table stays trivially queryable from any SQL engine.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

SUCCEEDED = "SUCCEEDED"
NO_DATA = "NO_DATA"
FAILED = "FAILED"

AUDIT_SCHEMA: tuple[tuple[str, str], ...] = (
    ("run_id", "string"),
    ("pipeline", "string"),
    ("table_name", "string"),
    ("stage", "string"),
    ("status", "string"),
    ("batch_id", "bigint"),
    ("recovered", "boolean"),
    ("started_at", "timestamp"),
    ("finished_at", "timestamp"),
    ("duration_ms", "bigint"),
    ("rows_read", "bigint"),
    ("rows_written", "bigint"),
    ("rows_quarantined", "bigint"),
    ("rows_warned", "bigint"),
    ("watermark_from", "string"),
    ("watermark_to", "string"),
    ("delta_version", "bigint"),
    ("quarantine_version", "bigint"),
    ("files_rewritten", "bigint"),
    ("partitions_touched", "string"),
    ("schema_changes", "string"),
    ("rule_failures", "string"),
    ("note", "string"),
    ("error", "string"),
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def describe_error(error: BaseException | str | None, limit: int = 2000) -> str | None:
    """Audit text for an error. Domain errors (quality gate, schema drift) read as
    written; anything else keeps its type. JVM stack traces are truncated."""
    if error is None or isinstance(error, str):
        return error
    text = str(error) if getattr(error, "audit_verbatim", False) else f"{type(error).__name__}: {error}"
    return text if len(text) <= limit else text[:limit] + " ..."


@dataclass
class StageRun:
    run_id: str
    pipeline: str
    table: str
    stage: str
    started_at: datetime = field(default_factory=utcnow)
    status: str = "RUNNING"
    batch_id: int | None = None
    recovered: bool = False
    finished_at: datetime | None = None
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    rows_warned: int = 0
    watermark_from: str | None = None
    watermark_to: str | None = None
    delta_version: int | None = None
    quarantine_version: int | None = None
    files_rewritten: int = 0
    partitions_touched: list[str] = field(default_factory=list)
    schema_changes: list[str] = field(default_factory=list)
    rule_failures: dict[str, int] = field(default_factory=dict)
    note: str | None = None
    error: str | None = None

    def finish(self, status: str, error: str | None = None) -> StageRun:
        self.status = status
        self.error = error
        self.finished_at = utcnow()
        return self

    @property
    def succeeded(self) -> bool:
        return self.status in (SUCCEEDED, NO_DATA)

    @property
    def duration_ms(self) -> int:
        end = self.finished_at or utcnow()
        return max(0, int((end - self.started_at).total_seconds() * 1000))

    @property
    def quarantine_ratio(self) -> float:
        return self.rows_quarantined / self.rows_read if self.rows_read else 0.0

    def to_record(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "table_name": self.table,
            "stage": self.stage,
            "status": self.status,
            "batch_id": self.batch_id,
            "recovered": self.recovered,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
            "rows_warned": self.rows_warned,
            "watermark_from": self.watermark_from,
            "watermark_to": self.watermark_to,
            "delta_version": self.delta_version,
            "quarantine_version": self.quarantine_version,
            "files_rewritten": self.files_rewritten,
            "partitions_touched": json.dumps(self.partitions_touched),
            "schema_changes": json.dumps(self.schema_changes),
            "rule_failures": json.dumps(self.rule_failures, sort_keys=True),
            "note": self.note,
            "error": self.error,
        }

    def summary(self) -> str:
        parts = [f"{self.table}.{self.stage} {self.status}"]
        if self.status == NO_DATA:
            parts.append(self.note or "nothing new")
            return " | ".join(parts)
        if self.stage == "extract":
            parts.append(f"{self.rows_written} rows -> bronze v{self.delta_version}")
            parts.append(f"window ({self.watermark_from}, {self.watermark_to}]")
        elif self.stage == "validate":
            parts.append(f"{self.rows_read} read, {self.rows_written} merged, {self.rows_quarantined} quarantined")
            if self.partitions_touched:
                parts.append(f"{len(self.partitions_touched)} partition(s) touched")
        if self.schema_changes:
            parts.append("schema " + ", ".join(self.schema_changes))
        if self.recovered:
            parts.append("recovered from checkpoint")
        if self.note:
            parts.append(self.note)
        if self.error:
            parts.append(self.error)
        return " | ".join(parts)
