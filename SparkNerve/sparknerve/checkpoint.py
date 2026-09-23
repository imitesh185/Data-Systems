"""Checkpoint protocol of the extract stage (source -> bronze).

The watermark state of a table is a one-row Delta table that works like
Structured Streaming's offset and commit logs:

1. plan    -> record (seq, high watermark) BEFORE reading the source
2. write   -> append the window (low, high] to bronze as an idempotent Delta
              write: txnAppId = sparknerve.<pipeline>.<table>.bronze,
              txnVersion = seq
3. commit  -> mark seq committed

A crash after 1 or 2 leaves a planned-but-uncommitted seq. The next run replays
exactly the same window with the same seq, so Delta skips the append if it had
already landed. Nothing is lost (the window is fixed before reading) and
nothing is duplicated (the write is idempotent). Recomputing the high watermark
on retry instead would silently skip the rows between the two highs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone

WATERMARK_SCHEMA: tuple[tuple[str, str], ...] = (
    ("pipeline", "string"),
    ("table_name", "string"),
    ("committed_seq", "bigint"),
    ("committed_watermark", "string"),
    ("planned_seq", "bigint"),
    ("planned_watermark", "string"),
    ("source_schema", "string"),
    ("updated_at", "timestamp"),
)


@dataclass(frozen=True)
class WatermarkState:
    committed_seq: int = 0
    committed_watermark: str | None = None
    planned_seq: int = 0
    planned_watermark: str | None = None
    # Source schema (JSON {column: type}) as of the last committed extract. Schema
    # drift is diffed against it, so every change is reported exactly once.
    source_schema: str | None = None

    @property
    def pending(self) -> bool:
        return self.planned_seq > self.committed_seq

    @property
    def last_source_schema(self) -> dict[str, str] | None:
        return json.loads(self.source_schema) if self.source_schema else None

    def plan(self, seq: int, watermark: str) -> WatermarkState:
        return replace(self, planned_seq=seq, planned_watermark=watermark)

    def commit(self, source_schema: dict[str, str] | None = None) -> WatermarkState:
        schema = json.dumps(source_schema) if source_schema is not None else self.source_schema
        return replace(self, committed_seq=self.planned_seq, committed_watermark=self.planned_watermark,
                       source_schema=schema)

    def to_record(self, pipeline: str, table: str) -> dict:
        return {
            "pipeline": pipeline,
            "table_name": table,
            "committed_seq": self.committed_seq,
            "committed_watermark": self.committed_watermark,
            "planned_seq": self.planned_seq,
            "planned_watermark": self.planned_watermark,
            "source_schema": self.source_schema,
            "updated_at": datetime.now(timezone.utc),
        }

    @classmethod
    def from_record(cls, row: dict) -> WatermarkState:
        return cls(
            committed_seq=int(row["committed_seq"] or 0),
            committed_watermark=row["committed_watermark"],
            planned_seq=int(row["planned_seq"] or 0),
            planned_watermark=row["planned_watermark"],
            source_schema=row.get("source_schema"),
        )


@dataclass(frozen=True)
class ExtractWindow:
    seq: int
    low: str | None  # exclusive; None = from the beginning
    high: str        # inclusive
    recovered: bool


def next_window(state: WatermarkState, source_high: Callable[[], str | None]) -> ExtractWindow | None:
    """The window to extract now, or None when the source has nothing new.

    Watermarks are fixed-width text ('YYYY-MM-DD HH:MM:SS.ffffff', SQL Server
    CONVERT style 121), so text order is time order and no precision is lost to
    a float or driver round trip.
    """
    if state.pending:
        return ExtractWindow(state.planned_seq, state.committed_watermark, state.planned_watermark, recovered=True)
    high = source_high()
    if high is None or (state.committed_watermark is not None and high <= state.committed_watermark):
        return None
    return ExtractWindow(state.committed_seq + 1, state.committed_watermark, high, recovered=False)


def watermark_epoch(watermark: str | None) -> float | None:
    if not watermark:
        return None
    parsed = datetime.fromisoformat(watermark[:26])
    return parsed.replace(tzinfo=timezone.utc).timestamp()
