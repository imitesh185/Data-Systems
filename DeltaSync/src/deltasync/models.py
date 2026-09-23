from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class Operation(str, Enum):
    SNAPSHOT = "r"
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"


@dataclass(frozen=True, slots=True)
class CdcEvent:
    event_id: str
    table: str
    operation: Operation
    key: Mapping[str, Any]
    before: Mapping[str, Any] | None
    after: Mapping[str, Any] | None
    topic: str
    partition: int
    offset: int
    timestamp_ms: int

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id cannot be empty")
        if not self.table:
            raise ValueError("table cannot be empty")
        if not self.key:
            raise ValueError("key cannot be empty")
        if self.partition < 0 or self.offset < 0:
            raise ValueError("partition and offset must be non-negative")
        if self.operation is Operation.DELETE and self.before is None:
            raise ValueError("delete events require a before image")
        if self.operation is not Operation.DELETE and self.after is None:
            raise ValueError("non-delete events require an after image")

    @property
    def position(self) -> tuple[str, int, int]:
        return self.topic, self.partition, self.offset

    def as_debezium_envelope(self) -> dict[str, Any]:
        return {
            "before": dict(self.before) if self.before is not None else None,
            "after": dict(self.after) if self.after is not None else None,
            "op": self.operation.value,
            "ts_ms": self.timestamp_ms,
            "source": {
                "table": self.table,
                "file": "mysql-bin.000001",
                "pos": self.offset,
            },
            "_metadata": {
                "event_id": self.event_id,
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
            },
        }

