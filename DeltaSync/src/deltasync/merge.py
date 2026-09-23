from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from deltasync.models import CdcEvent, Operation

Row = dict[str, Any]
TableState = dict[str, dict[tuple[Any, ...], Row]]


@dataclass(frozen=True, slots=True)
class MergeResult:
    tables: TableState
    processed_event_ids: frozenset[str]
    inserted: int
    updated: int
    deleted: int
    skipped: int

    @property
    def changed(self) -> int:
        return self.inserted + self.updated + self.deleted


def row_key(key: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(key[name] for name in sorted(key))


def apply_events(
    tables: TableState,
    events: Iterable[CdcEvent],
    processed_event_ids: Iterable[str] = (),
) -> MergeResult:
    next_tables: TableState = {
        name: {key: dict(row) for key, row in rows.items()}
        for name, rows in tables.items()
    }
    processed = set(processed_event_ids)
    inserted = updated = deleted = skipped = 0

    for event in events:
        if event.event_id in processed:
            skipped += 1
            continue

        table = next_tables.setdefault(event.table, {})
        key = row_key(event.key)
        if event.operation is Operation.DELETE:
            if table.pop(key, None) is not None:
                deleted += 1
        else:
            existed = key in table
            table[key] = dict(event.after or {})
            if existed:
                updated += 1
            else:
                inserted += 1
        processed.add(event.event_id)

    return MergeResult(
        tables=next_tables,
        processed_event_ids=frozenset(processed),
        inserted=inserted,
        updated=updated,
        deleted=deleted,
        skipped=skipped,
    )

