"""Schema evolution: diff the incoming schema against the lake table and apply
the table's policy. Types are compared as Spark SQL type names ("int",
"decimal(12,2)", ...), which both engines produce.

* added column    -> add_new_columns: the column is added to the lake table
                     (Spark: mergeSchema / autoMerge; delta-rs: schema_mode=merge).
                     fail: the stage fails before anything is written.
* dropped column  -> kept in the lake table and written as NULL from then on
                     (history stays readable); fails under the fail policy.
* changed type    -> always fails: Delta cannot change a column type in place,
                     so a silent cast would corrupt data. Fix the source or
                     rebuild the table deliberately.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from sparknerve.metadata import TableSpec

BRONZE_COLUMNS = ("_run_id", "_extract_seq", "_ingested_at", "_ingest_date")
SILVER_COLUMNS = ("_run_id", "_extract_seq", "_ingested_at", "_dq_warnings")
QUARANTINE_COLUMNS = ("_run_id", "_extract_seq", "_ingested_at", "_dq_errors", "_dq_warnings",
                      "_validate_run_id", "_batch_id", "_quarantined_at")
TECHNICAL_COLUMNS = frozenset(BRONZE_COLUMNS + SILVER_COLUMNS + QUARANTINE_COLUMNS)


@dataclass(frozen=True)
class SchemaChange:
    kind: str  # added | dropped | type_changed
    column: str
    old_type: str | None = None
    new_type: str | None = None

    def describe(self) -> str:
        if self.kind == "added":
            return f"+{self.column} {self.new_type}"
        if self.kind == "dropped":
            return f"-{self.column} {self.old_type} (kept, NULL from now on)"
        return f"~{self.column} {self.old_type} -> {self.new_type}"


class SchemaDriftError(RuntimeError):
    audit_verbatim = True

    def __init__(self, table: str, changes: list[SchemaChange], reason: str):
        self.table = table
        self.changes = changes
        detail = ", ".join(c.describe() for c in changes)
        super().__init__(f"Schema drift on '{table}' rejected: {reason} [{detail}]")


def business_columns(schema: Mapping[str, str]) -> dict[str, str]:
    return {name: kind for name, kind in schema.items() if name not in TECHNICAL_COLUMNS}


def diff_schema(current: Mapping[str, str] | None, incoming: Mapping[str, str]) -> list[SchemaChange]:
    """Changes needed to go from the lake table's schema to the incoming one."""
    if current is None:
        return []
    current = business_columns(current)
    incoming = business_columns(incoming)
    changes = [SchemaChange("added", c, new_type=t) for c, t in incoming.items() if c not in current]
    changes += [SchemaChange("dropped", c, old_type=t) for c, t in current.items() if c not in incoming]
    changes += [
        SchemaChange("type_changed", c, old_type=current[c], new_type=t)
        for c, t in incoming.items()
        if c in current and current[c] != t
    ]
    return changes


def enforce_policy(table: TableSpec, changes: Iterable[SchemaChange]) -> list[SchemaChange]:
    changes = list(changes)
    retyped = [c for c in changes if c.kind == "type_changed"]
    if retyped:
        raise SchemaDriftError(table.name, retyped, "column types cannot change in place")
    if changes and table.schema_evolution == "fail":
        raise SchemaDriftError(table.name, changes, "schema_evolution is 'fail' for this table")
    return changes
