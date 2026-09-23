"""An independent, row-at-a-time Python implementation of the rule semantics.

The engines compile rules to SQL; this oracle evaluates the same metadata with
plain Python, so the end-to-end tests catch a compiler bug instead of
reproducing it.
"""

from __future__ import annotations

import operator
import re
from datetime import date, datetime, timezone
from decimal import Decimal

from demo.engine import convert, lake_type
from sparknerve.metadata import Pipeline, TableSpec

OPERATORS = {"<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "=": operator.eq,
             "!=": operator.ne}


def _instant(value) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def violations(spec: TableSpec, row: dict, as_of: datetime, keys: dict[tuple[str, str], set]) -> list[str]:
    failed = []
    for rule in spec.rules:
        value = row.get(rule.column)
        p = rule.params
        if rule.type == "not_null":
            bad = value is None
        elif value is None:
            bad = False
        elif rule.type == "range":
            number = Decimal(str(value))
            bad = ("min" in p and number < Decimal(str(p["min"]))) or ("max" in p and number > Decimal(str(p["max"])))
        elif rule.type == "length":
            size = len(str(value))
            bad = ("min" in p and size < p["min"]) or ("max" in p and size > p["max"])
        elif rule.type == "allowed_values":
            bad = value not in p["values"]
        elif rule.type == "regex":
            bad = re.search(p["pattern"], str(value)) is None
        elif rule.type == "not_future":
            bad = _instant(value) > as_of
        elif rule.type == "compare":
            other = row.get(p["other_column"])
            bad = other is not None and not OPERATORS[p["operator"]](value, other)
        elif rule.type == "foreign_key":
            bad = value not in keys[rule.references]
        else:
            raise AssertionError(rule.type)
        if bad:
            failed.append(rule.name)
    return failed


def typed_rows(source, table: str) -> list[dict]:
    types = {c: lake_type(t) for c, t in source.columns(table).items()}
    return [{c: convert(r.get(c), types[c]) for c in types} for r in source.rows(table)]


def expected_silver(pipeline: Pipeline, source, as_of: datetime) -> dict[str, dict]:
    """{table: {pk: row}} of source rows that pass every error rule right now.

    Tables are evaluated in dependency order, so a foreign key is checked
    against the valid rows of the referenced table, as the DAG does."""
    expected: dict[str, dict] = {}
    for spec in sorted(pipeline.tables, key=lambda t: len(t.depends_on)):
        keys = {}
        for rule in spec.rules:
            if rule.type == "foreign_key":
                ref_table, ref_column = rule.references
                keys[rule.references] = {r[ref_column] for r in expected[ref_table].values()}
        errors = {r.name for r in spec.rules if r.severity == "error"}
        valid = {}
        for row in typed_rows(source, spec.name):
            if not set(violations(spec, row, as_of, keys)) & errors:
                valid[tuple(row[k] for k in spec.primary_key)] = row
        expected[spec.name] = valid
    return expected


def normalize(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value, Decimal):
        return value.normalize()
    if isinstance(value, date):
        return value.isoformat()
    return value
