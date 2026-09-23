"""Data quality engine: JSON rules -> SQL predicates.

Each rule compiles to one boolean SQL expression that is TRUE exactly when a
row violates it. The same compiled rules run on Spark SQL (the production jobs)
and on DuckDB (the live demo and the tests), so both engines quarantine the
same rows. Only three things differ by dialect: identifier quoting, string
literal escaping and the regex operator.

Semantics:
* not_null fails on NULL. Every other rule treats NULL as "not applicable" and
  passes, so a missing value is always reported by an explicit not_null rule
  instead of by every rule that touches the column.
* foreign_key fails when the value is not NULL and has no match among the
  distinct keys of the referenced silver table (a left join on a key set).
* not_future compares with the run's as-of time, rendered as a literal, so a
  replayed batch is judged exactly as it was the first time.
* Regexes are limited (at load time) to the subset Java regex and RE2 share.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from sparknerve.metadata import Rule

DIALECTS = ("spark", "duckdb")
COMPARISON_SQL = {"<": "<", "<=": "<=", ">": ">", ">=": ">=", "=": "=", "!=": "<>"}


@dataclass(frozen=True)
class Lookup:
    """A left join the engine must add before evaluating a foreign_key rule."""

    table: str          # referenced lake table
    column: str         # referenced column
    local_column: str   # column of the checked table
    key_alias: str      # join key column produced by the lookup
    flag_alias: str     # TRUE when a match exists, NULL otherwise


@dataclass(frozen=True)
class CompiledRule:
    rule: Rule
    failed_sql: str
    lookup: Lookup | None = None

    @property
    def name(self) -> str:
        return self.rule.name

    @property
    def severity(self) -> str:
        return self.rule.severity


def _check_dialect(dialect: str) -> None:
    if dialect not in DIALECTS:
        raise ValueError(f"Unknown SQL dialect '{dialect}', expected one of {DIALECTS}")


def quote_identifier(name: str, dialect: str) -> str:
    _check_dialect(dialect)
    if dialect == "spark":
        return "`" + name.replace("`", "``") + "`"
    return '"' + name.replace('"', '""') + '"'


def string_literal(value: str, dialect: str) -> str:
    _check_dialect(dialect)
    if dialect == "spark":
        # Spark SQL string literals process backslash escapes.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return "'" + value.replace("'", "''") + "'"


def number_literal(value: int | float | Decimal) -> str:
    if isinstance(value, bool):
        raise TypeError("booleans are not numbers here")
    if isinstance(value, int):
        return str(value)
    text = format(Decimal(str(value)), "f")
    return text if "." in text else text + ".0"


def timestamp_text(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def sql_literal(value: object, dialect: str) -> str:
    """A Python value as a SQL literal both Spark SQL and DuckDB (and delta-rs) parse."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return number_literal(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{timestamp_text(value)}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return string_literal(str(value), dialect)


def _as_string(column_sql: str, dialect: str) -> str:
    return f"CAST({column_sql} AS {'STRING' if dialect == 'spark' else 'VARCHAR'})"


def _violated(predicate: str) -> str:
    """TRUE only when the predicate is FALSE; NULL (value missing) passes."""
    return f"NOT COALESCE(({predicate}), TRUE)"


def compile_rule(rule: Rule, dialect: str, as_of: datetime, row_alias: str | None = None) -> CompiledRule:
    _check_dialect(dialect)

    def col(name: str) -> str:
        quoted = quote_identifier(name, dialect)
        return f"{row_alias}.{quoted}" if row_alias else quoted

    c = col(rule.column)
    p = rule.params
    kind = rule.type

    if kind == "not_null":
        return CompiledRule(rule, f"{c} IS NULL")

    if kind == "range":
        bounds = []
        if "min" in p:
            bounds.append(f"{c} >= {number_literal(p['min'])}")
        if "max" in p:
            bounds.append(f"{c} <= {number_literal(p['max'])}")
        return CompiledRule(rule, _violated(" AND ".join(bounds)))

    if kind == "length":
        length = f"length({_as_string(c, dialect)})"
        bounds = []
        if "min" in p:
            bounds.append(f"{length} >= {int(p['min'])}")
        if "max" in p:
            bounds.append(f"{length} <= {int(p['max'])}")
        return CompiledRule(rule, _violated(" AND ".join(bounds)))

    if kind == "allowed_values":
        values = ", ".join(sql_literal(v, dialect) for v in p["values"])
        return CompiledRule(rule, _violated(f"{c} IN ({values})"))

    if kind == "regex":
        pattern = string_literal(p["pattern"], dialect)
        text = _as_string(c, dialect)
        matches = f"{text} RLIKE {pattern}" if dialect == "spark" else f"regexp_matches({text}, {pattern})"
        return CompiledRule(rule, _violated(matches))

    if kind == "not_future":
        return CompiledRule(rule, _violated(f"{c} <= {sql_literal(as_of, dialect)}"))

    if kind == "compare":
        other = col(p["other_column"])
        return CompiledRule(rule, _violated(f"{c} {COMPARISON_SQL[p['operator']]} {other}"))

    if kind == "foreign_key":
        ref_table, ref_column = rule.references
        lookup = Lookup(
            table=ref_table,
            column=ref_column,
            local_column=rule.column,
            key_alias=f"__fk_{rule.name}_key",
            flag_alias=f"__fk_{rule.name}",
        )
        flag = quote_identifier(lookup.flag_alias, dialect)
        return CompiledRule(rule, f"({c} IS NOT NULL AND {flag} IS NULL)", lookup)

    raise ValueError(f"Rule '{rule.name}' has unsupported type '{kind}'")


def compile_rules(rules: Iterable[Rule], dialect: str, as_of: datetime,
                  row_alias: str | None = None) -> list[CompiledRule]:
    return [compile_rule(rule, dialect, as_of, row_alias) for rule in rules]


def failure_columns(compiled: list[CompiledRule]) -> dict[str, str]:
    """Name of the boolean column each engine materialises per rule."""
    return {c.name: f"__dq_{i}" for i, c in enumerate(compiled)}
