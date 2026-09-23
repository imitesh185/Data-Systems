"""The DQ compiler: rules are executed on DuckDB here; the Spark dialect is
pinned as text here and executed on Spark in tests/test_spark_jobs.py."""

from datetime import date, datetime, timezone
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest

from sparknerve.metadata import Rule
from sparknerve.rules import compile_rule, sql_literal, string_literal

AS_OF = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
ROWS = pa.table({
    "id": pa.array([1, 2, 3, 4], pa.int32()),
    "email": pa.array(["a.b@example.com", "not-an-email", None, "x@y.io"]),
    "amount": pa.array([Decimal("10.00"), Decimal("-1.00"), None, Decimal("0.00")], pa.decimal128(12, 2)),
    "status": pa.array(["PAID", "LOST", None, "PLACED"]),
    "country": pa.array(["IN", "IND", None, "U"]),
    "order_date": pa.array([date(2026, 9, 1), date(2026, 12, 1), None, date(2026, 9, 23)], pa.date32()),
    "delivered": pa.array([date(2026, 9, 3), date(2026, 11, 1), None, None], pa.date32()),
    "customer_id": pa.array([7, 99, None, 8], pa.int32()),
})
KNOWN_CUSTOMERS = pa.table({"customer_id": pa.array([7, 8], pa.int32())})


def failing_ids(rule: Rule) -> list[int]:
    compiled = compile_rule(rule, "duckdb", AS_OF, row_alias="b")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("batch", ROWS)
    join = ""
    if compiled.lookup:
        lk = compiled.lookup
        con.register("ref", KNOWN_CUSTOMERS)
        join = (f'LEFT JOIN (SELECT DISTINCT "{lk.column}" AS "{lk.key_alias}", TRUE AS "{lk.flag_alias}" FROM ref) fk '
                f'ON b."{lk.local_column}" = fk."{lk.key_alias}"')
    rows = con.execute(f"SELECT b.id FROM batch b {join} WHERE {compiled.failed_sql} ORDER BY b.id").fetchall()
    return [r[0] for r in rows]


@pytest.mark.parametrize(
    ("rule", "expected"),
    [
        (Rule("r", "not_null", "email"), [3]),
        (Rule("r", "regex", "email", params={"pattern": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"}), [2]),
        (Rule("r", "range", "amount", params={"min": 0}), [2]),
        (Rule("r", "range", "amount", params={"min": 0.01, "max": 100000}), [2, 4]),
        (Rule("r", "allowed_values", "status", params={"values": ["PLACED", "PAID"]}), [2]),
        (Rule("r", "length", "country", params={"min": 2, "max": 2}), [2, 4]),
        (Rule("r", "not_future", "order_date"), [2]),
        (Rule("r", "compare", "delivered", params={"operator": ">=", "other_column": "order_date"}), [2]),
        (Rule("r", "foreign_key", "customer_id",
              params={"references": {"table": "customers", "column": "customer_id"}}), [2]),
    ],
)
def test_rule_semantics_on_duckdb(rule, expected):
    assert failing_ids(rule) == expected


def test_null_only_fails_not_null():
    for rule in (Rule("r", "range", "amount", params={"min": 0}),
                 Rule("r", "allowed_values", "status", params={"values": ["X"]}),
                 Rule("r", "regex", "email", params={"pattern": "^x$"})):
        assert 3 not in failing_ids(rule)


def test_spark_dialect_rendering():
    rule = Rule("email_format", "regex", "email", params={"pattern": r"^\d+'s$"})
    sql = compile_rule(rule, "spark", AS_OF).failed_sql
    assert sql == r"NOT COALESCE((CAST(`email` AS STRING) RLIKE '^\\d+\'s$'), TRUE)"
    assert compile_rule(Rule("r", "not_future", "d"), "spark", AS_OF).failed_sql == (
        "NOT COALESCE((`d` <= TIMESTAMP '2026-09-23 12:00:00.000000'), TRUE)")
    fk = compile_rule(Rule("customer_exists", "foreign_key", "customer_id",
                           params={"references": {"table": "customers", "column": "customer_id"}}), "spark", AS_OF)
    assert fk.failed_sql == "(`customer_id` IS NOT NULL AND `__fk_customer_exists` IS NULL)"
    assert fk.lookup.table == "customers"


def test_literals():
    assert string_literal("it's", "duckdb") == "'it''s'"
    assert string_literal("it's \\d", "spark") == "'it\\'s \\\\d'"
    assert sql_literal(date(2026, 9, 1), "spark") == "DATE '2026-09-01'"
    assert sql_literal(Decimal("0.01"), "duckdb") == "0.01"
    assert sql_literal(1e-7, "duckdb") == "0.0000001"
    assert sql_literal(True, "spark") == "TRUE"
