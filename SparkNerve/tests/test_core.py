from sparknerve.checkpoint import WatermarkState, next_window, watermark_epoch
from sparknerve.evolution import SchemaChange, SchemaDriftError, diff_schema, enforce_policy
from sparknerve.planner import REPORT_TASK_ID, build_plan, cli_command


def test_plan_orders_validation_after_referenced_tables(pipeline):
    plan = build_plan(pipeline)
    order = [t.task_id for t in plan.topological()]
    assert order.index("orders.validate") > order.index("customers.validate")
    assert order.index("orders.validate") > order.index("products.validate")
    assert order[-1] == REPORT_TASK_ID
    assert plan.task("orders.validate").upstream == ("orders.extract", "customers.validate", "products.validate")
    assert all(not plan.task(f"{t}.extract").upstream for t in pipeline.table_names)


def test_cli_command_for_each_stage(pipeline):
    plan = build_plan(pipeline)
    assert cli_command("retail_sales", plan.task("orders.extract"), "{{ run_id }}") == (
        "python -m sparknerve run --pipeline retail_sales --table orders --stage extract --run-id '{{ run_id }}'")
    assert cli_command("retail_sales", plan.task(REPORT_TASK_ID), "r1").startswith("python -m sparknerve report")


def test_next_window_plans_then_replays_the_same_window():
    state = WatermarkState()
    first = next_window(state, lambda: "2026-09-20 10:00:00.000000")
    assert (first.seq, first.low, first.high, first.recovered) == (1, None, "2026-09-20 10:00:00.000000", False)

    planned = state.plan(first.seq, first.high)  # crash before commit
    replay = next_window(planned, lambda: "2026-09-20 11:00:00.000000")
    assert (replay.seq, replay.high, replay.recovered) == (1, "2026-09-20 10:00:00.000000", True)

    committed = planned.commit({"id": "int"})
    assert committed.last_source_schema == {"id": "int"}
    nxt = next_window(committed, lambda: "2026-09-20 11:00:00.000000")
    assert (nxt.seq, nxt.low, nxt.recovered) == (2, "2026-09-20 10:00:00.000000", False)
    assert next_window(committed, lambda: "2026-09-20 10:00:00.000000") is None
    assert next_window(WatermarkState(), lambda: None) is None


def test_watermark_epoch():
    assert watermark_epoch("1970-01-01 00:01:00.000000") == 60.0
    assert watermark_epoch(None) is None


def test_diff_schema_classifies_changes():
    current = {"id": "int", "fax": "string", "amount": "decimal(10,2)", "_run_id": "string"}
    incoming = {"id": "int", "amount": "decimal(12,2)", "tier": "string"}
    kinds = {(c.kind, c.column) for c in diff_schema(current, incoming)}
    assert kinds == {("added", "tier"), ("dropped", "fax"), ("type_changed", "amount")}
    assert diff_schema(None, incoming) == []


def test_policy(pipeline):
    added = [SchemaChange("added", "tier", new_type="string")]
    assert enforce_policy(pipeline.table("customers"), added) == added
    try:
        enforce_policy(pipeline.table("products"), added)
        raise AssertionError("expected SchemaDriftError")
    except SchemaDriftError as exc:
        assert "schema_evolution is 'fail'" in str(exc)
    retyped = [SchemaChange("type_changed", "amount", "decimal(10,2)", "decimal(12,2)")]
    try:
        enforce_policy(pipeline.table("customers"), retyped)
        raise AssertionError("expected SchemaDriftError")
    except SchemaDriftError as exc:
        assert "cannot change in place" in str(exc)
