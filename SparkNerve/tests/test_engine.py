"""End-to-end behaviour of the pipeline on real Delta tables (delta-rs + DuckDB).

Every claim in the README maps to a test here: metadata-generated DAG, DQ
quarantine, schema evolution, checkpoint recovery, partition pruning, the
circuit breaker, auditability and metrics.
"""

import json

import pyarrow as pa

from demo.engine import DemoEngine
from sparknerve.audit import FAILED, NO_DATA, SUCCEEDED
from tests.oracle import expected_silver, normalize


def layer_rows(engine: DemoEngine, layer: str, table: str) -> list[dict]:
    path = getattr(engine.lake, layer)(engine.pipeline.name, table)
    return engine.read(path).to_pylist() if engine.exists(path) else []


def assert_reconciled(engine: DemoEngine, run) -> None:
    """Silver holds exactly the source rows that pass every error rule, value for value."""
    expected = expected_silver(engine.pipeline, engine.source, run.as_of)
    for spec in engine.pipeline.tables:
        columns = list(engine.source.columns(spec.name))
        got = {tuple(r[k] for k in spec.primary_key): {c: normalize(r.get(c)) for c in columns}
               for r in layer_rows(engine, "silver", spec.name)}
        want = {k: {c: normalize(v.get(c)) for c in columns} for k, v in expected[spec.name].items()}
        assert got.keys() == want.keys(), f"{spec.name}: keys differ"
        for key in want:
            assert got[key] == want[key], f"{spec.name} {key}"


def stage(run, task_id):
    return run.result(task_id)


def test_initial_load_lands_every_valid_row(engine, source):
    run = engine.run_pipeline()
    assert run.state == "success"
    assert [r.task.task_id for r in run.results] == [
        "customers.extract", "products.extract", "orders.extract",
        "customers.validate", "products.validate", "orders.validate", "run_report"]
    assert len(layer_rows(engine, "silver", "orders")) == source.count("orders")
    assert layer_rows(engine, "quarantine", "orders") == []
    assert_reconciled(engine, run)


def test_invalid_rows_are_quarantined_and_valid_rows_continue(engine, source):
    engine.run_pipeline()
    source.business_as_usual(30)
    injected = {
        source.inject_defect("orders", "negative amount"),
        source.inject_defect("orders", "orphan customer_id"),
        source.inject_defect("orders", "NULL order_date"),
        source.inject_defect("customers", "malformed email"),
        source.inject_defect("orders", "delivered before ordered (warning only)"),
    }
    run = engine.run_pipeline()
    assert run.state == "success", [r.error for r in run.results]
    quarantined = {r["order_id"]: r["_dq_errors"] for r in layer_rows(engine, "quarantine", "orders")}
    assert sorted(quarantined.values()) == [["amount_non_negative"], ["customer_exists"],
                                            ["partition_order_date_not_null"]]
    assert [r["_dq_errors"] for r in layer_rows(engine, "quarantine", "customers")] == [["email_format"]]
    warned = [r for r in layer_rows(engine, "silver", "orders") if r["_dq_warnings"]]
    assert [r["_dq_warnings"] for r in warned] == [["delivered_after_ordered"]]
    orders = stage(run, "orders.validate").runs[0]
    assert orders.rows_quarantined == 3 and orders.rows_warned == 1
    assert orders.rule_failures["delivered_after_ordered"] == 1
    assert len(injected) == 5
    assert_reconciled(engine, run)


def test_idle_rerun_changes_nothing(engine):
    engine.run_pipeline()
    versions = {t: engine.version(engine.lake.silver("retail_sales", t)) for t in ("customers", "products", "orders")}
    run = engine.run_pipeline()
    assert run.state == "success"
    assert {r.runs[0].status for r in run.results if r.task.stage != "report"} == {NO_DATA}
    assert versions == {t: engine.version(engine.lake.silver("retail_sales", t)) for t in versions}


def test_extract_crash_after_bronze_commit_recovers_exactly_once(engine, source):
    engine.run_pipeline()
    source.business_as_usual(20)
    engine.chaos["orders.extract"] = "crash"
    crashed = engine.run_pipeline()
    extract = stage(crashed, "orders.extract").runs[0]
    assert extract.status == FAILED and "Simulated crash" in extract.error
    assert stage(crashed, "orders.validate").state == "upstream_failed"
    state = engine.watermark_state("orders")
    assert state.pending and state.planned_seq == extract.batch_id

    source.business_as_usual(15)  # the source keeps moving while we are down
    recovered = engine.run_pipeline()
    replay = stage(recovered, "orders.extract").runs[0]
    assert replay.recovered and replay.batch_id == extract.batch_id
    assert replay.watermark_to == extract.watermark_to  # the planned window, not a new high
    assert replay.rows_written == 0 and "append skipped" in replay.note
    assert not engine.watermark_state("orders").pending

    caught_up = engine.run_pipeline()
    assert caught_up.state == "success"
    bronze = layer_rows(engine, "bronze", "orders")
    versions = [(r["order_id"], r["modified_at"]) for r in bronze]
    assert len(versions) == len(set(versions)), "a row version landed in bronze twice"
    assert_reconciled(engine, caught_up)


def test_validate_crash_after_merge_replays_without_duplicates(engine, source):
    engine.run_pipeline()
    source.business_as_usual(20)
    source.inject_defect("orders", "quantity = 0")
    engine.chaos["orders.validate"] = "crash"
    crashed = engine.run_pipeline()
    batch = stage(crashed, "orders.validate").runs[0]
    assert batch.status == FAILED and engine.checkpoint("orders").uncommitted()["batch_id"] == batch.batch_id

    rerun = engine.run_pipeline()
    replay = stage(rerun, "orders.validate").runs[0]
    assert replay.recovered and replay.batch_id == batch.batch_id and replay.status == SUCCEEDED
    assert replay.rows_written == 0, "replayed MERGE must not change silver again"
    assert "append skipped" in replay.note
    assert len(layer_rows(engine, "quarantine", "orders")) == 1
    assert engine.checkpoint("orders").uncommitted() is None
    assert_reconciled(engine, rerun)


def test_quality_gate_trips_keeps_silver_and_retries_until_accepted(engine, source):
    engine.run_pipeline()
    silver_version = engine.version(engine.lake.silver("retail_sales", "orders"))
    source.bad_batch(24)
    tripped = engine.run_pipeline()
    gate = stage(tripped, "orders.validate").runs[0]
    assert gate.status == FAILED and "Quality gate tripped" in gate.error
    assert engine.version(engine.lake.silver("retail_sales", "orders")) == silver_version
    assert len(layer_rows(engine, "quarantine", "orders")) == 24  # inspectable before anyone accepts
    assert stage(tripped, "run_report").state == "failed"

    again = stage(engine.run_pipeline(), "orders.validate").runs[0]
    assert again.status == FAILED and again.recovered and again.batch_id == gate.batch_id

    engine.gate_overrides["orders"] = 1.0
    accepted = engine.run_pipeline()
    assert accepted.state == "success"
    assert len(layer_rows(engine, "quarantine", "orders")) == 24  # replay did not duplicate
    assert "orders" not in engine.gate_overrides
    assert_reconciled(engine, accepted)


def test_small_batches_do_not_trip_the_gate(engine, source):
    engine.run_pipeline()
    source.inject_defect("products", "price 0.00")
    source.inject_defect("products", "malformed SKU")
    run = engine.run_pipeline()
    products = stage(run, "products.validate").runs[0]
    assert products.status == SUCCEEDED and products.rows_quarantined == 2 and products.quarantine_ratio == 1.0


def test_new_source_column_evolves_bronze_and_silver(engine, source):
    engine.run_pipeline()
    source.add_column("customers", "loyalty_tier", "VARCHAR(10)")
    run = engine.run_pipeline()
    assert stage(run, "customers.extract").runs[0].schema_changes == ["+loyalty_tier string"]
    assert stage(run, "customers.validate").runs[0].schema_changes == ["+loyalty_tier string"]
    assert "loyalty_tier" in engine.schema_of(engine.lake.silver("retail_sales", "customers"))
    tiers = [r["loyalty_tier"] for r in layer_rows(engine, "silver", "customers") if r["loyalty_tier"]]
    assert len(tiers) == 6
    source.business_as_usual(10)
    later = engine.run_pipeline()
    assert stage(later, "customers.extract").runs[0].schema_changes == []  # reported once
    assert_reconciled(engine, later)


def test_fail_policy_blocks_table_and_its_dependents_until_reverted(engine, source):
    engine.run_pipeline()
    source.add_column("products", "weight_kg", "DECIMAL(6,2)")
    source.business_as_usual(10)
    blocked = engine.run_pipeline()
    extract = stage(blocked, "products.extract").runs[0]
    assert extract.status == FAILED and "schema_evolution is 'fail'" in extract.error
    assert stage(blocked, "products.validate").state == "upstream_failed"
    assert stage(blocked, "orders.validate").state == "upstream_failed"
    assert not engine.watermark_state("products").pending  # rejected before anything was planned

    source.drop_column("products", "weight_kg")
    healed = engine.run_pipeline()
    assert healed.state == "success"
    assert_reconciled(engine, healed)


def test_merge_is_pruned_to_touched_partitions(engine, source):
    engine.run_pipeline()
    all_days = {r["order_date"] for r in layer_rows(engine, "silver", "orders")}
    source.business_as_usual(12)
    run = engine.run_pipeline()
    batch = stage(run, "orders.validate").runs[0]
    assert 0 < len(batch.partitions_touched) < len(all_days)
    assert "skipped" in batch.note
    skipped = int(batch.note.split("skipped ")[1].split(" ")[0])
    assert skipped >= len(all_days) - len(batch.partitions_touched)


def test_fixing_rows_at_source_releases_them_from_quarantine(engine, source):
    engine.run_pipeline()
    source.inject_defect("orders", "unknown status 'LOST'")
    source.inject_defect("customers", "NULL email")
    engine.run_pipeline()
    bad_order = layer_rows(engine, "quarantine", "orders")[0]["order_id"]
    assert bad_order not in {r["order_id"] for r in layer_rows(engine, "silver", "orders")}
    source.fix_defects()
    run = engine.run_pipeline()
    assert bad_order in {r["order_id"] for r in layer_rows(engine, "silver", "orders")}
    assert len(layer_rows(engine, "quarantine", "orders")) == 1  # history is kept
    assert_reconciled(engine, run)


def test_random_workload_reconciles_with_the_oracle(engine, source):
    run = engine.run_pipeline()
    for step in range(8):
        source.business_as_usual(25)
        if step % 2:
            source.inject_bad_records(2)
        if step == 3:
            source.add_column("customers", "loyalty_tier", "VARCHAR(10)")
        if step == 5:
            source.fix_defects()
        run = engine.run_pipeline()
        assert run.state == "success", [r.error for r in run.results if r.error]
        assert_reconciled(engine, run)


def test_every_stage_is_audited_and_measured(engine, source):
    run = engine.run_pipeline()
    audit = engine.audit_log().to_pylist()
    mine = [r for r in audit if r["run_id"] == run.run_id]
    assert {(r["table_name"], r["stage"]) for r in mine} == {
        (t, s) for t in ("customers", "products", "orders") for s in ("extract", "validate")} | {("_all", "report")}
    assert all(r["status"] == SUCCEEDED and r["finished_at"] >= r["started_at"] for r in mine)
    orders = next(r for r in mine if r["table_name"] == "orders" and r["stage"] == "validate")
    assert json.loads(orders["partitions_touched"]) and orders["delta_version"] == 0

    metrics = stage(run, "orders.validate").metrics
    assert 'sparknerve_stage_success{pipeline="retail_sales",stage="validate",table="orders"} 1.0' in metrics
    assert 'sparknerve_dq_rule_failures{pipeline="retail_sales",rule="customer_exists"' in metrics
    assert "sparknerve_delta_table_version" in metrics
    assert "sparknerve_source_watermark_timestamp_seconds" in stage(run, "orders.extract").metrics
    assert 'sparknerve_pipeline_run_success{pipeline="retail_sales"} 1.0' in stage(run, "run_report").metrics


def test_commits_carry_the_run_id_for_lineage(engine):
    run = engine.run_pipeline()
    from deltalake import DeltaTable

    history = DeltaTable(str(engine.lake.silver("retail_sales", "orders"))).history()
    assert history[0]["sparknerve.run_id"] == run.run_id
    assert isinstance(engine.audit_log(), pa.Table)
