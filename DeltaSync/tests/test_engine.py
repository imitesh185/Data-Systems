import pytest

from deltasync.engine import DemoEngine


def test_initial_snapshot_converges_and_replay_does_not_change_rows() -> None:
    engine = DemoEngine()
    assert len(engine.waiting_events) == 18

    first = engine.run_batch()
    assert first is not None
    assert first.changed_count == 18
    assert len(engine.rows("delta", "orders")) == 12
    assert len(engine.rows("delta", "customers")) == 6

    replay = engine.replay_last_batch()
    assert replay is not None
    assert replay.changed_count == 0
    assert replay.skipped_count == 18


def test_source_changes_reach_delta_in_bounded_batches() -> None:
    engine = DemoEngine()
    engine.run_batch()
    engine.insert_order()
    engine.advance_order()
    engine.move_order_partition()
    engine.update_customer()
    engine.delete_order()

    assert len(engine.waiting_events) == 5
    assert engine.run_batch(max_events=2) is not None
    assert len(engine.waiting_events) == 3
    assert engine.run_batch(max_events=50) is not None
    assert len(engine.waiting_events) == 0

    for table in ("orders", "customers"):
        source = sorted(engine.rows("source", table), key=lambda row: tuple(row.values()))
        delta = sorted(engine.rows("delta", table), key=lambda row: tuple(row.values()))
        assert source == delta


def test_schema_change_is_idempotent_and_replicated() -> None:
    engine = DemoEngine()
    engine.run_batch()

    assert engine.add_loyalty_tier()
    assert not engine.add_loyalty_tier()
    assert len(engine.schema_changes) == 1
    engine.run_batch()

    assert all("loyalty_tier" in row for row in engine.rows("delta", "customers"))


def test_invalid_batch_size_is_rejected() -> None:
    engine = DemoEngine()
    with pytest.raises(ValueError, match="positive"):
        engine.run_batch(0)
