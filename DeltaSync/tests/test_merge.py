from deltasync.merge import apply_events
from deltasync.models import CdcEvent, Operation


def event(
    event_id: str,
    operation: Operation,
    before: dict | None,
    after: dict | None,
    offset: int,
) -> CdcEvent:
    image = after or before
    assert image
    return CdcEvent(
        event_id=event_id,
        table="orders",
        operation=operation,
        key={"order_id": image["order_id"]},
        before=before,
        after=after,
        topic="deltasync.inventory.orders",
        partition=0,
        offset=offset,
        timestamp_ms=1,
    )


def test_insert_update_delete_and_replay_are_idempotent() -> None:
    created = {"order_id": 1, "status": "created", "order_date": "2026-09-01"}
    moved = {"order_id": 1, "status": "paid", "order_date": "2026-10-01"}
    events = [
        event("one", Operation.CREATE, None, created, 0),
        event("two", Operation.UPDATE, created, moved, 1),
    ]

    first = apply_events({}, events)
    assert list(first.tables["orders"].values()) == [moved]
    assert (first.inserted, first.updated, first.skipped) == (1, 1, 0)

    replay = apply_events(first.tables, events, first.processed_event_ids)
    assert replay.tables == first.tables
    assert replay.changed == 0
    assert replay.skipped == 2

    deleted = apply_events(
        replay.tables,
        [event("three", Operation.DELETE, moved, None, 2)],
        replay.processed_event_ids,
    )
    assert deleted.tables["orders"] == {}
    assert deleted.deleted == 1

