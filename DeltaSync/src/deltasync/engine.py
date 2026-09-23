from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

from deltasync.merge import MergeResult, TableState, apply_events
from deltasync.models import CdcEvent, Operation
from deltasync.sink import DeltaLakeSink


@dataclass(frozen=True, slots=True)
class BatchRecord:
    batch_id: int
    event_count: int
    changed_count: int
    skipped_count: int
    first_offset: int
    last_offset: int
    duration_ms: int
    replay: bool = False


class DemoEngine:
    primary_keys = {"orders": "order_id", "customers": "customer_id"}

    def __init__(self, delta_root: Path | None = None, seed: int = 17) -> None:
        self._rng = random.Random(seed)
        self.delta_sink = DeltaLakeSink(delta_root) if delta_root else None
        self.reset()

    def reset(self) -> None:
        self.source: dict[str, list[dict[str, Any]]] = {
            "orders": self._initial_orders(),
            "customers": self._initial_customers(),
        }
        self.delta: TableState = {}
        self.events: list[CdcEvent] = []
        self.processed_event_ids: set[str] = set()
        self.batches: list[BatchRecord] = []
        self.schema_changes: list[dict[str, Any]] = []
        self._offset = 0
        self._last_batch: list[CdcEvent] = []
        for table, rows in self.source.items():
            for row in rows:
                self._emit(table, Operation.SNAPSHOT, None, row)

    @staticmethod
    def _initial_orders() -> list[dict[str, Any]]:
        statuses = ("created", "paid", "shipped", "delivered")
        start = date(2026, 9, 1)
        return [
            {
                "order_id": index,
                "customer_id": (index - 1) % 6 + 1,
                "status": statuses[(index - 1) % len(statuses)],
                "amount": float(35 + index * 11),
                "order_date": str(start + timedelta(days=index - 1)),
            }
            for index in range(1, 13)
        ]

    @staticmethod
    def _initial_customers() -> list[dict[str, Any]]:
        cities = ("Mumbai", "Pune", "Bengaluru", "Delhi", "Hyderabad", "Chennai")
        return [
            {
                "customer_id": index,
                "name": f"Customer {index}",
                "email": f"customer{index}@example.com",
                "city": city,
            }
            for index, city in enumerate(cities, start=1)
        ]

    @property
    def waiting_events(self) -> list[CdcEvent]:
        return [
            event for event in self.events
            if event.event_id not in self.processed_event_ids
        ]

    def rows(self, store: str, table: str) -> list[dict[str, Any]]:
        if store == "source":
            return [dict(row) for row in self.source[table]]
        if store != "delta":
            raise ValueError(f"unknown store: {store}")
        return [dict(row) for row in self.delta.get(table, {}).values()]

    def insert_order(self) -> None:
        next_id = max(row["order_id"] for row in self.source["orders"]) + 1
        row = {
            "order_id": next_id,
            "customer_id": self._rng.randint(1, len(self.source["customers"])),
            "status": "created",
            "amount": float(self._rng.randint(20, 500)),
            "order_date": str(date.today()),
        }
        self.source["orders"].append(row)
        self._emit("orders", Operation.CREATE, None, row)

    def advance_order(self) -> None:
        transitions = {
            "created": "paid",
            "paid": "shipped",
            "shipped": "delivered",
            "delivered": "returned",
            "returned": "returned",
        }
        row = self._rng.choice(self.source["orders"])
        before = dict(row)
        row["status"] = transitions[row["status"]]
        self._emit("orders", Operation.UPDATE, before, row)

    def move_order_partition(self) -> None:
        row = self._rng.choice(self.source["orders"])
        before = dict(row)
        row["order_date"] = str(date.fromisoformat(row["order_date"]) + timedelta(days=30))
        self._emit("orders", Operation.UPDATE, before, row)

    def delete_order(self) -> None:
        if not self.source["orders"]:
            raise ValueError("there are no orders to delete")
        index = self._rng.randrange(len(self.source["orders"]))
        before = self.source["orders"].pop(index)
        self._emit("orders", Operation.DELETE, before, None)

    def insert_customer(self) -> None:
        next_id = max(row["customer_id"] for row in self.source["customers"]) + 1
        row: dict[str, Any] = {
            "customer_id": next_id,
            "name": f"Customer {next_id}",
            "email": f"customer{next_id}@example.com",
            "city": self._rng.choice(("Jaipur", "Kolkata", "Ahmedabad")),
        }
        if "loyalty_tier" in self.source["customers"][0]:
            row["loyalty_tier"] = "bronze"
        self.source["customers"].append(row)
        self._emit("customers", Operation.CREATE, None, row)

    def update_customer(self) -> None:
        row = self._rng.choice(self.source["customers"])
        before = dict(row)
        row["city"] = self._rng.choice(("Jaipur", "Kolkata", "Ahmedabad", "Surat"))
        self._emit("customers", Operation.UPDATE, before, row)

    def add_loyalty_tier(self) -> bool:
        if "loyalty_tier" in self.source["customers"][0]:
            return False
        for row in self.source["customers"]:
            before = dict(row)
            row["loyalty_tier"] = self._rng.choice(("bronze", "silver", "gold"))
            self._emit("customers", Operation.UPDATE, before, row)
        self.schema_changes.append(
            {
                "ddl": "ALTER TABLE customers ADD COLUMN loyalty_tier VARCHAR(16)",
                "captured_at_ms": int(time.time() * 1000),
            }
        )
        return True

    def random_burst(self, count: int = 25) -> None:
        actions: tuple[Callable[[], None], ...] = (
            self.insert_order,
            self.advance_order,
            self.move_order_partition,
            self.insert_customer,
            self.update_customer,
        )
        for _ in range(count):
            self._rng.choice(actions)()

    def run_batch(self, max_events: int = 50) -> BatchRecord | None:
        if max_events < 1:
            raise ValueError("max_events must be positive")
        events = self.waiting_events[:max_events]
        if not events:
            return None
        record = self._apply_batch(events, replay=False)
        self._last_batch = list(events)
        return record

    def replay_last_batch(self) -> BatchRecord | None:
        if not self._last_batch:
            return None
        return self._apply_batch(self._last_batch, replay=True)

    def _apply_batch(self, events: Iterable[CdcEvent], replay: bool) -> BatchRecord:
        event_list = list(events)
        started = time.perf_counter()
        result: MergeResult = apply_events(
            self.delta,
            event_list,
            self.processed_event_ids,
        )
        self.delta = result.tables
        self.processed_event_ids = set(result.processed_event_ids)
        if result.changed and self.delta_sink:
            for table in {event.table for event in event_list}:
                partitions = ["order_date"] if table == "orders" else None
                self.delta_sink.replace_table(table, self.rows("delta", table), partitions)
        record = BatchRecord(
            batch_id=len(self.batches),
            event_count=len(event_list),
            changed_count=result.changed,
            skipped_count=result.skipped,
            first_offset=event_list[0].offset,
            last_offset=event_list[-1].offset,
            duration_ms=max(1, int((time.perf_counter() - started) * 1000)),
            replay=replay,
        )
        self.batches.append(record)
        return record

    def _emit(
        self,
        table: str,
        operation: Operation,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
    ) -> None:
        image = after if after is not None else before
        if image is None:
            raise ValueError("an event requires a before or after image")
        key_name = self.primary_keys[table]
        offset = self._offset
        self.events.append(
            CdcEvent(
                event_id=f"{table}:0:{offset}:{uuid.uuid4().hex[:8]}",
                table=table,
                operation=operation,
                key={key_name: image[key_name]},
                before=dict(before) if before is not None else None,
                after=dict(after) if after is not None else None,
                topic=f"deltasync.inventory.{table}",
                partition=0,
                offset=offset,
                timestamp_ms=int(time.time() * 1000),
            )
        )
        self._offset += 1

