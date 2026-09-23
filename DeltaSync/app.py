from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd
import streamlit as st

from deltasync.engine import DemoEngine

ROOT = Path(__file__).resolve().parent
REPO_URL = "https://github.com/imitesh185/Data-Systems/tree/main/DeltaSync"

st.set_page_config(
    page_title="DeltaSync - CDC to Delta Lake",
    page_icon="🔁",
    layout="wide",
)


def engine() -> DemoEngine:
    if "engine" not in st.session_state:
        st.session_state.engine = DemoEngine(ROOT / ".deltasync" / "delta")
    return st.session_state.engine


def run_action(label: str, action_name: str) -> None:
    if st.button(label, use_container_width=True):
        getattr(engine(), action_name)()
        st.rerun()


def frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame()


demo = engine()

with st.sidebar:
    st.header("1 · Change the source")
    st.caption("Each write becomes a Debezium-style event in the simulated Kafka topic.")
    run_action("➕ Insert order", "insert_order")
    run_action("✏️ Advance an order's status", "advance_order")
    run_action("📅 Change an order's date (moves partition)", "move_order_partition")
    run_action("🗑️ Delete an order", "delete_order")
    run_action("👤 Insert customer", "insert_customer")
    run_action("🏙️ Update a customer", "update_customer")
    run_action("🧬 ALTER TABLE customers ADD COLUMN loyalty_tier", "add_loyalty_tier")
    run_action("🎲 Random burst (25 changes)", "random_burst")

    st.header("2 · Run the stream")
    max_events = st.slider(
        "Max events per micro-batch (maxOffsetsPerTrigger)",
        min_value=5,
        max_value=200,
        value=50,
        step=5,
    )
    if st.button("▶️ Run micro-batch", type="primary", use_container_width=True):
        record = demo.run_batch(max_events)
        if record is None:
            st.info("Kafka is already drained.")
        st.rerun()
    if st.button("🔁 Replay last batch (crash before checkpoint)", use_container_width=True):
        replay = demo.replay_last_batch()
        if replay is None:
            st.info("Run a batch before replaying one.")
        st.rerun()
    if st.button("↺ Reset demo", use_container_width=True):
        demo.reset()
        st.rerun()
    st.divider()
    st.caption(f"[Source code]({REPO_URL})")

st.title("🔁 DeltaSync")
st.markdown(
    "**Change data capture from MySQL to Delta Lake with exactly-once results.**  \n"
    "`MySQL binlog → Debezium → Kafka → Spark Structured Streaming "
    "(foreachBatch MERGE) → Delta Lake`"
)
st.caption(
    "This hosted-friendly demo simulates MySQL, Debezium, and Kafka in memory, "
    "then applies the same deterministic merge contract to real local Delta tables. "
    "The Docker Compose profile runs the complete infrastructure pipeline."
)

source_count = sum(len(rows) for rows in demo.source.values())
delta_count = sum(len(rows) for rows in demo.delta.values())
metrics = st.columns(5)
metrics[0].metric("MySQL rows", source_count)
metrics[1].metric("CDC events captured", len(demo.events))
metrics[2].metric("Waiting in Kafka", len(demo.waiting_events))
metrics[3].metric("Micro-batches applied", len(demo.batches))
metrics[4].metric("Delta rows", delta_count)

if demo.waiting_events:
    st.info(
        f"⏳ {len(demo.waiting_events)} change event(s) are waiting in Kafka. "
        "Run a micro-batch to apply them."
    )
elif demo.events:
    st.success("✅ Source and Delta are caught up.")

source_tab, kafka_tab, batch_tab, history_tab, bronze_tab, schema_tab, guide_tab = st.tabs(
    [
        "🔎 Source vs Delta",
        "📨 Kafka topic",
        "⚙️ Micro-batches",
        "🕰️ Delta history",
        "📜 Bronze changelog",
        "🧬 Schema changes",
        "📖 How it works",
    ]
)

with source_tab:
    table = st.radio("Table", ("orders", "customers"), horizontal=True)
    left, right = st.columns(2)
    with left:
        st.subheader("MySQL (source)")
        st.dataframe(frame(demo.rows("source", table)), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Delta Lake (silver)")
        delta_rows = demo.rows("delta", table)
        if delta_rows:
            st.dataframe(frame(delta_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("Not created yet: run the first micro-batch.")

with kafka_tab:
    st.caption("Debezium envelopes ordered by Kafka offset.")
    kafka_rows = [
        {
            "offset": item.offset,
            "table": item.table,
            "op": item.operation.value,
            "key": dict(item.key),
            "processed": item.event_id in demo.processed_event_ids,
            "event_id": item.event_id,
        }
        for item in reversed(demo.events)
    ]
    st.dataframe(frame(kafka_rows), use_container_width=True, hide_index=True)

with batch_tab:
    if demo.batches:
        st.dataframe(
            frame([asdict(item) for item in reversed(demo.batches)]),
            use_container_width=True,
            hide_index=True,
        )
        latest = demo.batches[-1]
        if latest.replay and latest.changed_count == 0:
            st.success(
                "Exactly-once check passed: replayed events were recognized and skipped."
            )
    else:
        st.caption("No micro-batches have run.")

with history_tab:
    for table_name in ("orders", "customers"):
        st.subheader(table_name)
        if demo.delta_sink:
            history = demo.delta_sink.history(table_name)
            if history:
                st.dataframe(frame(history), use_container_width=True, hide_index=True)
            else:
                st.caption("No Delta commits yet.")

with bronze_tab:
    selected = st.selectbox("Event", range(len(demo.events)), format_func=lambda index: (
        f"offset {demo.events[index].offset} · {demo.events[index].table} · "
        f"op={demo.events[index].operation.value}"
    ))
    st.json(demo.events[selected].as_debezium_envelope())

with schema_tab:
    if demo.schema_changes:
        st.dataframe(frame(demo.schema_changes), use_container_width=True, hide_index=True)
        st.caption("New fields are propagated to the Delta schema on the next batch.")
    else:
        st.caption("No schema changes captured yet.")

with guide_tab:
    st.markdown(
        """
1. A source transaction is written to MySQL's binary log.
2. Debezium converts the row-level change into a CDC envelope.
3. Kafka retains and orders events by topic partition and offset.
4. Spark reads bounded micro-batches and deduplicates each source position.
5. `foreachBatch` merges inserts, updates, and deletes into Delta Lake.
6. The checkpoint advances only after the merge succeeds, so retries are safe.

Use **Replay last batch** after a successful batch. The row count and values remain
unchanged, demonstrating the same idempotent contract used by the Spark job.
"""
    )
