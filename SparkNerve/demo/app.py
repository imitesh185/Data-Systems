"""SparkNerve live demo: break the source system on purpose and watch the
metadata-driven pipeline quarantine, evolve, recover and report.

Runs the real SparkNerve stages (same metadata, planner, compiled DQ rules,
checkpoint protocol, audit and metrics) on delta-rs + DuckDB, so it can be
hosted without a JVM. Run locally:  streamlit run demo/app.py
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import streamlit as st  # noqa: E402
from deltalake import DeltaTable  # noqa: E402

from demo.engine import DemoEngine, PipelineRun, local, to_table  # noqa: E402
from simulator.retail import RetailDB, SqliteBackend  # noqa: E402
from sparknerve.metadata import load_pipelines  # noqa: E402
from sparknerve.planner import cli_command  # noqa: E402
from sparknerve.rules import compile_rules  # noqa: E402

REPO_URL = "https://github.com/imitesh185/Data-Systems/tree/main/SparkNerve"
PIPELINE = "retail_sales"
TABLES = ("customers", "products", "orders")
CHAOS = {
    "No crash": None,
    "Crash orders.extract after the bronze commit": "orders.extract",
    "Crash orders.validate after the silver MERGE": "orders.validate",
}
STATE_COLORS = {"success": "#c8e6c9", "failed": "#ffcdd2", "upstream_failed": "#ffe0b2", None: "#eceff1"}

st.set_page_config(page_title="SparkNerve — metadata-driven lakehouse ingestion", page_icon="⚡", layout="wide")


# ---- session ------------------------------------------------------------------------

def reset() -> None:
    source = RetailDB(SqliteBackend(), seed=2026)
    seeded = source.seed()
    engine = DemoEngine(tempfile.mkdtemp(prefix="sparknerve-demo-"), load_pipelines()[PIPELINE], source)
    run = engine.run_pipeline()
    st.session_state.update(
        source=source,
        engine=engine,
        activity=[f"DAG run {run.run_id}: initial load ({run.state})", f"SQL Server: {seeded}"],
        flash=("Initial load complete: every table extracted, validated and merged into silver.", "✅"),
        chaos_choice="No crash",
    )


if "engine" not in st.session_state:
    reset()
S = st.session_state
engine: DemoEngine = S.engine
source: RetailDB = S.source


def log(messages: list[str] | str, prefix: str = "") -> None:
    for message in [messages] if isinstance(messages, str) else messages:
        S.activity.insert(0, f"{prefix}{message}")
    del S.activity[200:]


def change(action, label: str) -> None:
    try:
        result = action()
        log(result if result else [f"{label}: nothing to change"], "SQL Server · ")
        S.flash = (f"{label}: done. Trigger a DAG run to process it.", "📝")
    except Exception as exc:  # noqa: BLE001 - shown to the user
        S.flash = (f"{label} failed: {exc}", "⚠️")


def trigger_run(accept: str | None = None) -> None:
    crash = CHAOS[S.chaos_choice]
    if crash:
        engine.chaos[crash] = "armed"
    if accept:
        engine.gate_overrides[accept] = 1.0
    run = engine.run_pipeline()
    failed = [r.task.task_id for r in run.results if r.state == "failed"]
    log(f"DAG run {run.run_id}: {run.state}" + (f" (failed: {', '.join(failed)})" if failed else ""), "Airflow · ")
    S.chaos_choice = "No crash"
    if run.state == "success":
        S.flash = ("DAG run succeeded: every task green.", "✅")
    else:
        S.flash = (f"DAG run failed at {', '.join(failed)}. See the DAG and Recovery tabs.", "🛑")


def last_run() -> PipelineRun | None:
    return engine.runs[-1] if engine.runs else None


def gate_tripped(run: PipelineRun | None) -> list[str]:
    if not run:
        return []
    return [r.task.table for r in run.results
            if r.task.stage == "validate" and r.runs and "Quality gate" in (r.runs[-1].error or "")]


def arrow_frame(table: pa.Table, sort: list[str] | None = None) -> pd.DataFrame:
    frame = table.to_pandas()
    if sort and not frame.empty:
        frame = frame.sort_values([c for c in sort if c in frame.columns], ascending=False)
    return frame


def layer_table(layer: str, table: str, version: int | None = None) -> pa.Table | None:
    path = getattr(engine.lake, layer)(PIPELINE, table)
    return engine.read(path, version) if engine.exists(path) else None


# ---- sidebar: controls ------------------------------------------------------------------

with st.sidebar:
    st.header("1 · Change the source")
    st.caption("RetailDB on SQL Server (simulated). Each change stamps `modified_at`, which drives extraction.")
    st.button("🛒 Business as usual (+25 changes)", width="stretch",
              on_click=change, args=(lambda: source.business_as_usual(25), "Business as usual"))
    st.button("🧪 Inject 6 bad records", width="stretch",
              on_click=change, args=(lambda: source.inject_bad_records(6), "Bad records"))
    st.button("💥 Broken upstream deploy (24 bad orders)", width="stretch",
              on_click=change, args=(lambda: source.bad_batch(24), "Broken deploy"))
    st.button("🩹 Fix bad records at the source", width="stretch",
              on_click=change, args=(source.fix_defects, "Fix at source"))
    has_tier = "loyalty_tier" in source.columns("customers")
    st.button("🧬 ALTER customers ADD loyalty_tier", width="stretch", disabled=has_tier,
              help="customers allows schema evolution: the column flows to bronze and silver automatically.",
              on_click=change, args=(lambda: source.add_column("customers", "loyalty_tier", "VARCHAR(10)"),
                                     "Schema drift on customers"))
    if "weight_kg" in source.columns("products"):
        st.button("↩️ Revert products.weight_kg", width="stretch",
                  on_click=change, args=(lambda: source.drop_column("products", "weight_kg"), "Revert products"))
    else:
        st.button("🔒 ALTER products ADD weight_kg", width="stretch",
                  help="products has schema_evolution = fail: the change is rejected and the table stops.",
                  on_click=change, args=(lambda: source.add_column("products", "weight_kg", "DECIMAL(6,2)"),
                                         "Schema drift on products"))

    st.header("2 · Chaos (optional)")
    st.radio("Crash the next run", list(CHAOS), key="chaos_choice",
             help="Kill a stage between its Delta commit and its checkpoint commit, then rerun to see "
                  "exactly-once recovery.")

    st.header("3 · Run the DAG")
    st.button("▶ Trigger DAG run", type="primary", width="stretch", on_click=trigger_run)
    for table in gate_tripped(last_run()):
        st.button(f"✅ Accept the tripped {table} batch", width="stretch", on_click=trigger_run, args=(table,),
                  help="Operator override: rerun the same batch with the threshold lifted. Invalid rows stay "
                       "quarantined, valid rows reach silver.")
    st.divider()
    st.button("Reset demo", on_click=reset, width="stretch")
    st.caption(f"[Source code]({REPO_URL}) · MIT")


# ---- header -------------------------------------------------------------------------------

st.title("⚡ SparkNerve")
st.markdown(
    "**Metadata → Airflow → Spark → Data Quality Engine → Delta Lake**, observed with Prometheus. "
    "One JSON file defines the tables, keys, partitions and validation rules; the DAG, the ingestion jobs and the "
    "quality checks are generated from it. This page runs the real stages on delta-rs + DuckDB (no JVM), "
    f"against real Delta tables. [Code and the full Docker stack]({REPO_URL})."
)
if S.get("flash"):
    text, icon = S.flash
    st.toast(text, icon=icon)
    S.flash = None

run = last_run()
cols = st.columns(5)
silver_rows = sum((layer_table("silver", t).num_rows if layer_table("silver", t) is not None else 0) for t in TABLES)
quarantined = sum((layer_table("quarantine", t).num_rows if layer_table("quarantine", t) is not None else 0)
                  for t in TABLES)
cols[0].metric("DAG runs", len(engine.runs))
cols[1].metric("Last run", run.state.upper() if run else "—")
cols[2].metric("Rows in silver", f"{silver_rows:,}")
cols[3].metric("Rows in quarantine", f"{quarantined:,}")
cols[4].metric("Source rows (SQL Server)", f"{sum(source.count(t) for t in TABLES):,}")

tabs = st.tabs(["🗺️ DAG", "🧪 Data quality", "🏞️ Delta Lake", "🛟 Recovery", "📈 Observability", "🧾 Audit",
                "🗄️ Source", "ℹ️ How it works"])


# ---- DAG ----------------------------------------------------------------------------------

with tabs[0]:
    left, right = st.columns([3, 2])
    states = {r.task.task_id: r for r in run.results} if run else {}

    def node(task_id: str, label: str) -> str:
        result = states.get(task_id)
        color = STATE_COLORS[result.state if result else None]
        detail = ""
        if result and result.runs:
            last = result.runs[-1]
            if last.stage == "extract":
                detail = f"\\n{last.rows_written} rows" if last.status != "NO_DATA" else "\\nno new rows"
            elif last.stage == "validate":
                detail = (f"\\n{sum(r.rows_written for r in result.runs)} merged · "
                          f"{sum(r.rows_quarantined for r in result.runs)} quarantined")
        elif result:
            detail = "\\nupstream failed"
        return f'"{task_id}" [label="{label}{detail}", fillcolor="{color}"];'

    lines = ['digraph { rankdir=LR; bgcolor="transparent";',
             'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11];']
    for table in TABLES:
        lines.append(f'subgraph cluster_{table} {{ label="{table}"; fontname="Helvetica"; style="rounded"; '
                     f'color="#90a4ae"; {node(f"{table}.extract", "extract")} '
                     f'{node(f"{table}.validate", "validate")} }}')
    lines.append(node("run_report", "run_report"))
    lines += [f'"{a}" -> "{b}";' for a, b in engine.plan.edges()]
    lines.append("}")
    with left:
        st.subheader("Generated DAG · sparknerve__retail_sales")
        st.graphviz_chart("\n".join(lines))
        st.caption("Green = success · red = failed · orange = upstream failed. orders.validate waits for customers "
                   "and products because its foreign-key rules check against their silver tables: that edge comes "
                   "from the metadata, not from DAG code.")
    with right:
        st.subheader("Last run")
        if run:
            st.caption(f"`{run.run_id}` · as-of {run.as_of:%Y-%m-%d %H:%M:%S} UTC")
            for result in run.results:
                icon = {"success": "✅", "failed": "❌", "upstream_failed": "⏭️"}[result.state]
                with st.expander(f"{icon} {result.task.task_id}", expanded=result.state == "failed"):
                    for stage_run in result.runs:
                        st.write(stage_run.summary())
                    if not result.runs:
                        st.write("Not run: an upstream task failed (Airflow state `upstream_failed`).")
                    st.code(cli_command(PIPELINE, result.task, run.run_id), language="bash")
    with st.expander("The metadata that generated all of this (metadata/pipelines/retail_sales.json)"):
        st.json(json.loads((ROOT / "metadata" / "pipelines" / "retail_sales.json").read_text(encoding="utf-8")))


# ---- Data quality ---------------------------------------------------------------------------

with tabs[1]:
    table = st.segmented_control("Table", TABLES, default="orders", key="dq_table") or "orders"
    spec = engine.pipeline.table(table)
    audit = engine.audit_log().to_pylist()
    validations = [r for r in audit if r["table_name"] == table and r["stage"] == "validate" and r["rows_read"]]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows validated", sum(r["rows_read"] for r in validations))
    c2.metric("Merged into silver", sum(r["rows_written"] for r in validations))
    c3.metric("Quarantined", sum(r["rows_quarantined"] for r in validations))
    c4.metric("Passed with warnings", sum(r["rows_warned"] for r in validations))
    failures: dict[str, int] = {}
    for r in validations:
        if r["status"] == "SUCCEEDED" or "Quality gate" in (r["error"] or ""):
            for rule, count in json.loads(r["rule_failures"] or "{}").items():
                failures[rule] = failures.get(rule, 0) + count
    left, right = st.columns([2, 3])
    with left:
        st.subheader("Failures by rule")
        if failures:
            st.bar_chart(pd.DataFrame({"rule": list(failures), "rows": list(failures.values())}).set_index("rule"),
                         horizontal=True)
        else:
            st.info("No rule has failed yet. Inject bad records in the sidebar, then trigger a run.")
    with right:
        st.subheader("Quarantine")
        quarantine = layer_table("quarantine", table)
        if quarantine is None or quarantine.num_rows == 0:
            st.info("Empty: nothing has been rejected for this table.")
        else:
            frame = arrow_frame(quarantine, ["_quarantined_at"])
            first = ["_dq_errors", *spec.primary_key]
            st.dataframe(frame[first + [c for c in frame.columns if c not in first]], hide_index=True, height=300)
    st.subheader("Rules, compiled from JSON to SQL")
    st.caption("Each rule compiles to one predicate that is TRUE when a row violates it. The Spark jobs run the "
               "Spark SQL column; this page runs the DuckDB column. NULLs only fail not_null rules.")
    as_of = run.as_of if run else source.now
    spark_sql = {c.name: c.failed_sql for c in compile_rules(spec.rules, "spark", as_of)}
    duck_sql = {c.name: c.failed_sql for c in compile_rules(spec.rules, "duckdb", as_of)}
    st.dataframe(pd.DataFrame([{
        "rule": r.name, "type": r.type, "column": r.column, "severity": r.severity,
        "implicit": r.implicit, "Spark SQL (violation)": spark_sql[r.name], "DuckDB SQL (violation)": duck_sql[r.name],
    } for r in spec.rules]), hide_index=True)
    st.caption(f"Circuit breaker: a batch of at least {spec.gate_min_rows} rows with more than "
               f"{spec.quarantine_threshold:.0%} invalid rows is rejected. Silver stays untouched and the batch is "
               "retried until fixed or accepted.")


# ---- Delta Lake -------------------------------------------------------------------------------

with tabs[2]:
    c1, c2 = st.columns(2)
    layer = c1.segmented_control("Layer", ["bronze", "silver", "quarantine"], default="silver", key="lake_layer") \
        or "silver"
    table = c2.segmented_control("Table ", TABLES, default="orders", key="lake_table") or "orders"
    path = getattr(engine.lake, layer)(PIPELINE, table)
    if not engine.exists(path):
        st.info(f"{layer}/{table} does not exist yet.")
    else:
        delta = DeltaTable(local(path))
        latest = delta.version()
        version = latest
        if latest > 0:
            version = st.slider("Time travel: table version", 0, latest, latest, key=f"tt_{layer}_{table}")
        data = engine.read(path, version)
        st.caption(f"`{path.replace(str(Path(tempfile.gettempdir())), '<tmp>')}` · version {version} of {latest} · "
                   f"{data.num_rows:,} rows · {len(data.column_names)} columns")
        spec = engine.pipeline.table(table)
        st.dataframe(arrow_frame(data, [spec.watermark_column]), hide_index=True, height=320)

        left, right = st.columns([3, 2])
        with left:
            st.subheader("Transaction log")
            history = []
            for h in delta.history():
                metrics = h.get("operationMetrics") or {}
                keep = {k: v for k, v in metrics.items() if k in (
                    "num_added_rows", "num_target_rows_inserted", "num_target_rows_updated",
                    "num_target_files_scanned", "num_target_files_skipped_during_scan", "num_target_files_removed")}
                history.append({"version": h.get("version"), "operation": h.get("operation"),
                                "run_id": h.get("sparknerve.run_id"), "idempotent txn": h.get("sparknerve.txn"),
                                "metrics": json.dumps(keep) if keep else ""})
            st.dataframe(pd.DataFrame(history), hide_index=True)
        with right:
            st.subheader("Files and partitions")
            actions = to_table(delta.get_add_actions(flatten=True))
            parts = [c for c in actions.column_names if c.startswith("partition.")]
            st.metric("Data files (current version)", actions.num_rows)
            if parts:
                values = actions[parts[0]].to_pylist()
                st.metric(f"Partitions ({parts[0].split('.', 1)[1]})", len(set(values)))
            last_validate = next((r for r in reversed(audit) if r["table_name"] == table and r["stage"] == "validate"
                                  and r["status"] == "SUCCEEDED" and r["partitions_touched"] != "[]"), None) \
                if layer == "silver" else None
            if last_validate:
                touched = json.loads(last_validate["partitions_touched"])
                st.write(f"Last MERGE was pruned to **{len(touched)}** partition(s): {', '.join(touched[:8])}"
                         f"{' …' if len(touched) > 8 else ''}")
                if last_validate["note"]:
                    st.caption(last_validate["note"])


# ---- Recovery ------------------------------------------------------------------------------------

with tabs[3]:
    st.markdown(
        "Two checkpoints make every stage safe to crash and rerun:\n"
        "* **Extract** plans its window (`planned_seq`, high watermark) *before* reading SQL Server and commits it "
        "*after* the bronze append. The append carries Delta `txnAppId`/`txnVersion = seq`, so a replay of a window "
        "that already landed is skipped.\n"
        "* **Validate** uses the Structured Streaming checkpoint (offsets written before a micro-batch, commits after "
        "it). Quarantine appends carry `txnVersion = batch id`; the silver MERGE only applies newer watermarks. A "
        "replayed batch changes nothing.\n\n"
        "Try it: pick a crash in the sidebar, trigger a run, then trigger another one."
    )
    rows = []
    for table in TABLES:
        state = engine.watermark_state(table)
        pending = engine.checkpoint(table).uncommitted()
        rows.append({
            "table": table, "committed seq": state.committed_seq, "committed watermark": state.committed_watermark,
            "planned seq": state.planned_seq, "extract pending": state.pending,
            "validate batches": len(engine.checkpoint(table).log()),
            "validate pending batch": pending["batch_id"] if pending else None,
        })
    st.subheader("Checkpoint state")
    st.dataframe(pd.DataFrame(rows), hide_index=True)
    table = st.segmented_control("Streaming checkpoint log", TABLES, default="orders", key="ckpt_table") or "orders"
    st.dataframe(pd.DataFrame(engine.checkpoint(table).log()), hide_index=True)
    recovered = [r for r in engine.audit_log().to_pylist() if r["recovered"]]
    st.subheader("Recovered stages")
    if recovered:
        st.dataframe(pd.DataFrame(recovered)[["finished_at", "table_name", "stage", "status", "batch_id", "note"]],
                     hide_index=True)
    else:
        st.info("No recovery yet. Arm a crash in the sidebar and trigger two runs.")


# ---- Observability ----------------------------------------------------------------------------------

with tabs[4]:
    audit_rows = engine.audit_log().to_pylist()
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Alert rules (docker/prometheus/alerts.yml) on the last run")
        alerts = []
        if run:
            for result in run.results:
                for stage_run in result.runs:
                    if stage_run.table == "_all":
                        continue
                    if not stage_run.succeeded:
                        alerts.append(("SparkNerveStageFailed", "critical", f"{stage_run.table}.{stage_run.stage}"))
                    if stage_run.stage == "validate" and stage_run.quarantine_ratio > 0.05:
                        alerts.append(("SparkNerveQuarantineRatioHigh", "warning",
                                       f"{stage_run.table}: {stage_run.quarantine_ratio:.0%} quarantined"))
                    if stage_run.schema_changes:
                        alerts.append(("SparkNerveSchemaChanged", "info",
                                       f"{stage_run.table}.{stage_run.stage}: {', '.join(stage_run.schema_changes)}"))
            if run.state != "success":
                alerts.append(("SparkNervePipelineFailed", "critical", PIPELINE))
        if alerts:
            st.dataframe(pd.DataFrame(alerts, columns=["alert", "severity", "labels"]), hide_index=True)
        else:
            st.success("No alert is firing.")
        st.subheader("Rows per run")
        runs_frame = pd.DataFrame([r for r in audit_rows if r["stage"] == "validate" and r["rows_read"]])
        if not runs_frame.empty:
            runs_frame["run"] = runs_frame["finished_at"].dt.strftime("%H:%M:%S")
            chart = runs_frame.pivot_table(index="run", columns="table_name", values="rows_written", aggfunc="sum")
            st.bar_chart(chart)
            quarantine_chart = runs_frame.pivot_table(index="run", columns="table_name", values="rows_quarantined",
                                                      aggfunc="sum")
            st.caption("Quarantined rows per run")
            st.bar_chart(quarantine_chart)
    with right:
        st.subheader("Prometheus metrics pushed by the last run")
        if run:
            choice = st.selectbox("Task", [r.task.task_id for r in run.results if r.metrics])
            st.code(next(r.metrics for r in run.results if r.task.task_id == choice), language="text")
        st.caption("In the Docker stack each stage pushes this registry to the Pushgateway; Prometheus scrapes it "
                   "and Grafana charts it (docker/grafana/dashboards/sparknerve.json).")


# ---- Audit ----------------------------------------------------------------------------------------------

with tabs[5]:
    st.subheader("audit/stage_runs (Delta)")
    st.caption("Every stage writes one row, success or failure: run id, window, versions, rows, rule failures, "
               "schema changes, recovery and errors.")
    audit_frame = arrow_frame(engine.audit_log())
    st.dataframe(audit_frame, hide_index=True, height=520)


# ---- Source -----------------------------------------------------------------------------------------------

with tabs[6]:
    table = st.segmented_control("Source table", TABLES, default="orders", key="src_table") or "orders"
    spec = engine.pipeline.table(table)
    src_rows = source.rows(table)
    silver = layer_table("silver", table)
    quarantine = layer_table("quarantine", table)
    source_keys = {tuple(r[k] for k in spec.primary_key) for r in src_rows}
    silver_keys = {tuple(r[k] for k in spec.primary_key) for r in silver.to_pylist()} if silver is not None else set()
    bad_keys = {tuple(r[k] for k in spec.primary_key) for r in quarantine.to_pylist()} if quarantine is not None \
        else set()
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows in sales." + table, len(source_keys))
    c2.metric("Keys in silver", len(silver_keys))
    c3.metric("Source keys never valid", len(source_keys - silver_keys),
              help="Rows still waiting for their first valid version (quarantined or not yet extracted).")
    st.caption(f"`{spec.source_table}` columns: " + ", ".join(f"{c} {t}" for c, t in source.columns(table).items()))
    st.dataframe(pd.DataFrame(src_rows).sort_values(spec.watermark_column, ascending=False), hide_index=True,
                 height=360)
    st.subheader("Activity")
    st.code("\n".join(S.activity[:40]), language="text")


# ---- How it works ------------------------------------------------------------------------------------------

with tabs[7]:
    st.markdown(f"""
**A guided tour (2 minutes)**
1. **🛒 Business as usual → ▶ Trigger DAG run.** Only changed rows are extracted (watermark window), and the silver
   MERGE touches only the partitions in the batch (Delta Lake tab → *Last MERGE was pruned to …*).
2. **🧪 Inject 6 bad records → ▶ run.** Invalid rows land in quarantine with the rules they broke; valid rows keep
   flowing. Warning-only rules let a row through and record the warning on it.
3. **💥 Broken upstream deploy → ▶ run.** 24 bad orders trip the circuit breaker: silver is untouched, the batch
   stays uncommitted and is retried. **✅ Accept** replays the *same* batch with the threshold lifted.
4. **🧬 ALTER customers ADD loyalty_tier → ▶ run.** Automatic schema evolution: bronze and silver gain the column.
   **🔒 ALTER products …** is rejected, because products is declared `schema_evolution: fail`.
5. **Chaos → crash → ▶ run twice.** The rerun replays the planned window / uncommitted batch; Delta's idempotent
   transactions skip what already landed. No loss, no duplicates (Recovery tab).
6. **🩹 Fix at the source → ▶ run.** Corrected rows are new versions, so they flow into silver on their own.

**What is real here:** Delta tables (delta-rs), the metadata, the generated plan, the compiled rules (DuckDB),
checkpoints, idempotent commits, audit rows and Prometheus exposition. **What is simulated:** SQL Server (SQLite
with the same schema) and Spark (the same stages on delta-rs). The Spark jobs, the Airflow DAG factory and the
Docker stack (SQL Server, Airflow, Spark, Prometheus, Pushgateway, Grafana) are in [the repository]({REPO_URL}).
""")
