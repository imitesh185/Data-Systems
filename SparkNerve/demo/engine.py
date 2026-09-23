"""SparkNerve's stages on delta-rs + DuckDB (no JVM), for the hosted live demo.

The engine runs the same metadata, planner, compiled DQ rules (DuckDB dialect),
checkpoint protocol, schema-evolution policy, quality gate, audit record and
Prometheus metrics as the Spark jobs, against real Delta tables on disk. Only
the compute engine is swapped:

    Spark job (sparknerve/spark)                    this engine
    ----------------------------------------------  -----------------------------------------------
    JDBC read of the window from SQL Server         SQL read of the window from the RetailDB simulator
    Delta append with txnAppId / txnVersion          delta-rs append + txn action, same skip check
    Structured Streaming availableNow + checkpoint   the same offsets/commits log; rows read via CDF
    rule predicates in Spark SQL                     the same rule predicates in DuckDB SQL
    Delta MERGE with a partition predicate           delta-rs MERGE with the same predicate
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from deltalake.transaction import CommitProperties, Transaction

from simulator.retail import RetailDB
from sparknerve.audit import AUDIT_SCHEMA, FAILED, NO_DATA, SUCCEEDED, StageRun, describe_error
from sparknerve.checkpoint import WATERMARK_SCHEMA, WatermarkState, next_window
from sparknerve.evolution import (
    SILVER_COLUMNS,
    TECHNICAL_COLUMNS,
    business_columns,
    diff_schema,
    enforce_policy,
)
from sparknerve.metadata import Pipeline, TableSpec
from sparknerve.observability import exposition, pipeline_registry, stage_registry
from sparknerve.planner import EXTRACT, REPORT, VALIDATE, Plan, Task, build_plan
from sparknerve.quality import check_gate
from sparknerve.rules import compile_rules, failure_columns, quote_identifier, sql_literal
from sparknerve.settings import LakeLayout, app_id

UTC_TS = pa.timestamp("us", tz="UTC")
MAX_PRUNING_VALUES = 1000
DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
CHAOS_POINTS = {
    EXTRACT: "after the bronze commit, before the watermark commit",
    VALIDATE: "after the silver MERGE, before the checkpoint commit",
}


class SimulatedCrash(RuntimeError):
    """Raised at a chaos point to prove that a rerun recovers exactly once."""

    audit_verbatim = True


# ---- type mapping (what Spark's SQL Server JDBC dialect produces) -----------------

def lake_type(sql_type: str) -> str:
    kind = sql_type.strip().upper()
    base = kind.split("(")[0].strip()
    if base in ("INT", "INTEGER"):
        return "int"
    if base == "BIGINT":
        return "bigint"
    if base in ("SMALLINT", "TINYINT"):
        return "smallint"
    if base in ("DECIMAL", "NUMERIC"):
        precision, scale = re.findall(r"\d+", kind)[:2] if "(" in kind else ("18", "0")
        return f"decimal({precision},{scale})"
    if base == "BIT":
        return "boolean"
    if base == "DATE":
        return "date"
    if base in ("DATETIME", "DATETIME2", "SMALLDATETIME"):
        return "timestamp"
    if base == "FLOAT":
        return "double"
    if base == "REAL":
        return "float"
    return "string"


def arrow_type(kind: str) -> pa.DataType:
    match = DECIMAL.match(kind)
    if match:
        return pa.decimal128(int(match[1]), int(match[2]))
    return {
        "int": pa.int32(), "bigint": pa.int64(), "smallint": pa.int16(), "boolean": pa.bool_(),
        "date": pa.date32(), "timestamp": UTC_TS, "double": pa.float64(), "float": pa.float32(),
        "string": pa.string(), "array<string>": pa.list_(pa.string()),
    }[kind]


def type_name(arrow: pa.DataType) -> str:
    """Arrow type -> Spark SQL type name (the names schema evolution compares)."""
    if pa.types.is_decimal(arrow):
        return f"decimal({arrow.precision},{arrow.scale})"
    if pa.types.is_timestamp(arrow):
        return "timestamp"
    if pa.types.is_date(arrow):
        return "date"
    if pa.types.is_int64(arrow):
        return "bigint"
    if pa.types.is_int16(arrow) or pa.types.is_int8(arrow):
        return "smallint"
    if pa.types.is_integer(arrow):
        return "int"
    if pa.types.is_float32(arrow):
        return "float"
    if pa.types.is_floating(arrow):
        return "double"
    if pa.types.is_boolean(arrow):
        return "boolean"
    if pa.types.is_list(arrow) or pa.types.is_large_list(arrow):
        return "array<string>"
    return "string"


def convert(value: object, kind: str) -> object:
    """A value as the SQL Server driver would hand it to Spark, typed per column."""
    if value is None:
        return None
    if kind in ("int", "bigint", "smallint"):
        return int(value)
    if kind == "boolean":
        return bool(int(value))
    if kind == "date":
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if kind == "timestamp":
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    if kind in ("double", "float"):
        return float(value)
    match = DECIMAL.match(kind)
    if match:
        return Decimal(str(value)).quantize(Decimal(1).scaleb(-int(match[2])))
    return str(value)


def schema_table(schema: tuple[tuple[str, str], ...], rows: list[dict]) -> pa.Table:
    return pa.table({name: pa.array([r.get(name) for r in rows], arrow_type(kind)) for name, kind in schema})


def _plain_type(kind: pa.DataType) -> pa.DataType:
    if pa.types.is_string_view(kind) or pa.types.is_large_string(kind):
        return pa.string()
    if pa.types.is_list_view(kind) or pa.types.is_large_list(kind) or pa.types.is_list(kind):
        return pa.list_(_plain_type(kind.value_type))
    return kind


def to_table(data) -> pa.Table:
    """Any Arrow-compatible result as a pyarrow Table with plain (non-view, non-large) types."""
    if isinstance(data, pa.RecordBatchReader):
        data = data.read_all()
    elif not isinstance(data, pa.Table):
        data = pa.table(data)
    plain = pa.schema([f.with_type(_plain_type(f.type)) for f in data.schema])
    return data if plain.equals(data.schema) else data.cast(plain)


def fetch(con: duckdb.DuckDBPyConnection, sql: str) -> pa.Table:
    return to_table(con.execute(sql).arrow())


def local(path: str) -> str:
    return str(Path(path))


# ---- streaming checkpoint (the layout Structured Streaming uses) ----------------

class StreamCheckpoint:
    """offsets/<id> is written before a micro-batch runs and commits/<id> after its
    sinks committed. An offsets entry without a commit is a batch to replay."""

    def __init__(self, path: str):
        self.root = Path(path)
        self.offsets = self.root / "offsets"
        self.commits = self.root / "commits"

    def _ids(self, folder: Path) -> list[int]:
        return sorted(int(p.stem) for p in folder.glob("*.json")) if folder.exists() else []

    def _read(self, batch_id: int) -> dict:
        return json.loads((self.offsets / f"{batch_id}.json").read_text(encoding="utf-8"))

    def uncommitted(self) -> dict | None:
        offsets, commits = self._ids(self.offsets), set(self._ids(self.commits))
        if offsets and offsets[-1] not in commits:
            return self._read(offsets[-1])
        return None

    def last_end_version(self) -> int:
        offsets = self._ids(self.offsets)
        return self._read(offsets[-1])["end_version"] if offsets else -1

    def plan(self, end_version: int) -> dict:
        offsets = self._ids(self.offsets)
        batch = {"batch_id": offsets[-1] + 1 if offsets else 0, "start_version": self.last_end_version(),
                 "end_version": end_version}
        self.offsets.mkdir(parents=True, exist_ok=True)
        (self.offsets / f"{batch['batch_id']}.json").write_text(json.dumps(batch), encoding="utf-8")
        return batch

    def commit(self, batch_id: int) -> None:
        self.commits.mkdir(parents=True, exist_ok=True)
        (self.commits / f"{batch_id}.json").write_text(json.dumps({"batch_id": batch_id}), encoding="utf-8")

    def log(self) -> list[dict]:
        commits = set(self._ids(self.commits))
        return [{**self._read(i), "committed": i in commits} for i in self._ids(self.offsets)]


# ---- results --------------------------------------------------------------------

@dataclass
class TaskResult:
    task: Task
    state: str  # success | failed | upstream_failed
    runs: list[StageRun] = field(default_factory=list)
    metrics: str = ""

    @property
    def error(self) -> str | None:
        return next((r.error for r in self.runs if r.error), None)


@dataclass
class PipelineRun:
    run_id: str
    as_of: datetime
    results: list[TaskResult]

    @property
    def state(self) -> str:
        return "success" if all(r.state == "success" for r in self.results) else "failed"

    def result(self, task_id: str) -> TaskResult:
        return next(r for r in self.results if r.task.task_id == task_id)


@dataclass
class MergeOutcome:
    inserted: int = 0
    updated: int = 0
    files_rewritten: int = 0
    files_scanned: int | None = None
    files_skipped: int | None = None
    partitions: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    version: int | None = None


class DemoEngine:
    def __init__(self, lake_root: str | Path, pipeline: Pipeline, source: RetailDB):
        self.lake = LakeLayout(str(lake_root))
        self.pipeline = pipeline
        self.source = source
        self.plan: Plan = build_plan(pipeline)
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone = 'UTC'")
        self.chaos: dict[str, str] = {}             # task_id -> armed crash (one-shot)
        self.gate_overrides: dict[str, float] = {}  # table -> threshold for the next validate (one-shot)
        self.runs: list[PipelineRun] = []

    # ---- orchestration (Airflow semantics) -------------------------------------

    def run_pipeline(self, run_id: str | None = None, as_of: datetime | None = None) -> PipelineRun:
        as_of = as_of or self.source.now.replace(tzinfo=timezone.utc)
        run_id = run_id or f"manual__{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%S.%f}"
        states: dict[str, str] = {}
        results: list[TaskResult] = []
        for task in self.plan.topological():
            if task.stage != REPORT and any(states[u] != "success" for u in task.upstream):
                result = TaskResult(task, "upstream_failed")
            elif task.stage == EXTRACT:
                run = self.extract(self.pipeline.table(task.table), run_id)
                result = TaskResult(task, "success" if run.succeeded else "failed", [run], self._metrics(run))
            elif task.stage == VALIDATE:
                runs = self.validate(self.pipeline.table(task.table), run_id, as_of)
                ok = all(r.succeeded for r in runs)
                result = TaskResult(task, "success" if ok else "failed", runs, self._metrics(runs[-1]))
            else:
                result = self.report(task, run_id, results)
            states[task.task_id] = result.state
            results.append(result)
        pipeline_run = PipelineRun(run_id, as_of, results)
        self.runs.append(pipeline_run)
        return pipeline_run

    def _metrics(self, run: StageRun) -> str:
        spec = self.pipeline.table(run.table)
        layers = {"bronze": run.delta_version} if run.stage == EXTRACT else {
            "silver": run.delta_version, "quarantine": run.quarantine_version}
        return exposition(stage_registry(run, spec.severities, layers))

    def _crash_point(self, table: str, stage: str) -> None:
        task = f"{table}.{stage}"
        if self.chaos.pop(task, None):
            raise SimulatedCrash(f"Simulated crash in {task} {CHAOS_POINTS[stage]}")

    # ---- Delta helpers ------------------------------------------------------------

    def exists(self, path: str) -> bool:
        return DeltaTable.is_deltatable(local(path))

    def read(self, path: str, version: int | None = None) -> pa.Table:
        return to_table(DeltaTable(local(path), version=version).to_pyarrow_table())

    def schema_of(self, path: str) -> dict[str, str] | None:
        if not self.exists(path):
            return None
        return {f.name: type_name(f.type) for f in DeltaTable(local(path)).to_pyarrow_dataset().schema}

    def version(self, path: str) -> int | None:
        return DeltaTable(local(path)).version() if self.exists(path) else None

    def _append(self, path: str, data: pa.Table, run_id: str, txn: tuple[str, int] | None = None,
                partition_by: list[str] | None = None, cdf: bool = False) -> tuple[bool, int]:
        """Append, idempotent when `txn` is given (Delta's txnAppId/txnVersion rule:
        skip if the table already holds a commit for this app at >= this version)."""
        target = local(path)
        exists = DeltaTable.is_deltatable(target)
        if exists and txn is not None:
            committed = DeltaTable(target).transaction_version(txn[0])
            if committed is not None and committed >= txn[1]:
                return False, DeltaTable(target).version()
        metadata = {"sparknerve.run_id": run_id}
        transactions = None
        if txn is not None:
            metadata["sparknerve.txn"] = f"{txn[0]}@{txn[1]}"
            transactions = [Transaction(app_id=txn[0], version=txn[1])]
        write_deltalake(
            target,
            data,
            mode="append",
            schema_mode="merge" if exists else None,
            partition_by=None if exists else partition_by,
            configuration=None if exists or not cdf else {"delta.enableChangeDataFeed": "true"},
            commit_properties=CommitProperties(app_transactions=transactions, custom_metadata=metadata),
        )
        return True, DeltaTable(target).version()

    # ---- control state ------------------------------------------------------------

    def watermark_state(self, table: str) -> WatermarkState:
        path = self.lake.watermark(self.pipeline.name, table)
        if not self.exists(path):
            return WatermarkState()
        rows = self.read(path).to_pylist()
        return WatermarkState.from_record(rows[0]) if rows else WatermarkState()

    def _save_watermark(self, table: str, state: WatermarkState) -> None:
        data = schema_table(WATERMARK_SCHEMA, [state.to_record(self.pipeline.name, table)])
        write_deltalake(local(self.lake.watermark(self.pipeline.name, table)), data, mode="overwrite")

    def checkpoint(self, table: str) -> StreamCheckpoint:
        return StreamCheckpoint(local(self.lake.checkpoint(self.pipeline.name, table, VALIDATE)))

    def _audit(self, run: StageRun) -> None:
        self._append(self.lake.audit, schema_table(AUDIT_SCHEMA, [run.to_record()]), run.run_id)

    def audit_log(self) -> pa.Table:
        if not self.exists(self.lake.audit):
            return schema_table(AUDIT_SCHEMA, [])
        return self.read(self.lake.audit).sort_by([("finished_at", "descending")])

    # ---- extract: source -> bronze -------------------------------------------------

    def source_schema(self, spec: TableSpec) -> dict[str, str]:
        return {name: lake_type(kind) for name, kind in self.source.columns(spec.name).items()}

    def _source_high(self, spec: TableSpec) -> str | None:
        rows = self.source.db.query(f"SELECT MAX({spec.watermark_column}) AS hwm FROM {spec.source_table}")
        return rows[0]["hwm"] if rows else None

    def _read_window(self, spec: TableSpec, low: str | None, high: str, schema: dict[str, str]) -> pa.Table:
        wm = spec.watermark_column
        where, params = f"{wm} <= ?", [high]
        if low is not None:
            where, params = f"{wm} > ? AND {wm} <= ?", [low, high]
        rows = self.source.db.query(
            f"SELECT * FROM {spec.source_table} WHERE {where} ORDER BY {', '.join(spec.primary_key)}", tuple(params))
        return pa.table({c: pa.array([convert(r.get(c), k) for r in rows], arrow_type(k)) for c, k in schema.items()})

    def extract(self, spec: TableSpec, run_id: str) -> StageRun:
        run = StageRun(run_id, self.pipeline.name, spec.name, EXTRACT)
        bronze = self.lake.bronze(self.pipeline.name, spec.name)
        try:
            state = self.watermark_state(spec.name)
            window = next_window(state, lambda: self._source_high(spec))
            if window is None:
                run.watermark_from = run.watermark_to = state.committed_watermark
                run.delta_version = self.version(bronze)
                run.note = "no source rows newer than the watermark"
                return self._finish(run, NO_DATA)
            run.batch_id, run.recovered = window.seq, window.recovered
            run.watermark_from, run.watermark_to = window.low, window.high

            incoming = self.source_schema(spec)
            previous = state.last_source_schema or business_columns(self.schema_of(bronze) or {}) or None
            changes = enforce_policy(spec, diff_schema(previous, incoming))
            run.schema_changes = [c.describe() for c in changes]

            if not window.recovered:
                state = state.plan(window.seq, window.high)
                self._save_watermark(spec.name, state)

            data = self._read_window(spec, window.low, window.high, incoming)
            now = datetime.now(timezone.utc)
            data = (data.append_column("_run_id", pa.array([run_id] * data.num_rows, pa.string()))
                    .append_column("_extract_seq", pa.array([window.seq] * data.num_rows, pa.int64()))
                    .append_column("_ingested_at", pa.array([now] * data.num_rows, UTC_TS))
                    .append_column("_ingest_date", pa.array([now.date()] * data.num_rows, pa.date32())))
            written, run.delta_version = self._append(
                bronze, data, run_id, txn=(app_id(self.pipeline.name, spec.name, "bronze"), window.seq),
                partition_by=["_ingest_date"], cdf=True)
            run.rows_read = data.num_rows
            run.rows_written = data.num_rows if written else 0
            if not written:
                run.note = f"bronze already holds seq {window.seq}: append skipped (idempotent txn)"
            self._crash_point(spec.name, EXTRACT)
            self._save_watermark(spec.name, state.commit(incoming))
            return self._finish(run, SUCCEEDED)
        except Exception as exc:  # noqa: BLE001 - every failure is audited
            return self._finish(run, FAILED, exc)

    # ---- validate: bronze -> DQ -> quarantine + silver -------------------------------

    def validate(self, spec: TableSpec, run_id: str, as_of: datetime) -> list[StageRun]:
        bronze = self.lake.bronze(self.pipeline.name, spec.name)
        if not self.exists(bronze):
            run = StageRun(run_id, self.pipeline.name, spec.name, VALIDATE, note="bronze table does not exist yet")
            return [self._finish(run, NO_DATA)]
        checkpoint = self.checkpoint(spec.name)
        runs: list[StageRun] = []
        pending = checkpoint.uncommitted()
        if pending:
            runs.append(self._validate_batch(spec, run_id, as_of, pending, checkpoint, recovered=True))
            if not runs[-1].succeeded:
                return runs
        latest = DeltaTable(local(bronze)).version()
        if latest > checkpoint.last_end_version():
            batch = checkpoint.plan(latest)
            runs.append(self._validate_batch(spec, run_id, as_of, batch, checkpoint, recovered=False))
        if not runs:
            run = StageRun(run_id, self.pipeline.name, spec.name, VALIDATE, note="no new bronze commits")
            run.delta_version = self.version(self.lake.silver(self.pipeline.name, spec.name))
            runs.append(self._finish(run, NO_DATA))
        return runs

    def _bronze_rows(self, path: str, start_version: int, end_version: int) -> pa.Table:
        """Rows appended to bronze in (start_version, end_version], like the Delta streaming source."""
        changes = to_table(DeltaTable(local(path)).load_cdf(starting_version=start_version + 1,
                                                           ending_version=end_version))
        changes = changes.filter(pc.equal(changes["_change_type"], "insert"))
        return changes.drop_columns([c for c in ("_change_type", "_commit_version", "_commit_timestamp")
                                     if c in changes.column_names])

    def _lookup_table(self, spec: TableSpec, ref_table: str, ref_column: str, local_type: pa.DataType) -> pa.Table:
        path = self.lake.silver(self.pipeline.name, ref_table)
        if self.exists(path):
            return to_table(DeltaTable(local(path)).to_pyarrow_dataset().to_table(columns=[ref_column]))
        return pa.table({ref_column: pa.array([], local_type)})

    def apply_rules(self, spec: TableSpec, rows: pa.Table, as_of: datetime) -> tuple[pa.Table, dict[str, int]]:
        """Evaluate every rule; adds _dq_errors / _dq_warnings (rule names, in metadata order)."""
        compiled = compile_rules(spec.rules, "duckdb", as_of, row_alias="b")
        flag_names = failure_columns(compiled)
        self.con.register("batch", rows)
        joins = []
        for i, rule in enumerate(c for c in compiled if c.lookup):
            lk = rule.lookup
            ref = self._lookup_table(spec, lk.table, lk.column, rows.schema.field(lk.local_column).type)
            self.con.register(f"ref_{i}", ref)
            joins.append(
                f"LEFT JOIN (SELECT DISTINCT {quote_identifier(lk.column, 'duckdb')} AS "
                f"{quote_identifier(lk.key_alias, 'duckdb')}, TRUE AS {quote_identifier(lk.flag_alias, 'duckdb')} "
                f"FROM ref_{i}) AS fk_{i} ON b.{quote_identifier(lk.local_column, 'duckdb')} = "
                f"fk_{i}.{quote_identifier(lk.key_alias, 'duckdb')}")
        flags = ", ".join(f"{c.failed_sql} AS {quote_identifier(flag_names[c.name], 'duckdb')}" for c in compiled)
        result = fetch(self.con, f"SELECT b.*{', ' + flags if flags else ''} FROM batch AS b {' '.join(joins)}")
        self.con.unregister("batch")

        values = {c.name: result[flag_names[c.name]].to_pylist() for c in compiled}

        def failed(severity: str, row: int) -> list[str]:
            return [c.name for c in compiled if c.severity == severity and values[c.name][row]]

        errors = [failed("error", i) for i in range(result.num_rows)]
        warnings = [failed("warn", i) for i in range(result.num_rows)]
        failures = {c.name: sum(1 for v in values[c.name] if v) for c in compiled}
        checked = result.drop_columns(list(flag_names.values()))
        checked = checked.append_column("_dq_errors", pa.array(errors, pa.list_(pa.string())))
        checked = checked.append_column("_dq_warnings", pa.array(warnings, pa.list_(pa.string())))
        return checked, failures

    def _latest_per_key(self, spec: TableSpec, valid: pa.Table) -> pa.Table:
        if valid.num_rows == 0:
            return valid
        self.con.register("valid_rows", valid)
        keys = ", ".join(quote_identifier(k, "duckdb") for k in spec.primary_key)
        wm = quote_identifier(spec.watermark_column, "duckdb")
        latest = fetch(self.con, f"SELECT * FROM valid_rows QUALIFY row_number() OVER "
                                 f"(PARTITION BY {keys} ORDER BY {wm} DESC, _extract_seq DESC) = 1")
        self.con.unregister("valid_rows")
        return latest

    def _merge_silver(self, spec: TableSpec, rows: pa.Table, run_id: str) -> MergeOutcome:
        path = self.lake.silver(self.pipeline.name, spec.name)
        outcome = MergeOutcome()
        if rows.num_rows == 0:
            outcome.version = self.version(path)
            return outcome
        part = spec.partition_by
        values = sorted(set(rows[part].to_pylist())) if part else []
        outcome.partitions = [str(v) for v in values]
        metadata = {"sparknerve.run_id": run_id}
        if not self.exists(path):
            write_deltalake(local(path), rows, mode="append", partition_by=[part] if part else None,
                            commit_properties=CommitProperties(custom_metadata=metadata))
            outcome.inserted = rows.num_rows
            outcome.version = self.version(path)
            return outcome

        current = self.schema_of(path)
        incoming = {f.name: type_name(f.type) for f in rows.schema}
        outcome.new_columns = [c.describe() for c in diff_schema(current, incoming) if c.kind == "added"]

        def q(name: str) -> str:
            return quote_identifier(name, "duckdb")

        predicate = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in spec.primary_key)
        if part and len(values) <= MAX_PRUNING_VALUES:
            predicate += f" AND t.{q(part)} IN ({', '.join(sql_literal(v, 'duckdb') for v in values)})"
        else:
            outcome.partitions = []
        guard = f"s.{q(spec.watermark_column)} > t.{q(spec.watermark_column)}"
        metrics = (
            DeltaTable(local(path))
            .merge(rows, predicate, source_alias="s", target_alias="t", merge_schema=True,
                   commit_properties=CommitProperties(custom_metadata=metadata))
            .when_matched_update_all(predicate=guard)
            .when_not_matched_insert_all()
            .execute()
        )
        outcome.inserted = int(metrics.get("num_target_rows_inserted", 0))
        outcome.updated = int(metrics.get("num_target_rows_updated", 0))
        outcome.files_rewritten = int(metrics.get("num_target_files_removed", 0))
        outcome.files_scanned = metrics.get("num_target_files_scanned")
        outcome.files_skipped = metrics.get("num_target_files_skipped_during_scan")
        outcome.version = self.version(path)
        return outcome

    def _validate_batch(self, spec: TableSpec, run_id: str, as_of: datetime, batch: dict,
                        checkpoint: StreamCheckpoint, recovered: bool) -> StageRun:
        run = StageRun(run_id, self.pipeline.name, spec.name, VALIDATE, batch_id=batch["batch_id"], recovered=recovered)
        p, t = self.pipeline.name, spec.name
        try:
            rows = self._bronze_rows(self.lake.bronze(p, t), batch["start_version"], batch["end_version"])
            run.rows_read = rows.num_rows
            checked, failures = self.apply_rules(spec, rows, as_of)
            run.rule_failures = {name: n for name, n in failures.items() if n}
            has_errors = pc.greater(pc.list_value_length(checked["_dq_errors"]), 0)
            invalid = checked.filter(has_errors)
            valid = checked.filter(pc.invert(has_errors))
            run.rows_quarantined = invalid.num_rows
            run.rows_warned = int(pc.sum(pc.greater(pc.list_value_length(valid["_dq_warnings"]), 0)).as_py() or 0)

            business = [c for c in rows.column_names if c not in TECHNICAL_COLUMNS]
            if invalid.num_rows:
                now = datetime.now(timezone.utc)
                quarantine = invalid.select(business + ["_run_id", "_extract_seq", "_ingested_at", "_dq_errors",
                                                        "_dq_warnings"])
                quarantine = (quarantine.append_column("_validate_run_id", pa.array([run_id] * invalid.num_rows))
                              .append_column("_batch_id", pa.array([batch["batch_id"]] * invalid.num_rows, pa.int64()))
                              .append_column("_quarantined_at", pa.array([now] * invalid.num_rows, UTC_TS)))
                written, run.quarantine_version = self._append(
                    self.lake.quarantine(p, t), quarantine, run_id, txn=(app_id(p, t, "quarantine"), batch["batch_id"]))
                if not written:
                    run.note = f"quarantine already holds batch {batch['batch_id']}: append skipped (idempotent txn)"
            else:
                run.quarantine_version = self.version(self.lake.quarantine(p, t))

            check_gate(spec, run.rows_read, run.rows_quarantined, self.gate_overrides.get(t))

            silver_rows = self._latest_per_key(spec, valid).select(business + list(SILVER_COLUMNS))
            outcome = self._merge_silver(spec, silver_rows, run_id)
            run.rows_written = outcome.inserted + outcome.updated
            run.files_rewritten = outcome.files_rewritten
            run.partitions_touched = outcome.partitions
            run.schema_changes = outcome.new_columns
            run.delta_version = outcome.version
            if outcome.files_scanned is not None:
                pruning = (f"MERGE scanned {outcome.files_scanned} file(s), skipped {outcome.files_skipped} "
                           f"by partition pruning; +{outcome.inserted} ~{outcome.updated}")
                run.note = f"{run.note}; {pruning}" if run.note else pruning

            self._crash_point(t, VALIDATE)
            checkpoint.commit(batch["batch_id"])
            self.gate_overrides.pop(t, None)
            return self._finish(run, SUCCEEDED)
        except Exception as exc:  # noqa: BLE001 - every failure is audited
            return self._finish(run, FAILED, exc)

    # ---- report ----------------------------------------------------------------------

    def report(self, task: Task, run_id: str, results: list[TaskResult]) -> TaskResult:
        by_table: dict[str, str] = {}
        for spec in self.pipeline.tables:
            stages = [r for r in results if r.task.table == spec.name]
            if any(r.state == "failed" for r in stages):
                by_table[spec.name] = "failed"
            elif any(r.state != "success" for r in stages):
                by_table[spec.name] = "not_run"
            elif all(run.status == NO_DATA for r in stages for run in r.runs):
                by_table[spec.name] = "no_data"
            else:
                by_table[spec.name] = "succeeded"
        ok = all(state in ("succeeded", "no_data") for state in by_table.values())
        run = StageRun(run_id, self.pipeline.name, "_all", REPORT)
        run.note = json.dumps(by_table)
        unhealthy = ", ".join(f"{t}={s}" for t, s in by_table.items() if s not in ("succeeded", "no_data"))
        self._finish(run, SUCCEEDED if ok else FAILED, None if ok else f"Tables not healthy: {unhealthy}")
        registry = pipeline_registry(self.pipeline.name, ok, Counter(by_table.values()),
                                     run.finished_at.timestamp())
        return TaskResult(task, "success" if ok else "failed", [run], exposition(registry))

    def _finish(self, run: StageRun, status: str, error: BaseException | str | None = None) -> StageRun:
        run.finish(status, describe_error(error))
        self._audit(run)
        return run
