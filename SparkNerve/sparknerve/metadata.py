"""Pipeline metadata: JSON files checked against pipeline.schema.json, plus the
cross-field checks a JSON Schema cannot express. Pure Python (no Spark), so the
Airflow DAG factory, the Spark jobs and the live demo all load the same objects.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMA_FILE = Path(__file__).with_name("pipeline.schema.json")
DEFAULT_METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata" / "pipelines"

DEFAULTS: dict[str, Any] = {"schema_evolution": "add_new_columns", "quarantine_threshold": 0.3, "gate_min_rows": 20,
                            "read_partitions": 1}

# Regex constructs outside the common subset of Java regex (Spark RLIKE) and
# RE2 (DuckDB): lookaround, named groups, backreferences, possessive quantifiers.
NON_PORTABLE_REGEX = re.compile(r"\(\?[=!<>P]|\\[1-9]|[*+?}]\+")


class MetadataError(ValueError):
    """Invalid pipeline metadata. The message names the file and the offending field."""


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    column: str
    severity: str = "error"
    description: str = ""
    params: dict[str, Any] = field(default_factory=dict, hash=False)
    implicit: bool = False

    @property
    def references(self) -> tuple[str, str] | None:
        ref = self.params.get("references")
        return (ref["table"], ref["column"]) if ref else None


@dataclass(frozen=True)
class TableSpec:
    name: str
    source_table: str
    primary_key: tuple[str, ...]
    watermark_column: str
    partition_by: str | None
    schema_evolution: str
    quarantine_threshold: float
    read_partitions: int
    rules: tuple[Rule, ...]
    description: str = ""
    gate_min_rows: int = 20

    @property
    def depends_on(self) -> tuple[str, ...]:
        """Tables whose silver data this table's foreign-key rules check against."""
        return tuple(sorted({r.references[0] for r in self.rules if r.type == "foreign_key"}))

    @property
    def severities(self) -> dict[str, str]:
        return {r.name: r.severity for r in self.rules}


@dataclass(frozen=True)
class Pipeline:
    name: str
    description: str
    owner: str
    schedule: str | None
    connection: str
    fetch_size: int
    tables: tuple[TableSpec, ...]
    source_path: str | None = None

    @property
    def table_names(self) -> list[str]:
        return [t.name for t in self.tables]

    def table(self, name: str) -> TableSpec:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f"Pipeline '{self.name}' has no table '{name}' (tables: {', '.join(self.table_names)})")


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _implicit_rules(pk: tuple[str, ...], partition_by: str | None, explicit: list[Rule]) -> list[Rule]:
    """Structural rules every table gets: a row without its key or partition value
    cannot be merged, so it is quarantined instead of silently dropped."""
    covered = {r.column for r in explicit if r.type == "not_null" and r.severity == "error"}
    implicit = [
        Rule(name=f"pk_{col.lower()}_not_null", type="not_null", column=col, implicit=True,
             description="Primary key must be present (implicit rule).")
        for col in pk if col not in covered
    ]
    if partition_by and partition_by not in covered and partition_by not in pk:
        implicit.append(Rule(name=f"partition_{partition_by.lower()}_not_null", type="not_null", column=partition_by,
                             implicit=True, description="Partition column must be present (implicit rule)."))
    return implicit


def _parse_rule(raw: dict[str, Any]) -> Rule:
    params = {k: v for k, v in raw.items() if k not in ("name", "type", "column", "severity", "description")}
    return Rule(
        name=raw["name"],
        type=raw["type"],
        column=raw["column"],
        severity=raw.get("severity", "error"),
        description=raw.get("description", ""),
        params=params,
    )


def _check_rule(where: str, rule: Rule, table_names: set[str], table: str) -> None:
    p = rule.params
    if rule.type in ("range", "length") and "min" in p and "max" in p and p["min"] > p["max"]:
        raise MetadataError(f"{where}: rule '{rule.name}' has min {p['min']} > max {p['max']}")
    if rule.type == "regex":
        pattern = p["pattern"]
        try:
            re.compile(pattern)
        except re.error as exc:
            raise MetadataError(f"{where}: rule '{rule.name}' has an invalid regex: {exc}") from exc
        if NON_PORTABLE_REGEX.search(pattern):
            raise MetadataError(
                f"{where}: rule '{rule.name}' uses lookaround, named groups, backreferences or possessive "
                "quantifiers, which Spark (Java regex) and DuckDB (RE2) do not both support"
            )
    if rule.type == "foreign_key":
        ref_table, _ = rule.references
        if ref_table == table:
            raise MetadataError(f"{where}: rule '{rule.name}': self-referencing foreign keys are not supported")
        if ref_table not in table_names:
            raise MetadataError(f"{where}: rule '{rule.name}' references unknown table '{ref_table}'")
    if rule.type == "compare" and p["other_column"] == rule.column:
        raise MetadataError(f"{where}: rule '{rule.name}' compares column '{rule.column}' with itself")


def _check_acyclic(source: str, tables: list[TableSpec]) -> None:
    deps = {t.name: t.depends_on for t in tables}
    state: dict[str, int] = {}

    def visit(name: str, path: list[str]) -> None:
        if state.get(name) == 2:
            return
        if state.get(name) == 1:
            cycle = " -> ".join(path[path.index(name):] + [name])
            raise MetadataError(f"{source}: foreign keys form a dependency cycle: {cycle}")
        state[name] = 1
        for dep in deps[name]:
            visit(dep, path + [name])
        state[name] = 2

    for table in deps:
        visit(table, [])


def parse_pipeline(doc: dict[str, Any], source: str = "<metadata>") -> Pipeline:
    errors = sorted(_validator().iter_errors(doc), key=lambda e: [str(p) for p in e.absolute_path])
    if errors:
        lines = [f"{source}: /{'/'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errors[:10]]
        raise MetadataError("\n".join(lines))

    defaults = {**DEFAULTS, **(doc.get("defaults") or {})}
    names = [t["name"] for t in doc["tables"]]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise MetadataError(f"{source}: duplicate table names {duplicates}")

    tables: list[TableSpec] = []
    for index, raw in enumerate(doc["tables"]):
        where = f"{source}: /tables/{index} ({raw['name']})"
        pk = tuple(raw["primary_key"])
        partition_by = raw.get("partition_by")
        explicit = [_parse_rule(r) for r in raw.get("rules") or []]
        rules = _implicit_rules(pk, partition_by, explicit) + explicit
        rule_names = [r.name for r in rules]
        clashes = sorted({n for n in rule_names if rule_names.count(n) > 1})
        if clashes:
            raise MetadataError(f"{where}: duplicate rule names {clashes}")
        for rule in explicit:
            _check_rule(where, rule, set(names), raw["name"])
        tables.append(
            TableSpec(
                name=raw["name"],
                source_table=raw["source_table"],
                primary_key=pk,
                watermark_column=raw["watermark_column"],
                partition_by=partition_by,
                schema_evolution=raw.get("schema_evolution", defaults["schema_evolution"]),
                quarantine_threshold=float(raw.get("quarantine_threshold", defaults["quarantine_threshold"])),
                read_partitions=int(raw.get("read_partitions", defaults["read_partitions"])),
                rules=tuple(rules),
                description=raw.get("description", ""),
                gate_min_rows=int(raw.get("gate_min_rows", defaults["gate_min_rows"])),
            )
        )

    _check_acyclic(source, tables)
    source_cfg = doc["source"]
    return Pipeline(
        name=doc["pipeline"],
        description=doc.get("description", ""),
        owner=doc.get("owner", ""),
        schedule=doc.get("schedule"),
        connection=source_cfg["connection"],
        fetch_size=int(source_cfg.get("fetch_size", 10000)),
        tables=tuple(tables),
        source_path=None if source == "<metadata>" else source,
    )


def load_pipeline(path: str | Path) -> Pipeline:
    path = Path(path)
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MetadataError(f"{path}: invalid JSON: {exc}") from exc
    return parse_pipeline(doc, str(path))


def metadata_dir(directory: str | Path | None = None) -> Path:
    return Path(directory or os.getenv("SPARKNERVE_METADATA_DIR") or DEFAULT_METADATA_DIR)


def load_pipelines(directory: str | Path | None = None) -> dict[str, Pipeline]:
    """Every pipeline in the metadata directory, keyed by pipeline name."""
    root = metadata_dir(directory)
    pipelines: dict[str, Pipeline] = {}
    for path in sorted(root.glob("*.json")):
        pipeline = load_pipeline(path)
        if pipeline.name in pipelines:
            other = pipelines[pipeline.name].source_path
            raise MetadataError(f"{path}: pipeline '{pipeline.name}' is also defined in {other}")
        pipelines[pipeline.name] = pipeline
    if not pipelines:
        raise MetadataError(f"No pipeline metadata (*.json) found in {root}")
    return pipelines
