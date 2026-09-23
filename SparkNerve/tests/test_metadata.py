import copy
import json

import pytest

from sparknerve.metadata import DEFAULT_METADATA_DIR, MetadataError, load_pipelines, parse_pipeline

BASE = json.loads((DEFAULT_METADATA_DIR / "retail_sales.json").read_text(encoding="utf-8"))


def doc(**changes):
    d = copy.deepcopy(BASE)
    d.update(changes)
    return d


def with_rule(table: str, rule: dict) -> dict:
    d = copy.deepcopy(BASE)
    next(t for t in d["tables"] if t["name"] == table)["rules"].append(rule)
    return d


def test_repository_metadata_is_valid():
    pipeline = load_pipelines()["retail_sales"]
    assert pipeline.table_names == ["customers", "products", "orders"]
    orders = pipeline.table("orders")
    assert orders.depends_on == ("customers", "products")
    assert orders.partition_by == "order_date"
    assert orders.read_partitions == 4
    assert pipeline.table("products").schema_evolution == "fail"
    assert pipeline.table("customers").quarantine_threshold == 0.3


def test_structural_rules_are_implicit():
    orders = load_pipelines()["retail_sales"].table("orders")
    implicit = [(r.name, r.column) for r in orders.rules if r.implicit]
    assert implicit == [("pk_order_id_not_null", "order_id"), ("partition_order_date_not_null", "order_date")]


def test_schema_errors_name_the_field():
    bad = doc()
    bad["tables"][0]["primary_key"] = []
    with pytest.raises(MetadataError, match="/tables/0/primary_key"):
        parse_pipeline(bad, "retail_sales.json")


def test_rule_parameters_are_checked_per_type():
    with pytest.raises(MetadataError, match="values"):
        parse_pipeline(with_rule("orders", {"name": "x", "type": "allowed_values", "column": "status"}))
    with pytest.raises(MetadataError, match="Additional properties"):
        parse_pipeline(with_rule("orders", {"name": "x", "type": "not_null", "column": "status", "min": 1}))


def test_min_above_max_is_rejected():
    with pytest.raises(MetadataError, match="min 5 > max 1"):
        parse_pipeline(with_rule("orders", {"name": "x", "type": "range", "column": "quantity", "min": 5, "max": 1}))


@pytest.mark.parametrize("pattern", [r"^(?=a)", r"(a)\1", r"(?P<n>a)", r"a++"])
def test_non_portable_regex_is_rejected(pattern):
    with pytest.raises(MetadataError, match="Java regex"):
        parse_pipeline(with_rule("customers", {"name": "x", "type": "regex", "column": "email", "pattern": pattern}))


def test_invalid_regex_is_rejected():
    with pytest.raises(MetadataError, match="invalid regex"):
        parse_pipeline(with_rule("customers", {"name": "x", "type": "regex", "column": "email", "pattern": "("}))


def test_foreign_key_must_reference_a_pipeline_table():
    rule = {"name": "x", "type": "foreign_key", "column": "store_id", "references": {"table": "stores", "column": "id"}}
    with pytest.raises(MetadataError, match="unknown table 'stores'"):
        parse_pipeline(with_rule("orders", rule))


def test_foreign_key_cycles_are_rejected():
    rule = {"name": "x", "type": "foreign_key", "column": "last_order_id",
            "references": {"table": "orders", "column": "order_id"}}
    with pytest.raises(MetadataError, match="cycle"):
        parse_pipeline(with_rule("customers", rule))


def test_duplicate_rule_names_are_rejected():
    with pytest.raises(MetadataError, match="duplicate rule names"):
        parse_pipeline(with_rule("orders", {"name": "status_known", "type": "not_null", "column": "status"}))


def test_table_settings_fall_back_to_defaults():
    d = doc(defaults={"quarantine_threshold": 0.5})
    pipeline = parse_pipeline(d)
    assert pipeline.table("orders").quarantine_threshold == 0.5
    assert pipeline.table("orders").schema_evolution == "add_new_columns"
