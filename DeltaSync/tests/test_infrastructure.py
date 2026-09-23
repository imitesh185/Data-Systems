from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_connector_targets_expected_tables_with_single_partition_topics() -> None:
    config = json.loads((ROOT / "debezium" / "connector.json").read_text())

    assert config["connector.class"] == "io.debezium.connector.mysql.MySqlConnector"
    assert config["table.include.list"] == "deltasync.orders,deltasync.customers"
    assert config["topic.creation.default.partitions"] == "1"
    assert config["tombstones.on.delete"] == "false"
    # Debezium 2.7 rejects any other temporal precision mode.
    assert config.get("time.precision.mode", "adaptive_time_microseconds") in {
        "adaptive",
        "adaptive_time_microseconds",
        "connect",
    }


def test_compose_images_are_pinned_and_storage_is_persistent() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()

    assert ":latest" not in compose
    assert "mysql_data:/var/lib/mysql" in compose
    assert "kafka_data:/var/lib/kafka/data" in compose
    assert "delta_data:/data/delta" in compose
    assert "spark_checkpoints:/data/checkpoints" in compose
    assert compose.count("healthcheck:") >= 3
    # The Spark parser expects bare Debezium envelopes, not schema/payload wrappers.
    assert 'CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"' in compose


def test_mysql_is_configured_for_full_row_cdc() -> None:
    mysql_config = (ROOT / "mysql" / "conf.d" / "deltasync.cnf").read_text()
    schema = (ROOT / "mysql" / "init" / "001_schema.sql").read_text()

    assert "binlog_format=ROW" in mysql_config
    assert "binlog_row_image=FULL" in mysql_config
    assert "CREATE TABLE customers" in schema
    assert "CREATE TABLE orders" in schema
    assert "REPLICATION CLIENT" in schema


def test_spark_job_is_syntactically_valid_and_merges_deletes() -> None:
    job = (ROOT / "spark" / "jobs" / "cdc_to_delta.py").read_text()

    ast.parse(job)
    assert "foreachBatch" in job
    assert "THEN DELETE" in job
    assert "UPDATE SET *" in job
    assert "source._cdc_offset > COALESCE(target._cdc_offset, -1)" in job
