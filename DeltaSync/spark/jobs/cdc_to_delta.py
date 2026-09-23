from __future__ import annotations

import os
import re
from collections.abc import Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

TABLE_TYPES = {
    "customers": {
        "id": "long",
        "email": "string",
        "full_name": "string",
        "status": "string",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
    "orders": {
        "id": "long",
        "customer_id": "long",
        "order_date": "date",
        "status": "string",
        "total_amount": "decimal(12,2)",
        "created_at": "timestamp",
        "updated_at": "timestamp",
    },
}
PRIMARY_KEYS = {"customers": ("id",), "orders": ("id",)}
PARTITIONS = {"customers": (), "orders": ("order_date",)}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ENVELOPE_SCHEMA = StructType(
    [
        StructField("before", MapType(StringType(), StringType()), True),
        StructField("after", MapType(StringType(), StringType()), True),
        StructField(
            "source",
            StructType(
                [
                    StructField("db", StringType(), True),
                    StructField("table", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ]
)


def _quoted(identifier: str) -> str:
    if not IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"Unsafe MySQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _typed(value: Column, data_type: str) -> Column:
    if data_type == "date":
        # Debezium encodes MySQL DATE values as days since the Unix epoch.
        return F.when(
            value.rlike(r"^-?\d+$"),
            F.date_add(F.lit("1970-01-01").cast("date"), value.cast("int")),
        ).otherwise(value.cast("date"))
    return value.cast(data_type)


def _discover_fields(table_batch: DataFrame) -> list[str]:
    fields = (
        table_batch.select(F.explode(F.map_keys("_row")).alias("field"))
        .where(F.col("field").isNotNull())
        .distinct()
        .collect()
    )
    names = sorted(row.field for row in fields)
    for name in names:
        _quoted(name)
    return names


def _project_rows(table: str, table_batch: DataFrame) -> DataFrame:
    fields = _discover_fields(table_batch)
    primary_keys = PRIMARY_KEYS[table]
    missing_keys = set(primary_keys) - set(fields)
    if missing_keys:
        raise ValueError(f"{table} events are missing primary keys: {missing_keys}")

    type_map = TABLE_TYPES[table]
    row_columns = [
        _typed(
            F.element_at(F.col("_row"), F.lit(name)),
            type_map.get(name, "string"),
        ).alias(name)
        for name in fields
    ]
    projected = table_batch.select(
        *row_columns,
        F.col("_op"),
        F.col("_kafka_offset").alias("_cdc_offset"),
        F.col("_kafka_timestamp").alias("_cdc_timestamp"),
    )

    null_key = F.lit(False)
    for key in primary_keys:
        null_key = null_key | F.col(key).isNull()
    if projected.where(null_key).limit(1).count():
        raise ValueError(f"{table} event contains a null primary key")

    latest = Window.partitionBy(*primary_keys).orderBy(F.col("_cdc_offset").desc())
    return (
        projected.withColumn("_event_rank", F.row_number().over(latest))
        .where(F.col("_event_rank") == 1)
        .drop("_event_rank")
    )


def _delta_table_exists(table_path: str) -> bool:
    return os.path.isdir(os.path.join(table_path, "_delta_log"))


def _create_table(table: str, rows: DataFrame, table_path: str) -> bool:
    initial = rows.where(F.col("_op") != "d")
    if initial.limit(1).count() == 0:
        return False

    writer = initial.write.format("delta").mode("errorifexists")
    partitions = PARTITIONS[table]
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(table_path)
    return True


def _merge_table(
    table: str,
    rows: DataFrame,
    table_path: str,
    batch_id: int,
) -> None:
    if not _delta_table_exists(table_path) and not _create_table(table, rows, table_path):
        return

    # foreachBatch DataFrames belong to a cloned session, so the temp view must be
    # registered and queried through that same session.
    session = rows.sparkSession
    view = f"_deltasync_{table}_{batch_id}"
    rows.createOrReplaceTempView(view)
    join = " AND ".join(
        f"target.{_quoted(key)} = source.{_quoted(key)}"
        for key in PRIMARY_KEYS[table]
    )
    newer = "source._cdc_offset > COALESCE(target._cdc_offset, -1)"
    session.sql(
        f"""
        MERGE INTO delta.`{table_path}` AS target
        USING {view} AS source
        ON {join}
        WHEN MATCHED AND source._op = 'd' AND {newer} THEN DELETE
        WHEN MATCHED AND source._op <> 'd' AND {newer} THEN UPDATE SET *
        WHEN NOT MATCHED AND source._op <> 'd' THEN INSERT *
        """
    )
    session.catalog.dropTempView(view)


def _single_partition(partitions: Iterable[int], table: str) -> None:
    values = set(partitions)
    if values - {0}:
        raise RuntimeError(
            f"{table} has Kafka partitions {sorted(values)}; "
            "idempotent offset ordering requires one partition per table topic"
        )


def process_batch(
    delta_root: str,
    batch: DataFrame,
    batch_id: int,
) -> None:
    batch.persist()
    try:
        tables = [row._table for row in batch.select("_table").distinct().collect()]
        for table in tables:
            if table not in TABLE_TYPES:
                raise ValueError(f"Unexpected CDC table: {table!r}")

            table_batch = batch.where(F.col("_table") == table)
            partitions = [
                row._kafka_partition
                for row in table_batch.select("_kafka_partition").distinct().collect()
            ]
            _single_partition(partitions, table)
            rows = _project_rows(table, table_batch)
            _merge_table(
                table,
                rows,
                os.path.join(delta_root, table),
                batch_id,
            )
    finally:
        batch.unpersist()


def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic_pattern = os.environ.get(
        "KAFKA_TOPIC_PATTERN", r"deltasync\.deltasync\.(orders|customers)"
    )
    delta_root = os.environ.get("DELTA_ROOT", "/data/delta")
    checkpoint_root = os.environ.get("CHECKPOINT_ROOT", "/data/checkpoints")

    spark = SparkSession.builder.appName("DeltaSyncCDC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribePattern", topic_pattern)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    envelope = F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA)
    events = (
        kafka.select(
            envelope.alias("_event"),
            F.col("topic").alias("_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            F.element_at(F.split("_topic", r"\."), -1).alias("_table"),
            F.col("_event.op").alias("_op"),
            F.when(F.col("_event.op") == "d", F.col("_event.before"))
            .otherwise(F.col("_event.after"))
            .alias("_row"),
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
        )
        .where(F.col("_op").isin("c", "u", "d", "r"))
        .where(F.col("_row").isNotNull())
    )

    query = (
        events.writeStream.queryName("deltasync-cdc-to-delta")
        .option("checkpointLocation", os.path.join(checkpoint_root, "cdc-to-delta"))
        .foreachBatch(
            lambda batch, batch_id: process_batch(delta_root, batch, batch_id)
        )
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
