"""SparkSession factory with Delta Lake and the SQL Server JDBC driver."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"


def build_spark(app_name: str, master: str | None = None, with_jdbc: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MERGE ... UPDATE SET * / INSERT * adds new source columns to silver.
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # datetime2 values carry no zone: read, compare and store them as UTC.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    jars = os.getenv("SPARKNERVE_JARS")
    if jars:
        # Jars baked into the image: no Maven/Ivy resolution at run time.
        spark = builder.config("spark.jars", jars).getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        extra = [MSSQL_JDBC_PACKAGE] if with_jdbc else None
        spark = configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
