"""SparkNerve: metadata-driven ingestion from SQL Server to Delta Lake.

Metadata (JSON) -> Airflow (generated DAGs) -> Spark (extract) -> data quality
engine (validate, quarantine) -> Delta Lake (bronze, silver, quarantine), with
Prometheus metrics and an audit record for every stage.
"""

__version__ = "1.0.0"
