<div align="center">

# Data Systems

### Executable reference implementations for reliable data movement and lakehouse ingestion

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)

</div>

---

## About

Data Systems is a collection of focused, runnable projects that explore the
hard parts of modern data engineering: change data capture, replay safety,
incremental ingestion, schema evolution, data quality, orchestration, and
operational visibility.

Each project is self-contained and includes an interactive demo, automated
tests, and production-oriented implementation patterns.

## Projects

| Project | Data flow | Focus |
| --- | --- | --- |
| [DeltaSync](./DeltaSync/) | MySQL -> Debezium -> Kafka -> Spark -> Delta Lake | Change data capture, idempotent replay, deletes, checkpoints, and schema evolution |
| [SparkNerve](./SparkNerve/) | SQL Server -> Airflow -> Spark -> Delta Lake | Metadata-driven ingestion, data-quality gates, quarantine, lineage, and observability |

---

## DeltaSync

DeltaSync is a compact CDC system that continuously moves operational changes
from MySQL into Delta Lake.

```text
MySQL binlog -> Debezium -> Kafka -> Spark Structured Streaming -> Delta Lake
```

It demonstrates:

- insert, update, and delete propagation;
- retry-safe micro-batches and idempotent replay;
- bronze and silver Delta tables;
- checkpoint and source-offset management;
- partition-moving updates;
- additive schema evolution;
- an in-process Streamlit demo and a complete Docker Compose stack.

### Run the DeltaSync demo

```bash
cd DeltaSync
python -m venv .venv
```

Activate the environment:

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

```bash
# macOS or Linux
source .venv/bin/activate
```

Install and run:

```bash
python -m pip install --upgrade pip
pip install -e ".[dev]"
streamlit run app.py
```

For the Debezium, Kafka, MySQL, and Spark deployment:

```bash
docker compose up -d
docker compose run --rm spark
```

See the [DeltaSync documentation](./DeltaSync/README.md) for the complete
walkthrough and failure-recovery scenarios.

---

## SparkNerve

SparkNerve is a metadata-driven ingestion platform that turns declarative JSON
pipeline definitions into validated execution plans for Airflow and Spark.

```text
SQL Server -> Airflow -> Spark -> Bronze -> Quality Gate -> Silver / Quarantine
                                             |
                                             +-> Audit + Prometheus metrics
```

It demonstrates:

- JSON Schema and semantic validation of pipeline metadata;
- generated task graphs with explicit table dependencies;
- incremental extraction through per-table watermarks;
- bronze, silver, and quarantine data paths;
- configurable row-level data-quality rules;
- circuit breaking when invalid-row thresholds are exceeded;
- additive and fail-fast schema-evolution policies;
- audit records, run reports, and Prometheus metrics;
- deterministic simulation of healthy traffic, bad records, and schema drift.

### Run the SparkNerve demo

```bash
cd SparkNerve
python -m venv .venv
```

Activate the environment using the command for your platform shown above, then:

```bash
python -m pip install --upgrade pip
pip install -e ".[dev]"
streamlit run demo/app.py --server.port 8502
```

Inspect the metadata and generated execution plan:

```bash
python -m sparknerve validate-metadata
python -m sparknerve plan --pipeline retail_sales
```

---

## Run the tests

Tests are scoped to each project:

```bash
cd DeltaSync
pytest
```

```bash
cd SparkNerve
pytest
ruff check .
```

## Repository layout

```text
Data-Systems/
|-- DeltaSync/    # CDC replication from MySQL to Delta Lake
|-- SparkNerve/   # Metadata-driven lakehouse ingestion
`-- README.md
```

## Engineering principles

The projects favor observable behavior over architecture diagrams alone:

1. Model delivery guarantees explicitly.
2. Make retries safe before adding throughput.
3. Preserve raw data before applying business rules.
4. Treat schema drift and bad records as expected operating conditions.
5. Keep checkpoints, audit records, and metrics aligned with committed data.
6. Prove recovery behavior with tests and reproducible failure scenarios.
