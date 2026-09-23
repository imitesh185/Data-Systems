# SparkNerve

SparkNerve is a metadata-driven lakehouse ingestion platform and interactive
demo. It models a SQL Server to Airflow to Spark pipeline with incremental
watermarks, Delta Lake storage, data-quality gates, quarantine, schema
evolution, audit records, and Prometheus metrics.

## Run the interactive demo

From the Data-Systems repository root:

```bash
python -m venv .venv
```

Activate the environment, then install and run the application:

```bash
python -m pip install --upgrade pip
pip install -r SparkNerve/demo/requirements.txt
streamlit run SparkNerve/demo/app.py
```

The demo uses local temporary storage and does not require SQL Server, Airflow,
Spark, or Prometheus services.

## Inspect the generated pipeline

From the `SparkNerve` directory:

```bash
python -m sparknerve validate-metadata
python -m sparknerve plan --pipeline retail_sales
```

## Run the tests

```bash
cd SparkNerve
pytest
ruff check .
```

See the [repository guide](../README.md) for an overview of both Data-Systems
projects.
