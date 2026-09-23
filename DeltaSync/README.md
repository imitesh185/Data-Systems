<div align="center">

# 🔁 DeltaSync

### MySQL change data capture into Delta Lake, with retry-safe results

**MySQL binlog → Debezium → Kafka → Spark Structured Streaming → Delta Lake**

[![Python](https://img.shields.io/badge/Python-3.10--3.13-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-CDC-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID-00ADD8?logo=databricks&logoColor=white)](https://delta.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Interactive_Demo-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)

</div>

---

## Why DeltaSync exists

DeltaSync was created as a compact, hands-on reference for one of the most
important data-engineering workflows: continuously moving operational database
changes into an analytical lakehouse without duplicating or losing data.

It demonstrates inserts, updates, deletes, partition-moving updates, schema
evolution, bounded micro-batches, checkpointing, and idempotent replay in a
project that can be explored locally.

## What is included

- **Interactive demo** — a Streamlit application that simulates MySQL,
  Debezium, and Kafka while writing real local Delta tables through `delta-rs`.
- **Full CDC stack** — Docker Compose services for MySQL, Kafka, Debezium,
  and Spark.
- **Spark streaming job** — consumes Debezium envelopes and applies each
  micro-batch to Delta Lake.
- **Exactly-once merge contract** — event identity and source offsets make
  replay safe after a failure.
- **Two example tables** — `orders` and `customers`, including delete handling,
  date-partition movement, and a customer schema change.
- **Automated tests** — verify convergence and prove that replaying an applied
  batch does not change the result.

## Architecture

```text
┌──────────────┐    binlog     ┌──────────────┐    CDC events    ┌───────────┐
│    MySQL     │ ────────────► │   Debezium   │ ───────────────► │   Kafka   │
│ orders/users │               │  connector   │                  │  topics   │
└──────────────┘               └──────────────┘                  └─────┬─────┘
                                                                       │
                                                        bounded micro-batches
                                                                       │
                                                                       ▼
┌──────────────┐    MERGE + checkpoint    ┌──────────────────────────────────┐
│  Delta Lake  │ ◄──────────────────────── │ Spark Structured Streaming       │
│ bronze/silver│                           │ foreachBatch + deterministic keys │
└──────────────┘                           └──────────────────────────────────┘
```

The Streamlit demo uses the same event and merge semantics in a lightweight
in-process simulation, so the behavior is easy to inspect without first
starting the infrastructure stack.

---

## Run the interactive demo

### Prerequisites

- Git
- **Python 3.10, 3.11, 3.12, or 3.13**

> Python 3.14 is not currently supported because the pinned PyArrow range does
> not provide a compatible prebuilt wheel.

### 1. Open the project

From the Data-Systems repository:

```bash
cd DeltaSync
```

### 2. Create a virtual environment

<details open>
<summary><strong>Windows PowerShell</strong></summary>

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

</details>

<details>
<summary><strong>macOS or Linux</strong></summary>

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

</details>

If Python 3.12 is not installed, use another supported version in the commands
above.

### 3. Install DeltaSync

```bash
pip install -e .
```

For contributors who also want the test tools:

```bash
pip install -e ".[dev]"
```

### 4. Start the dashboard

```bash
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) if it does not open
automatically.

### 5. Try the CDC workflow

1. Click **Run micro-batch** to apply the initial 18-row snapshot.
2. Create, update, or delete source records from the sidebar.
3. Run another micro-batch and compare MySQL with Delta Lake.
4. Click **Replay last batch** to simulate a crash before checkpoint
   persistence.
5. Open **Micro-batches** and confirm that replayed events were skipped and
   zero rows changed.
6. Add `loyalty_tier`, run a batch, and inspect **Schema changes**.

Local demo data is written beneath `.deltasync/`, which is intentionally
ignored by Git.

---

## Run the complete CDC stack

### Prerequisites

- Docker Desktop, or Docker Engine with the Compose plugin
- At least **6 GB of memory** available to Docker
- Ports `3306`, `8083`, and `9092` available

### 1. Start the services

From the `DeltaSync` folder:

```bash
docker compose up -d
```

The Compose health checks wait for MySQL, Kafka, and Kafka Connect before
dependent services start.

### 2. Register the Debezium connector

The connector definition is stored with the project. Register it against
Kafka Connect:

<details open>
<summary><strong>macOS or Linux</strong></summary>

```bash
curl -i -X POST \
  -H "Content-Type: application/json" \
  --data @debezium/register-mysql.json \
  http://localhost:8083/connectors
```

</details>

<details>
<summary><strong>Windows PowerShell</strong></summary>

```powershell
Invoke-RestMethod `
  -Method Post `
  -ContentType "application/json" `
  -InFile ".\debezium\register-mysql.json" `
  -Uri "http://localhost:8083/connectors"
```

</details>

Verify its status:

```bash
curl http://localhost:8083/connectors/deltasync-mysql/status
```

### 3. Start the Spark stream

```bash
docker compose run --rm spark
```

The job reads Debezium topics, stores the append-only bronze changelog, and
merges current records into silver Delta tables. Checkpoints and tables are
written into the Docker volumes declared by Compose.

### 4. Generate source changes

Connect to MySQL:

```bash
docker compose exec mysql mysql -udeltasync -pdeltasync deltasync
```

Then run any transaction, for example:

```sql
INSERT INTO orders (customer_id, status, amount, order_date)
VALUES (1, 'created', 249.00, CURRENT_DATE);

UPDATE orders
SET status = 'paid'
WHERE order_id = 1;

DELETE FROM orders
WHERE order_id = 2;

ALTER TABLE customers
ADD COLUMN loyalty_tier VARCHAR(16) DEFAULT 'bronze';
```

### 5. Stop the stack

Preserve volumes:

```bash
docker compose down
```

Remove containers **and all generated local data**:

```bash
docker compose down -v
```

---

## Run the tests

After installing the development dependencies:

```bash
pytest
```

With coverage:

```bash
pytest --cov=deltasync --cov-report=term-missing
```

The main replay test applies a batch twice and verifies that the second
application changes no rows.

## Exactly-once behavior

DeltaSync treats exactly-once as a result guarantee:

1. Kafka provides an ordered `(topic, partition, offset)` source position.
2. Every CDC envelope has a stable event identity.
3. A micro-batch applies deterministic upserts and deletes by primary key.
4. Previously committed identities are skipped during replay.
5. The streaming checkpoint advances only after the Delta transaction
   succeeds.

If the stream fails after a merge but before its checkpoint is persisted,
Spark can deliver the same batch again. Reapplying it produces the same table,
which prevents duplicate business rows.

## Project layout

```text
DeltaSync/
├── app.py                    # Interactive Streamlit demonstration
├── docker-compose.yml        # Complete local CDC environment
├── debezium/                 # MySQL connector registration
├── mysql/                    # Source schema and seed records
├── spark/                    # Structured Streaming CDC job
├── src/deltasync/
│   ├── engine.py             # Demo source, queue, and batch orchestration
│   ├── merge.py              # Deterministic idempotent merge contract
│   ├── models.py             # Typed CDC event model
│   └── sink.py               # Local delta-rs table writer
├── tests/                    # Convergence and replay tests
├── pyproject.toml            # Python package and dependency metadata
└── README.md
```

## Troubleshooting

### `No matching distribution found for pyarrow`

Confirm that the active interpreter is Python 3.10–3.13:

```bash
python --version
```

Delete the virtual environment, recreate it with a supported interpreter, and
install again.

### The dashboard remains on `CONNECTING`

The Streamlit process has stopped or port `8501` is unavailable. Restart it:

```bash
streamlit run app.py
```

### The Debezium connector is not running

Inspect its status and Kafka Connect logs:

```bash
curl http://localhost:8083/connectors/deltasync-mysql/status
docker compose logs connect
```

### Reset everything

For the demo, stop Streamlit and remove `.deltasync/`. For the full stack:

```bash
docker compose down -v
docker compose up -d
```

---

<div align="center">

Built as a practical reference for CDC, streaming reliability, and lakehouse
merge design.

</div>
