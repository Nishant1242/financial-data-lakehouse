# 📊 Financial Data Lakehouse

> A production-grade financial data lakehouse streaming real-time crypto market data through a complete modern data stack — built entirely on free, open-source tools.

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?style=flat-square&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=flat-square&logo=apachespark)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?style=flat-square&logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql)
![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B?style=flat-square&logo=dbt)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?style=flat-square&logo=mongodb)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?style=flat-square&logo=streamlit)

---

## Architecture

```
Alpaca Markets WebSocket
        │
        ▼
  Apache Kafka ──────────── 3 partitions · trades topic
        │
        ▼
   MinIO (Bronze) ───────── JSONL · Hive partitioned
        │
        ▼
  PySpark Pipeline ─────── Schema validation · Deduplication · Quality rules
        │
        ▼
   MinIO (Silver) ──────── Clean Parquet · Snappy compressed
        │
        ▼
 PostgreSQL (Gold) ──────── Star schema · fact_trades · dim_instrument · dim_time
        │
        ▼
    dbt Models ──────────── 4 models · 8 tests · analytics schema
        │
        ▼
     MongoDB ─────────────── Portfolio summary documents
        │
        ▼
 Streamlit Dashboard ──── 5-page executive intelligence platform
        │
Apache Airflow ──────────── @hourly orchestration · 7 tasks · retry logic
```

---

## Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Alpaca Markets WebSocket | Real-time BTC/USD, ETH/USD, SOL/USD trades |
| **Streaming** | Apache Kafka 7.5 | Message queue · 3 partitions |
| **Storage** | MinIO (S3-compatible) | Bronze (raw) and Silver (clean) object store |
| **Processing** | PySpark 3.5.1 | Bronze → Silver batch transformation |
| **Warehouse** | PostgreSQL 15 | Gold layer star schema |
| **Transformation** | dbt Core 1.7.4 | Analytics models and data quality tests |
| **Document Store** | MongoDB 7 | Portfolio summary documents |
| **Orchestration** | Apache Airflow 2.8.1 | Hourly pipeline scheduling |
| **Dashboard** | Streamlit 1.32 | 5-page executive intelligence platform |
| **Infrastructure** | Docker Compose | 8 containerized services |

---

## Data Flow

### Bronze Layer
Raw trades land in MinIO as JSONL with Hive-style partitioning:
```
s3a://bronze/trades/year=2026/month=05/day=09/hour=20/trades_20260509_204800.json
```

### Silver Layer
PySpark applies 6 transformation steps:
1. Cast types (price, quantity, notional as Decimal)
2. Add derived columns (trade_date, trade_hour, notional calculation)
3. Deduplicate on trade_id
4. Fill nulls with sensible defaults
5. Round numerics to 8 decimal places
6. Apply quality rules — reject records failing validation

Output: clean Parquet files in MinIO Silver, Snappy compressed.

### Gold Layer (Star Schema)
```sql
fact_trades          -- one row per trade
dim_instrument       -- BTC/USD, ETH/USD, SOL/USD metadata
dim_time             -- trading date calendar
```

### dbt Analytics Models
```
stg_trades              -- staging view on fact_trades
stg_instruments         -- staging view on dim_instrument
mart_daily_summary      -- daily OHLCV per symbol
mart_symbol_performance -- all-time performance metrics
```

---

## Dashboard Pages

| Page | Description |
|---|---|
| 🏠 **Overview** | Executive KPIs, volume by asset, portfolio exposure, daily trend |
| 📈 **Market Analysis** | Filters by symbol and date, volume trend, price range, hourly heatmap |
| 💎 **Asset Deep Dive** | Per-asset OHLC candlestick, price history, volatility metrics |
| ⚙️ **Pipeline Health** | All 8 pipeline stages, data quality metrics, ingestion patterns |
| 📋 **Reports & Export** | CSV download, PDF executive report generation |

---

## Airflow DAG

7-task pipeline running every hour:

```
check_services
      ↓
run_bronze_consumer      (Kafka → MinIO Bronze)
      ↓
run_batch_pipeline       (PySpark Bronze → Silver)
      ↓
run_gold_loader          (Silver → PostgreSQL Gold)
      ↓
run_dbt_models           (Gold → Analytics models)
      ↓
run_mongodb_loader       (Gold → MongoDB documents)
      ↓
pipeline_summary         (Log run metrics)
```

Features: retry logic (2 retries, 5min delay), 30min execution timeout, XCom for inter-task data passing, health checks before pipeline start.

---

## Project Structure

```
financial-data-lakehouse/
├── infra/
│   └── docker-compose.yml          # 8 services: Kafka, MinIO, PostgreSQL, MongoDB, Airflow
├── services/
│   ├── ingestion/
│   │   ├── alpaca_producer.py      # WebSocket → Kafka
│   │   └── bronze_consumer.py      # Kafka → MinIO Bronze
│   └── processing/
│       ├── silver_schema.py        # PySpark schema + quality rules
│       ├── batch_pipeline.py       # Bronze → Silver transformation
│       ├── gold_loader.py          # Silver → PostgreSQL
│       └── mongodb_loader.py       # PostgreSQL → MongoDB
├── orchestration/
│   └── dags/
│       └── lakehouse_pipeline.py   # Airflow DAG
├── lakehouse_dbt/
│   ├── models/
│   │   ├── staging/                # stg_trades, stg_instruments
│   │   └── marts/                  # mart_daily_summary, mart_symbol_performance
│   └── tests/                      # 8 data quality tests
├── sql/
│   └── ddl/
│       └── create_gold_schema.sql  # Star schema DDL
├── dashboard/
│   ├── app.py                      # Main entry + navigation
│   ├── views/                      # 5 page modules
│   └── utils/                      # Shared DB queries + styles
└── .env                            # Environment variables
```

---

## Setup & Running

### Prerequisites
- Docker Desktop
- Python 3.11
- Java 17 (for PySpark)
- Alpaca Markets free account (for crypto stream)
- Git Bash (Windows) or Terminal (Mac/Linux)

### 1. Clone and configure

```bash
git clone https://github.com/Nishant1242/financial-data-lakehouse.git
cd financial-data-lakehouse
cp .env.example .env
# Add your Alpaca API keys to .env
```

### 2. Start Docker stack

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d
```

### 3. Install dependencies

```bash
python -m venv .lvenv
source .lvenv/Scripts/activate      # Windows Git Bash
pip install -r requirements.txt
```

### 4. Start data producer

```bash
python services/ingestion/alpaca_producer.py
```

### 5. Run the pipeline manually (first time)

```bash
python services/ingestion/bronze_consumer.py
python services/processing/batch_pipeline.py
python services/processing/gold_loader.py
python services/processing/mongodb_loader.py
cd lakehouse_dbt && dbt run && dbt test && cd ..
```

### 6. Launch dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501

### 7. Open Airflow (automated pipeline)

Open http://localhost:8080 — login: admin / admin

Unpause `financial_lakehouse_pipeline` DAG for hourly automation.

---

## Daily Startup

```bash
# 1. Start Docker
docker compose --env-file .env -f infra/docker-compose.yml up -d

# 2. Activate venv
source .lvenv/Scripts/activate

# 3. Start producer (keep running)
python services/ingestion/alpaca_producer.py

# 4. Launch dashboard
streamlit run dashboard/app.py
```

Airflow handles everything else automatically at http://localhost:8080.

---

## Key Technical Decisions

**Why Kafka over direct ingestion?**
Kafka decouples the producer from consumers. The Airflow DAG can consume at its own pace without blocking the live stream. Supports multiple consumers (Bronze writer, real-time analytics) from the same topic.

**Why PySpark for Silver transformation?**
Demonstrates production-grade processing at scale. The same code runs locally and on a cluster — no rewrite needed when the data volume grows.

**Why star schema in PostgreSQL?**
Fact + dimension tables enable fast aggregations across any combination of symbol, date, and trade attributes. dbt models build on top cleanly.

**Why dbt for analytics models?**
Version-controlled SQL transformations with built-in testing. Every model is testable, documented, and reproducible. Industry standard at data-mature companies.

**Why MongoDB for portfolio documents?**
Flexible schema for portfolio summaries that vary by asset class. Complements the structured PostgreSQL Gold layer — right tool for the right job.

---

## Data Quality

The Silver pipeline enforces 5 quality rules:
- `price_positive` — price > 0
- `quantity_positive` — quantity > 0
- `notional_positive` — notional > 0
- `symbol_not_empty` — symbol is not null or empty
- `trade_id_not_empty` — trade_id is not null

Failed records go to a dead letter path in MinIO for investigation:
```
s3a://bronze/dead_letter/trades_YYYYMMDD_HHMMSS/
```

dbt runs 8 tests on the Gold layer: not_null, unique, accepted_values, relationships.

---

## Performance

| Metric | Value |
|---|---|
| Pipeline latency | ~60 seconds end-to-end |
| Spark startup | ~2-3 minutes (JVM cold start) |
| dbt run time | < 1 second (4 models) |
| Dashboard refresh | 60 seconds (TTL cache) |
| Kafka partitions | 3 (trades topic) |

---

## Author

**Nishant Kadam** — Data Engineer

Built as a portfolio project demonstrating production-grade data engineering skills across the full modern data stack.

- GitHub: [Nishant1242](https://github.com/Nishant1242)
- LinkedIn: [Connect](https://linkedin.com/in/nishant-kadam)

---

## License

MIT License — free to use, learn from, and build on.
