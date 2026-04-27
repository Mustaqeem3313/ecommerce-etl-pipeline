# 🛒 E-Commerce Batch ETL Pipeline

A production-grade batch ETL pipeline that pulls raw CSV data from **AWS S3**, orchestrates processing with **Apache Airflow**, and loads clean data into **PostgreSQL** staging tables — all running inside **Docker**.

---

## 🏗️ Architecture

```
AWS S3 (Raw Layer)
   customers/customers_2024_10.csv
   orders/orders_2024_10.csv
   products/products_2024_10.csv
   date/date_2024_10.csv
         │
         ▼
Apache Airflow DAG: s3_to_staging
  ┌─────────────────────────────────┐
  │  S3KeySensor  (file existence)  │
  │  extract_*    (S3Hook → bytes)  │
  │  transform_*  (clean + validate)│
  │  load_*       (COPY → Postgres) │
  │  pipeline_summary               │
  └─────────────────────────────────┘
         │
         ▼
PostgreSQL — staging schema
   staging.customers_staging
   staging.orders_staging
   staging.products_staging
   staging.date_staging
   staging.pipeline_run_log  (audit)
         │
         ▼
  Ready for BI / Analytics / DWH
```

---

## 📁 Project Structure

```
ecommerce-etl-pipeline/
├── docker-compose.yml          # Full stack: Airflow + 2× Postgres
├── .env.example                # Copy → .env and fill credentials
├── requirements.txt
│
├── dags/
│   ├── s3_to_staging_dag.py    # Main DAG definition
│   └── etl/
│       ├── __init__.py
│       ├── extract.py          # S3Hook → BytesIO
│       ├── transform.py        # Schema validation + light cleaning
│       └── load.py             # COPY FROM STDIN → Postgres
│
├── sql/
│   └── init_staging.sql        # DDL for all staging tables
│
├── scripts/
│   └── init_db.sh              # Postgres container init script
│
└── data/                       # Sample CSV files (dev / local testing)
    ├── customers/customers_2024_10.csv
    ├── orders/orders_2024_10.csv
    ├── products/products_2024_10.csv
    └── date/date_2024_10.csv
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 4.x
- Docker Compose v2
- AWS account with an S3 bucket (or use local data for dev)

---

### Step 1 — Clone and configure

```bash
git clone <your-repo-url>
cd ecommerce-etl-pipeline
cp .env.example .env
```

Edit `.env` with your values:

```dotenv
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=your-ecommerce-data-lake
STAGING_PASSWORD=choose_a_strong_password
AIRFLOW__CORE__FERNET_KEY=<generate below>
```

Generate a Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

### Step 2 — Upload sample data to S3

```bash
aws s3 cp data/customers/customers_2024_10.csv  s3://$S3_BUCKET/customers/
aws s3 cp data/orders/orders_2024_10.csv        s3://$S3_BUCKET/orders/
aws s3 cp data/products/products_2024_10.csv    s3://$S3_BUCKET/products/
aws s3 cp data/date/date_2024_10.csv            s3://$S3_BUCKET/date/
```

---

### Step 3 — Start the stack

```bash
# Create required host directories
mkdir -p logs plugins config

# Set AIRFLOW_UID (Linux only — skip on Mac/Windows)
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# Launch everything
docker compose up -d

# Watch logs until healthy
docker compose logs -f airflow-webserver
```

The stack is ready when you see:
```
airflow_webserver  | [INFO] Listening at: http://0.0.0.0:8080
```

---

### Step 4 — Access Airflow UI

Open **http://localhost:8080**

| Field    | Value   |
|----------|---------|
| Username | `admin` |
| Password | `admin` |

---

### Step 5 — Trigger the DAG

1. Navigate to **DAGs** → `s3_to_staging`
2. Toggle the DAG **ON**
3. Click ▶ **Trigger DAG**
4. Watch the task graph — all 4 table branches run in parallel

---

### Step 6 — Query staging tables

Connect to the staging database on port **5433**:

```bash
psql -h localhost -p 5433 -U staging_user -d ecommerce_staging
```

```sql
-- Check loaded data
SELECT COUNT(*) FROM staging.customers_staging;
SELECT COUNT(*) FROM staging.orders_staging;
SELECT COUNT(*) FROM staging.products_staging;
SELECT COUNT(*) FROM staging.date_staging;

-- Check pipeline audit log
SELECT * FROM staging.pipeline_run_log ORDER BY run_date DESC;
```

---

## ⚙️ Configuration (Airflow Variables)

You can override any S3 path without touching code — set these in the Airflow UI under **Admin → Variables**:

| Variable Key       | Default Value                        |
|--------------------|--------------------------------------|
| `s3_bucket`        | `your-ecommerce-data-lake`           |
| `s3_key_customers` | `customers/customers_2024_10.csv`    |
| `s3_key_orders`    | `orders/orders_2024_10.csv`          |
| `s3_key_products`  | `products/products_2024_10.csv`      |
| `s3_key_date`      | `date/date_2024_10.csv`              |
| `aws_conn_id`      | `aws_default`                        |
| `pg_conn_id`       | `postgres_staging`                   |

---

## 🔁 Pipeline Type Summary

| Dimension        | This Pipeline              |
|------------------|---------------------------|
| Trigger model    | Batch (daily @ 06:00 UTC) |
| Pattern          | ETL (Extract → Transform → Load) |
| Orchestrator     | Apache Airflow             |
| Raw storage      | AWS S3                     |
| Target           | PostgreSQL staging layer   |
| Deployment       | Docker Compose             |
| Load strategy    | Truncate-and-reload (full refresh) |

---

## 🐛 Troubleshooting

### Airflow webserver not starting
```bash
docker compose logs airflow-webserver | tail -50
```
Most common cause: `FERNET_KEY` not set in `.env`.

### S3 connection failing
```bash
# Test AWS credentials inside the Airflow container
docker exec -it airflow_scheduler bash
python -c "import boto3; boto3.client('s3').list_buckets()"
```

### Cannot connect to staging Postgres
```bash
# From host machine
psql -h localhost -p 5433 -U staging_user -d ecommerce_staging

# From inside Docker network
docker exec -it postgres_staging psql -U staging_user -d ecommerce_staging
```

### Reset everything
```bash
docker compose down -v   # removes all volumes (wipes both DBs)
docker compose up -d
```

---

## 🛣️ Next Steps (DWH Layer)

The staging tables are now ready to feed a downstream warehouse:

- **dbt** — run models on top of staging to build `dim_customers`, `fact_orders`, etc.
- **Redshift / BigQuery / Snowflake** — COPY staging data up to cloud DWH
- **Metabase / Grafana** — point BI tools at `staging.*` for quick dashboards

---

## 📝 License

MIT
