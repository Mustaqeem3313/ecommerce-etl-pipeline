"""
s3_to_staging_dag.py
────────────────────
Airflow DAG: s3_to_staging

Batch ETL pipeline — pulls raw CSVs from AWS S3,
applies light transforms, and loads into PostgreSQL staging tables.

Schedule: daily at 06:00 UTC  (override via Airflow Variables)

DAG structure (all tasks run in parallel per table):

    check_s3_files
         │
    ┌────┼────┬────────┐
    │    │    │        │
 ext_c  ext_o ext_p  ext_d     (extract per table)
    │    │    │        │
 tfm_c  tfm_o tfm_p  tfm_d     (transform per table)
    │    │    │        │
 ld_c   ld_o  ld_p   ld_d      (load per table)
    └────┴────┴────────┘
              │
        pipeline_summary
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from etl.extract import extract_all_tables
from etl.transform import transform_all
from etl.load import load_all

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Configuration  (override via Airflow Variables in the UI)
# ──────────────────────────────────────────────────────────────
S3_BUCKET      = Variable.get("s3_bucket",      default_var="your-ecommerce-data-lake")
AWS_CONN_ID    = Variable.get("aws_conn_id",     default_var="aws_default")
PG_CONN_ID     = Variable.get("pg_conn_id",      default_var="postgres_staging")

S3_KEYS: dict[str, str] = {
    "customers": Variable.get("s3_key_customers", default_var="customers/customers_2024_10.csv"),
    "orders":    Variable.get("s3_key_orders",    default_var="orders/orders_2024_10.csv"),
    "products":  Variable.get("s3_key_products",  default_var="products/products_2024_10.csv"),
    "date":      Variable.get("s3_key_date",      default_var="date/date_2024_10_v2.csv"),
}

TABLES = list(S3_KEYS.keys())

# ──────────────────────────────────────────────────────────────
# Default DAG arguments
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ──────────────────────────────────────────────────────────────
# Task callables
# ──────────────────────────────────────────────────────────────

def extract_task(table_name: str, **context) -> None:
    """Extract one table from S3 and push bytes to XCom."""
    from etl.extract import extract_from_s3

    file_obj = extract_from_s3(
        bucket_name=S3_BUCKET,
        s3_key=S3_KEYS[table_name],
        aws_conn_id=AWS_CONN_ID,
    )
    # XCom stores serialisable types — push raw bytes
    context["ti"].xcom_push(key=f"raw_{table_name}", value=file_obj.getvalue().decode("utf-8"))
    logger.info("[%s] Extract pushed to XCom (%d bytes)", table_name, len(file_obj.getvalue()))


def transform_task(table_name: str, **context) -> None:
    """Pull raw bytes from XCom, run transform, push cleaned rows."""
    import io
    from etl.transform import transform

    raw_bytes = context["ti"].xcom_pull(key=f"raw_{table_name}", task_ids=f"extract_{table_name}")
    result = transform(table_name, io.BytesIO(raw_bytes.encode("utf-8")))

    context["ti"].xcom_push(key=f"rows_{table_name}",    value=result.rows)
    context["ti"].xcom_push(key=f"skipped_{table_name}", value=result.skipped_rows)
    logger.info(
        "[%s] Transform: %d rows, %d skipped",
        table_name, result.row_count, result.skipped_rows,
    )


def load_task(table_name: str, **context) -> None:
    """Pull cleaned rows from XCom and COPY into staging."""
    from etl.transform import TransformResult
    from etl.load import load_table

    rows = context["ti"].xcom_pull(key=f"rows_{table_name}", task_ids=f"transform_{table_name}")

    result = TransformResult(
        table_name=table_name,
        rows=rows,
        row_count=len(rows),
    )

    loaded = load_table(
        result=result,
        source_file=f"s3://{S3_BUCKET}/{S3_KEYS[table_name]}",
        postgres_conn_id=PG_CONN_ID,
        dag_id="s3_to_staging",
    )
    context["ti"].xcom_push(key=f"loaded_{table_name}", value=loaded)
    logger.info("[%s] Load complete — %d rows in staging", table_name, loaded)


def pipeline_summary(**context) -> None:
    """Log a final summary of all table loads."""
    ti = context["ti"]
    lines = ["", "=" * 50, "  PIPELINE RUN SUMMARY", "=" * 50]
    total = 0
    for table in TABLES:
        loaded = ti.xcom_pull(key=f"loaded_{table}", task_ids=f"load_{table}") or 0
        skipped = ti.xcom_pull(key=f"skipped_{table}", task_ids=f"transform_{table}") or 0
        lines.append(f"  {table:<12} loaded={loaded:>5}  skipped={skipped:>3}")
        total += loaded
    lines += ["-" * 50, f"  TOTAL ROWS LOADED: {total}", "=" * 50, ""]
    logger.info("\n".join(lines))


# ──────────────────────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="s3_to_staging",
    description="Batch ETL: AWS S3 → PostgreSQL staging layer",
    schedule_interval="0 6 * * *",        # daily at 06:00 UTC
    start_date=datetime(2024, 10, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "etl", "staging", "s3"],
    doc_md=__doc__,
) as dag:

    # ── Dummy start
    start = EmptyOperator(task_id="start")

    # ── S3 file existence checks (run before extract)
    s3_sensors = {}
    for table in TABLES:
        s3_sensors[table] = S3KeySensor(
            task_id=f"check_s3_{table}",
            bucket_name=S3_BUCKET,
            bucket_key=S3_KEYS[table],
            aws_conn_id=AWS_CONN_ID,
            timeout=60 * 5,           # wait up to 5 min
            poke_interval=30,
            mode="poke",
        )

    # ── Per-table extract / transform / load tasks
    extract_tasks   = {}
    transform_tasks = {}
    load_tasks      = {}

    for table in TABLES:
        extract_tasks[table] = PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_task,
            op_kwargs={"table_name": table},
        )

        transform_tasks[table] = PythonOperator(
            task_id=f"transform_{table}",
            python_callable=transform_task,
            op_kwargs={"table_name": table},
        )

        load_tasks[table] = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_task,
            op_kwargs={"table_name": table},
        )

    # ── Final summary
    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        trigger_rule="all_done",   # run even if individual tables fail
    )

    # ──────────────────────────────────────────────
    # Wire up dependencies
    # ──────────────────────────────────────────────
    for table in TABLES:
        (
            start
            >> s3_sensors[table]
            >> extract_tasks[table]
            >> transform_tasks[table]
            >> load_tasks[table]
            >> summary
        )
