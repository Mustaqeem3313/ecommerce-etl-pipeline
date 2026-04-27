"""
load.py — Load Phase
Truncates staging tables and bulk-loads cleaned rows using
PostgreSQL's COPY FROM STDIN (fastest method, no ORM overhead).
Also writes to the pipeline_run_log audit table.
"""

import io
import csv
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.transform import TransformResult

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Column maps: which CSV columns land in which staging table
# ──────────────────────────────────────────────────────────────
STAGING_COLUMNS: dict[str, list[str]] = {
    "customers": [
        "customer_id", "first_name", "last_name", "email",
        "phone", "country", "signup_date", "is_active",
    ],
    "products": [
        "product_id", "product_name", "category", "sub_category",
        "price", "cost", "stock_quantity", "supplier_id", "created_at",
    ],
    "orders": [
        "order_id", "customer_id", "product_id", "order_date",
        "ship_date", "quantity", "unit_price", "discount",
        "total_amount", "status", "payment_method", "shipping_country",
    ],
    "date": [
        "date_id", "full_date", "day", "month", "month_name",
        "quarter", "year", "week_of_year", "day_of_week",
        "day_name", "is_weekend", "is_holiday",
    ],
}

STAGING_TABLE_MAP = {
    "customers": "staging.customers_staging",
    "products":  "staging.products_staging",
    "orders":    "staging.orders_staging",
    "date":      "staging.date_staging",
}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _rows_to_csv_buffer(rows: list[dict], columns: list[str]) -> io.StringIO:
    """Serialise rows into a CSV buffer suitable for COPY FROM STDIN."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=columns,
        extrasaction="ignore",
        quoting=csv.QUOTE_MINIMAL,
    )
    for row in rows:
        # Replace Python True/False with Postgres-friendly t/f
        cleaned = {}
        for col in columns:
            val = row.get(col, "")
            if isinstance(val, bool):
                cleaned[col] = "t" if val else "f"
            else:
                cleaned[col] = val if val is not None else ""
        writer.writerow(cleaned)
    buf.seek(0)
    return buf


@contextmanager
def _get_connection(postgres_conn_id: str):
    """Yield a raw psycopg2 connection via Airflow's PostgresHook."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _log_run(
    conn,
    dag_id: str,
    table_name: str,
    rows_loaded: int,
    source_file: str,
    status: str,
    error_message: str | None = None,
) -> None:
    """Insert one row into the audit log table."""
    sql = """
        INSERT INTO staging.pipeline_run_log
            (dag_id, table_name, rows_loaded, source_file, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (dag_id, table_name, rows_loaded, source_file, status, error_message))


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def load_table(
    result: TransformResult,
    source_file: str,
    postgres_conn_id: str = "postgres_staging",
    dag_id: str = "s3_to_staging",
) -> int:
    """
    Truncate-and-load a single staging table using COPY FROM STDIN.

    Args:
        result           : TransformResult from the transform phase
        source_file      : S3 key used as provenance metadata
        postgres_conn_id : Airflow connection ID for the staging DB
        dag_id           : Written to the audit log

    Returns:
        Number of rows loaded
    """
    table_name    = result.table_name
    staging_table = STAGING_TABLE_MAP[table_name]
    columns       = STAGING_COLUMNS[table_name]
    # Extra metadata columns appended in Postgres DEFAULT or explicit insert
    extra_cols    = ["_source_file"]
    all_cols      = columns + extra_cols

    if not result.rows:
        logger.warning("[%s] No rows to load — skipping COPY", table_name)
        return 0

    logger.info("[%s] Loading %d rows → %s", table_name, result.row_count, staging_table)

    with _get_connection(postgres_conn_id) as conn:
        with conn.cursor() as cur:

            # 1. Truncate (full refresh pattern)
            cur.execute(f"TRUNCATE TABLE {staging_table};")
            logger.debug("[%s] Table truncated", table_name)

            # 2. Build CSV buffer (add _source_file column)
            rows_with_meta = [
                {**row, "_source_file": source_file}
                for row in result.rows
            ]
            csv_buf = _rows_to_csv_buffer(rows_with_meta, all_cols)

            # 3. COPY FROM STDIN
            copy_sql = (
                f"COPY {staging_table} ({', '.join(all_cols)}) "
                f"FROM STDIN WITH (FORMAT CSV, NULL '')"
            )
            cur.copy_expert(copy_sql, csv_buf)

            rows_loaded = cur.rowcount if cur.rowcount != -1 else result.row_count
            logger.info("[%s] COPY complete — %d rows loaded", table_name, rows_loaded)

        # 4. Write audit log
        _log_run(
            conn,
            dag_id=dag_id,
            table_name=staging_table,
            rows_loaded=result.row_count,
            source_file=source_file,
            status="success",
        )

    return result.row_count


def load_all(
    transform_results: dict[str, TransformResult],
    s3_keys: dict[str, str],
    postgres_conn_id: str = "postgres_staging",
    dag_id: str = "s3_to_staging",
) -> dict[str, int]:
    """
    Load every transformed table into staging.

    Returns:
        { "customers": 10, "orders": 15, ... }
    """
    summary: dict[str, int] = {}
    for table_name, result in transform_results.items():
        source_file = s3_keys.get(table_name, "unknown")
        try:
            rows = load_table(
                result=result,
                source_file=source_file,
                postgres_conn_id=postgres_conn_id,
                dag_id=dag_id,
            )
            summary[table_name] = rows
        except Exception as exc:
            logger.error("[%s] Load FAILED: %s", table_name, exc, exc_info=True)
            summary[table_name] = -1
            raise

    logger.info("Load summary: %s", summary)
    return summary
