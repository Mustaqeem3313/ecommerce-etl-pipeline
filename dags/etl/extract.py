"""
extract.py — Extract Phase
Pulls CSV files from AWS S3 using Airflow's S3Hook.
Returns a file-like object (BytesIO) ready for the Transform phase.
"""

import logging
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Config: map table names to their S3 keys
# Override via Airflow Variables or pass directly when calling.
# ──────────────────────────────────────────────────────────────
DEFAULT_S3_KEYS = {
    "customers": "customers/customers_2024_10.csv",
    "orders":    "orders/orders_2024_10.csv",
    "products":  "products/products_2024_10.csv",
    "date":      "date/date_2024_10.csv",
}


def extract_from_s3(
    bucket_name: str,
    s3_key: str,
    aws_conn_id: str = "aws_default",
) -> io.BytesIO:
    """
    Download a single file from S3 and return it as BytesIO.

    Args:
        bucket_name : S3 bucket name (e.g. 'my-ecommerce-data-lake')
        s3_key      : Object key inside the bucket
        aws_conn_id : Airflow connection ID for AWS credentials

    Returns:
        io.BytesIO  : In-memory file-like object with CSV content
    """
    logger.info("Extracting s3://%s/%s", bucket_name, s3_key)

    hook = S3Hook(aws_conn_id=aws_conn_id)

    # get_key returns an S3 object; read() fetches its body
    s3_object = hook.get_key(key=s3_key, bucket_name=bucket_name)
    raw_bytes = s3_object.get()["Body"].read()

    logger.info(
        "Extracted %d bytes from s3://%s/%s",
        len(raw_bytes), bucket_name, s3_key,
    )

    return io.BytesIO(raw_bytes)


def extract_all_tables(
    bucket_name: str,
    s3_keys: dict | None = None,
    aws_conn_id: str = "aws_default",
) -> dict[str, io.BytesIO]:
    """
    Extract every table defined in s3_keys (or DEFAULT_S3_KEYS).

    Returns:
        { "customers": BytesIO(...), "orders": BytesIO(...), ... }
    """
    if s3_keys is None:
        s3_keys = DEFAULT_S3_KEYS

    extracted: dict[str, io.BytesIO] = {}
    for table_name, s3_key in s3_keys.items():
        extracted[table_name] = extract_from_s3(
            bucket_name=bucket_name,
            s3_key=s3_key,
            aws_conn_id=aws_conn_id,
        )

    logger.info("Extraction complete — tables: %s", list(extracted.keys()))
    return extracted
