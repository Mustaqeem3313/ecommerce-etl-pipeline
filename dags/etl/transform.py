"""
transform.py — Transform Phase
Light-touch cleaning and schema enforcement before staging load.
Design philosophy: Keep it ELT-style — don't over-transform here.
Heavy business logic belongs in the DWH layer downstream.
"""

import io
import logging
import csv
from dataclasses import dataclass, field
from typing import Iterator

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Expected schemas per table
# ──────────────────────────────────────────────────────────────
EXPECTED_COLUMNS: dict[str, list[str]] = {
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


@dataclass
class TransformResult:
    table_name: str
    rows: list[dict]
    row_count: int = 0
    skipped_rows: int = 0
    warnings: list[str] = field(default_factory=list)


# ──────────────────────────────────────────────────────────────
# Core helpers
# ──────────────────────────────────────────────────────────────

def _strip_row(row: dict) -> dict:
    """Strip leading/trailing whitespace from all string values."""
    return {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def _validate_columns(actual: list[str], expected: list[str], table: str) -> None:
    """Raise ValueError if the CSV header doesn't match the expected schema."""
    missing = set(expected) - set(actual)
    extra   = set(actual) - set(expected)

    if missing:
        raise ValueError(
            f"[{table}] Missing columns in source file: {missing}"
        )
    if extra:
        # Log but don't fail — extra columns are dropped during load
        logger.warning("[%s] Extra columns found (will be ignored): %s", table, extra)


def _iter_csv_rows(file_obj: io.BytesIO) -> Iterator[dict]:
    """Decode BytesIO → text → DictReader rows."""
    file_obj.seek(0)
    text = file_obj.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    yield from reader


# ──────────────────────────────────────────────────────────────
# Per-table light transforms
# ──────────────────────────────────────────────────────────────

def _transform_customers(rows: list[dict]) -> tuple[list[dict], list[str]]:
    clean, warnings = [], []
    for i, row in enumerate(rows, start=2):  # row 1 = header
        if not row.get("customer_id"):
            warnings.append(f"Row {i}: missing customer_id — skipped")
            continue
        # Normalise boolean
        row["is_active"] = str(row.get("is_active", "")).lower() in ("true", "1", "yes")
        clean.append(row)
    return clean, warnings


def _transform_products(rows: list[dict]) -> tuple[list[dict], list[str]]:
    clean, warnings = [], []
    for i, row in enumerate(rows, start=2):
        if not row.get("product_id"):
            warnings.append(f"Row {i}: missing product_id — skipped")
            continue
        # Ensure numeric types won't blow up Postgres COPY
        for col in ("price", "cost"):
            try:
                float(row[col])
            except (ValueError, TypeError):
                warnings.append(f"Row {i}: non-numeric {col}='{row[col]}' — set to NULL")
                row[col] = ""
        clean.append(row)
    return clean, warnings


def _transform_orders(rows: list[dict]) -> tuple[list[dict], list[str]]:
    clean, warnings = [], []
    valid_statuses = {"delivered", "shipped", "processing", "cancelled", "returned"}
    for i, row in enumerate(rows, start=2):
        if not row.get("order_id"):
            warnings.append(f"Row {i}: missing order_id — skipped")
            continue
        status = str(row.get("status", "")).lower()
        if status not in valid_statuses:
            warnings.append(f"Row {i}: unknown status '{status}' — kept as-is")
        row["status"] = status
        clean.append(row)
    return clean, warnings


def _transform_date(rows: list[dict]) -> tuple[list[dict], list[str]]:
    clean, warnings = [], []
    for i, row in enumerate(rows, start=2):
        if not row.get("date_id"):
            warnings.append(f"Row {i}: missing date_id — skipped")
            continue
        for col in ("is_weekend", "is_holiday"):
            row[col] = str(row.get(col, "")).lower() in ("true", "1", "yes")
        clean.append(row)
    return clean, warnings


_TRANSFORMERS = {
    "customers": _transform_customers,
    "products":  _transform_products,
    "orders":    _transform_orders,
    "date":      _transform_date,
}


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def transform(table_name: str, file_obj: io.BytesIO) -> TransformResult:
    """
    Run the transform phase for a single table.

    Args:
        table_name : One of customers | products | orders | date
        file_obj   : Raw BytesIO from the extract phase

    Returns:
        TransformResult with cleaned rows and metadata
    """
    expected_cols = EXPECTED_COLUMNS.get(table_name)
    if expected_cols is None:
        raise ValueError(f"Unknown table: '{table_name}'")

    # Parse CSV
    raw_rows = [_strip_row(r) for r in _iter_csv_rows(file_obj)]
    if not raw_rows:
        logger.warning("[%s] Source file is empty — nothing to load", table_name)
        return TransformResult(table_name=table_name, rows=[], row_count=0)

    # Schema check
    _validate_columns(list(raw_rows[0].keys()), expected_cols, table_name)

    # Keep only expected columns (drop extras)
    raw_rows = [{col: r.get(col, "") for col in expected_cols} for r in raw_rows]

    # Table-specific transforms
    transformer = _TRANSFORMERS.get(table_name)
    if transformer:
        clean_rows, warnings = transformer(raw_rows)
    else:
        clean_rows, warnings = raw_rows, []

    skipped = len(raw_rows) - len(clean_rows)
    logger.info(
        "[%s] Transform complete — %d rows kept, %d skipped, %d warnings",
        table_name, len(clean_rows), skipped, len(warnings),
    )
    for w in warnings:
        logger.warning("[%s] %s", table_name, w)

    return TransformResult(
        table_name=table_name,
        rows=clean_rows,
        row_count=len(clean_rows),
        skipped_rows=skipped,
        warnings=warnings,
    )


def transform_all(extracted: dict[str, io.BytesIO]) -> dict[str, TransformResult]:
    """Transform all extracted tables. Returns map of table_name -> TransformResult."""
    return {name: transform(name, fobj) for name, fobj in extracted.items()}
