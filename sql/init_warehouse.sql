-- ============================================================
-- E-Commerce ETL Pipeline — Warehouse Layer (Star Schema)
-- Runs once on first startup via /docker-entrypoint-initdb.d
-- (after init_staging.sql — files execute alphabetically)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ─────────────────────────────────────────
-- Dimension: customers
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_id     INTEGER PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    country         VARCHAR(100)
);

-- ─────────────────────────────────────────
-- Dimension: products
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_id      INTEGER PRIMARY KEY,
    product_name    VARCHAR(255),
    category        VARCHAR(100)
);

-- ─────────────────────────────────────────
-- Dimension: date
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id         INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL,
    day             SMALLINT,
    month           SMALLINT,
    year            SMALLINT,
    quarter         SMALLINT,
    day_name        VARCHAR(20),
    month_name      VARCHAR(20),
    week_of_year    SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
);

-- ─────────────────────────────────────────
-- Fact: orders  (grain = one order line)
-- Column order must match load_fact_orders() in staging_to_warehouse_dag.py.
-- customer_id / product_id / date_id are logical foreign keys to the dimensions.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_id        VARCHAR(50),
    customer_id     INTEGER,
    product_id      INTEGER,
    date_id         INTEGER,
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    total_amount    NUMERIC(12, 2)
);

CREATE INDEX IF NOT EXISTS ix_fact_orders_customer ON warehouse.fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_product  ON warehouse.fact_orders (product_id);
CREATE INDEX IF NOT EXISTS ix_fact_orders_date     ON warehouse.fact_orders (date_id);
