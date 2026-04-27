-- ============================================================
-- E-Commerce ETL Pipeline — Staging Layer
-- Run once on startup via Docker entrypoint
-- ============================================================

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- ─────────────────────────────────────────
-- 1. customers_staging
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.customers_staging (
    customer_id     INTEGER,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    phone           VARCHAR(50),
    country         VARCHAR(100),
    signup_date     DATE,
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_file    VARCHAR(500)
);

-- ─────────────────────────────────────────
-- 2. products_staging
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.products_staging (
    product_id          INTEGER,
    product_name        VARCHAR(255),
    category            VARCHAR(100),
    sub_category        VARCHAR(100),
    price               NUMERIC(10, 2),
    cost                NUMERIC(10, 2),
    stock_quantity      INTEGER,
    supplier_id         VARCHAR(20),
    created_at          DATE,
    _loaded_at          TIMESTAMP DEFAULT NOW(),
    _source_file        VARCHAR(500)
);

-- ─────────────────────────────────────────
-- 3. orders_staging
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.orders_staging (
    order_id            VARCHAR(50),
    customer_id         INTEGER,
    product_id          INTEGER,
    order_date          DATE,
    ship_date           DATE,
    quantity            INTEGER,
    unit_price          NUMERIC(10, 2),
    discount            NUMERIC(5, 2),
    total_amount        NUMERIC(10, 2),
    status              VARCHAR(50),
    payment_method      VARCHAR(50),
    shipping_country    VARCHAR(100),
    _loaded_at          TIMESTAMP DEFAULT NOW(),
    _source_file        VARCHAR(500)
);

-- ─────────────────────────────────────────
-- 4. date_staging
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.date_staging (
    date_id         INTEGER,
    full_date       DATE,
    day             SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(20),
    quarter         SMALLINT,
    year            SMALLINT,
    week_of_year    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_file    VARCHAR(500)
);

-- ─────────────────────────────────────────
-- Audit / pipeline run log table
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.pipeline_run_log (
    run_id          SERIAL PRIMARY KEY,
    dag_id          VARCHAR(200),
    run_date        TIMESTAMP DEFAULT NOW(),
    table_name      VARCHAR(200),
    rows_loaded     INTEGER,
    source_file     VARCHAR(500),
    status          VARCHAR(50),
    error_message   TEXT
);
