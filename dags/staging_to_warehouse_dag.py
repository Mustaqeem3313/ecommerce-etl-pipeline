from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2
}

# -------------------------------
# TASK 1: TRUNCATE FACT TABLE FIRST
# -------------------------------
def truncate_fact_orders():
    pg = PostgresHook(postgres_conn_id="postgres_staging")

    sql = """
    TRUNCATE TABLE warehouse.fact_orders;
    """

    pg.run(sql)


# -------------------------------
# TASK 2: LOAD DIM CUSTOMERS
# -------------------------------
def load_dim_customers():
    pg = PostgresHook(postgres_conn_id="postgres_staging")

    sql = """

    INSERT INTO warehouse.dim_customers (
    customer_id,
    first_name,
    last_name,
    email,
    country
)
SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    email,
    country
FROM staging.customers_staging
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    country = EXCLUDED.country;
    """

    pg.run(sql)


# -------------------------------
# TASK 3: LOAD DIM PRODUCTS
# -------------------------------
def load_dim_products():
    pg = PostgresHook(postgres_conn_id="postgres_staging")

    sql = """

    INSERT INTO warehouse.dim_products (
    product_id,
    product_name,
    category
)
SELECT DISTINCT
    product_id,
    product_name,
    category
FROM staging.products_staging
ON CONFLICT (product_id) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category;
    """

    pg.run(sql)


# -------------------------------
# TASK 4: LOAD DIM DATE
# -------------------------------
def load_dim_date():
    pg = PostgresHook(postgres_conn_id="postgres_staging")

    sql = """
    INSERT INTO warehouse.dim_date (
    date_id,
    full_date,
    day,
    month,
    year,
    quarter,
    day_name,
    month_name,
    week_of_year,
    is_weekend,
    is_holiday
)
SELECT
    date_id,
    full_date,
    day,
    month,
    year,
    quarter,
    day_name,
    month_name,
    week_of_year,
    is_weekend,
    is_holiday
FROM staging.date_staging
ON CONFLICT (date_id) DO NOTHING;
    """

    pg.run(sql)


# -------------------------------
# TASK 5: LOAD FACT ORDERS
# -------------------------------
def load_fact_orders():
    pg = PostgresHook(postgres_conn_id="postgres_staging")

    sql = """
    INSERT INTO warehouse.fact_orders
SELECT 
    o.order_id,
    o.customer_id,
    o.product_id,
    d.date_id,
    o.quantity,
    o.unit_price,
    (o.quantity * o.unit_price) AS total_amount
FROM staging.orders_staging o
JOIN staging.date_staging d
ON DATE(o.order_date) = DATE(d.full_date);
    """

    pg.run(sql)


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="staging_to_warehouse",
    default_args=default_args,
    description="Load data from staging to warehouse layer",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["warehouse", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="truncate_fact_orders",
        python_callable=truncate_fact_orders
    )

    t2 = PythonOperator(
        task_id="load_dim_customers",
        python_callable=load_dim_customers
    )

    t3 = PythonOperator(
        task_id="load_dim_products",
        python_callable=load_dim_products
    )

    t4 = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    t5 = PythonOperator(
        task_id="load_fact_orders",
        python_callable=load_fact_orders
    )

    # -------------------------------
    # EXECUTION ORDER (CRITICAL)
    # -------------------------------
    t1 >> t2 >> t3 >> t4 >> t5