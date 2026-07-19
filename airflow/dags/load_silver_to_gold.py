from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import trino


def load():

    conn = trino.dbapi.connect(
        host="trino",
        port=8080,
        user="airflow",
        catalog="iceberg",
        schema="gold",
    )

    cursor = conn.cursor()

    cursor.execute("DELETE FROM iceberg.gold.fact_sales")

    cursor.execute("""
    INSERT INTO iceberg.gold.fact_sales

    SELECT

        t.transaction_id,
        t.ts,
        t.store_id,

        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        c.segment,

        p.product_id,
        p.name,
        p.category,
        p.brand,

        ti.quantity,
        ti.unit_price,
        ti.quantity * ti.unit_price,

        t.payment_method

    FROM iceberg.silver.pos_transactions t

    JOIN iceberg.silver.customers c
        ON t.customer_id = c.customer_id

    JOIN iceberg.silver.transaction_items ti
        ON t.transaction_id = ti.transaction_id

    JOIN iceberg.silver.products p
        ON ti.product_id = p.product_id
    """)

    # ==========================
    # sales_daily
    # ==========================

    cursor.execute("DELETE FROM iceberg.gold.sales_daily")

    cursor.execute("""
    INSERT INTO iceberg.gold.sales_daily

    SELECT

        DATE(ts) AS sale_date,

        COUNT(DISTINCT transaction_id) AS transactions_count,

        SUM(quantity) AS items_sold,

        SUM(line_amount) AS revenue,

        SUM(line_amount) / COUNT(DISTINCT transaction_id) AS avg_check

    FROM iceberg.gold.fact_sales

    GROUP BY DATE(ts)
    """)

    # ==========================
    # product_sales
    # ==========================

    cursor.execute("DELETE FROM iceberg.gold.product_sales")

    cursor.execute("""
    INSERT INTO iceberg.gold.product_sales

    SELECT

        product_id,
        product_name,
        category,
        brand,

        SUM(quantity) AS units_sold,

        SUM(line_amount) AS revenue

    FROM iceberg.gold.fact_sales

    GROUP BY

        product_id,
        product_name,
        category,
        brand
    """)

    conn.close()


with DAG(

    dag_id="load_silver_to_gold",

    start_date=datetime(2025, 1, 1),

    catchup=False,

    schedule=None

) as dag:

    PythonOperator(

        task_id="load",

        python_callable=load

    )