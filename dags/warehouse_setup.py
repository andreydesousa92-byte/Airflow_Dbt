from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# DAG de setup do warehouse
with DAG(
    "setup_olist_warehouse",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "warehouse"]
) as dag:

    def create_tables():
        hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        conn = hook.get_conn()
        cur = conn.cursor()

        # -------------------------------
        # TABLE CUSTOMERS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS customers CASCADE;
        CREATE TABLE customers (
            customer_id VARCHAR PRIMARY KEY,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix INT,
            customer_city VARCHAR,
            customer_state CHAR(2)
        );
        """)

        # -------------------------------
        # TABLE ORDERS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS orders CASCADE;
        CREATE TABLE orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR REFERENCES customers(customer_id),
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );
        """)

        # -------------------------------
        # TABLE ORDER_ITEMS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS order_items CASCADE;
        CREATE TABLE order_items (
            order_id VARCHAR REFERENCES orders(order_id),
            order_item_id INT,
            product_id VARCHAR,
            seller_id VARCHAR,
            shipping_limit_date TIMESTAMP,
            price NUMERIC,
            freight_value NUMERIC,
            PRIMARY KEY(order_id, order_item_id)
        );
        """)

        # -------------------------------
        # TABLE PAYMENTS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS payments CASCADE;
        CREATE TABLE payments (
            order_id VARCHAR REFERENCES orders(order_id),
            payment_sequential INT,
            payment_type VARCHAR,
            payment_installments INT,
            payment_value NUMERIC,
            PRIMARY KEY(order_id, payment_sequential)
        );
        """)

        # -------------------------------
        # TABLE REVIEWS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS reviews CASCADE;
        CREATE TABLE reviews (
            review_id VARCHAR PRIMARY KEY,
            order_id VARCHAR REFERENCES orders(order_id),
            review_score INT,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );
        """)

        # -------------------------------
        # TABLE PRODUCTS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS products CASCADE;
        CREATE TABLE products (
            product_id VARCHAR PRIMARY KEY,
            product_category_name VARCHAR,
            product_name_lenght INT,
            product_description_lenght INT,
            product_photos_qty INT,
            product_weight_g NUMERIC,
            product_length_cm NUMERIC,
            product_height_cm NUMERIC,
            product_width_cm NUMERIC
        );
        """)

        # -------------------------------
        # TABLE SELLERS
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS sellers CASCADE;
        CREATE TABLE sellers (
            seller_id VARCHAR PRIMARY KEY,
            seller_zip_code_prefix INT,
            seller_city VARCHAR,
            seller_state CHAR(2)
        );
        """)

        # -------------------------------
        # TABLE PRODUCTS CATEGORY
        # -------------------------------
        cur.execute("""
        DROP TABLE IF EXISTS products_category CASCADE;
        CREATE TABLE products_category (
            product_category_name VARCHAR PRIMARY KEY,
            product_category_name_english VARCHAR
        );
        """)

        conn.commit()
        cur.close()
        conn.close()

    # Task to craete tables
    task_create_tables = PythonOperator(
        task_id="create_all_tables",
        python_callable=create_tables
    )