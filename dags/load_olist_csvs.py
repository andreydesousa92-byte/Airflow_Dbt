from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd 
import os 

#Path to CSV's
DATASETS_PATH = "/usr/local/airflow/include/dataset"

def load_csv_to_postgres(filename, table_name):
    
    # Stablishes the connection
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Convert CSV to Pandas
    df = pd.read_csv(os.path.join(DATASETS_PATH, filename))

    # Removes previous table if it exists
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Mapp all columns and create the table on wh
    cols = ", ".join([f"{c} TEXT" for c in df.columns])
    cur.execute(f"CREATE TABLE {table_name} ({cols})")

    # Insert the data on the newly created table
    for i, row in df.iterrows():
        values = tuple(row.astype(str))
        placeholders = ", ".join(["%s"] * len(values))
        cur.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)

    # Commit and close the Postgres connection
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "load_olist_csvs",
    start_date=datetime(2026, 2, 1),
    schedule=None,  
    catchup=False
) as dag:

    task_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_customers_dataset.csv", "table_name": "customers"}
    )

    task_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_order_items_dataset.csv", "table_name": "order_items"}
    )

    task_payments = PythonOperator(
        task_id="load_payments",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_order_payments_dataset.csv", "table_name": "payments"}
    )

    task_reviews = PythonOperator(
        task_id="load_reviews",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_order_reviews_dataset.csv", "table_name": "reviews"}
    )

    task_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_orders_dataset.csv", "table_name": "orders"}
    )

    task_products = PythonOperator(
        task_id="load_products",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_products_dataset.csv", "table_name": "products"}
    )

    task_sellers = PythonOperator(
        task_id="load_sellers",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "olist_sellers_dataset.csv", "table_name": "sellers"}
    )

    task_products_category = PythonOperator(
        task_id="load_products_category",
        python_callable=load_csv_to_postgres,
        op_kwargs={"filename": "product_category_name_translation.csv", "table_name": "products_category"}
    )

    task_customers >> task_order_items >> task_payments >> task_reviews >> task_orders >> task_products >> task_sellers >> task_products_category


