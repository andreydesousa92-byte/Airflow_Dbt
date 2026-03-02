from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

# CSVs Path
DATASETS_PATH = "/usr/local/airflow/include/dataset"

# Mapping CSV -> Final Table
TABLES_MAPPING = {
    "olist_customers_dataset.csv": ("customers", ["customer_id"]),
    "olist_orders_dataset.csv": ("orders", ["order_id"]),
    "olist_order_items_dataset.csv": ("order_items", ["order_id", "order_item_id"]),
    "olist_order_payments_dataset.csv": ("payments", ["order_id", "payment_sequential"]),
    "olist_order_reviews_dataset.csv": ("reviews", ["review_id"]),
    "olist_products_dataset.csv": ("products", ["product_id"]),
    "olist_sellers_dataset.csv": ("sellers", ["seller_id"]),
    "product_category_name_translation.csv": ("products_category", ["product_category_name"])
}

TABLE_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ],
    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "order_items": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ],
    "payments": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    ],
    "reviews": [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ],
    "products": [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ],
    "sellers": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ],
    "products_category": [
        "product_category_name",
        "product_category_name_english"
    ]
}

def upsert_csv_to_postgres(filename, table_name, pk_columns):
    """
    Function to do mass UPSERT
    """
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cur = conn.cursor()

    csv_path = os.path.join(DATASETS_PATH, filename)

    # Creating temp table staging.
    staging_table = f"staging_{table_name}"
    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)")

    # Load CSV into staging table
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(f"COPY {staging_table} FROM STDIN WITH CSV HEADER DELIMITER ','", f)

    # Build query for upsert
    columns = TABLE_COLUMNS[table_name]
    update_cols = ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in pk_columns])
    pk_condition = ", ".join(pk_columns)

    upsert_sql = f"""
        WITH dedup AS (
            SELECT DISTINCT ON ({pk_condition}) *
            FROM {staging_table}
            ORDER BY {pk_condition}
        )
        INSERT INTO {table_name} ({', '.join(columns)})
        SELECT {', '.join(columns)} FROM dedup
        ON CONFLICT ({pk_condition}) DO UPDATE
        SET {update_cols};
    """
    cur.execute(upsert_sql)

    conn.commit()
    cur.close()
    conn.close()


def ingest_all_csvs():
    for csv_file, (table_name, pk_columns) in TABLES_MAPPING.items():
        upsert_csv_to_postgres(csv_file, table_name, pk_columns)


# -----------------------------
# DAG
# -----------------------------
with DAG(
    "ingest_olist_csvs_optimized",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["ingest", "warehouse", "optimized"]
) as dag:

    task_ingest_all = PythonOperator(
        task_id="ingest_all_csvs",
        python_callable=ingest_all_csvs
    )