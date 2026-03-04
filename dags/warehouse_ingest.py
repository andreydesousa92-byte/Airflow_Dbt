from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import logging

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# Using env var instead of hardcoded path so the code works
# across different environments (local, staging, cloud) without changes.
# ─────────────────────────────────────────────────────────────────
DATASETS_PATH = os.getenv("DATASETS_PATH", "/usr/local/airflow/include/dataset")

# ─────────────────────────────────────────────────────────────────
# CSV → TABLE MAPPING
# Single source of truth for all ingestion targets.
# To add a new table, just add one line here — nothing else changes.
# Format: "csv_filename": ("table_name", ["primary_key_columns"])
# ─────────────────────────────────────────────────────────────────
TABLES_MAPPING = {
    "olist_customers_dataset.csv":           ("customers",         ["customer_id"]),
    "olist_orders_dataset.csv":              ("orders",            ["order_id"]),
    "olist_order_items_dataset.csv":         ("order_items",       ["order_id", "order_item_id"]),
    "olist_order_payments_dataset.csv":      ("payments",          ["order_id", "payment_sequential"]),
    "olist_order_reviews_dataset.csv":       ("reviews",           ["review_id"]),
    "olist_products_dataset.csv":            ("products",          ["product_id"]),
    "olist_sellers_dataset.csv":             ("sellers",           ["seller_id"]),
    "product_category_name_translation.csv": ("products_category", ["product_category_name"]),
}

# ─────────────────────────────────────────────────────────────────
# TABLE COLUMNS
# Explicit column lists per table.
# Used to build INSERT and UPDATE statements dynamically,
# ─────────────────────────────────────────────────────────────────
TABLE_COLUMNS = {
    "customers": [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state"
    ],
    "orders": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "order_items": [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value"
    ],
    "payments": [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value"
    ],
    "reviews": [
        "review_id", "order_id", "review_score",
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp"
    ],
    "products": [
        "product_id", "product_category_name",
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm"
    ],
    "sellers": [
        "seller_id", "seller_zip_code_prefix",
        "seller_city", "seller_state"
    ],
    "products_category": [
        "product_category_name", "product_category_name_english"
    ],
}


# ─────────────────────────────────────────────────────────────────
# UPSERT FUNCTION
# Kept separate from the DAG definition so it can be unit tested
# independently. Receives all inputs as explicit parameters
#
# Flow:
#   1. Validate the CSV file exists before touching the database
#   2. Create a temporary staging table (mirrors the target table)
#   3. Load CSV into staging using COPY (bulk load, much faster than INSERT)
#   4. Deduplicate staging rows using DISTINCT ON
#   5. Upsert from staging into the final table using ON CONFLICT
# ─────────────────────────────────────────────────────────────────
def upsert_csv_to_postgres(filename: str, table_name: str, pk_columns: list):
    logger = logging.getLogger(__name__)

    csv_path = os.path.join(DATASETS_PATH, filename)

    # Fail early with a clear message if the file is missing.
    # Better than letting Postgres raise a cryptic error later.
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV not found: {csv_path}. "
            f"Check that the file exists under {DATASETS_PATH}"
        )

    # Pre-build SQL fragments used in the upsert query
    columns       = TABLE_COLUMNS[table_name]
    staging_table = f"staging_{table_name}"
    pk_condition  = ", ".join(pk_columns)

    # Build "col = EXCLUDED.col" pairs for every non-PK column.
    # EXCLUDED refers to the row that failed to insert due to conflict.
    update_cols = ", ".join([
        f"{col}=EXCLUDED.{col}"
        for col in columns
        if col not in pk_columns
    ])

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()

    try:
        cur = conn.cursor()
        logger.info(f"[{table_name}] Starting ingestion from {filename}")

        # ── Staging table ──────────────────────────────────────────
        # TEMP TABLE exists only for this session, so parallel tasks
        # targeting different tables won't collide on the same name.
        # LIKE ... INCLUDING DEFAULTS copies column types and defaults.
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        cur.execute(f"""
            CREATE TEMP TABLE {staging_table}
            (LIKE {table_name} INCLUDING DEFAULTS)
        """)

        # ── Bulk load via COPY ─────────────────────────────────────
        # COPY is Postgres's fastest ingestion method — it streams
        # the entire file in one operation instead of row-by-row inserts.
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {staging_table} FROM STDIN WITH CSV HEADER DELIMITER ','",
                f
            )

        # Log how many rows landed in staging for observability
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        staging_count = cur.fetchone()[0]
        logger.info(f"[{table_name}] {staging_count} rows loaded into staging")

        # ── Upsert with deduplication ──────────────────────────────
        # DISTINCT ON removes duplicate PKs that may exist in the CSV
        # before they reach the INSERT, preventing ON CONFLICT errors.
        # ON CONFLICT handles re-runs safely: existing rows are updated,
        # new rows are inserted — making the operation idempotent.
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

        # Log final row count to confirm the upsert outcome
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cur.fetchone()[0]
        logger.info(f"[{table_name}] Final row count: {final_count}. Ingestion complete.")

        conn.commit()

    except Exception as e:
        # Roll back any partial changes so the table is never left
        # in a half-written state. Airflow will retry based on default_args.
        conn.rollback()
        logger.error(f"[{table_name}] Ingestion failed: {e}")
        raise  # Re-raise so Airflow marks the task as failed

    finally:
        # Always close the connection — even if an exception occurred.
        # Without this, idle connections accumulate (connection leak).
        cur.close()
        conn.close()


# ─────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# Applied to every task in this DAG automatically.
# retries=3 means Airflow will retry a failed task 3 times
# before marking it as permanently failed.
# retry_delay controls how long to wait between attempts.
# ─────────────────────────────────────────────────────────────────
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────
with DAG(
    "ingest_olist_csvs",
    start_date=datetime(2026, 2, 1),
    schedule=None,       # Triggered manually or by upstream DAG
    catchup=False,       # Don't backfill historical runs on first deploy
    default_args=default_args,
    tags=["ingest", "warehouse"],
    doc_md="""
    ## Olist CSV Ingestion
    Loads all Olist CSV files into the Postgres warehouse.
    Each table is an independent task — one failure does not block others.
    The upsert logic makes every run idempotent: safe to re-run without duplicating data.
    Task dependencies mirror the foreign key relationships in the schema.
    """
) as dag:

    # ─────────────────────────────────────────────────────────────
    # ONE TASK PER TABLE
    # Instead of looping inside a single function, each CSV becomes
    # its own Airflow task. This gives you:
    #   - Per-table visibility in the Airflow UI
    #   - Granular retries (a reviews failure won't re-run customers)
    #   - Potential parallelism for independent tables
    # ─────────────────────────────────────────────────────────────
    ingest_tasks = {}

    for csv_file, (table_name, pk_columns) in TABLES_MAPPING.items():
        ingest_tasks[table_name] = PythonOperator(
            task_id=f"ingest_{table_name}",
            python_callable=upsert_csv_to_postgres,
            op_kwargs={
                "filename":   csv_file,
                "table_name": table_name,
                "pk_columns": pk_columns,
            },
        )

    # ─────────────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    # Mirrors the foreign key constraints defined in the warehouse schema.
    # A >> B means "A must succeed before B starts".
    # This prevents FK violation errors during parallel ingestion.
    #
    # Dependency map:
    #   customers ──► orders ──► order_items
    #   sellers   ──────────────► order_items
    #   products  ──────────────► order_items
    #                 orders ──► payments
    #                 orders ──► reviews
    # ─────────────────────────────────────────────────────────────
    ingest_tasks["customers"] >> ingest_tasks["orders"]
    ingest_tasks["orders"]    >> ingest_tasks["order_items"]
    ingest_tasks["sellers"]   >> ingest_tasks["order_items"]
    ingest_tasks["products"]  >> ingest_tasks["order_items"]
    ingest_tasks["orders"]    >> ingest_tasks["payments"]
    ingest_tasks["orders"]    >> ingest_tasks["reviews"]