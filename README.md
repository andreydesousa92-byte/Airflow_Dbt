# Analytics Engineering Pipeline — Olist

A production-grade data pipeline built with **Apache Airflow**, **dbt**, and **Docker**, using the public [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) as the data source.

The project demonstrates end-to-end analytics engineering practices: reliable ingestion, layered transformations, automated data quality testing, and full pipeline orchestration.

---

## Architecture

```
CSV Files
   ↓
Airflow — Ingestion DAGs
   ↓
Postgres — Raw Warehouse
   ↓
dbt via Cosmos — Transformation
   ├── Staging Layer
   └── Mart Layer
        ↓
   Analytics-ready tables
```

---

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Astronomer Runtime) |
| Containerisation | Docker + Astro CLI |
| Transformation | dbt (data build tool) |
| dbt-Airflow Integration | Astronomer Cosmos |
| Warehouse | PostgreSQL |

---

## Project Structure

```
├── dags/
│   ├── warehouse_setup.py          # One-time raw table creation
│   ├── warehouse_ingest.py         # CSV ingestion DAG
│   ├── dbt_analytics.py            # Cosmos DAG — dbt orchestration
│   └── dbt/
│       └── olist_transformation/
│           ├── dbt_project.yml
│           ├── models/
│           │   ├── staging/
│           │   │   ├── stg_customers.sql
│           │   │   ├── stg_orders.sql
│           │   │   ├── stg_order_items.sql
│           │   │   ├── stg_payments.sql
│           │   │   ├── stg_reviews.sql
│           │   │   ├── stg_products.sql
│           │   │   ├── stg_sellers.sql
│           │   │   └── stg_products_category.sql
│           │   └── marts/
│           │       ├── orders_per_customer.sql
│           │       ├── delivery_performance.sql
│           │       └── revenue_per_seller.sql
│           └── schema.yml
├── include/
│   └── dataset/                    # Olist CSV files
├── Dockerfile
├── docker-compose.override.yml
├── requirements.txt
└── packages.txt
```

---

## Pipeline Detail

### Ingestion

Each CSV file is ingested as an **independent Airflow task** — failures are isolated and retried individually without reprocessing the entire pipeline.

The ingestion uses a **staging table + upsert pattern**:

1. Data is bulk-loaded into a temporary staging table via Postgres's native `COPY` command
2. Duplicates are removed using `DISTINCT ON`
3. Data is upserted into the final table using `ON CONFLICT`

This makes every run fully **idempotent** — safe to re-run without duplicating data. Task dependencies mirror the foreign key relationships in the schema, ensuring referential integrity during parallel execution.

### Transformation

All transformations are handled by **dbt**, integrated into Airflow via **Cosmos** — which automatically converts each dbt model into an Airflow task, preserving model dependencies as task dependencies in the DAG graph.

#### Staging Layer

One model per source table. No joins or business logic — only cleaning and standardisation:

- Column renaming for consistency
- Explicit type casting
- Value normalisation (e.g. uppercasing city names)

All models reference raw tables via dbt's `source()` macro and are consumed downstream via `ref()`, enabling full lineage tracking.

#### Mart Layer

Business-oriented models built on top of staging, computing aggregated metrics ready for analytical consumption.

| Model | Description |
|---|---|
| `orders_per_customer` | Order volume and delivery rate per unique customer |
| `delivery_performance` | Lead times and on-time delivery rate per city and state |
| `revenue_per_seller` | Revenue, ticket size, and delivery performance per seller |

### Data Quality

Every model is covered by automated dbt tests defined in `schema.yml`:

| Test | Description |
|---|---|
| `not_null` | Ensures critical columns never contain null values |
| `unique` | Ensures primary keys are never duplicated |
| `accepted_values` | Validates categorical columns against allowed values |
| `relationships` | Validates referential integrity between models |

**47 automated tests** run across all models. A failed test blocks every downstream model from executing — the pipeline breaks loudly, not silently.

---

## How to Run

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Setup

```bash
# Clone the repository
git clone https://github.com/andreydesousa92-byte/Airflow_Dbt
cd Airflow_Dbt

# Start the Airflow environment
astro dev start
```

Access the Airflow UI at `http://localhost:8080`

### Running the Pipeline

Run the DAGs in the following order:

1. `setup_olist_warehouse` — creates the raw tables (run once)
2. `ingest_olist_csvs` — loads all CSV files into Postgres
3. `dbt_analytics_olist` — runs all dbt models and tests

---

## Key Engineering Decisions

**Independent tasks per table** — rather than ingesting all CSVs in a single function, each table is an independent Airflow task. This enables granular retries, parallel execution, and clear visibility in the Airflow UI.

**Idempotent upsert pattern** — the staging + upsert approach ensures the pipeline can be re-run at any time without data corruption, which is critical in production environments where retries are inevitable.

**Separation of concerns across dbt layers** — staging models never contain business logic; mart models never touch raw sources directly. This makes the pipeline easier to maintain, test, and extend.

**Tests at every layer** — source tables, staging models, and marts are all tested. Data quality issues are caught at the earliest possible point in the pipeline.

---

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — a real commercial dataset containing 100k orders from 2016 to 2018, including orders, products, sellers, customers, payments, and reviews.
