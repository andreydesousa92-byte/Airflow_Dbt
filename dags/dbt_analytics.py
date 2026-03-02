from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# 1. Path to dbt_project.yml
DBT_PROJECT_PATH = Path("/usr/local/airflow/dags/dbt/olist_transformation")

# 2. Configure connection (Mapp Airflow to dbt)
profile_config = ProfileConfig(
    profile_name="olist_transformation",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_warehouse",
        profile_args={"schema": "public"},
    ),
)

# 3. Create dbt DAG
dbt_analytics_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    schedule="@daily",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    dag_id="dbt_analytics_olist",
)