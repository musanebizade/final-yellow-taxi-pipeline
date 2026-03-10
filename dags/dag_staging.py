"""
dag_staging.py
--------------
Staging DAG for the Yellow Taxi pipeline.
Triggered by dag_ingestion via dataset signal.
Runs dbt stg_trips and PySpark transform in parallel.
If PySpark transform fails, DAG still continues and emits seeds_signal.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

STAGING_SIGNAL = Dataset("staging_signal")
SEEDS_SIGNAL   = Dataset("seeds_signal")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
SCRIPTS_DIR      = "/opt/airflow/dags/scripts"

with DAG(
    dag_id="dag_staging",
    start_date=datetime(2024, 1, 1),
    schedule=[STAGING_SIGNAL],
    catchup=False,
    tags=["yellow_taxi", "staging"]
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_stg_trips = BashOperator(
        task_id="dbt_stg_trips",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select stg_trips"
        ),
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select stg_trips"
        ),
    )

    pyspark_transform = BashOperator(
        task_id="pyspark_transform",
        bash_command=f"python {SCRIPTS_DIR}/transform_trips.py",
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[SEEDS_SIGNAL],
        trigger_rule="all_done"  # ← emits signal even if pyspark fails
    )

    start >> [dbt_stg_trips, pyspark_transform]
    dbt_stg_trips >> dbt_test_staging
    [dbt_test_staging, pyspark_transform] >> end