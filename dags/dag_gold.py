"""
dag_gold.py
-----------
Gold layer DAG for the Yellow Taxi pipeline.
Triggered by dag_seeds via dataset signal.
Builds fact and dimension tables in the gold schema.
Emits marts_signal on completion to trigger dag_marts.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

GOLD_SIGNAL  = Dataset("gold_signal")
MARTS_SIGNAL = Dataset("marts_signal")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dag_gold",
    start_date=datetime(2024, 1, 1),
    schedule=[GOLD_SIGNAL],
    catchup=False,
    tags=["yellow_taxi", "gold"]
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_fct_trips = BashOperator(
        task_id="dbt_fct_trips",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select fct_trips"
        ),
    )

    dbt_dim_vendor = BashOperator(
        task_id="dbt_dim_vendor",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select dim_vendor"
        ),
    )

    dbt_dim_rate = BashOperator(
        task_id="dbt_dim_rate",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select dim_rate"
        ),
    )

    dbt_dim_locations = BashOperator(
        task_id="dbt_dim_locations",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select dim_locations"
        ),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select fct_trips dim_vendor dim_rate dim_locations"
        ),
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[MARTS_SIGNAL]
    )

    start >> [dbt_fct_trips, dbt_dim_vendor, dbt_dim_rate, dbt_dim_locations] >> dbt_test_gold >> end