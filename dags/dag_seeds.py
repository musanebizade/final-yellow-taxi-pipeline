"""
dag_seeds.py
------------
Seeds DAG for the Yellow Taxi pipeline.
Triggered by dag_staging via dataset signal.
Loads static reference CSV files into the database using dbt seed.
Emits gold_signal on completion to trigger dag_gold.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

SEEDS_SIGNAL = Dataset("seeds_signal")
GOLD_SIGNAL  = Dataset("gold_signal")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dag_seeds",
    start_date=datetime(2024, 1, 1),
    schedule=[SEEDS_SIGNAL],
    catchup=False,
    tags=["yellow_taxi", "seeds"]
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[GOLD_SIGNAL]
    )

    start >> dbt_seed >> end