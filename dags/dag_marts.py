"""
dag_marts.py
------------
Marts layer DAG for the Yellow Taxi pipeline.
Triggered by dag_gold via dataset signal.
Builds business-ready aggregated tables in the mart schema.
Emits end_signal on completion to trigger dag_end.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

MARTS_SIGNAL = Dataset("marts_signal")
END_SIGNAL   = Dataset("end_signal")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dag_marts",
    start_date=datetime(2024, 1, 1),
    schedule=[MARTS_SIGNAL],
    catchup=False,
    tags=["yellow_taxi", "marts"]
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_mart_monthly_summary = BashOperator(
        task_id="dbt_mart_monthly_summary",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select mart_monthly_summary"
        ),
    )

    dbt_mart_top_zones = BashOperator(
        task_id="dbt_mart_top_zones",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select mart_top_zones"
        ),
    )

    dbt_mart_payment_breakdown = BashOperator(
        task_id="dbt_mart_payment_breakdown",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select mart_payment_breakdown"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select mart_monthly_summary mart_top_zones mart_payment_breakdown"
        ),
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[END_SIGNAL]
    )

    start >> [dbt_mart_monthly_summary, dbt_mart_top_zones, dbt_mart_payment_breakdown] >> dbt_test_marts >> end