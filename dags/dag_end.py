"""
dag_end.py
----------
Final DAG of the Yellow Taxi pipeline.
Triggered by dag_marts via dataset signal.
Marks the entire pipeline as complete.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

END_SIGNAL = Dataset("end_signal")

with DAG(
    dag_id="dag_end",
    start_date=datetime(2024, 1, 1),
    schedule=[END_SIGNAL],
    catchup=False,
    tags=["yellow_taxi"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end