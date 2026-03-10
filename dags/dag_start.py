"""
dag_start.py
------------
Entry point of the Yellow Taxi pipeline.
Scheduled at 00:06 daily. Emits a dataset signal to trigger dag_ingestion.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

INGESTION_SIGNAL = Dataset("ingestion_signal")

with DAG(
    dag_id="dag_start",
    start_date=datetime(2024, 1, 1),
    schedule="6 0 * * *",
    catchup=False,
    tags=["yellow_taxi"]
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
        task_id="end",
        outlets=[INGESTION_SIGNAL]
    )

    start >> end