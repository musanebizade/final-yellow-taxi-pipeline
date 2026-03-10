"""
dag_ingestion.py
----------------
Ingestion DAG for the Yellow Taxi pipeline.
Triggered by dag_start via dataset signal.
Reads raw CSV data using PySpark and loads it into raw.taxi_trips.
Emits staging_signal on completion to trigger dag_staging.
"""

import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

INGESTION_SIGNAL = Dataset("ingestion_signal")
STAGING_SIGNAL   = Dataset("staging_signal")

SCRIPTS_DIR = "/opt/airflow/dags/scripts"

with DAG(
    dag_id="dag_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=[INGESTION_SIGNAL],
    catchup=False,
    tags=["yellow_taxi", "ingestion"]
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_with_pyspark = BashOperator(
        task_id="ingest_with_pyspark",
        bash_command=f"python {SCRIPTS_DIR}/ingest_trips.py",
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[STAGING_SIGNAL]
    )

    start >> ingest_with_pyspark >> end