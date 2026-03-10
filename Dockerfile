FROM apache/airflow:3.1.7

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    dbt-postgres \
    sqlalchemy \
    pyspark \
    psycopg2-binary