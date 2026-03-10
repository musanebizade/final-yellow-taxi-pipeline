"""
ingest_trips.py
---------------
Ingests raw yellow taxi trip data from a CSV file into PostgreSQL
using PySpark. Loads data into the raw.taxi_trips table.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType
)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
CSV_FILE_PATH = "/opt/airflow/data/yellow_taxi.csv"
DB_URL = f"jdbc:postgresql://{os.environ['TARGET_DB_HOST']}:5432/{os.environ['TARGET_DB_NAME']}"
DB_PROPERTIES = {
    "user":     os.environ['TARGET_DB_USER'],
    "password": os.environ['TARGET_DB_PASSWORD'],
    "driver":   "org.postgresql.Driver"
}

def create_spark_session():
    """Creates and returns a SparkSession configured for PostgreSQL."""
    return SparkSession.builder \
        .appName("YellowTaxiIngestion") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def read_csv(spark):
    """
    Reads the yellow taxi CSV file into a Spark DataFrame.
    
    Args:
        spark: Active SparkSession
    Returns:
        Spark DataFrame with raw taxi data
    """
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(CSV_FILE_PATH)

def write_to_postgres(df):
    """
    Writes a Spark DataFrame to PostgreSQL raw.taxi_trips table.
    
    Args:
        df: Spark DataFrame to write
    """
    df.write \
        .jdbc(
            url=DB_URL,
            table="raw.taxi_trips",
            mode="overwrite",
            properties=DB_PROPERTIES
        )

def main():
    """Main entry point for the ingestion pipeline."""
    spark = create_spark_session()
    
    print("Reading CSV file...")
    df = read_csv(spark)
    print(f"Loaded {df.count()} rows from CSV")
    
    print("Writing to PostgreSQL raw.taxi_trips...")
    write_to_postgres(df)
    print("Ingestion complete!")
    
    spark.stop()

if __name__ == "__main__":
    main()