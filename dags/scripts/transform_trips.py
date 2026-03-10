"""
transform_trips.py
------------------
PySpark equivalent of the dbt stg_trips model.
Reads from raw.taxi_trips, applies the same cleaning and 
transformation logic as stg_trips.sql, and saves the result
to silver.stg_trips_spark for comparison with dbt output.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round, when, hour, extract
)
 
from dotenv import load_dotenv
load_dotenv()   

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
DB_URL = f"jdbc:postgresql://{os.environ['TARGET_DB_HOST']}:5432/{os.environ['TARGET_DB_NAME']}"
DB_PROPERTIES = {
    "user":     os.environ['TARGET_DB_USER'],
    "password": os.environ['TARGET_DB_PASSWORD'],
    "driver":   "org.postgresql.Driver"
}

def create_spark_session():
    """Creates and returns a SparkSession configured for PostgreSQL."""
    return SparkSession.builder \
        .appName("YellowTaxiTransform") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

def read_from_postgres(spark):
    """
    Reads raw taxi trips from PostgreSQL.
    
    Args:
        spark: Active SparkSession
    Returns:
        Spark DataFrame with raw taxi data
    """
    return spark.read \
        .jdbc(
            url=DB_URL,
            table="raw.taxi_trips",
            properties=DB_PROPERTIES
        )

def assign_location_id(df):
    """
    Assigns a pickup_location_id based on coordinate boundaries.
    Mirrors the CASE WHEN logic in stg_trips.sql.
    
    Args:
        df: Spark DataFrame with pickup_longitude and pickup_latitude
    Returns:
        Spark DataFrame with pickup_location_id column added
    """
    return df.withColumn("pickup_location_id",
        when((col("pickup_latitude").between(40.50, 40.65)) & (col("pickup_longitude").between(-74.30, -74.00)), 1)
        .when((col("pickup_latitude").between(40.50, 40.65)) & (col("pickup_longitude").between(-74.00, -73.85)), 2)
        .when((col("pickup_latitude").between(40.50, 40.65)) & (col("pickup_longitude").between(-73.85, -73.70)), 3)
        .when((col("pickup_latitude").between(40.65, 40.75)) & (col("pickup_longitude").between(-74.30, -74.00)), 4)
        .when((col("pickup_latitude").between(40.65, 40.75)) & (col("pickup_longitude").between(-74.00, -73.85)), 5)
        .when((col("pickup_latitude").between(40.65, 40.75)) & (col("pickup_longitude").between(-73.85, -73.70)), 6)
        .when((col("pickup_latitude").between(40.75, 40.90)) & (col("pickup_longitude").between(-74.30, -74.00)), 7)
        .when((col("pickup_latitude").between(40.75, 40.90)) & (col("pickup_longitude").between(-74.00, -73.85)), 8)
        .when((col("pickup_latitude").between(40.75, 40.90)) & (col("pickup_longitude").between(-73.85, -73.70)), 9)
        .otherwise(0)
    )

def transform(df):
    """
    Applies the same cleaning and transformation logic as stg_trips.sql.
    
    Args:
        df: Raw Spark DataFrame
    Returns:
        Transformed Spark DataFrame
    """
    df = df.filter(
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0) &
        (col("total_amount") > 0) &
        (col("RateCodeID") != 99) &
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
        (col("pickup_latitude").between(40.5, 40.9)) &
        (col("pickup_longitude").between(-74.3, -73.7))
    )

    df = df.select(
        col("VendorID").cast("integer").alias("vendorid"),
        col("RateCodeID").cast("integer").alias("ratecodeid"),
        col("tpep_pickup_datetime").cast("timestamp"),
        col("tpep_dropoff_datetime").cast("timestamp"),
        col("passenger_count").cast("integer"),
        col("trip_distance").cast("double"),
        col("total_amount").cast("double"),
        col("tip_amount").cast("double"),
        col("payment_type").cast("integer"),
        col("pickup_longitude").cast("double"),
        col("pickup_latitude").cast("double"),
        round(
            (col("tpep_dropoff_datetime").cast("long") -
             col("tpep_pickup_datetime").cast("long")) / 60, 2
        ).alias("trip_duration_minutes"),
        when(hour(col("tpep_pickup_datetime")).between(6, 11), "morning")
        .when(hour(col("tpep_pickup_datetime")).between(12, 17), "afternoon")
        .when(hour(col("tpep_pickup_datetime")).between(18, 21), "evening")
        .otherwise("night")
        .alias("time_of_day")
    )

    df = assign_location_id(df)

    return df

def write_to_postgres(df):
    """
    Writes transformed DataFrame to PostgreSQL silver.stg_trips_spark.
    Repartitions data to write in smaller batches to avoid OOM errors.
    
    Args:
        df: Transformed Spark DataFrame
    """
    print(f"{df.count()} Writing {df.count()} rows to PostgreSQL...")
    
    df.repartition(1).write \
        .option("batchsize", "1000") \
        .option("numPartitions", "1") \
        .jdbc(
            url=DB_URL,
            table="silver.stg_trips_spark",
            mode="overwrite",
            properties=DB_PROPERTIES
        )
    
def create_schema_if_not_exists():
    """Creates silver schema in PostgreSQL if it doesn't exist."""
    import psycopg2
    conn = psycopg2.connect(
        host=os.environ['TARGET_DB_HOST'],
        port=5432,
        dbname=os.environ['TARGET_DB_NAME'],
        user=os.environ['TARGET_DB_USER'],
        password=os.environ['TARGET_DB_PASSWORD']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    cursor.close()
    conn.close()
    print("Silver schema ready.")    

def main():
    """Main entry point for the transformation pipeline."""
    spark = create_spark_session()

    print("Creating schemas if not exists...")
    create_schema_if_not_exists()

    print("Reading from raw.taxi_trips...")
    df = read_from_postgres(spark)
    print("Columns in raw table:", df.columns)

    print("Applying transformations...")
    df = transform(df)
    print(f"Transformation applied successfully")

    print("Writing to silver.stg_trips_spark...")
    write_to_postgres(df)
    print("Transform complete!")

    spark.stop()

if __name__ == "__main__":
    main()