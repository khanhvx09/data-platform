"""Spark job submitted by the spark_test_table DAG.

Connects to the spark-master standalone cluster, creates a Delta table
registered in the Hive metastore, inserts a few rows, and reads them back
to prove the round trip worked.
"""

from pyspark.sql import SparkSession

DB_NAME = "test_db"
TABLE_NAME = "test_table"
WAREHOUSE_DIR = "s3a://data-platform/managed"
TABLE_LOCATION = f"s3a://data-platform/landing/{TABLE_NAME}"

spark = (
    SparkSession.builder.appName("airflow-create-test-table")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "airflow-worker")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} LOCATION '{WAREHOUSE_DIR}/{DB_NAME}.db'")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
        id BIGINT,
        name STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION '{TABLE_LOCATION}'
""")

spark.sql(f"""
    INSERT INTO {DB_NAME}.{TABLE_NAME} (id, name, created_at)
    VALUES
        (1, 'Alice', current_timestamp()),
        (2, 'Bob', current_timestamp()),
        (3, 'Charlie', current_timestamp())
""")

spark.sql(f"SELECT * FROM {DB_NAME}.{TABLE_NAME} ORDER BY id").show(truncate=False)

spark.stop()
