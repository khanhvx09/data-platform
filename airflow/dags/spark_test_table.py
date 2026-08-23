from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Same Delta + Hadoop-AWS package pins used by spark-master's own driver
# (see startup.sh) so the client-mode driver launched here speaks the same
# protocol version as the standalone cluster.
DELTA_PACKAGE = "io.delta:delta-spark_4.1_2.13:4.3.1"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"

with DAG(
    dag_id="spark_test_table",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "test"],
):

    create_and_insert = SparkSubmitOperator(
        task_id="create_and_insert_test_table",
        conn_id="spark_standalone",
        application="/opt/airflow/dags/spark_jobs/create_test_table.py",
        name="airflow-create-test-table",
        packages=f"{DELTA_PACKAGE},{HADOOP_AWS_PACKAGE}",
        conf={
            # The driver runs inside the airflow-worker container; executors
            # on spark-worker need a resolvable address to call back to it.
            "spark.driver.host": "airflow-worker",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.extraJavaOptions": (
                "-Divy.cache.dir=/tmp -Divy.home=/tmp "
                "-Dio.netty.tryReflectionSetAccessible=true"
            ),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
            "spark.hadoop.hive.metastore.schema.verification": "false",
            # fs.s3a.* (endpoint/keys/path-style) come from SPARK_CONF_DIR's
            # core-site.xml (mounted from hive-metastore/core-site.xml), the
            # same file spark-master/spark-worker use — see docker-compose-airflow.yaml.
        },
        verbose=True,
    )
