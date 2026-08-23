#!/bin/bash
#
# Optional add-on, not run automatically by startup.sh. Sets up what's needed
# to use `spark-pipelines` (the Spark 4.1 Declarative Pipelines CLI), which
# requires a Spark Connect server that the normal startup.sh doesn't start.
#
# Run manually, from the host, whenever you want SDP available:
#   docker exec spark-master bash /opt/startup-sdp.sh
#
# Then, inside the container:
#   $SPARK_HOME/bin/spark-pipelines run --remote sc://localhost:15002

export DELTA_PACKAGE_VERSION=delta-spark_4.1_2.13:4.3.1

# The base image's PIP_INDEX_URL points at an internal Databricks proxy that
# isn't reachable from here; override it with public PyPI for this install.
# Needed because the Spark Connect client (used by spark-pipelines) requires
# pandas/pyarrow/grpc/zstandard, none of which are in the base image.
PIP_INDEX_URL=https://pypi.org/simple python3 -m pip install --user -r /opt/requirements.txt

# `spark-pipelines` only talks to Spark over Spark Connect, so start a
# Connect server for it. Runs self-contained in local mode instead of on the
# standalone cluster so it doesn't compete with the worker's cores (only 2,
# already claimed by the pyspark shell started by startup.sh). $SPARK_HOME/logs
# isn't writable by NBuser, so redirect Spark's own log/pid dirs to /tmp.
mkdir -p /tmp/spark-logs /tmp/spark-pid
SPARK_LOG_DIR=/tmp/spark-logs SPARK_PID_DIR=/tmp/spark-pid \
  $SPARK_HOME/sbin/start-connect-server.sh \
  --master "local[2]" \
  --packages io.delta:${DELTA_PACKAGE_VERSION} \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.warehouse.dir=file:///opt/work/spark-warehouse" \
  > /tmp/connect-server.log 2>&1

echo "Spark Connect server started (log: /tmp/connect-server.log). Run pipelines with:"
echo "  \$SPARK_HOME/bin/spark-pipelines run --remote sc://localhost:15002"
