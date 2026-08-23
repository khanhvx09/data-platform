from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.materialized_view(comment="Synthetic raw events, generated for the SDP smoke test.")
def sdp_bronze_events() -> DataFrame:
    return spark.range(20).withColumn(
        "event_type", F.when(F.col("id") % 2 == 0, "click").otherwise("view")
    )
