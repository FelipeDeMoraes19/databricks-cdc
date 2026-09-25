from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

BRONZE_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("lsn", LongType(), False),
        StructField("op", StringType(), False),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


def build_bronze_df(spark: SparkSession, events: List[Dict[str, Any]]) -> DataFrame:
    df = spark.createDataFrame(events, schema=BRONZE_SCHEMA)
    return df.withColumn("_ingested_at", F.current_timestamp())
