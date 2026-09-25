from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
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


def read_bronze_stream(spark: SparkSession, source_path: str) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(BRONZE_SCHEMA)
        .load(source_path)
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze_stream(stream_df: DataFrame, table_name: str, checkpoint_path: str) -> StreamingQuery:
    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    query.awaitTermination()
    return query
