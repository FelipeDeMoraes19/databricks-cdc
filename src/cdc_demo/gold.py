from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_dim_payments_scd2(events_df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("payment_id").orderBy("lsn")
    next_updated_at = F.lead("updated_at").over(window_spec)
    return events_df.withColumn("valid_from", F.col("updated_at")).withColumn(
        "valid_to", next_updated_at
    ).withColumn("is_current", next_updated_at.isNull()).withColumn(
        "is_deleted", F.col("op") == F.lit("DELETE")
    ).select(
        "payment_id",
        "lsn",
        "op",
        "status",
        "amount",
        "valid_from",
        "valid_to",
        "is_current",
        "is_deleted",
    )


def build_status_transitions_daily(events_df: DataFrame) -> DataFrame:
    return (
        events_df.withColumn("event_date", F.to_date("updated_at"))
        .groupBy("event_date", "status")
        .agg(F.count("*").alias("transition_count"))
        .orderBy("event_date", "status")
    )


def build_captured_volume_daily(events_df: DataFrame) -> DataFrame:
    return (
        events_df.filter(F.col("status") == "CAPTURED")
        .withColumn("event_date", F.to_date("updated_at"))
        .groupBy("event_date")
        .agg(
            F.count("*").alias("captured_count"),
            F.sum("amount").alias("captured_amount"),
        )
        .orderBy("event_date")
    )


def write_gold_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
