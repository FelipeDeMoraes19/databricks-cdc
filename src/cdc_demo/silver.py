from typing import Callable, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def dedup_exact_duplicates(df: DataFrame, key_cols: Tuple[str, ...] = ("payment_id", "lsn")) -> DataFrame:
    return df.dropDuplicates(list(key_cols))


def dedup_latest_by_lsn(df: DataFrame, key_col: str = "payment_id", lsn_col: str = "lsn") -> DataFrame:
    window_spec = Window.partitionBy(key_col).orderBy(F.col(lsn_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_valid_and_quarantine(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    typed_df = (
        df.withColumn("payment_id_typed", F.expr("try_cast(payment_id AS BIGINT)"))
        .withColumn("amount_typed", F.expr("try_cast(amount AS DECIMAL(14,2))"))
        .withColumn("updated_at_typed", F.expr("try_to_timestamp(updated_at)"))
    )

    reason = (
        F.when(F.col("_rescued_data").isNotNull(), F.lit("rescued_data_present"))
        .when(F.col("payment_id_typed").isNull(), F.lit("invalid_payment_id"))
        .when(F.col("amount_typed").isNull(), F.lit("invalid_amount"))
        .when(F.col("updated_at_typed").isNull(), F.lit("invalid_updated_at"))
    )

    classified_df = typed_df.withColumn("_quarantine_reason", reason)

    valid_df = classified_df.filter(F.col("_quarantine_reason").isNull()).select(
        F.col("payment_id_typed").alias("payment_id"),
        "lsn",
        "op",
        F.col("amount_typed").alias("amount"),
        "status",
        F.col("updated_at_typed").alias("updated_at"),
    )

    quarantine_df = classified_df.filter(F.col("_quarantine_reason").isNotNull()).select(
        "payment_id",
        "lsn",
        "op",
        "amount",
        "status",
        "updated_at",
        "_rescued_data",
        "_source_file",
        "_ingested_at",
        F.col("_quarantine_reason").alias("reason"),
    )

    return valid_df, quarantine_df


def build_merge_sql(
    target_table: str,
    source_view: str,
    key_col: str = "payment_id",
    lsn_col: str = "lsn",
) -> str:
    return f"""
    MERGE INTO {target_table} AS target
    USING {source_view} AS source
    ON target.{key_col} = source.{key_col}
    WHEN MATCHED AND source.op = 'DELETE' AND source.{lsn_col} > target.{lsn_col} THEN
      DELETE
    WHEN MATCHED AND source.{lsn_col} > target.{lsn_col} THEN
      UPDATE SET
        target.{lsn_col} = source.{lsn_col},
        target.op = source.op,
        target.amount = source.amount,
        target.status = source.status,
        target.updated_at = source.updated_at,
        target._silver_processed_at = current_timestamp()
    WHEN NOT MATCHED AND source.op != 'DELETE' THEN
      INSERT ({key_col}, {lsn_col}, op, amount, status, updated_at, _silver_processed_at)
      VALUES (source.{key_col}, source.{lsn_col}, source.op, source.amount, source.status, source.updated_at, current_timestamp())
    """


def ensure_silver_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          payment_id BIGINT,
          lsn BIGINT,
          op STRING,
          amount DECIMAL(14,2),
          status STRING,
          updated_at TIMESTAMP,
          _silver_processed_at TIMESTAMP
        ) USING DELTA
        """
    )


def ensure_quarantine_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          payment_id STRING,
          lsn BIGINT,
          op STRING,
          amount STRING,
          status STRING,
          updated_at STRING,
          _rescued_data STRING,
          _source_file STRING,
          _ingested_at TIMESTAMP,
          reason STRING
        ) USING DELTA
        """
    )


def process_batch(batch_df: DataFrame, silver_table: str, quarantine_table: str) -> None:
    deduped_df = dedup_latest_by_lsn(dedup_exact_duplicates(batch_df))
    valid_df, quarantine_df = split_valid_and_quarantine(deduped_df)

    if not quarantine_df.isEmpty():
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    if not valid_df.isEmpty():
        valid_df.createOrReplaceTempView("silver_batch_events")
        batch_df.sparkSession.sql(build_merge_sql(silver_table, "silver_batch_events"))


def make_batch_processor(silver_table: str, quarantine_table: str) -> Callable[[DataFrame, int], None]:
    def _process(batch_df: DataFrame, batch_id: int) -> None:
        process_batch(batch_df, silver_table, quarantine_table)

    return _process
