import hashlib
import hmac as hmac_lib
from typing import Callable, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
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


def hash_document(document: Optional[str], secret_key: str) -> Optional[str]:
    if document is None:
        return None
    return hmac_lib.new(secret_key.encode("utf-8"), document.encode("utf-8"), hashlib.sha256).hexdigest()


def _make_document_hash_udf(secret_key: str):
    return F.udf(lambda document: hash_document(document, secret_key), StringType())


def split_valid_and_quarantine(df: DataFrame, document_hash_key: str) -> Tuple[DataFrame, DataFrame]:
    document_hash_udf = _make_document_hash_udf(document_hash_key)

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
        "customer_email",
        document_hash_udf(F.col("customer_document")).alias("customer_document_hash"),
    )

    quarantine_df = classified_df.filter(F.col("_quarantine_reason").isNotNull()).select(
        "payment_id",
        "lsn",
        "op",
        "amount",
        "status",
        "updated_at",
        "customer_email",
        "customer_document",
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
        target.customer_email = source.customer_email,
        target.customer_document_hash = source.customer_document_hash,
        target._silver_processed_at = current_timestamp()
    WHEN NOT MATCHED AND source.op != 'DELETE' THEN
      INSERT (
        {key_col}, {lsn_col}, op, amount, status, updated_at,
        customer_email, customer_document_hash, _silver_processed_at
      )
      VALUES (
        source.{key_col}, source.{lsn_col}, source.op, source.amount, source.status, source.updated_at,
        source.customer_email, source.customer_document_hash, current_timestamp()
      )
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
          customer_email STRING,
          customer_document_hash STRING,
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
          customer_email STRING,
          customer_document STRING,
          _rescued_data STRING,
          _source_file STRING,
          _ingested_at TIMESTAMP,
          reason STRING
        ) USING DELTA
        """
    )


def ensure_silver_events_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          payment_id BIGINT,
          lsn BIGINT,
          op STRING,
          amount DECIMAL(14,2),
          status STRING,
          updated_at TIMESTAMP,
          customer_email STRING,
          customer_document_hash STRING,
          _silver_processed_at TIMESTAMP
        ) USING DELTA
        """
    )


def write_quarantine(quarantine_df: DataFrame, quarantine_table: str, batch_id: int) -> None:
    if quarantine_df.isEmpty():
        return
    (
        quarantine_df.write.format("delta")
        .option("txnVersion", batch_id)
        .option("txnAppId", "silver_quarantine_writer")
        .mode("append")
        .saveAsTable(quarantine_table)
    )


def write_silver_events(valid_df: DataFrame, history_table: str, batch_id: int) -> None:
    if valid_df.isEmpty():
        return
    (
        valid_df.withColumn("_silver_processed_at", F.current_timestamp())
        .write.format("delta")
        .option("txnVersion", batch_id)
        .option("txnAppId", "silver_history_writer")
        .mode("append")
        .saveAsTable(history_table)
    )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    silver_table: str,
    history_table: str,
    quarantine_table: str,
    document_hash_key: str,
) -> None:
    exact_deduped_df = dedup_exact_duplicates(batch_df)
    valid_df, quarantine_df = split_valid_and_quarantine(exact_deduped_df, document_hash_key)

    write_quarantine(quarantine_df, quarantine_table, batch_id)
    write_silver_events(valid_df, history_table, batch_id)

    latest_valid_df = dedup_latest_by_lsn(valid_df)
    if not latest_valid_df.isEmpty():
        latest_valid_df.createOrReplaceTempView("silver_batch_events")
        batch_df.sparkSession.sql(build_merge_sql(silver_table, "silver_batch_events"))


def make_batch_processor(
    silver_table: str, history_table: str, quarantine_table: str, document_hash_key: str
) -> Callable[[DataFrame, int], None]:
    def _process(batch_df: DataFrame, batch_id: int) -> None:
        process_batch(batch_df, batch_id, silver_table, history_table, quarantine_table, document_hash_key)

    return _process
