from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType
from pyspark.sql.window import Window


def dedup_latest_by_lsn(df: DataFrame, key_col: str = "payment_id", lsn_col: str = "lsn") -> DataFrame:
    window_spec = Window.partitionBy(key_col).orderBy(F.col(lsn_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def filter_deletes(df: DataFrame, op_col: str = "op") -> DataFrame:
    return df.filter(F.col(op_col) != "DELETE")


def cast_silver_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("payment_id", F.col("payment_id").cast(LongType()))
        .withColumn("amount", F.col("amount").cast(DecimalType(14, 2)))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )


def validate_cast(
    original_df: DataFrame,
    typed_df: DataFrame,
    cast_cols=("payment_id", "amount", "updated_at"),
) -> None:
    failures = typed_df.select(
        *[F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cast_cols]
    ).first()

    bad_cols = [c for c in cast_cols if failures[c] and failures[c] > 0]
    if bad_cols:
        raise ValueError(f"Contrato de tipos violado nas colunas: {bad_cols}")


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
