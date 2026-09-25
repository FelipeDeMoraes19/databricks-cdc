from pyspark.sql import SparkSession


def create_email_mask_function(
    spark: SparkSession, catalog: str, schema: str, function_name: str, authorized_group: str
) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.{function_name}(email STRING)
        RETURNS STRING
        RETURN CASE
          WHEN is_member('{authorized_group}') THEN email
          ELSE CONCAT('***@', SPLIT(email, '@')[1])
        END
        """
    )


def apply_email_mask(spark: SparkSession, table_name: str, function_name: str, column: str = "customer_email") -> None:
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} SET MASK {function_name}")
