# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F

from src.cdc_demo.config import get_config
from src.cdc_demo.silver import cast_silver_types, dedup_latest_by_lsn, filter_deletes, validate_cast

cfg = get_config()
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

bronze_df = spark.table(cfg.full_table(cfg.bronze_table))
print(f"Bronze: {bronze_df.count()} linhas")

# COMMAND ----------

deduped_df = dedup_latest_by_lsn(bronze_df)
deduped_df = filter_deletes(deduped_df)

typed_df = cast_silver_types(deduped_df)
validate_cast(deduped_df, typed_df)

print("Contrato de tipos OK.")

# COMMAND ----------

silver_df = (
    typed_df
    .select("payment_id", "lsn", "op", "amount", "status", "updated_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.full_table(cfg.silver_table))
)

print(f"Gravado em {cfg.full_table(cfg.silver_table)}")
display(spark.table(cfg.full_table(cfg.silver_table)).orderBy("payment_id"))
