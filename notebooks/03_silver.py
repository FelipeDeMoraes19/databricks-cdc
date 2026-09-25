# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config
from src.cdc_demo.silver import (
    ensure_quarantine_table,
    ensure_silver_events_table,
    ensure_silver_table,
    make_batch_processor,
)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

silver_table = cfg.full_table(cfg.silver_table)
history_table = cfg.full_table(cfg.silver_events_table)
quarantine_table = cfg.full_table(cfg.quarantine_table)

ensure_silver_table(spark, silver_table)
ensure_silver_events_table(spark, history_table)
ensure_quarantine_table(spark, quarantine_table)

# COMMAND ----------

bronze_stream = spark.readStream.table(cfg.full_table(cfg.bronze_table))

query = (
    bronze_stream.writeStream
    .foreachBatch(make_batch_processor(silver_table, history_table, quarantine_table))
    .option("checkpointLocation", cfg.silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print(f"Gravado em {silver_table}")
display(spark.table(silver_table).orderBy("payment_id"))
