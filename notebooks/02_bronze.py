# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.bronze import read_bronze_stream, write_bronze_stream
from src.cdc_demo.config import get_config

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

# COMMAND ----------

stream_df = read_bronze_stream(spark, cfg.events_path)
write_bronze_stream(stream_df, cfg.full_table(cfg.bronze_table), cfg.bronze_checkpoint_path)

print(f"Gravado em {cfg.full_table(cfg.bronze_table)}")
display(spark.table(cfg.full_table(cfg.bronze_table)).orderBy("payment_id", "lsn"))
