# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config
from src.cdc_demo.gold import build_dim_payments_scd2, build_metrics_daily_status, write_gold_table

cfg = get_config()
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

events_df = spark.table(cfg.full_table(cfg.silver_events_table))

# COMMAND ----------

dim_df = build_dim_payments_scd2(events_df)
write_gold_table(dim_df, cfg.full_table(cfg.dim_table))

metrics_df = build_metrics_daily_status(events_df)
write_gold_table(metrics_df, cfg.full_table(cfg.metrics_table))

print(f"Gravado em {cfg.full_table(cfg.dim_table)} e {cfg.full_table(cfg.metrics_table)}")
display(spark.table(cfg.full_table(cfg.dim_table)).orderBy("payment_id", "lsn"))
display(spark.table(cfg.full_table(cfg.metrics_table)).orderBy("event_date", "status"))
