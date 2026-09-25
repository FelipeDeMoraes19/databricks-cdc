# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config
from src.cdc_demo.gold import (
    build_captured_volume_daily,
    build_dim_payments_scd2,
    build_status_transitions_daily,
    write_gold_table,
)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

events_df = spark.table(cfg.full_table(cfg.silver_events_table))

# COMMAND ----------

dim_df = build_dim_payments_scd2(events_df)
write_gold_table(dim_df, cfg.full_table(cfg.dim_table))

transitions_df = build_status_transitions_daily(events_df)
write_gold_table(transitions_df, cfg.full_table(cfg.status_transitions_table))

captured_df = build_captured_volume_daily(events_df)
write_gold_table(captured_df, cfg.full_table(cfg.captured_volume_table))

print(
    f"Gravado em {cfg.full_table(cfg.dim_table)}, "
    f"{cfg.full_table(cfg.status_transitions_table)} e {cfg.full_table(cfg.captured_volume_table)}"
)
display(spark.table(cfg.full_table(cfg.dim_table)).orderBy("payment_id", "lsn"))
display(spark.table(cfg.full_table(cfg.status_transitions_table)).orderBy("event_date", "status"))
display(spark.table(cfg.full_table(cfg.captured_volume_table)).orderBy("event_date"))
