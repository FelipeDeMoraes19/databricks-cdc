# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from datetime import datetime

from src.cdc_demo.bronze import build_bronze_df
from src.cdc_demo.config import get_config
from src.cdc_demo.events import generate_clean_events, inject_duplicates_and_shuffle

cfg = get_config()
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

# COMMAND ----------

clean_events = generate_clean_events(num_payments=40, base_ts=datetime(2024, 1, 15, 8, 0, 0), seed=42)
all_events = inject_duplicates_and_shuffle(clean_events, duplicate_fraction=0.15, seed=42)

print(f"Eventos gerados: {len(clean_events)} limpos -> {len(all_events)} apos duplicatas/shuffle")

# COMMAND ----------

bronze_df = build_bronze_df(spark, all_events)

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.full_table(cfg.bronze_table))
)

print(f"Gravado em {cfg.full_table(cfg.bronze_table)}")
display(spark.table(cfg.full_table(cfg.bronze_table)).orderBy("payment_id", "lsn"))
