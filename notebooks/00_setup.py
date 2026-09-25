# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config

cfg = get_config()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volume_name}")

print(f"Schema pronto: {cfg.catalog}.{cfg.schema}")
print(f"Volume pronto: {cfg.volume_path}")
