# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Governance

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config
from src.cdc_demo.governance import apply_email_mask, create_email_mask_function

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

# COMMAND ----------

create_email_mask_function(spark, cfg.catalog, cfg.schema, cfg.mask_function_name, cfg.pii_authorized_group)
apply_email_mask(spark, cfg.full_table(cfg.silver_table), cfg.mask_function_name)

print(f"Mask aplicada em {cfg.full_table(cfg.silver_table)}.customer_email")
display(
    spark.table(cfg.full_table(cfg.silver_table)).select("payment_id", "customer_email").orderBy("payment_id").limit(5)
)
