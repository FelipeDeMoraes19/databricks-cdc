# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Governance

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from src.cdc_demo.config import get_config
from src.cdc_demo.governance import apply_column_mask, create_document_mask_function, create_email_mask_function

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

# COMMAND ----------

create_email_mask_function(spark, cfg.catalog, cfg.schema, cfg.email_mask_function_name, cfg.pii_authorized_group)
apply_column_mask(spark, cfg.full_table(cfg.silver_table), cfg.email_mask_function_name, "customer_email")

create_document_mask_function(spark, cfg.catalog, cfg.schema, cfg.document_mask_function_name, cfg.pii_authorized_group)
apply_column_mask(spark, cfg.full_table(cfg.bronze_table), cfg.document_mask_function_name, "customer_document")

print(f"Email mask aplicada em {cfg.full_table(cfg.silver_table)}.customer_email")
print(f"Document mask aplicada em {cfg.full_table(cfg.bronze_table)}.customer_document")
display(
    spark.table(cfg.full_table(cfg.silver_table)).select("payment_id", "customer_email").orderBy("payment_id").limit(5)
)
display(
    spark.table(cfg.full_table(cfg.bronze_table)).select("payment_id", "customer_document").orderBy("payment_id").limit(5)
)
