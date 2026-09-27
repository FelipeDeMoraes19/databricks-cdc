# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Delta features tour

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from pyspark.sql import functions as F

from src.cdc_demo.config import get_config

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "payments_cdc")
cfg = get_config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
spark.sql(f"USE CATALOG {cfg.catalog}")
spark.sql(f"USE SCHEMA {cfg.schema}")

history_table = cfg.full_table(cfg.silver_events_table)

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {history_table}")
display(
    history_df.select("version", "timestamp", "operation", "operationParameters", "operationMetrics").orderBy(
        "version"
    )
)
print(f"Total de versoes: {history_df.count()}")

# COMMAND ----------

latest_version_row = history_df.orderBy(F.col("version").desc()).select("version").first()

if latest_version_row is not None and latest_version_row["version"] > 0:
    version_after = latest_version_row["version"]
    version_before = version_after - 1

    before_df = spark.sql(f"SELECT * FROM {history_table} VERSION AS OF {version_before}")
    after_df = spark.sql(f"SELECT * FROM {history_table} VERSION AS OF {version_after}")

    print(f"Linhas na versao {version_before}: {before_df.count()}")
    print(f"Linhas na versao {version_after}: {after_df.count()}")
else:
    print("Tabela tem uma unica versao ate agora.")

# COMMAND ----------

optimize_result = spark.sql(f"OPTIMIZE {history_table}")
display(optimize_result)

# COMMAND ----------

zorder_result = spark.sql(f"OPTIMIZE {history_table} ZORDER BY (payment_id)")
display(zorder_result)

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {history_table} CLUSTER BY (payment_id)")
    cluster_optimize_result = spark.sql(f"OPTIMIZE {history_table}")
    display(cluster_optimize_result)
    display(
        spark.sql(f"DESCRIBE TABLE EXTENDED {history_table}").filter(F.col("col_name") == "Clustering Information")
    )
except Exception as e:
    print(str(e).splitlines()[0])
