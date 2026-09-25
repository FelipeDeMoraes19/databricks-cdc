# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Generate CDC events

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))

from datetime import datetime

from src.cdc_demo.config import get_config
from src.cdc_demo.events import (
    generate_clean_events,
    inject_duplicates_and_shuffle,
    split_into_batches,
    write_events_as_json,
)

cfg = get_config()

# COMMAND ----------

clean_events = generate_clean_events(num_payments=40, base_ts=datetime(2024, 1, 15, 8, 0, 0), seed=42)
all_events = inject_duplicates_and_shuffle(clean_events, duplicate_fraction=0.15, seed=42)
batches = split_into_batches(all_events, num_batches=4)

# COMMAND ----------

run_ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
for index, batch in enumerate(batches):
    file_name = f"batch_{run_ts}_{index:03d}.json"
    path = write_events_as_json(batch, cfg.events_path, file_name)
    print(f"{path}: {len(batch)} events")
