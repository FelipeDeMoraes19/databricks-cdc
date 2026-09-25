# databricks-cdc

End-to-end data pipeline on Databricks simulating Change Data Capture (CDC)
for a `payments` table, built in Python/PySpark following the medallion
architecture (bronze -> silver -> gold), with Unity Catalog, Auto Loader,
automated tests, and orchestration via Databricks Asset Bundles.

Built and deployed on Databricks Free Edition (serverless compute).

## Stack

- **Databricks** - Unity Catalog, Volumes, Auto Loader, Delta Lake, Jobs, Asset Bundles
- **Apache Spark / PySpark** - Structured Streaming, `foreachBatch`, `MERGE INTO`
- **Python** - transformation modules in `src/` tested with pytest, CI via GitHub Actions

## Structure

```
databricks.yml       Databricks Asset Bundle definition (dev/prod targets)
resources/           Asset Bundle job definitions
src/cdc_demo/        transformation code (bronze/silver/gold), testable outside Databricks
notebooks/           thin notebooks orchestrating the modules in src/
notebooks/archive/   first, exploratory batch version of the project, kept for reference
tests/               pytest tests for the functions in src/
```

## Gold tables

- **`dim_payments_scd2`** - SCD Type 2 dimension built from `silver_payment_events`.
  One row per status transition, with `valid_from`/`valid_to`/`is_current`.
  A `DELETE` event produces a tombstone row (`is_current = true`,
  `is_deleted = true`) instead of a normal state.
- **`fact_status_transitions_daily`** - count of status transition *events* per
  day and status. A single payment contributes one row per status it passes
  through (e.g. PENDING, AUTHORIZED, CAPTURED), so this counts process
  throughput, not distinct payments or money.
- **`fact_captured_volume_daily`** - count and total `amount` of payments that
  reached `CAPTURED` status per day. This is the correct place to sum money:
  each payment reaches `CAPTURED` at most once, so the sum isn't inflated by
  earlier transitions (unlike grouping by status and summing `amount`, which
  would count the same payment's amount once per status it passed through).

Architecture diagram, technical decisions, run instructions, and results are
documented at the end of the project.
