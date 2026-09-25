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
src/cdc_demo/        transformation code (bronze/silver/gold), testable outside Databricks
notebooks/           thin notebooks orchestrating the modules in src/
notebooks/archive/   first, exploratory batch version of the project, kept for reference
tests/               pytest tests for the functions in src/
resources/           Databricks Asset Bundle job definitions
```

Architecture diagram, technical decisions, run instructions, and results are
documented at the end of the project.
