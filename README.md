# databricks-cdc

[![CI](https://github.com/FelipeDeMoraes19/databricks-cdc/actions/workflows/ci.yml/badge.svg)](https://github.com/FelipeDeMoraes19/databricks-cdc/actions/workflows/ci.yml)

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
tests/               pytest tests for the functions in src/, run locally and in CI
.github/workflows/   GitHub Actions CI (pytest on every push/PR)
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

## Governance (PII)

Each CDC event carries `customer_email` and `customer_document` (a synthetic
CPF with valid check digits). They're handled differently depending on the
layer:

- **Bronze** keeps both fields raw, like everything else in bronze (capture
  as-is from the source) - but `customer_document` is not left unprotected:
  it carries the same kind of Unity Catalog column mask as `customer_email`
  (a dedicated function, since a CPF has no `@domain` to fall back on -
  unauthorized readers get a fixed `***.***.***-**` instead of a partial
  reveal, since a national ID is more sensitive than an email address).
  A table-level `GRANT` restricting the whole bronze table to an engineering
  group would also have worked, but the column mask was chosen to reuse the
  same mechanism and authorized group (`pii_readers`) already built for
  `customer_email`, instead of introducing a second governance model.
- **Silver** replaces `customer_document` with `customer_document_hash`, an
  HMAC-SHA256 of the document. The HMAC key is never in the code: it lives in
  a Databricks secret (`databricks secrets create-scope databricks-cdc` /
  `put-secret databricks-cdc document_hmac_key`) and is read at runtime with
  `dbutils.secrets.get(...)` inside the `03_silver` notebook, then passed into
  `foreachBatch` as a plain string. The raw document is never written to
  silver, gold, or the quarantine table.
- **`customer_email`** is kept in clear text in the table, but protected with
  a Unity Catalog **column mask** (`notebooks/05_governance.py`): a SQL
  function checks group membership and returns the full email to members of
  an authorized group, or a masked value (`***@domain.com`) to everyone else.
  Verified end-to-end on Free Edition: querying `silver_payments_current` as
  a member of the authorized group returns the full address; removing that
  membership (`databricks groups patch ...`) makes the same query return the
  masked value a few dozen seconds later (group membership isn't
  instantaneous - Unity Catalog caches it briefly). The same
  member/non-member toggle was verified against `bronze_payments_cdc.customer_document`.

**Free Edition note:** the Unity-Catalog-recommended function for this is
`is_account_group_member()`, which checks *account*-level groups. A group
created through the CLI's workspace-level Groups API
(`databricks groups create`) was not recognized by it in this environment -
only by the older, workspace-scoped `is_member()`, which is what the mask
function in this repo actually uses. This is a one-time manual setup step
(create the group, add members) done outside the pipeline code, documented
here for reproducibility rather than automated in a notebook.

## Tests

`tests/` covers the pure Python event generator, the silver dedup/validation/
quarantine logic (including the exact scenario in
`tests/fixtures/cdc_scenario_batch.json`), the `MERGE` LSN guard (including a
delayed `DELETE`), idempotent `foreachBatch` writes, and the gold SCD Type 2 /
metrics builders - all against a local Spark + Delta session
(`tests/conftest.py`), not mocks. The local session sets
`spark.sql.ansi.enabled=true` to match Databricks serverless: without it, the
`.cast()`-vs-ANSI bug fixed in Fase 3 would pass locally and only surface on
Databricks.

```
pip install -r requirements-dev.txt
pytest
```

**Windows-only setup:** plain `pyspark` needs a Hadoop `winutils.exe` (and
`hadoop.dll`) on Windows even for local-only tests - without it the JVM
doesn't start. Download a build matching the Hadoop version PySpark ships
with (e.g. from `cdarlint/winutils` on GitHub) into `C:\hadoop\bin\`, then set:

```
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%
set PYSPARK_PYTHON=<full path to your python.exe>
set PYSPARK_DRIVER_PYTHON=<full path to your python.exe>
```

`PYSPARK_PYTHON` matters if `python`/`python3` on PATH could resolve to the
Windows Store alias instead of the real interpreter - Spark's worker
processes fail silently otherwise. Linux (including GitHub Actions) needs
none of this.

Architecture diagram, technical decisions, run instructions, and results are
documented at the end of the project.
