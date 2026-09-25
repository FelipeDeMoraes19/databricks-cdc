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

## Governance (PII)

Each CDC event carries `customer_email` and `customer_document` (a synthetic
CPF with valid check digits). They're handled differently depending on the
layer:

- **Bronze** keeps both fields raw, like everything else in bronze (capture
  as-is from the source).
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
  instantaneous - Unity Catalog caches it briefly).

**Free Edition note:** the Unity-Catalog-recommended function for this is
`is_account_group_member()`, which checks *account*-level groups. A group
created through the CLI's workspace-level Groups API
(`databricks groups create`) was not recognized by it in this environment -
only by the older, workspace-scoped `is_member()`, which is what the mask
function in this repo actually uses. This is a one-time manual setup step
(create the group, add members) done outside the pipeline code, documented
here for reproducibility rather than automated in a notebook.

Architecture diagram, technical decisions, run instructions, and results are
documented at the end of the project.
