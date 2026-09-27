from datetime import datetime
from decimal import Decimal

from src.cdc_demo.bronze import BRONZE_SCHEMA
from src.cdc_demo.silver import (
    ensure_quarantine_table,
    ensure_silver_events_table,
    ensure_silver_table,
    process_batch,
)

TEST_HMAC_KEY = "unit-test-key"


def make_event(
    payment_id="1",
    lsn=1,
    op="INSERT",
    amount="10.00",
    status="PENDING",
    updated_at="2024-01-01T00:00:00",
    customer_email="customer1@example.com",
    customer_document="123.456.789-00",
):
    return {
        "payment_id": payment_id,
        "lsn": lsn,
        "op": op,
        "amount": amount,
        "status": status,
        "updated_at": updated_at,
        "customer_email": customer_email,
        "customer_document": customer_document,
    }


def make_bronze_batch(spark, events):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(events, schema=BRONZE_SCHEMA)
    return (
        df.withColumn("_rescued_data", F.lit(None).cast("string"))
        .withColumn("_source_file", F.lit("test"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def setup_tables(spark, unique_table_name):
    silver_table = unique_table_name("t_silver")
    history_table = unique_table_name("t_history")
    quarantine_table = unique_table_name("t_quarantine")
    ensure_silver_table(spark, silver_table)
    ensure_silver_events_table(spark, history_table)
    ensure_quarantine_table(spark, quarantine_table)
    return silver_table, history_table, quarantine_table


def test_insert_then_update_via_merge(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    process_batch(
        make_bronze_batch(spark, [make_event(lsn=10, op="INSERT", status="PENDING")]),
        0,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )
    process_batch(
        make_bronze_batch(spark, [make_event(lsn=20, op="UPDATE", status="CAPTURED")]),
        1,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )

    rows = spark.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "CAPTURED"
    assert rows[0]["lsn"] == 20


def test_lsn_guard_blocks_stale_update(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    process_batch(
        make_bronze_batch(spark, [make_event(lsn=20, op="INSERT", status="CAPTURED")]),
        0,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )
    process_batch(
        make_bronze_batch(spark, [make_event(lsn=10, op="UPDATE", status="PENDING")]),
        1,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )

    rows = spark.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "CAPTURED"
    assert rows[0]["lsn"] == 20


def test_delete_removes_row(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    process_batch(
        make_bronze_batch(spark, [make_event(lsn=10, op="INSERT", status="PENDING")]),
        0,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )
    process_batch(
        make_bronze_batch(spark, [make_event(lsn=20, op="DELETE", status="PENDING")]),
        1,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )

    assert spark.table(silver_table).count() == 0


def test_delayed_delete_is_ignored(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    process_batch(
        make_bronze_batch(spark, [make_event(lsn=20, op="INSERT", status="CAPTURED")]),
        0,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )
    process_batch(
        make_bronze_batch(spark, [make_event(lsn=5, op="DELETE", status="CAPTURED")]),
        1,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )

    rows = spark.table(silver_table).collect()
    assert len(rows) == 1
    assert rows[0]["lsn"] == 20


def test_idempotent_reprocessing_same_batch_id(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    batch = make_bronze_batch(
        spark,
        [
            make_event(payment_id="1", lsn=1, amount="not-a-number"),
            make_event(payment_id="2", lsn=1, amount="20.00"),
        ],
    )

    process_batch(batch, 42, silver_table, history_table, quarantine_table, TEST_HMAC_KEY)
    process_batch(batch, 42, silver_table, history_table, quarantine_table, TEST_HMAC_KEY)
    process_batch(batch, 42, silver_table, history_table, quarantine_table, TEST_HMAC_KEY)

    assert spark.table(quarantine_table).count() == 1
    assert spark.table(history_table).count() == 1
    assert spark.table(silver_table).count() == 1


def test_quarantine_receives_invalid_amount_with_reason(spark, unique_table_name):
    silver_table, history_table, quarantine_table = setup_tables(spark, unique_table_name)

    process_batch(
        make_bronze_batch(spark, [make_event(payment_id="1", lsn=1, amount="garbage")]),
        0,
        silver_table,
        history_table,
        quarantine_table,
        TEST_HMAC_KEY,
    )

    rows = spark.table(quarantine_table).collect()
    assert len(rows) == 1
    assert rows[0]["reason"] == "invalid_amount"
    assert spark.table(silver_table).count() == 0
