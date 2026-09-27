import hashlib
import hmac
import os

from pyspark.sql import functions as F

from src.cdc_demo.bronze import BRONZE_SCHEMA
from src.cdc_demo.silver import (
    dedup_exact_duplicates,
    dedup_latest_by_lsn,
    hash_document,
    split_valid_and_quarantine,
)

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "cdc_scenario_batch.json")
TEST_HMAC_KEY = "unit-test-key"


def load_scenario_batch_with_simulated_rescued_data(spark):
    return (
        spark.read.format("json")
        .schema(BRONZE_SCHEMA)
        .load(FIXTURE_PATH)
        .withColumn(
            "_rescued_data",
            F.when(F.col("payment_id") == "998", F.lit('{"extra_field":"surprise"}')).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def test_hash_document_matches_hmac_sha256():
    doc = "123.456.789-00"
    expected = hmac.new(TEST_HMAC_KEY.encode("utf-8"), doc.encode("utf-8"), hashlib.sha256).hexdigest()
    assert hash_document(doc, TEST_HMAC_KEY) == expected


def test_hash_document_none_returns_none():
    assert hash_document(None, TEST_HMAC_KEY) is None


def test_dedup_exact_duplicates_collapses_repeated_event(spark):
    df = load_scenario_batch_with_simulated_rescued_data(spark)
    before = df.filter(df.payment_id == "997").count()
    after = dedup_exact_duplicates(df).filter(F.col("payment_id") == "997").count()
    assert before == 2
    assert after == 1


def test_scenario_batch_quarantine_reasons(spark):
    df = load_scenario_batch_with_simulated_rescued_data(spark)
    deduped = dedup_exact_duplicates(df)
    _, quarantine_df = split_valid_and_quarantine(deduped, TEST_HMAC_KEY)

    reasons = {row["payment_id"]: row["reason"] for row in quarantine_df.select("payment_id", "reason").collect()}

    assert reasons["999"] == "invalid_amount"
    assert reasons["998"] == "rescued_data_present"
    assert reasons["500"] == "invalid_amount"
    assert len(reasons) == 3


def test_scenario_batch_valid_rows_are_typed_and_document_is_hashed(spark):
    df = load_scenario_batch_with_simulated_rescued_data(spark)
    deduped = dedup_exact_duplicates(df)
    valid_df, _ = split_valid_and_quarantine(deduped, TEST_HMAC_KEY)

    row_997 = valid_df.filter(F.col("payment_id") == 997).collect()
    assert len(row_997) == 1

    schema_types = {f.name: f.dataType.typeName() for f in valid_df.schema.fields}
    assert schema_types["payment_id"] == "long"
    assert schema_types["amount"] == "decimal"
    assert schema_types["updated_at"] == "timestamp"
    assert "customer_document" not in valid_df.columns
    assert "customer_document_hash" in valid_df.columns


def test_ordering_fix_valid_event_survives_when_newer_event_in_same_batch_is_invalid(spark):
    df = load_scenario_batch_with_simulated_rescued_data(spark)
    deduped = dedup_exact_duplicates(df)
    valid_df, quarantine_df = split_valid_and_quarantine(deduped, TEST_HMAC_KEY)

    latest_valid_df = dedup_latest_by_lsn(valid_df)
    row_500 = latest_valid_df.filter(F.col("payment_id") == 500).collect()

    assert len(row_500) == 1
    assert row_500[0]["lsn"] == 10
    assert row_500[0]["status"] == "PENDING"

    quarantined_500 = quarantine_df.filter(F.col("payment_id") == "500").collect()
    assert len(quarantined_500) == 1
    assert quarantined_500[0]["lsn"] == 20


def test_dedup_latest_by_lsn_keeps_highest_lsn_per_key(spark):
    df = spark.createDataFrame(
        [
            ("1", 10, "PENDING"),
            ("1", 30, "CAPTURED"),
            ("1", 20, "AUTHORIZED"),
            ("2", 5, "PENDING"),
        ],
        ["payment_id", "lsn", "status"],
    )

    result = dedup_latest_by_lsn(df).orderBy("payment_id").collect()

    assert len(result) == 2
    assert result[0]["payment_id"] == "1"
    assert result[0]["lsn"] == 30
    assert result[0]["status"] == "CAPTURED"
    assert result[1]["payment_id"] == "2"
    assert result[1]["lsn"] == 5


def test_dedup_latest_by_lsn_breaks_ties_to_exactly_one_row(spark):
    df = spark.createDataFrame(
        [("1", 10, "A"), ("1", 10, "B")],
        ["payment_id", "lsn", "status"],
    )

    result = dedup_latest_by_lsn(df).collect()

    assert len(result) == 1
