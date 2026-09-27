from datetime import datetime
from decimal import Decimal

from src.cdc_demo.gold import build_captured_volume_daily, build_dim_payments_scd2, build_status_transitions_daily


def make_events_df(spark, rows):
    return spark.createDataFrame(
        rows,
        ["payment_id", "lsn", "op", "status", "amount", "updated_at"],
    )


def test_scd2_chains_valid_from_valid_to_and_marks_current(spark):
    events = make_events_df(
        spark,
        [
            (1, 1, "INSERT", "PENDING", Decimal("10.00"), datetime(2024, 1, 1, 0, 0, 0)),
            (1, 2, "UPDATE", "CAPTURED", Decimal("10.00"), datetime(2024, 1, 1, 1, 0, 0)),
        ],
    )

    rows = build_dim_payments_scd2(events).orderBy("lsn").collect()

    assert rows[0]["valid_from"] == datetime(2024, 1, 1, 0, 0, 0)
    assert rows[0]["valid_to"] == datetime(2024, 1, 1, 1, 0, 0)
    assert rows[0]["is_current"] is False
    assert rows[0]["is_deleted"] is False

    assert rows[1]["valid_from"] == datetime(2024, 1, 1, 1, 0, 0)
    assert rows[1]["valid_to"] is None
    assert rows[1]["is_current"] is True
    assert rows[1]["is_deleted"] is False


def test_scd2_delete_produces_current_tombstone(spark):
    events = make_events_df(
        spark,
        [
            (1, 1, "INSERT", "PENDING", Decimal("10.00"), datetime(2024, 1, 1, 0, 0, 0)),
            (1, 2, "DELETE", "PENDING", Decimal("10.00"), datetime(2024, 1, 2, 0, 0, 0)),
        ],
    )

    rows = build_dim_payments_scd2(events).orderBy("lsn").collect()

    tombstone = rows[1]
    assert tombstone["op"] == "DELETE"
    assert tombstone["is_current"] is True
    assert tombstone["is_deleted"] is True


def test_status_transitions_daily_counts_each_transition_separately(spark):
    events = make_events_df(
        spark,
        [
            (1, 1, "INSERT", "PENDING", Decimal("10.00"), datetime(2024, 1, 1, 0, 0, 0)),
            (1, 2, "UPDATE", "AUTHORIZED", Decimal("10.00"), datetime(2024, 1, 1, 1, 0, 0)),
            (1, 3, "UPDATE", "CAPTURED", Decimal("10.00"), datetime(2024, 1, 1, 2, 0, 0)),
        ],
    )

    rows = {r["status"]: r["transition_count"] for r in build_status_transitions_daily(events).collect()}

    assert rows == {"PENDING": 1, "AUTHORIZED": 1, "CAPTURED": 1}


def test_captured_volume_daily_sums_only_captured_and_does_not_double_count(spark):
    events = make_events_df(
        spark,
        [
            (1, 1, "INSERT", "PENDING", Decimal("10.00"), datetime(2024, 1, 1, 0, 0, 0)),
            (1, 2, "UPDATE", "CAPTURED", Decimal("10.00"), datetime(2024, 1, 1, 1, 0, 0)),
            (2, 1, "INSERT", "CAPTURED", Decimal("5.00"), datetime(2024, 1, 1, 2, 0, 0)),
        ],
    )

    rows = build_captured_volume_daily(events).collect()

    assert len(rows) == 1
    assert rows[0]["captured_count"] == 2
    assert rows[0]["captured_amount"] == Decimal("15.00")
