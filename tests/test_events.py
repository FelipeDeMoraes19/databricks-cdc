import random
from datetime import datetime

from src.cdc_demo.events import (
    generate_clean_events,
    generate_cpf,
    generate_customer_email,
    inject_duplicates_and_shuffle,
    split_into_batches,
)


def cpf_check_digits(digits):
    d = list(digits[:9])
    for _ in range(2):
        weights = range(len(d) + 1, 1, -1)
        total = sum(x * w for x, w in zip(d, weights))
        remainder = total % 11
        d.append(0 if remainder < 2 else 11 - remainder)
    return d[9], d[10]


def test_generate_cpf_has_valid_check_digits():
    rng = random.Random(123)
    for _ in range(20):
        cpf = generate_cpf(rng)
        digits = [int(c) for c in cpf if c.isdigit()]
        assert len(digits) == 11
        expected_d10, expected_d11 = cpf_check_digits(digits)
        assert digits[9] == expected_d10
        assert digits[10] == expected_d11


def test_generate_cpf_is_deterministic_for_same_seed():
    assert generate_cpf(random.Random(7)) == generate_cpf(random.Random(7))


def test_generate_customer_email_is_tied_to_payment_id():
    assert generate_customer_email(42) == "customer42@example.com"


def test_generate_clean_events_lsn_is_strictly_increasing():
    events = generate_clean_events(num_payments=10, base_ts=datetime(2024, 1, 1), seed=1)
    lsns = [e["lsn"] for e in events]
    assert lsns == sorted(lsns)
    assert len(set(lsns)) == len(lsns)


def test_generate_clean_events_same_customer_across_payment_lifecycle():
    events = generate_clean_events(num_payments=5, base_ts=datetime(2024, 1, 1), seed=1)
    by_payment = {}
    for e in events:
        by_payment.setdefault(e["payment_id"], set()).add((e["customer_email"], e["customer_document"]))
    for identities in by_payment.values():
        assert len(identities) == 1


def test_inject_duplicates_and_shuffle_preserves_and_adds_events():
    events = generate_clean_events(num_payments=20, base_ts=datetime(2024, 1, 1), seed=2)
    result = inject_duplicates_and_shuffle(events, duplicate_fraction=0.2, seed=2)
    assert len(result) == len(events) + int(len(events) * 0.2)
    assert sorted(result, key=lambda e: (e["payment_id"], e["lsn"])) != events or len(events) <= 1


def test_split_into_batches_preserves_all_events():
    events = generate_clean_events(num_payments=15, base_ts=datetime(2024, 1, 1), seed=3)
    batches = split_into_batches(events, num_batches=4)
    assert len(batches) == 4
    flattened = [e for batch in batches for e in batch]
    assert len(flattened) == len(events)
    assert sorted(e["lsn"] for e in flattened) == sorted(e["lsn"] for e in events)
