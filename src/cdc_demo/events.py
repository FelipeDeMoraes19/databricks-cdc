import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List

STATUS_FLOW_OPTIONS = [
    ["PENDING", "AUTHORIZED", "CAPTURED"],
    ["PENDING", "AUTHORIZED", "FAILED"],
    ["PENDING", "AUTHORIZED", "CAPTURED", "REFUNDED"],
    ["PENDING", "CAPTURED"],
]


def generate_clean_events(
    num_payments: int,
    base_ts: datetime,
    seed: int,
    start_lsn: int = 1000,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    events: List[Dict[str, Any]] = []
    lsn_counter = start_lsn

    for payment_id in range(1, num_payments + 1):
        flow = rng.choice(STATUS_FLOW_OPTIONS)
        amount = Decimal(rng.randrange(500, 500000)) / 100
        current_ts = base_ts + timedelta(minutes=payment_id * 3)

        for step_index, status in enumerate(flow):
            op = "INSERT" if step_index == 0 else "UPDATE"
            current_ts = current_ts + timedelta(minutes=rng.randint(1, 45))
            events.append(
                {
                    "payment_id": str(payment_id),
                    "lsn": lsn_counter,
                    "op": op,
                    "amount": str(amount),
                    "status": status,
                    "updated_at": current_ts.isoformat(),
                }
            )
            lsn_counter += rng.randint(1, 5)

    return events


def inject_duplicates_and_shuffle(
    events: List[Dict[str, Any]],
    duplicate_fraction: float,
    seed: int,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    duplicated = rng.sample(events, k=int(len(events) * duplicate_fraction))
    all_events = events + duplicated
    rng.shuffle(all_events)
    return all_events
