import json
import os
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


def split_into_batches(events: List[Dict[str, Any]], num_batches: int) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = [[] for _ in range(num_batches)]
    for index, event in enumerate(events):
        batches[index % num_batches].append(event)
    return batches


def write_events_as_json(events: List[Dict[str, Any]], output_dir: str, file_name: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, file_name)
    with open(output_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    return output_path
