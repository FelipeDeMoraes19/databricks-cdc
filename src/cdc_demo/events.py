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


def generate_cpf(rng: random.Random) -> str:
    digits = [rng.randint(0, 9) for _ in range(9)]
    for _ in range(2):
        weights = range(len(digits) + 1, 1, -1)
        total = sum(d * w for d, w in zip(digits, weights))
        remainder = total % 11
        digits.append(0 if remainder < 2 else 11 - remainder)
    d = digits
    return f"{d[0]}{d[1]}{d[2]}.{d[3]}{d[4]}{d[5]}.{d[6]}{d[7]}{d[8]}-{d[9]}{d[10]}"


def generate_customer_email(payment_id: int) -> str:
    return f"customer{payment_id}@example.com"


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
        customer_email = generate_customer_email(payment_id)
        customer_document = generate_cpf(rng)

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
                    "customer_email": customer_email,
                    "customer_document": customer_document,
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
