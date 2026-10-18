"""Deterministic arrival-order DLQ data, independent of development proof scripts."""
from datetime import date, timedelta
import random


def arrival_dataset():
    """Return b-then-a records and the last-arrival value of every pair.

    Preserve the original proof's seed and draw order: 1,500 people, 30
    companies, 800 sampled pairs and 1,500 rows per topic.
    """
    rng = random.Random(11)
    pool = list(dict.fromkeys(
        (rng.randint(1, 1500), f"co-{rng.randint(1, 30):03d}") for _ in range(800)
    ))
    batches, expected = [], {}
    position = 0
    for topic, tag in (("arrival-b", "b"), ("arrival-a", "a")):
        rows = []
        for _ in range(1500):
            person, company = rng.choice(pool)
            since, role = date(2000, 1, 1) + timedelta(days=position), f"{tag}{position}"
            rows.append((topic, person, company, since, role))
            expected[person, company] = (since, role)
            position += 1
        batches.append(rows)
    return *batches, expected
