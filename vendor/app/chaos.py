"""Per-batch chaos transforms.

All functions are pure: they take a batch (list of reading dicts) and return a
possibly-modified new batch. Probabilities are read from the env at module
import; the delivery worker re-imports on restart so `make vendor-chaos` /
`make vendor-calm` take effect immediately.
"""
from __future__ import annotations

import os
import random
from datetime import datetime
from typing import Sequence


DUPLICATE_RATE = float(os.environ.get("VENDOR_DUPLICATE_RATE", "0.0"))
REORDER_RATE = float(os.environ.get("VENDOR_REORDER_RATE", "0.0"))
LATE_RATE = float(os.environ.get("VENDOR_LATE_RATE", "0.0"))


def should_duplicate(rng: random.Random) -> bool:
    return rng.random() < DUPLICATE_RATE


def maybe_shuffle(rng: random.Random, batch: list[dict]) -> list[dict]:
    if rng.random() < REORDER_RATE and len(batch) > 1:
        out = list(batch)
        rng.shuffle(out)
        return out
    return batch


def maybe_append_late(rng: random.Random, batch: list[dict], history: Sequence[dict]) -> list[dict]:
    """If rolling, drop one historical reading at the end with a perturbed kwh."""
    if rng.random() >= LATE_RATE or not history:
        return batch
    src = rng.choice(list(history))
    corrected = dict(src)
    # Bump kWh by ±5% to simulate a corrected reading.
    delta = 1.0 + (rng.random() - 0.5) * 0.1
    corrected["kwh"] = float(src.get("kwh", 0.0)) * delta
    return batch + [corrected]


# ───────────────────────── Outage windows ───────────────────────────────────

def parse_outage_schedule(raw: str | None) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    """Parse 'HH:MM-HH:MM,HH:MM-HH:MM' into [( (h1,m1),(h2,m2) ), ...]."""
    if not raw:
        return []
    out: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            a, b = piece.split("-", 1)
            ah, am = (int(x) for x in a.split(":", 1))
            bh, bm = (int(x) for x in b.split(":", 1))
            out.append(((ah, am), (bh, bm)))
        except Exception:
            continue
    return out


def in_outage(now: datetime, windows: list[tuple[tuple[int, int], tuple[int, int]]]) -> bool:
    cur = now.hour * 60 + now.minute
    for (h1, m1), (h2, m2) in windows:
        start = h1 * 60 + m1
        end = h2 * 60 + m2
        if start <= cur < end:
            return True
    return False
