#!/usr/bin/env python3
"""Inject demo readings through the ingest gateway to exercise the dashboard.

Each batch carries deliberately-invalid readings of every rejection type, so the
"Invalid Readings (5 min)" and "Rejection Rate by Type" panels light up, and it
re-sends the previous batch verbatim so "Duplicates (5 min)" ticks too. The
readings flow the real path: ingest gateway → Kafka → Spark → validation →
Redis counters + PostgreSQL rejected_readings.

Reading times are spread across 2014-02-27 (inside the dashboard's time window)
so the rejection timeseries shows several hourly buckets.

Usage:
  python tools/inject_demo.py                          # one batch
  python tools/inject_demo.py --loop 15 --duration 180 # every 15 s for 3 min
"""
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from datetime import datetime, timedelta, timezone

INGEST = "http://localhost:18080/ingest"
BASE = datetime(2014, 2, 27, tzinfo=timezone.utc)


def _rand_time() -> str:
    dt = BASE + timedelta(hours=random.randint(0, 23), minutes=random.choice([0, 30]))
    return dt.isoformat().replace("+00:00", "Z")


def _make_batch(seq: int) -> tuple[str, dict]:
    delivery_id = f"demo-{int(time.time())}-{seq}"
    readings = []
    for i in range(12):
        kind = i % 4
        rt = _rand_time()
        if kind == 0:          # kwh_out_of_range (too high)
            meter, kwh = f"MAC{random.randint(0, 5000):06d}", round(random.uniform(60, 120), 2)
        elif kind == 1:        # kwh_out_of_range (negative)
            meter, kwh = f"MAC{random.randint(0, 5000):06d}", round(random.uniform(-10, -1), 2)
        elif kind == 2:        # missing_kwh
            meter, kwh = f"MAC{random.randint(0, 5000):06d}", None
        else:                  # invalid_reading_time
            meter, kwh, rt = f"MAC{random.randint(0, 5000):06d}", 1.0, "not-a-timestamp"
        readings.append({
            "meter_id": meter,
            "household_id": "demo",
            "reading_time": rt,
            "kwh": kwh,
            "received_at": rt if rt != "not-a-timestamp" else _rand_time(),
        })
    return delivery_id, {"delivery_id": delivery_id, "readings": readings, "source": "live"}


def _post(payload: dict) -> int:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(INGEST, data=data, headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=float, default=0, help="seconds between batches (0 = one shot)")
    ap.add_argument("--duration", type=float, default=0, help="stop after N seconds (0 = no limit)")
    args = ap.parse_args()

    prev: tuple[str, dict] | None = None
    seq = 0
    deadline = time.time() + args.duration if args.duration else None

    while True:
        seq += 1
        did, batch = _make_batch(seq)
        print(f"sent {did}  ({len(batch['readings'])} invalid readings) -> {_post(batch)}", flush=True)
        if prev is not None:
            print(f"  re-sent {prev[0]} (duplicate) -> {_post(prev[1])}", flush=True)
        prev = (did, batch)

        if not args.loop:
            break
        if deadline and time.time() >= deadline:
            break
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
