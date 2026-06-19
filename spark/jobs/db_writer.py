"""Layer 5 — PostgreSQL writes.

All functions accept an open psycopg connection and commit at the end.
They are idempotent: re-running the same data produces the same DB state.

upsert_clean_readings    ON CONFLICT DO UPDATE only when received_at is newer.
                         The correction-tracking trigger fires automatically.
insert_rejected_readings Append-only; duplicates within a batch are fine.
log_batch                One row per delivery; ON CONFLICT DO NOTHING.
upsert_lag_stats         Running avg + max per meter; exponential smoothing.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("db_writer")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ts(value) -> Optional[datetime]:
    """Parse any timestamp-like value to a timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            t = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


# ─── clean_readings ───────────────────────────────────────────────────────────

_UPSERT_CLEAN = """
INSERT INTO clean_readings (meter_id, reading_time, household_id, kwh, received_at, ingested_at)
VALUES (%(meter_id)s, %(reading_time)s, %(household_id)s, %(kwh)s, %(received_at)s, %(now)s)
ON CONFLICT (meter_id, reading_time) DO UPDATE
    SET kwh          = EXCLUDED.kwh,
        received_at  = EXCLUDED.received_at,
        household_id = EXCLUDED.household_id,
        ingested_at  = EXCLUDED.ingested_at
WHERE EXCLUDED.received_at > clean_readings.received_at
"""


def upsert_clean_readings(conn, readings: list[dict]) -> int:
    """Upsert valid readings; returns number of rows attempted."""
    if not readings:
        return 0
    now = datetime.now(timezone.utc)
    rows = [
        {
            "meter_id":     r.get("meter_id"),
            "reading_time": _ts(r.get("reading_time")),
            "household_id": r.get("household_id"),
            "kwh":          float(r["kwh"]),
            "received_at":  _ts(r.get("received_at")),
            "now":          now,
        }
        for r in readings
    ]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_CLEAN, rows)
    conn.commit()
    return len(rows)


# ─── rejected_readings ────────────────────────────────────────────────────────

_INSERT_REJECTED = """
INSERT INTO rejected_readings
    (delivery_id, meter_id, reading_time, kwh, received_at,
     rejection_type, rejection_detail, source)
VALUES
    (%(delivery_id)s, %(meter_id)s, %(reading_time)s, %(kwh)s, %(received_at)s,
     %(rejection_type)s, %(rejection_detail)s, %(source)s)
"""


def insert_rejected_readings(conn, rejected: list[dict]) -> None:
    if not rejected:
        return
    rows = [
        {
            "delivery_id":      r.get("delivery_id"),
            "meter_id":         r.get("meter_id"),
            "reading_time":     _ts(r.get("reading_time")),
            "kwh":              r.get("kwh"),
            "received_at":      _ts(r.get("received_at")),
            "rejection_type":   r["rejection_type"],
            "rejection_detail": r.get("rejection_detail"),
            "source":           r.get("source", "live"),
        }
        for r in rejected
    ]
    with conn.cursor() as cur:
        cur.executemany(_INSERT_REJECTED, rows)
    conn.commit()


# ─── batch_log ────────────────────────────────────────────────────────────────

_LOG_BATCH = """
INSERT INTO batch_log (delivery_id, source, received_at, reading_count, status)
VALUES (%(delivery_id)s, %(source)s, %(received_at)s, %(reading_count)s, %(status)s)
ON CONFLICT (delivery_id) DO NOTHING
"""


def log_batch(
    conn,
    delivery_id: str,
    source: str,
    reading_count: int,
    status: str,
    received_at=None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_LOG_BATCH, {
            "delivery_id":   delivery_id,
            "source":        source,
            "received_at":   received_at or datetime.now(timezone.utc),
            "reading_count": reading_count,
            "status":        status,
        })
    conn.commit()


# ─── meter_lag_stats ──────────────────────────────────────────────────────────

_UPSERT_LAG = """
INSERT INTO meter_lag_stats (meter_id, avg_lag_sec, p95_lag_sec, max_lag_sec, updated_at)
VALUES (%(meter_id)s, %(avg_lag)s, %(p95_lag)s, %(max_lag)s, %(now)s)
ON CONFLICT (meter_id) DO UPDATE
    SET avg_lag_sec = meter_lag_stats.avg_lag_sec * 0.8 + EXCLUDED.avg_lag_sec * 0.2,
        p95_lag_sec = GREATEST(meter_lag_stats.p95_lag_sec, EXCLUDED.p95_lag_sec),
        max_lag_sec = GREATEST(meter_lag_stats.max_lag_sec, EXCLUDED.max_lag_sec),
        updated_at  = EXCLUDED.updated_at
"""


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[max(0, int(len(s) * 0.95) - 1)]


def upsert_lag_stats(conn, readings: list[dict]) -> None:
    """Compute lag (received_at − reading_time) per meter; upsert running stats."""
    meter_lags: dict[str, list[float]] = defaultdict(list)
    for r in readings:
        meter_id    = r.get("meter_id")
        rt          = _ts(r.get("reading_time"))
        ra          = _ts(r.get("received_at"))
        if meter_id and rt and ra:
            lag = (ra - rt).total_seconds()
            if lag >= 0:
                meter_lags[meter_id].append(lag)

    if not meter_lags:
        return

    now = datetime.now(timezone.utc)
    rows = [
        {
            "meter_id": mid,
            "avg_lag":  sum(lags) / len(lags),
            "p95_lag":  _p95(lags),
            "max_lag":  max(lags),
            "now":      now,
        }
        for mid, lags in meter_lags.items()
    ]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_LAG, rows)
    conn.commit()
