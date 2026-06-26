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

# Bulk upsert via COPY into a temp table + a single set-based merge.  This is
# orders of magnitude faster than row-by-row executemany at the volumes here
# (hundreds of thousands of readings per micro-batch): COPY streams the rows in
# one pass, and the INSERT…SELECT…ON CONFLICT applies them in one statement.
_CREATE_STAGE = """
CREATE TEMP TABLE IF NOT EXISTS _stage_clean (
    meter_id     VARCHAR,
    reading_time TIMESTAMPTZ,
    household_id VARCHAR,
    kwh          DOUBLE PRECISION,
    received_at  TIMESTAMPTZ,
    ingested_at  TIMESTAMPTZ
) ON COMMIT DELETE ROWS
"""

_COPY_STAGE = (
    "COPY _stage_clean "
    "(meter_id, reading_time, household_id, kwh, received_at, ingested_at) FROM STDIN"
)

# DISTINCT ON keeps only the newest received_at per (meter_id, reading_time)
# *within this batch* — so out-of-order or duplicate readings inside one batch
# collapse to the correct latest value before they ever hit clean_readings.
_MERGE_CLEAN = """
INSERT INTO clean_readings (meter_id, reading_time, household_id, kwh, received_at, ingested_at)
SELECT DISTINCT ON (meter_id, reading_time)
       meter_id, reading_time, household_id, kwh, received_at, ingested_at
FROM   _stage_clean
WHERE  meter_id IS NOT NULL AND reading_time IS NOT NULL
ORDER  BY meter_id, reading_time, received_at DESC NULLS LAST
ON CONFLICT (meter_id, reading_time) DO UPDATE
    SET kwh          = EXCLUDED.kwh,
        received_at  = EXCLUDED.received_at,
        household_id = EXCLUDED.household_id,
        ingested_at  = EXCLUDED.ingested_at
-- A strictly newer received_at always wins. But the vendor does NOT guarantee a
-- late correction carries a later received_at — it may re-emit the same
-- (meter_id, reading_time) with the SAME received_at and a corrected kwh. With a
-- plain ">" that correction would be silently dropped (breaking "late
-- corrections must overwrite"). So we also overwrite when received_at is equal
-- (NULL-safe) AND this ingest is newer AND the value actually changed — the
-- kwh-distinct guard keeps a true duplicate (identical value) a no-op.
WHERE EXCLUDED.received_at > clean_readings.received_at
   OR (EXCLUDED.received_at IS NOT DISTINCT FROM clean_readings.received_at
       AND EXCLUDED.ingested_at > clean_readings.ingested_at
       AND EXCLUDED.kwh IS DISTINCT FROM clean_readings.kwh)
"""


def upsert_clean_readings(conn, readings: list[dict]) -> int:
    """Upsert valid readings via COPY + merge; returns rows staged."""
    if not readings:
        return 0
    now = datetime.now(timezone.utc)
    staged = 0
    with conn.cursor() as cur:
        cur.execute(_CREATE_STAGE)
        with cur.copy(_COPY_STAGE) as cp:
            for r in readings:
                rt = _ts(r.get("reading_time"))
                if rt is None:
                    continue
                cp.write_row((
                    r.get("meter_id"),
                    rt,
                    r.get("household_id"),
                    float(r["kwh"]),
                    _ts(r.get("received_at")),
                    now,
                ))
                staged += 1
        cur.execute(_MERGE_CLEAN)
    conn.commit()
    return staged


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
        -- Exponentially smoothed like avg_lag_sec, so it tracks a true rolling
        -- p95 that can fall as the feed recovers — not a monotonic all-time max.
        p95_lag_sec = meter_lag_stats.p95_lag_sec * 0.8 + EXCLUDED.p95_lag_sec * 0.2,
        -- max stays an all-time high water mark (GREATEST is correct here).
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
