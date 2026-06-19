"""Layer 3 — Batch-level deduplication via Redis SET NX + EX.

Called as the first step inside process.py's foreachBatch handler.

For each delivery_id in the micro-batch:
  • Attempt  SET dedup:<id> 1 NX EX 86400  — one atomic command.
    – Returns True  → key was new    → process this payload.
    – Returns None  → key existed    → duplicate; log + skip.
  • Write one row to batch_duplicates for every skipped payload.
  • Increment a 5-minute rolling counter in Redis for the dashboard.

Fallback when Redis is unavailable:
  All payloads pass through with a WARNING.  Layer 5 (conditional upsert)
  absorbs true duplicates at the database level, just more slowly.

Atomicity guarantee (Architecture §Layer 3):
  SET NX EX is a single Redis command — not two commands.  If we used
  SET NX followed by EXPIRE in separate calls the process could crash
  between them, leaving a key with no TTL that leaks memory forever.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import redis as redis_lib

log = logging.getLogger("dedup")

DEDUP_TTL_S   = 24 * 3600  # duplicates can arrive hours after original
COUNTER_TTL_S = 5  * 60    # rolling 5-minute window for the dashboard


# ─── Core check ───────────────────────────────────────────────────────────────

def _is_new(r: redis_lib.Redis, delivery_id: str) -> bool:
    """Register delivery_id atomically.  Return True if first time seen."""
    result = r.set(f"dedup:{delivery_id}", 1, nx=True, ex=DEDUP_TTL_S)
    # SET NX returns True when the key was just created, None when it already existed.
    return result is not None


# ─── Public entry point ───────────────────────────────────────────────────────

def process_batch_dedup(
    payloads: list[dict],
    redis_client: redis_lib.Redis | None,
    pg_conn,
) -> tuple[list[dict], int]:
    """Split payloads into new vs. duplicate.

    Args:
        payloads:     list of Kafka message dicts, each with a delivery_id.
        redis_client: live Redis connection, or None if Redis is unreachable.
        pg_conn:      open psycopg2 connection for writing duplicate records.

    Returns:
        (new_payloads, duplicate_count)
    """
    if redis_client is None:
        log.warning(
            "Redis unavailable — bypassing dedup for %d payload(s); "
            "Layer 5 will absorb any duplicates at the DB level",
            len(payloads),
        )
        return payloads, 0

    new_payloads: list[dict] = []
    dup_rows: list[tuple] = []

    for payload in payloads:
        delivery_id = payload.get("delivery_id", "")
        source      = payload.get("source", "live")
        readings    = payload.get("readings", [])

        if _is_new(redis_client, delivery_id):
            new_payloads.append(payload)
        else:
            dup_rows.append((
                delivery_id,
                datetime.now(timezone.utc),
                source,
                len(readings),
            ))
            log.debug(
                "Duplicate skipped: delivery_id=%s source=%s readings=%d",
                delivery_id, source, len(readings),
            )

    if dup_rows:
        _write_duplicates(pg_conn, dup_rows)
        _increment_dup_counter(redis_client, len(dup_rows))

    return new_payloads, len(dup_rows)


# ─── Side effects ─────────────────────────────────────────────────────────────

def _write_duplicates(pg_conn, rows: list[tuple]) -> None:
    """Insert one row per duplicate batch into batch_duplicates."""
    sql = """
        INSERT INTO batch_duplicates (delivery_id, detected_at, source, reading_count)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    with pg_conn.cursor() as cur:
        cur.executemany(sql, rows)
    pg_conn.commit()


def _increment_dup_counter(r: redis_lib.Redis, count: int) -> None:
    """Increment the rolling 5-minute duplicate counter for Grafana."""
    key = "stats:dups:5m"
    pipe = r.pipeline()
    pipe.incrby(key, count)
    pipe.expire(key, COUNTER_TTL_S)
    pipe.execute()
