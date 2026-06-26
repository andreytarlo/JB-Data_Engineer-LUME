"""Layer 7 — Scheduled maintenance jobs.

Three tasks run on a fixed schedule:

  1. Every 30 min  — Meter silence check
     Finds meters with readings in the last 30 days that have not reported
     in the last 2 hours. Inserts a row into meter_silence_log per meter.

  2. Daily 03:00   — Orphan meter summary
     Finds meter IDs that appear in clean_readings but not in homes.
     Upserts one row per meter into orphan_meters_summary.

  3. Daily 04:00   — Archive + purge rejected_readings
     Rolls up rejected_readings (grouped by hour + rejection_type) into
     rejection_hourly_summary, then deletes rows older than 7 days.
     The hourly summary is kept forever (~4 MB/year) for quality trending.
"""
from __future__ import annotations

import logging
import os
import time

import psycopg
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

log = logging.getLogger("scheduler")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

PG_DSN              = os.environ.get("POSTGRES_DSN", "postgresql://lume:lume@postgres:5432/lume")
SILENCE_THRESHOLD_H = int(os.environ.get("SILENCE_THRESHOLD_H", "2"))
REJECTED_RETAIN_D   = int(os.environ.get("REJECTED_RETAIN_D", "7"))


# ─── DB connection ────────────────────────────────────────────────────────────

def _connect() -> psycopg.Connection:
    for attempt in range(20):
        try:
            conn = psycopg.connect(PG_DSN, autocommit=False)
            log.info("PostgreSQL connected")
            return conn
        except Exception as exc:
            log.warning("PG not ready (%s) — retry %d/20", exc, attempt + 1)
            time.sleep(5)
    raise RuntimeError("Could not connect to PostgreSQL after 20 retries")


# ─── Task 1: Meter silence check (every 30 min) ───────────────────────────────

# "Now" is the latest reading_time we hold, NOT wall-clock NOW().  The replay
# streams 2013-2014 data while the wall clock is years later, so NOW() would put
# every meter past the silence threshold (or, via the 30-day pre-filter, exclude
# them all and return nothing).  Anchoring on MAX(reading_time) measures silence
# in data-time, which is what the operator actually cares about.
_SILENCE_QUERY = """
WITH data_now AS (
    SELECT MAX(reading_time) AS now_ts FROM clean_readings
),
last_reading AS (
    SELECT
        cr.meter_id,
        h.postcode_area,
        MAX(cr.reading_time) AS last_seen
    FROM  clean_readings cr
    LEFT  JOIN homes h USING (meter_id)
    WHERE cr.reading_time > (SELECT now_ts FROM data_now) - INTERVAL '30 days'
    GROUP BY cr.meter_id, h.postcode_area
)
SELECT
    meter_id,
    postcode_area,
    last_seen,
    EXTRACT(EPOCH FROM ((SELECT now_ts FROM data_now) - last_seen)) / 3600.0 AS silent_hours
FROM last_reading
WHERE last_seen < (SELECT now_ts FROM data_now) - (%s * INTERVAL '1 hour')
ORDER BY silent_hours DESC
"""

_INSERT_SILENCE = """
INSERT INTO meter_silence_log (meter_id, postcode_area, last_seen, silent_hours)
VALUES (%(meter_id)s, %(postcode_area)s, %(last_seen)s, %(silent_hours)s)
"""


def check_meter_silence(conn: psycopg.Connection) -> None:
    log.info("Task: check_meter_silence (threshold=%dh)", SILENCE_THRESHOLD_H)
    try:
        with conn.cursor() as cur:
            cur.execute(_SILENCE_QUERY, (SILENCE_THRESHOLD_H,))
            rows = cur.fetchall()

        if not rows:
            log.info("check_meter_silence: no silent meters")
            conn.commit()
            return

        records = [
            {
                "meter_id":     row[0],
                "postcode_area": row[1],
                "last_seen":    row[2],
                "silent_hours": float(row[3]),
            }
            for row in rows
        ]
        with conn.cursor() as cur:
            cur.executemany(_INSERT_SILENCE, records)
        conn.commit()
        log.info("check_meter_silence: inserted %d silent-meter rows", len(records))
    except Exception as exc:
        log.error("check_meter_silence failed: %s", exc)
        conn.rollback()


# ─── Task 2: Orphan meter summary (daily 03:00) ───────────────────────────────

_ORPHAN_QUERY = """
SELECT
    cr.meter_id,
    MIN(cr.reading_time)  AS first_seen,
    MAX(cr.reading_time)  AS last_seen,
    COUNT(*)              AS reading_count,
    COALESCE(SUM(cr.kwh), 0) AS total_kwh
FROM  clean_readings cr
WHERE cr.meter_id NOT IN (SELECT meter_id FROM homes)
GROUP BY cr.meter_id
"""

_UPSERT_ORPHAN = """
INSERT INTO orphan_meters_summary
    (meter_id, first_seen, last_seen, reading_count, total_kwh)
VALUES
    (%(meter_id)s, %(first_seen)s, %(last_seen)s, %(reading_count)s, %(total_kwh)s)
ON CONFLICT (meter_id) DO UPDATE
    SET last_seen     = EXCLUDED.last_seen,
        reading_count = EXCLUDED.reading_count,
        total_kwh     = EXCLUDED.total_kwh
"""


def refresh_orphan_summary(conn: psycopg.Connection) -> None:
    log.info("Task: refresh_orphan_summary")
    try:
        with conn.cursor() as cur:
            cur.execute(_ORPHAN_QUERY)
            rows = cur.fetchall()

        if not rows:
            log.info("refresh_orphan_summary: no orphan meters")
            conn.commit()
            return

        records = [
            {
                "meter_id":     row[0],
                "first_seen":   row[1],
                "last_seen":    row[2],
                "reading_count": int(row[3]),
                "total_kwh":    float(row[4]),
            }
            for row in rows
        ]
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_ORPHAN, records)
        conn.commit()
        log.info("refresh_orphan_summary: upserted %d orphan meters", len(records))
    except Exception as exc:
        log.error("refresh_orphan_summary failed: %s", exc)
        conn.rollback()


# ─── Task 3: Archive + purge rejected_readings (daily 04:00) ─────────────────

# Bucket by reading_time (data-time), falling back to rejected_at only when the
# reading had no parseable timestamp.  rejected_at is wall-clock (2026) and would
# make the permanent trend table disagree with every reading_time-based query and
# dashboard.  The retention filter below still uses rejected_at — that is real
# insert age, which is the right basis for purging.
# Cumulative summation, not GREATEST. Rows live in rejected_readings for the
# retention window (7 days) and stay eligible across several daily runs, so a
# plain re-aggregate would either double-count (sum) or under-count (GREATEST).
# We mark each row archived=TRUE the moment it is folded in, and only aggregate
# NOT-archived rows — so every reject is counted exactly once and the hourly
# totals are a true cumulative sum. All in one statement (atomic).
_ARCHIVE_REJECTED = """
WITH to_archive AS (
    SELECT id,
           date_trunc('hour', COALESCE(reading_time, rejected_at)) AS ts_hour,
           rejection_type                                          AS rejection_reason
    FROM   rejected_readings
    WHERE  rejected_at < NOW() - INTERVAL '1 day'
      AND  NOT archived
),
agg AS (
    SELECT ts_hour, rejection_reason, COUNT(*) AS count
    FROM   to_archive
    GROUP  BY ts_hour, rejection_reason
),
ins AS (
    INSERT INTO rejection_hourly_summary (ts_hour, rejection_reason, count)
    SELECT ts_hour, rejection_reason, count FROM agg
    ON CONFLICT (ts_hour, rejection_reason) DO UPDATE
        SET count = rejection_hourly_summary.count + EXCLUDED.count
)
UPDATE rejected_readings
   SET archived = TRUE
 WHERE id IN (SELECT id FROM to_archive)
"""

_PURGE_REJECTED = """
DELETE FROM rejected_readings
WHERE rejected_at < NOW() - (%s * INTERVAL '1 day')
"""


def archive_and_purge_rejected(conn: psycopg.Connection) -> None:
    log.info("Task: archive_and_purge_rejected (retain=%dd)", REJECTED_RETAIN_D)
    try:
        with conn.cursor() as cur:
            cur.execute(_ARCHIVE_REJECTED)
            archived = cur.rowcount
        conn.commit()
        log.info("archive_and_purge_rejected: folded %d rejected rows into hourly summary", archived)

        with conn.cursor() as cur:
            cur.execute(_PURGE_REJECTED, (REJECTED_RETAIN_D,))
            purged = cur.rowcount
        conn.commit()
        log.info("archive_and_purge_rejected: purged %d rows older than %dd",
                 purged, REJECTED_RETAIN_D)
    except Exception as exc:
        log.error("archive_and_purge_rejected failed: %s", exc)
        conn.rollback()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    conn = _connect()

    # Run the dashboard-feeding jobs once at startup so their panels are populated
    # immediately instead of staying empty until the first scheduled tick (up to
    # 30 min for the silence check).
    check_meter_silence(conn)
    refresh_orphan_summary(conn)

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        lambda: check_meter_silence(conn),
        IntervalTrigger(minutes=30),
        id="meter_silence",
        name="Meter silence check",
        max_instances=1,
        # A heavy query can push the fire time past the default 1 s grace window,
        # which silently SKIPS the run.  Allow lateness and coalesce missed runs.
        misfire_grace_time=600,
        coalesce=True,
    )

    scheduler.add_job(
        lambda: refresh_orphan_summary(conn),
        CronTrigger(hour=3, minute=0),
        id="orphan_summary",
        name="Orphan meter summary",
        max_instances=1,
        misfire_grace_time=600,
        coalesce=True,
    )

    scheduler.add_job(
        lambda: archive_and_purge_rejected(conn),
        CronTrigger(hour=4, minute=0),
        id="archive_rejected",
        name="Archive + purge rejected readings",
        max_instances=1,
        misfire_grace_time=600,
        coalesce=True,
    )

    log.info(
        "Scheduler started — silence_threshold=%dh  retain_rejected=%dd",
        SILENCE_THRESHOLD_H,
        REJECTED_RETAIN_D,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
