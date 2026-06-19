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

_SILENCE_QUERY = """
WITH last_reading AS (
    SELECT
        cr.meter_id,
        h.postcode_area,
        MAX(cr.reading_time) AS last_seen
    FROM  clean_readings cr
    LEFT  JOIN homes h USING (meter_id)
    WHERE cr.reading_time > NOW() - INTERVAL '30 days'
    GROUP BY cr.meter_id, h.postcode_area
)
SELECT
    meter_id,
    postcode_area,
    last_seen,
    EXTRACT(EPOCH FROM (NOW() - last_seen)) / 3600.0 AS silent_hours
FROM last_reading
WHERE last_seen < NOW() - INTERVAL '%s hours'
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
            cur.execute(_SILENCE_QUERY % SILENCE_THRESHOLD_H)
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

_ARCHIVE_REJECTED = """
INSERT INTO rejection_hourly_summary (ts_hour, rejection_reason, count)
SELECT
    date_trunc('hour', rejected_at) AS ts_hour,
    rejection_type                  AS rejection_reason,
    COUNT(*)                        AS count
FROM  rejected_readings
WHERE rejected_at < NOW() - INTERVAL '1 day'
GROUP BY 1, 2
ON CONFLICT (ts_hour, rejection_reason) DO UPDATE
    SET count = GREATEST(rejection_hourly_summary.count, EXCLUDED.count)
"""

_PURGE_REJECTED = """
DELETE FROM rejected_readings
WHERE rejected_at < NOW() - INTERVAL '%s days'
"""


def archive_and_purge_rejected(conn: psycopg.Connection) -> None:
    log.info("Task: archive_and_purge_rejected (retain=%dd)", REJECTED_RETAIN_D)
    try:
        with conn.cursor() as cur:
            cur.execute(_ARCHIVE_REJECTED)
            archived = cur.rowcount
        conn.commit()
        log.info("archive_and_purge_rejected: archived %d hour-buckets", archived)

        with conn.cursor() as cur:
            cur.execute(_PURGE_REJECTED % REJECTED_RETAIN_D)
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

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        lambda: check_meter_silence(conn),
        IntervalTrigger(minutes=30),
        id="meter_silence",
        name="Meter silence check",
        max_instances=1,
    )

    scheduler.add_job(
        lambda: refresh_orphan_summary(conn),
        CronTrigger(hour=3, minute=0),
        id="orphan_summary",
        name="Orphan meter summary",
        max_instances=1,
    )

    scheduler.add_job(
        lambda: archive_and_purge_rejected(conn),
        CronTrigger(hour=4, minute=0),
        id="archive_rejected",
        name="Archive + purge rejected readings",
        max_instances=1,
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
