"""Load household and weather dimension tables into PostgreSQL.

Reads households.parquet and weather.parquet from the meter-data volume
(written by data-init) and inserts them into the homes and weather_hourly
tables.  Idempotent: ON CONFLICT DO NOTHING — safe to re-run.

Waits for both the _READY sentinel (data-init finished) and PostgreSQL
to be accepting connections before doing any work.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import timezone
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

log = logging.getLogger("load_dimensions")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

DATA_DIR   = Path(os.environ.get("DATA_DIR", "/data"))
PG_DSN     = os.environ.get("POSTGRES_DSN", "postgresql://lume:lume@postgres:5432/lume")
BATCH_SIZE = int(os.environ.get("LOAD_BATCH_SIZE", "500"))


# ─── Startup waits ────────────────────────────────────────────────────────────

def _wait_for_data(ready: Path, retries: int = 120, delay: float = 5.0) -> None:
    for i in range(retries):
        if ready.exists():
            log.info("data-init complete — _READY found")
            return
        log.info("Waiting for data-init _READY (%d/%d) ...", i + 1, retries)
        time.sleep(delay)
    raise SystemExit(f"{ready} never appeared — data-init did not finish")


def _connect_pg(retries: int = 30, delay: float = 3.0) -> psycopg.Connection:
    for i in range(retries):
        try:
            conn = psycopg.connect(PG_DSN)
            log.info("Connected to PostgreSQL")
            return conn
        except Exception as exc:
            log.warning("PostgreSQL not ready (%s) — retry %d/%d", exc, i + 1, retries)
            time.sleep(delay)
    raise SystemExit("Could not connect to PostgreSQL after retries")


# ─── Homes ───────────────────────────────────────────────────────────────────

def load_homes(conn: psycopg.Connection, path: Path) -> None:
    table = pq.read_table(path)
    rows  = table.to_pylist()
    sql   = """
        INSERT INTO homes (household_id, meter_id, postcode_area, household_size, acorn_group, tariff_type)
        VALUES (%(household_id)s, %(meter_id)s, %(postcode_area)s,
                %(household_size)s, %(acorn_group)s, %(tariff_type)s)
        ON CONFLICT (household_id) DO NOTHING
    """
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            cur.executemany(sql, rows[i : i + BATCH_SIZE])
    conn.commit()
    log.info("homes: %d rows inserted", len(rows))


# ─── Weather ─────────────────────────────────────────────────────────────────

def _hour_floor(ts) -> object:
    """Truncate a datetime-like object to the hour in UTC."""
    if ts is None:
        return None
    if hasattr(ts, "replace"):
        t = ts.replace(minute=0, second=0, microsecond=0)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t
    return ts


def load_weather(conn: psycopg.Connection, path: Path) -> None:
    table = pq.read_table(path)
    rows  = table.to_pylist()

    # Truncate to hour; prefer FM-12 reports when multiple entries per hour.
    best: dict = {}
    for r in rows:
        ts = _hour_floor(r.get("observed_at"))
        if ts is None:
            continue
        rtype = (r.get("report_type") or "").strip()
        if ts not in best or rtype == "FM-12":
            best[ts] = {
                "observed_at": ts,
                "station":     r.get("station", "EGLL-Heathrow"),
                "temp_c":      r.get("temp_c"),
                "report_type": rtype,
            }

    deduped = list(best.values())
    sql = """
        INSERT INTO weather_hourly (observed_at, station, temp_c, report_type)
        VALUES (%(observed_at)s, %(station)s, %(temp_c)s, %(report_type)s)
        ON CONFLICT (observed_at) DO NOTHING
    """
    with conn.cursor() as cur:
        for i in range(0, len(deduped), BATCH_SIZE):
            cur.executemany(sql, deduped[i : i + BATCH_SIZE])
    conn.commit()
    log.info("weather_hourly: %d rows inserted", len(deduped))


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    _wait_for_data(DATA_DIR / "_READY")

    homes_path   = DATA_DIR / "households.parquet"
    weather_path = DATA_DIR / "weather.parquet"

    for p in (homes_path, weather_path):
        if not p.exists():
            raise SystemExit(f"Expected file not found: {p}")

    conn = _connect_pg()
    try:
        load_homes(conn, homes_path)
        load_weather(conn, weather_path)
    finally:
        conn.close()

    log.info("Dimension tables loaded successfully")


if __name__ == "__main__":
    main()
