"""Layer 2 — Historical Archive Backfill Service.

Reads the meter-vendor's paginated historical archive and pushes every
page to Kafka `readings-raw` partition 1 (the backfill lane).

Two jobs, one loop:
  1. Initial historical load — on first start (no watermark file) fetches
     all readings from REPLAY_WINDOW_START forward, so the pipeline has
     the full dataset before the live stream even starts.
  2. Automatic gap-fill — after a live-feed outage the vendor's archive
     already holds the readings that were missed.  On the next pass this
     service fetches them and pushes to partition 1; Layer 3 dedup and
     Layer 5 conditional upsert absorb any overlap with readings already
     delivered via partition 0.

Why partition 1 matters (Architecture §Layer 2):
  The PySpark consumer (Layer 4) reads both partitions in parallel with
  independent offsets.  A large backfill burst on partition 1 cannot
  delay live readings sitting in partition 0 — each partition advances
  at its own pace.

Watermark:
  The latest reading_time seen is persisted to STATE_DIR/watermark.txt.
  On restart the service picks up from that point, so a restart never
  re-downloads the full history.  Kafka dedup (Layer 3) handles the
  small overlap on the seam between passes.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from kafka import KafkaProducer

log = logging.getLogger("backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

VENDOR_URL          = os.environ.get("VENDOR_URL", "http://meter-vendor:8000")
KAFKA_BOOTSTRAP     = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REPLAY_WINDOW_START = os.environ.get("REPLAY_WINDOW_START", "2013-12-01")
STATE_DIR           = Path(os.environ.get("STATE_DIR", "/state"))
PAGE_LIMIT          = int(os.environ.get("BACKFILL_PAGE_LIMIT", "1000"))
# How long to sleep after exhausting the archive before the next pass.
# Short enough to detect and fill outage gaps promptly; long enough not
# to hammer the vendor API when caught up.
IDLE_SLEEP_S        = float(os.environ.get("BACKFILL_IDLE_SLEEP_S", "30"))

KAFKA_TOPIC        = "readings-raw"
BACKFILL_PARTITION = 1  # 0 = live (Layer 1); 1 = backfill


# ─── Watermark ────────────────────────────────────────────────────────────────

def _wm_path() -> Path:
    return STATE_DIR / "watermark.txt"


def load_watermark() -> str:
    path = _wm_path()
    if path.exists():
        ts = path.read_text().strip()
        log.info("Resuming from watermark %s", ts)
        return ts
    # First ever run: start from the beginning of the replay window.
    ts = f"{REPLAY_WINDOW_START}T00:00:00Z"
    log.info("No watermark found — starting full load from %s", ts)
    return ts


def save_watermark(ts: str) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    _wm_path().write_text(ts)


# ─── Stable delivery_id ──────────────────────────────────────────────────────

def _content_hash(readings: list[dict]) -> str:
    """Derive a delivery_id from page content so the same page always maps to
    the same id regardless of when it was fetched.

    This is the crash-safe fix: if backfill restarts after saving the watermark
    but before finishing all pages, the re-fetched pages produce identical ids
    and Layer 3 Redis dedup skips them instead of processing them twice.
    """
    key = "|".join(
        f"{r.get('meter_id', '')},{r.get('reading_time', '')}"
        for r in sorted(readings, key=lambda r: (r.get("meter_id", ""), r.get("reading_time", "")))
    )
    return "bf_" + hashlib.sha256(key.encode()).hexdigest()[:16]


# ─── Archive fetch ────────────────────────────────────────────────────────────

def fetch_page(since: str, cursor: str | None) -> tuple[list[dict], str | None]:
    """GET one page from the vendor archive; return (readings, next_cursor)."""
    params: dict = {"since": since, "limit": PAGE_LIMIT}
    if cursor:
        params["cursor"] = cursor
    resp = requests.get(f"{VENDOR_URL}/readings", params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    return body["readings"], body.get("next_cursor")


# ─── Single backfill pass ─────────────────────────────────────────────────────

def run_pass(producer: KafkaProducer) -> int:
    """Fetch all pages since the current watermark and push to Kafka.

    Returns the total number of readings sent in this pass.
    """
    since  = load_watermark()
    cursor: str | None = None
    page   = 0
    total  = 0

    while True:
        try:
            readings, next_cursor = fetch_page(since, cursor)
        except requests.RequestException as exc:
            log.warning("Archive request failed: %s — retry in 10 s", exc)
            time.sleep(10)
            # Do not advance cursor; retry the same page.
            continue

        if not readings:
            # Archive is exhausted up to simulated-now.
            break

        # The vendor sets the same delivery_id on every reading in a page
        # (see vendor/app/main.py).  Extract it as the batch-level id so
        # Layer 3 can dedup whole pages efficiently.
        delivery_id = readings[0].get("delivery_id") or _content_hash(readings)
        ingest_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        payload = {
            "delivery_id": delivery_id,
            "source": "backfill",  # Layer 6 filters on reading_time, not source,
            "ingest_time": ingest_time,  # but this is useful for observability.
            "readings": readings,
        }
        producer.send(KAFKA_TOPIC, value=payload, partition=BACKFILL_PARTITION)

        # Advance watermark to the latest reading_time in this page.
        # The next pass starts from here; the tiny overlap is absorbed by dedup.
        latest = max(r["reading_time"] for r in readings)
        save_watermark(latest)

        page  += 1
        total += len(readings)
        log.info("Page %4d | %5d readings | watermark → %s", page, len(readings), latest)

        if next_cursor:
            cursor = next_cursor
        else:
            break

    # Ensure all buffered messages reach Kafka before sleeping.
    producer.flush()
    return total


# ─── Kafka producer with startup retry ───────────────────────────────────────

def build_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                # Larger linger than the ingest gateway: backfill is bulk work,
                # not latency-sensitive.  Bigger batches → fewer round-trips.
                linger_ms=100,
                compression_type="gzip",
                retries=5,
            )
            log.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)
            return p
        except Exception as exc:
            log.warning("Kafka not ready (%s) — retry in 5 s", exc)
            time.sleep(5)


# ─── Main loop ────────────────────────────────────────────────────────────────

def main() -> None:
    producer = build_producer()

    while True:
        try:
            total = run_pass(producer)
            if total:
                log.info("Pass complete: %d readings pushed to partition %d",
                         total, BACKFILL_PARTITION)
            else:
                log.debug("Pass complete: archive is current, nothing new")
        except Exception as exc:
            log.error("Unexpected error in backfill pass: %s", exc, exc_info=True)

        log.info("Sleeping %g s before next pass", IDLE_SLEEP_S)
        time.sleep(IDLE_SLEEP_S)


if __name__ == "__main__":
    main()
