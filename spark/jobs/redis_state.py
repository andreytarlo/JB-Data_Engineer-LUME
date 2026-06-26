"""Layer 6 — Real-time window state in Redis.

Eight active keys:
  window:start          ISO timestamp of the current 30-min settlement window.
  window:kwh            Running kWh sum (INCRBYFLOAT, atomic).
  window:meters_hll     HyperLogLog of meter IDs that reported this window.
                        PFADD + PFCOUNT: ~1% error, 12 KB fixed memory.
  window:meters         Plain mirror of PFCOUNT(window:meters_hll) so Grafana can
                        read it via GET (the redis-datasource PFCOUNT command
                        returns no data frame).
  window:last_batch     Timestamp of the last processed batch (TTL 150 s).
                        Dashboard shows "feed dead" when this key expires.
  window:forecast       Projected end-of-window total (recalculated each batch).
  window:backfill_active  Present (TTL 60 s) while Spark is processing backfill
                        payloads. Grafana reads this key to show a "filling gaps"
                        banner, explaining sudden kWh spikes to operators.
                        The key self-expires 60 s after the last backfill batch,
                        so no explicit cleanup is needed.
  stats:dups:5m         Rolling 5-min duplicate counter (updated by dedup.py).
  stats:invalid:5m      Rolling 5-min invalid-reading counter.

Window rollover:
  When a reading's reading_time belongs to a new 30-min slot, ALL window:*
  keys are deleted and the new window starts from zero.

Backfill filter:
  Readings from the historical backfill (source="backfill") whose
  reading_time falls outside the current window must NOT update window:*
  keys — they would corrupt the live consumption total.
  However, when backfill payloads ARE present in a batch (regardless of
  whether their readings fall in the current window), window:backfill_active
  is set so the dashboard can annotate the kWh spike.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("redis_state")

WINDOW_MINUTES        = 30
LAST_BATCH_TTL_S      = 150   # "feed dead" fires after 150 s of silence (~30 batches)
STATS_TTL_S           = 5 * 60
BACKFILL_ACTIVE_TTL_S = 60    # banner stays up 60 s after last backfill batch


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ts(value) -> Optional[datetime]:
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


def _window_start(dt: datetime) -> datetime:
    """Truncate a UTC datetime to the 30-minute settlement boundary."""
    utc = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return utc.replace(minute=(utc.minute // WINDOW_MINUTES) * WINDOW_MINUTES)


# ─── Public API ──────────────────────────────────────────────────────────────

def update_window_state(
    redis_client,
    readings: list[dict],
    invalid_count: int = 0,
    backfill_active: bool = False,
    book_meter_count: int = 0,
) -> None:
    """Update Redis window state for one micro-batch of valid readings.

    Args:
        redis_client:     Live Redis client, or None (silently skipped).
        readings:         Validated clean readings for this batch.
        invalid_count:    Number of readings rejected in this batch.
        backfill_active:  True when the batch contains backfill payloads.
                          Sets window:backfill_active (TTL 60 s) so Grafana
                          can display a "filling gaps" banner during spikes.
        book_meter_count: Total meters in the book (homes table). Used for the
                          coverage-based end-of-window forecast; 0 disables it.
    """
    if redis_client is None:
        log.warning("Redis unavailable — skipping window state update")
        return

    now_utc = datetime.now(timezone.utc)

    # Use the most recent reading_time in the batch as the reference for which
    # settlement window we are in.  In production the vendor sends readings with
    # current timestamps so data_now ≈ now_utc.  In simulation mode (replaying
    # 2013-2014 data) data_now follows the simulated clock instead, so the
    # window keys stay meaningful and Grafana shows real data rather than zeros.
    reading_times = [t for r in readings if (t := _ts(r.get("reading_time"))) is not None]
    data_now   = max(reading_times) if reading_times else now_utc
    cur_window = _window_start(data_now)

    # Include all readings whose reading_time falls in the current data window.
    current: list[dict] = []
    for r in readings:
        rt = _ts(r.get("reading_time"))
        if rt and _window_start(rt) == cur_window:
            current.append(r)

    if current:
        _apply_to_window(redis_client, current, now_utc, cur_window, book_meter_count)

    # Backfill banner — refresh TTL every batch while backfill is active.
    # Key self-expires 60 s after the last backfill batch, no cleanup needed.
    if backfill_active:
        redis_client.set("window:backfill_active", "1", ex=BACKFILL_ACTIVE_TTL_S)
        log.debug("window:backfill_active refreshed (TTL %ds)", BACKFILL_ACTIVE_TTL_S)

    if invalid_count > 0:
        pipe = redis_client.pipeline()
        pipe.incrby("stats:invalid:5m", invalid_count)
        pipe.expire("stats:invalid:5m", STATS_TTL_S)
        pipe.execute()


# ─── Internal ─────────────────────────────────────────────────────────────────

def _apply_to_window(
    r,
    readings: list[dict],
    now_utc: datetime,
    cur_window: datetime,
    book_meter_count: int = 0,
) -> None:
    # Roll over if the stored window start doesn't match the current one.
    stored_raw = r.get("window:start")
    if stored_raw is not None:
        stored_str = stored_raw.decode() if isinstance(stored_raw, bytes) else stored_raw
        stored_dt  = _ts(stored_str)
    else:
        stored_dt = None

    stored_window = _window_start(stored_dt) if stored_dt is not None else None

    # Roll the window FORWARD only. A pure-backfill batch can carry an older
    # max(reading_time) than the live window (heavy history push with no live
    # readings in the same trigger); rolling the window back to that old time
    # would corrupt the live kWh total on the dashboard (the DB stays correct).
    # So if this batch's window is older than what we already track, skip it.
    if stored_window is not None and cur_window < stored_window:
        log.debug("Skipping old window %s (stored %s) — backfill, no rollback",
                  cur_window.isoformat(), stored_window.isoformat())
        return
    if stored_window is None or cur_window > stored_window:
        _rollover(r, cur_window)

    # Accumulate kWh and register meters.
    total_kwh = sum(float(rd.get("kwh", 0)) for rd in readings)
    meter_ids = [rd["meter_id"] for rd in readings if rd.get("meter_id")]

    pipe = r.pipeline()
    pipe.incrbyfloat("window:kwh", total_kwh)
    if meter_ids:
        pipe.pfadd("window:meters_hll", *meter_ids)
    pipe.set("window:last_batch", now_utc.isoformat(), ex=LAST_BATCH_TTL_S)
    pipe.execute()

    # Forecast — coverage-based projection.
    #
    # A time-based projection (kWh ÷ elapsed-minutes × 30) cannot work here:
    # every reading_time is stamped on the :00/:30 settlement boundary, so all
    # readings in a window share the same timestamp and "elapsed minutes" is
    # always 0.  Mixing wall-clock now (2026) with the data-time window (2013-14)
    # made it worse, pinning the forecast at ~0.
    #
    # Instead we project from coverage: kWh seen so far scaled up by the share
    # of the book that has reported.  If 40% of meters have reported, the
    # end-of-window total is projected at kWh / 0.40.  Once every meter has
    # reported the forecast equals the actual running total.
    kwh_now     = float(r.get("window:kwh") or 0)
    meters_seen = int(r.pfcount("window:meters_hll") or 0)
    # Mirror the HyperLogLog count into a plain string key.  The redis-datasource
    # Grafana plugin returns no data frame for the PFCOUNT command, but it does
    # for GET — so the "Active Meters" panel reads window:meters, not the HLL.
    r.set("window:meters", meters_seen)
    if book_meter_count > 0 and meters_seen > 0:
        coverage = min(meters_seen / book_meter_count, 1.0)
        forecast = round(kwh_now / coverage, 2)
    else:
        forecast = kwh_now
    r.set("window:forecast", forecast)


def _rollover(r, new_start: datetime) -> None:
    log.info("Window rollover → %s", new_start.isoformat())
    pipe = r.pipeline()
    for key in ("window:kwh", "window:meters_hll", "window:meters", "window:last_batch", "window:forecast"):
        pipe.delete(key)
    pipe.set("window:start", new_start.isoformat())
    pipe.set("window:kwh", 0)
    pipe.execute()
