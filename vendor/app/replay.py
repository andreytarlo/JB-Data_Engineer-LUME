"""Background replay loop + per-subscriber delivery state.

Holds `simulated_now`, advances it at REPLAY_SECONDS_PER_DAY pace, and emits
one batch per simulated half-hour slice to each subscriber's delivery queue.
Persists state to <DATA_DIR>/_replay_state.json every 5 wall-seconds.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import data as data_mod


SECONDS_PER_DAY = float(os.environ.get("REPLAY_SECONDS_PER_DAY", "3"))
WINDOW_START = datetime.fromisoformat(os.environ.get("REPLAY_WINDOW_START", "2013-12-01")).replace(tzinfo=timezone.utc)
WINDOW_END = datetime.fromisoformat(os.environ.get("REPLAY_WINDOW_END", "2014-02-28")).replace(tzinfo=timezone.utc)
LOOP_AT_END = os.environ.get("REPLAY_LOOP", "true").lower() in ("1", "true", "yes")
SLICE_MINUTES = 30  # vendor's half-hour cadence

STATE_PATH = Path(os.environ.get("DATA_DIR", "/data")) / "_replay_state.json"


@dataclass
class Subscriber:
    sub_id: str
    webhook_url: str
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=64))
    last_acked_slice: datetime | None = None     # exclusive watermark
    deliveries_sent: int = 0
    deliveries_acked: int = 0
    deliveries_retrying: int = 0


@dataclass
class ReplayState:
    simulated_now: datetime = field(default_factory=lambda: WINDOW_START)
    subscribers: dict[str, Subscriber] = field(default_factory=dict)
    deliveries_total: int = 0
    deliveries_unacked: int = 0
    retries_total: int = 0
    # Rolling history of recent reading dicts, for chaos.maybe_append_late.
    history: deque = field(default_factory=lambda: deque(maxlen=2000))
    rng: random.Random = field(default_factory=lambda: random.Random(0xC0FFEE))

    def snapshot(self) -> dict[str, Any]:
        return {
            "simulated_now": self.simulated_now.isoformat(),
            "subscribers": {
                sid: {
                    "webhook_url": s.webhook_url,
                    "last_acked_slice": s.last_acked_slice.isoformat() if s.last_acked_slice else None,
                    "deliveries_sent": s.deliveries_sent,
                    "deliveries_acked": s.deliveries_acked,
                }
                for sid, s in self.subscribers.items()
            },
        }

    def restore_clock(self, payload: dict[str, Any]) -> None:
        try:
            self.simulated_now = datetime.fromisoformat(payload["simulated_now"])
            if self.simulated_now.tzinfo is None:
                self.simulated_now = self.simulated_now.replace(tzinfo=timezone.utc)
        except Exception:
            pass


def load_persisted_clock() -> dict[str, Any] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


async def _persist_loop(state: ReplayState) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            STATE_PATH.write_text(json.dumps(state.snapshot(), indent=2))
        except Exception:
            pass


def _next_slice_end(t: datetime) -> datetime:
    """Round up to the next half-hour boundary (exclusive)."""
    minute = (t.minute // SLICE_MINUTES + 1) * SLICE_MINUTES
    if minute >= 60:
        return t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return t.replace(minute=minute, second=0, microsecond=0)


async def replay_loop(state: ReplayState) -> None:
    """Advance simulated_now at wall-clock pace and fan out half-hour batches.

    Wall-time anchored: every TICK_S the loop computes how many simulated
    seconds should have elapsed by now and emits enough slices to catch up.
    This keeps `simulated_now` advancing at exactly SECONDS_PER_DAY regardless
    of per-slice materialisation cost, instead of the previous design where a
    sleep-per-slice put a floor on real time per simulated day.
    """
    import time as _time

    TICK_S = 0.1                                # how often we wake to catch up
    SLICE_TIMEDELTA = timedelta(minutes=SLICE_MINUTES)
    sim_per_wall_s = 86400.0 / SECONDS_PER_DAY  # simulated seconds per wall second

    # If we restored, start from where we were; otherwise from the window start.
    if state.simulated_now < WINDOW_START:
        state.simulated_now = WINDOW_START

    wall_origin = _time.monotonic()
    sim_origin = state.simulated_now

    while True:
        # Where should the simulated clock be by now?
        wall_elapsed = _time.monotonic() - wall_origin
        target_sim_now = sim_origin + timedelta(seconds=wall_elapsed * sim_per_wall_s)

        # Emit every slice whose end is at-or-before the target.
        while state.simulated_now + SLICE_TIMEDELTA <= target_sim_now:
            slice_start = state.simulated_now
            slice_end = _next_slice_end(slice_start)

            if slice_end > WINDOW_END:
                if LOOP_AT_END:
                    state.simulated_now = WINDOW_START
                    wall_origin = _time.monotonic()
                    sim_origin = state.simulated_now
                    break
                # Pause: re-anchor when sim_now advances past END for good.
                await asyncio.sleep(60)
                break

            try:
                readings = list(data_mod.iter_readings_for_window(slice_start, slice_end))
            except Exception as e:
                print(f"[replay] iter error: {e}", flush=True)
                readings = []

            # Rolling history sample (cheap; 200 dicts) for chaos.maybe_append_late.
            for r in readings[:200]:
                state.history.append(r)

            # Enqueue per subscriber with drop-oldest backpressure so a slow
            # subscriber never stalls the replay clock.
            if readings:
                for sub in list(state.subscribers.values()):
                    if sub.queue.full():
                        try:
                            sub.queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    sub.queue.put_nowait({
                        "slice_start": slice_start,
                        "slice_end": slice_end,
                        "readings": readings,
                    })

            state.simulated_now = slice_end

            # Cooperate with delivery workers between slices.
            await asyncio.sleep(0)

        await asyncio.sleep(TICK_S)
