"""Per-subscriber async delivery worker.

One task per subscriber: pulls slices off the subscriber's queue, applies
chaos transforms, POSTs to the webhook, retries with exponential backoff on
non-200, advances the watermark only on 200.
"""
from __future__ import annotations

import asyncio
import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from . import chaos
from .replay import ReplayState, Subscriber


HTTP_TIMEOUT_S = 10.0
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 300.0


def _new_delivery_id() -> str:
    # ULID-shaped (time-prefix + random). The vendor only requires uniqueness
    # within the lifetime of the mock; a UUID4 is fine.
    return "del_" + uuid.uuid4().hex[:24]


async def _post_once(client: httpx.AsyncClient, url: str, payload: dict[str, Any]) -> int:
    try:
        r = await client.post(url, json=payload, timeout=HTTP_TIMEOUT_S)
        return r.status_code
    except Exception as e:
        print(f"[delivery] POST {url} failed: {e}", flush=True)
        return 0


async def deliver_subscriber(state: ReplayState, sub: Subscriber, outage_windows) -> None:
    rng = random.Random(uuid.uuid4().int & 0xFFFFFFFF)
    async with httpx.AsyncClient() as client:
        while True:
            slice_ = await sub.queue.get()
            readings = slice_["readings"]
            if not readings:
                continue

            # Outage: pause sending entirely while in any configured window.
            while chaos.in_outage(state.simulated_now, outage_windows):
                await asyncio.sleep(1.0)

            # Apply chaos: shuffle within batch, then maybe append a late correction.
            batch = chaos.maybe_shuffle(rng, list(readings))
            batch = chaos.maybe_append_late(rng, batch, state.history)

            delivery_id = _new_delivery_id()
            payload = {"delivery_id": delivery_id, "readings": [
                {**r, "received_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
                for r in batch
            ]}

            # Optional duplicate: send the same delivery_id twice.
            attempts = 0
            backoff = INITIAL_BACKOFF_S
            while True:
                attempts += 1
                status = await _post_once(client, sub.webhook_url, payload)
                if status == 200:
                    sub.deliveries_sent += 1
                    sub.deliveries_acked += 1
                    state.deliveries_total += 1
                    if chaos.should_duplicate(rng):
                        # Fire-and-forget the duplicate (don't retry it).
                        await _post_once(client, sub.webhook_url, payload)
                    break
                sub.deliveries_retrying += 1
                state.retries_total += 1
                jitter = 1.0 + (rng.random() - 0.5) * 0.4
                await asyncio.sleep(min(backoff * jitter, MAX_BACKOFF_S))
                backoff = min(backoff * 2, MAX_BACKOFF_S)

            sub.last_acked_slice = slice_["slice_end"]
