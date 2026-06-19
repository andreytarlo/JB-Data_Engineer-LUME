"""FastAPI app for the meter-vendor mock.

Routes:
  GET  /healthz
  GET  /stats
  GET  /readings
  POST /subscriptions
  DELETE /subscriptions/{id}
  POST /subscriptions/{id}/ack
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, HttpUrl

from . import chaos
from . import data as data_mod
from . import delivery as delivery_mod
from . import replay as replay_mod


DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
SUBS_PATH = DATA_DIR / "_subs.json"


# ───────────────────────── Models ───────────────────────────────────────────

class SubscribeRequest(BaseModel):
    webhook_url: HttpUrl


class SubscribeResponse(BaseModel):
    subscription_id: str


class AckRequest(BaseModel):
    delivery_id: str


# ───────────────────────── App state ────────────────────────────────────────

state: replay_mod.ReplayState = replay_mod.ReplayState()
worker_tasks: dict[str, asyncio.Task] = {}
outage_windows = chaos.parse_outage_schedule(os.environ.get("VENDOR_OUTAGE_SCHEDULE"))


def _persist_subs() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SUBS_PATH.write_text(json.dumps(
            {sid: {"webhook_url": s.webhook_url} for sid, s in state.subscribers.items()},
            indent=2,
        ))
    except Exception as e:
        print(f"[main] failed to persist subs: {e}", flush=True)


def _load_subs() -> dict[str, str]:
    if not SUBS_PATH.exists():
        return {}
    try:
        return {sid: row["webhook_url"] for sid, row in json.loads(SUBS_PATH.read_text()).items()}
    except Exception:
        return {}


def _spawn_worker(sub: replay_mod.Subscriber) -> None:
    task = asyncio.create_task(delivery_mod.deliver_subscriber(state, sub, outage_windows))
    worker_tasks[sub.sub_id] = task


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Restore clock + subscribers if present.
    persisted = replay_mod.load_persisted_clock()
    if persisted:
        state.restore_clock(persisted)

    for sid, url in _load_subs().items():
        sub = replay_mod.Subscriber(sub_id=sid, webhook_url=url)
        state.subscribers[sid] = sub
        _spawn_worker(sub)

    # Start the replay loop + persistence loop.
    replay_task = asyncio.create_task(replay_mod.replay_loop(state))
    persist_task = asyncio.create_task(replay_mod._persist_loop(state))
    try:
        yield
    finally:
        replay_task.cancel()
        persist_task.cancel()
        for t in worker_tasks.values():
            t.cancel()


app = FastAPI(title="Lume meter-vendor mock", version="0.1.0", lifespan=lifespan)


# ───────────────────────── Routes ───────────────────────────────────────────

@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "ready": True,
        "simulated_now": state.simulated_now.isoformat().replace("+00:00", "Z"),
        "subscriptions": len(state.subscribers),
    }


@app.get("/stats")
def stats() -> dict[str, Any]:
    return {
        "simulated_now": state.simulated_now.isoformat().replace("+00:00", "Z"),
        "deliveries_total": state.deliveries_total,
        "retries_total": state.retries_total,
        "subscribers": {
            sid: {
                "webhook_url": s.webhook_url,
                "deliveries_sent": s.deliveries_sent,
                "deliveries_acked": s.deliveries_acked,
                "deliveries_retrying": s.deliveries_retrying,
                "last_acked_slice": s.last_acked_slice.isoformat().replace("+00:00", "Z") if s.last_acked_slice else None,
                "queue_depth": s.queue.qsize(),
            }
            for sid, s in state.subscribers.items()
        },
        "chaos": {
            "duplicate_rate": chaos.DUPLICATE_RATE,
            "reorder_rate": chaos.REORDER_RATE,
            "late_rate": chaos.LATE_RATE,
            "outage_windows": [f"{a[0]:02d}:{a[1]:02d}-{b[0]:02d}:{b[1]:02d}" for a, b in outage_windows],
        },
    }


@app.get("/readings")
def get_readings(
    since: str = Query(..., description="ISO-8601 lower bound on reading_time (UTC)"),
    cursor: str | None = Query(None, description="Opaque pagination cursor from prior response"),
    limit: int = Query(1000, ge=1, le=10_000),
) -> dict[str, Any]:
    try:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if since_dt.tzinfo is None:
            since_dt = since_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="`since` must be ISO-8601")

    rows, next_cursor = data_mod.iter_readings_paginated(since_dt, cursor, limit)
    # Synthesise a stable delivery_id per page for at-least-once semantics on
    # the archive route too (matches the live webhook contract).
    delivery_id = "arch_" + uuid.uuid4().hex[:24]
    for r in rows:
        r.setdefault("received_at", r["reading_time"])
        r["delivery_id"] = delivery_id
    return {"readings": rows, "next_cursor": next_cursor}


@app.post("/subscriptions", response_model=SubscribeResponse)
async def subscribe(req: SubscribeRequest) -> SubscribeResponse:
    # async so `asyncio.create_task` inside `_spawn_worker` sees the running loop.
    sid = "sub_" + uuid.uuid4().hex[:20]
    sub = replay_mod.Subscriber(sub_id=sid, webhook_url=str(req.webhook_url))
    state.subscribers[sid] = sub
    _spawn_worker(sub)
    _persist_subs()
    return SubscribeResponse(subscription_id=sid)


@app.delete("/subscriptions/{sub_id}")
async def unsubscribe(sub_id: str) -> dict[str, Any]:
    sub = state.subscribers.pop(sub_id, None)
    if not sub:
        raise HTTPException(status_code=404, detail="unknown subscription_id")
    task = worker_tasks.pop(sub_id, None)
    if task:
        task.cancel()
    _persist_subs()
    return {"ok": True, "subscription_id": sub_id}


@app.post("/subscriptions/{sub_id}/ack")
async def ack(sub_id: str, req: AckRequest) -> dict[str, Any]:
    # 200 from webhook is already treated as ack; this route is a no-op echo
    # for subscribers that want an explicit ack channel.
    if sub_id not in state.subscribers:
        raise HTTPException(status_code=404, detail="unknown subscription_id")
    return {"ok": True, "delivery_id": req.delivery_id}
