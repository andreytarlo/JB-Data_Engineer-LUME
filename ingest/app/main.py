"""Layer 1 — Ingest Gateway (שגשר הכניסה).

Sole responsibility: receive a webhook delivery from the meter vendor,
stamp it with the wall-clock arrival time (ingest_time), forward the raw
batch to Kafka topic "readings-raw" on partition 0 (live stream), and
return HTTP 200 to the vendor before any heavy processing begins.

What this service deliberately does NOT do:
  - validate kWh values
  - detect duplicate delivery_ids
  - write to any database
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, Response
from kafka import KafkaProducer

log = logging.getLogger("ingest")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

VENDOR_URL = os.environ.get("VENDOR_URL", "http://meter-vendor:8000")
INGEST_WEBHOOK_URL = os.environ.get("INGEST_WEBHOOK_URL", "http://ingest-gateway:8080/ingest")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

KAFKA_TOPIC = "readings-raw"
LIVE_PARTITION = 0  # partition 0 = live stream; partition 1 = backfill (Layer 2 concern)

_producer: KafkaProducer | None = None
_subscription_id: str | None = None
# FIX 3: track registration failure so healthz can report not-ready and
# trigger a Docker restart via the compose health check.
_registration_failed: bool = False


async def _subscribe_with_retry(client: httpx.AsyncClient, attempts: int = 10) -> str | None:
    """Register this gateway as a webhook subscriber; retry on transient failures."""
    for i in range(attempts):
        try:
            r = await client.post(
                f"{VENDOR_URL}/subscriptions",
                json={"webhook_url": INGEST_WEBHOOK_URL},
                timeout=10.0,
            )
            r.raise_for_status()
            sid = r.json()["subscription_id"]
            log.info("Subscribed to vendor webhook, subscription_id=%s", sid)
            return sid
        except Exception as exc:
            log.warning("Subscribe attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                await asyncio.sleep(3)
    return None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _producer, _subscription_id, _registration_failed

    # FIX 2: wrap producer creation so a Kafka outage at startup is reflected
    # in healthz (returns 503) rather than crashing the container silently.
    try:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,           # Leader acknowledges — durable enough, fast enough.
            linger_ms=5,      # Micro-batch to reduce per-message round-trips.
            compression_type="lz4",  # FIX 1: lz4 is 3× faster than gzip at similar ratio.
            retries=5,
        )
        log.info("Kafka producer initialised, bootstrap=%s", KAFKA_BOOTSTRAP)
    except Exception as exc:
        log.error("Kafka producer failed to initialise: %s", exc)
        yield  # start the app so healthz is reachable, but it will return 503
        return

    # Register with the meter vendor so it starts pushing batches here.
    async with httpx.AsyncClient() as client:
        _subscription_id = await _subscribe_with_retry(client)

    # FIX 3: if all registration attempts failed the vendor will never push
    # to us.  Mark the service unhealthy so Docker restarts it.
    if _subscription_id is None:
        _registration_failed = True
        log.error(
            "Vendor registration failed after all retries — "
            "reporting unhealthy so Docker will restart this container"
        )

    try:
        yield
    finally:
        # Graceful shutdown: tell the vendor to stop sending, then drain Kafka.
        if _subscription_id:
            async with httpx.AsyncClient() as client:
                try:
                    await client.delete(
                        f"{VENDOR_URL}/subscriptions/{_subscription_id}",
                        timeout=5.0,
                    )
                    log.info("Unsubscribed from vendor")
                except Exception as exc:
                    log.warning("Could not unsubscribe: %s", exc)

        if _producer:
            _producer.flush(timeout=10)
            _producer.close()
            log.info("Kafka producer closed")


app = FastAPI(title="Lume ingest gateway", version="0.1.0", lifespan=lifespan)


@app.post("/ingest")
async def ingest(request: Request) -> Response:
    """Receive one batch from the meter vendor and forward it to Kafka.

    The 200 response is returned as soon as the message is enqueued in the
    producer's internal buffer — before the network round-trip to Kafka
    completes.  This satisfies the vendor's 10-second ack requirement with
    a large margin while keeping the gateway stateless.
    """
    body = await request.json()

    # Add the gateway's wall-clock receipt time.  This is distinct from
    # received_at on each reading (when the vendor collected it) and is used
    # to measure feed lag and detect outages.
    body["ingest_time"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Non-blocking enqueue.  The KafkaProducer's background sender thread
    # handles batching, compression, and retries without blocking this coroutine.
    _producer.send(KAFKA_TOPIC, value=body, partition=LIVE_PARTITION)

    return Response(status_code=200)


@app.get("/healthz")
def healthz() -> Response:
    # FIX 2+3: return 503 if Kafka never connected or if vendor registration
    # failed — curl -f in the compose healthcheck will then fail, making
    # Docker mark this container unhealthy and restart it.
    ready = _producer is not None and not _registration_failed
    body = json.dumps({
        "ready": ready,
        "subscription_id": _subscription_id,
        "kafka_bootstrap": KAFKA_BOOTSTRAP,
    })
    return Response(content=body, status_code=200 if ready else 503, media_type="application/json")
