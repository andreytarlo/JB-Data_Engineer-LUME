"""Layer 4 — PySpark Structured Streaming processor.

Reads from Kafka topic 'readings-raw' (both partitions) and for each
micro-batch executes the 8-step pipeline:

  1. Deserialise Kafka JSON messages into Python dicts.
  2. Redis dedup — skip duplicate delivery_ids (dedup.py).
  3. Flatten each payload's readings list into individual dicts.
  4. Validate kWh range [0, 50] (validate.py).
  5. Upsert clean readings to PostgreSQL (db_writer.py).
     The correction-tracking DB trigger fires automatically on UPDATE.
  6. Append rejected readings to PostgreSQL (db_writer.py).
  7. Update Redis window state — live readings only (redis_state.py).
  8. Commit Kafka offset (implicit via Spark checkpoint).

foreachBatch runs in the driver process, so psycopg and redis connections
are created once at startup and reused across batches.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

# Ensure the jobs/ directory is on the path so sibling modules resolve.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg
import redis as redis_lib
from pyspark.sql import SparkSession, DataFrame

from dedup        import process_batch_dedup
from validate     import validate_readings
from db_writer    import (
    upsert_clean_readings,
    insert_rejected_readings,
    log_batch,
    upsert_lag_stats,
)
from redis_state  import update_window_state

log = logging.getLogger("process")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# ─── Config ──────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = "readings-raw"
PG_DSN          = os.environ.get("POSTGRES_DSN", "postgresql://lume:lume@postgres:5432/lume")
REDIS_HOST      = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT      = int(os.environ.get("REDIS_PORT", "6379"))
CHECKPOINT_DIR  = os.environ.get("CHECKPOINT_DIR", "/checkpoint")
BATCH_INTERVAL  = os.environ.get("BATCH_INTERVAL_S", "5")


# ─── Connection helpers ───────────────────────────────────────────────────────

def _connect_pg() -> psycopg.Connection:
    for attempt in range(15):
        try:
            conn = psycopg.connect(PG_DSN, autocommit=False)
            log.info("PostgreSQL connected")
            return conn
        except Exception as exc:
            log.warning("PG not ready (%s) — retry %d/15", exc, attempt + 1)
            time.sleep(4)
    raise RuntimeError("Could not connect to PostgreSQL")


def _connect_redis() -> redis_lib.Redis | None:
    try:
        r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=5)
        r.ping()
        log.info("Redis connected at %s:%d", REDIS_HOST, REDIS_PORT)
        return r
    except Exception as exc:
        log.warning("Redis unavailable (%s) — will skip dedup + window state", exc)
        return None


# ─── Batch handler ────────────────────────────────────────────────────────────

def _make_handler(pg_conn: psycopg.Connection, redis_client):
    """Return a foreachBatch callback closed over the shared connections."""

    def handle_batch(batch_df: DataFrame, epoch_id: int) -> None:
        if batch_df.isEmpty():
            return

        # ── Step 1: Deserialise ───────────────────────────────────────────────
        payloads: list[dict] = []
        for row in batch_df.select("value").collect():
            try:
                payloads.append(json.loads(row["value"]))
            except Exception as exc:
                log.warning("epoch=%d  bad Kafka message: %s", epoch_id, exc)

        if not payloads:
            return

        log.info("epoch=%d  raw payloads=%d", epoch_id, len(payloads))

        # ── Step 2: Redis dedup ───────────────────────────────────────────────
        new_payloads, dup_count = process_batch_dedup(payloads, redis_client, pg_conn)
        if dup_count:
            log.info("epoch=%d  duplicates skipped=%d", epoch_id, dup_count)
        if not new_payloads:
            return

        # ── Step 3 + 4: Flatten and validate ─────────────────────────────────
        all_valid:    list[dict] = []
        all_rejected: list[dict] = []
        has_backfill: bool = any(p.get("source") == "backfill" for p in new_payloads)

        for payload in new_payloads:
            delivery_id = payload.get("delivery_id", "")
            source      = payload.get("source", "live")
            readings    = payload.get("readings", [])

            try:
                log_batch(pg_conn, delivery_id, source, len(readings), "accepted",
                          received_at=None)
            except Exception as exc:
                log.warning("epoch=%d  batch_log failed: %s", epoch_id, exc)
                try:
                    pg_conn.rollback()
                except Exception:
                    pass

            valid, rejected = validate_readings(readings, delivery_id, source)
            all_valid.extend(valid)
            all_rejected.extend(rejected)

        log.info("epoch=%d  valid=%d  rejected=%d", epoch_id, len(all_valid), len(all_rejected))

        # ── Step 5: Upsert clean readings ─────────────────────────────────────
        try:
            upsert_clean_readings(pg_conn, all_valid)
        except Exception as exc:
            log.error("epoch=%d  upsert_clean_readings failed: %s", epoch_id, exc)
            try:
                pg_conn.rollback()
            except Exception:
                pass

        # ── Step 6: Write rejected readings ──────────────────────────────────
        try:
            insert_rejected_readings(pg_conn, all_rejected)
        except Exception as exc:
            log.error("epoch=%d  insert_rejected failed: %s", epoch_id, exc)
            try:
                pg_conn.rollback()
            except Exception:
                pass

        # ── Lag stats (best-effort; non-blocking) ─────────────────────────────
        try:
            upsert_lag_stats(pg_conn, all_valid)
        except Exception as exc:
            log.warning("epoch=%d  upsert_lag_stats failed: %s", epoch_id, exc)
            try:
                pg_conn.rollback()
            except Exception:
                pass

        # ── Step 7: Redis window state ────────────────────────────────────────
        try:
            update_window_state(
                redis_client,
                all_valid,
                invalid_count=len(all_rejected),
                backfill_active=has_backfill,
            )
        except Exception as exc:
            log.warning("epoch=%d  redis_state failed: %s", epoch_id, exc)

    return handle_batch


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    pg_conn      = _connect_pg()
    redis_client = _connect_redis()

    spark = (
        SparkSession.builder
        .appName("lume-processor")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10_000)
        .option("kafka.group.id", "lume-processor")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Cast value bytes to string; keep partition for observability.
    from pyspark.sql.functions import col
    df = df.withColumn("value", col("value").cast("string"))

    query = (
        df.writeStream
        .foreachBatch(_make_handler(pg_conn, redis_client))
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=f"{BATCH_INTERVAL} seconds")
        .start()
    )

    log.info(
        "Streaming started — topic=%s  checkpoint=%s  interval=%ss",
        KAFKA_TOPIC, CHECKPOINT_DIR, BATCH_INTERVAL,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
