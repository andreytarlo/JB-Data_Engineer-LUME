-- Lume Control Room — Database Schema
-- Runs automatically on first PostgreSQL start (mounted in /docker-entrypoint-initdb.d/).
-- Idempotent: all objects use IF NOT EXISTS.

-- ─── Dimension tables (loaded once by load-dimensions) ──────────────────────

CREATE TABLE IF NOT EXISTS homes (
    household_id   VARCHAR          PRIMARY KEY,
    meter_id       VARCHAR          NOT NULL,
    postcode_area  VARCHAR,
    household_size INTEGER,
    acorn_group    VARCHAR,
    tariff_type    VARCHAR
);

-- Every cross-table join goes through meter_id; needs a unique index.
CREATE UNIQUE INDEX IF NOT EXISTS homes_meter_id_idx ON homes (meter_id);

CREATE TABLE IF NOT EXISTS weather_hourly (
    observed_at  TIMESTAMPTZ      PRIMARY KEY,
    station      VARCHAR          NOT NULL DEFAULT 'EGLL-Heathrow',
    temp_c       DOUBLE PRECISION,
    report_type  VARCHAR
);

-- ─── Core readings table ─────────────────────────────────────────────────────

-- Range-partitioned by reading_time (monthly). Each write touches only the
-- current partition's small B-tree instead of one ever-growing index over
-- 13M+ rows, which is the single biggest write bottleneck at high replay rates.
-- reading_time is part of the PK, as required for a partition key. INSERT routes
-- rows to the right partition automatically; ON CONFLICT (meter_id,
-- reading_time) still works because the conflict target is the PK.
CREATE TABLE IF NOT EXISTS clean_readings (
    meter_id     VARCHAR          NOT NULL,
    reading_time TIMESTAMPTZ      NOT NULL,
    household_id VARCHAR,
    kwh          DOUBLE PRECISION NOT NULL,
    received_at  TIMESTAMPTZ,
    ingested_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (meter_id, reading_time)
) PARTITION BY RANGE (reading_time);

-- Monthly partitions covering the replay window (preprocess keeps
-- [REPLAY_WINDOW_START - 30 days, END]; default window ≈ 2013-11 .. 2014-02).
CREATE TABLE IF NOT EXISTS clean_readings_2013_10 PARTITION OF clean_readings
    FOR VALUES FROM ('2013-10-01+00') TO ('2013-11-01+00');
CREATE TABLE IF NOT EXISTS clean_readings_2013_11 PARTITION OF clean_readings
    FOR VALUES FROM ('2013-11-01+00') TO ('2013-12-01+00');
CREATE TABLE IF NOT EXISTS clean_readings_2013_12 PARTITION OF clean_readings
    FOR VALUES FROM ('2013-12-01+00') TO ('2014-01-01+00');
CREATE TABLE IF NOT EXISTS clean_readings_2014_01 PARTITION OF clean_readings
    FOR VALUES FROM ('2014-01-01+00') TO ('2014-02-01+00');
CREATE TABLE IF NOT EXISTS clean_readings_2014_02 PARTITION OF clean_readings
    FOR VALUES FROM ('2014-02-01+00') TO ('2014-03-01+00');
CREATE TABLE IF NOT EXISTS clean_readings_2014_03 PARTITION OF clean_readings
    FOR VALUES FROM ('2014-03-01+00') TO ('2014-04-01+00');
-- Safety net: anything outside the pre-created ranges (e.g. injected demo
-- readings stamped at wall-clock) still lands somewhere instead of erroring.
CREATE TABLE IF NOT EXISTS clean_readings_default PARTITION OF clean_readings DEFAULT;

-- Range queries on reading_time dominate all 6 defence queries.
-- On a partitioned parent this creates a matching local index on every partition.
CREATE INDEX IF NOT EXISTS cr_reading_time_idx ON clean_readings (reading_time);

-- ─── Correction tracking trigger ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS meter_corrections_log (
    id           BIGSERIAL        PRIMARY KEY,
    meter_id     VARCHAR          NOT NULL,
    reading_time TIMESTAMPTZ      NOT NULL,
    old_kwh      DOUBLE PRECISION,
    new_kwh      DOUBLE PRECISION NOT NULL,
    delta        DOUBLE PRECISION GENERATED ALWAYS AS (new_kwh - old_kwh) STORED,
    corrected_at TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mcl_meter_time_idx ON meter_corrections_log (meter_id, reading_time);

-- Fires on every UPDATE to clean_readings; records old→new kwh automatically.
CREATE OR REPLACE FUNCTION _log_kwh_correction() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.kwh IS DISTINCT FROM OLD.kwh THEN
        INSERT INTO meter_corrections_log (meter_id, reading_time, old_kwh, new_kwh)
        VALUES (NEW.meter_id, NEW.reading_time, OLD.kwh, NEW.kwh);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS clean_readings_correction ON clean_readings;
CREATE TRIGGER clean_readings_correction
    AFTER UPDATE ON clean_readings
    FOR EACH ROW EXECUTE FUNCTION _log_kwh_correction();

-- ─── Rejected readings (7-day rolling window) ────────────────────────────────

CREATE TABLE IF NOT EXISTS rejected_readings (
    id               BIGSERIAL        PRIMARY KEY,
    delivery_id      VARCHAR,
    meter_id         VARCHAR,
    reading_time     TIMESTAMPTZ,
    kwh              DOUBLE PRECISION,
    received_at      TIMESTAMPTZ,
    rejection_type   VARCHAR          NOT NULL,
    rejection_detail VARCHAR,
    source           VARCHAR,
    rejected_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    -- Set TRUE once the daily archive job has folded this row into
    -- rejection_hourly_summary, so cumulative summation counts it exactly once.
    archived         BOOLEAN          NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS rr_received_at_idx  ON rejected_readings (received_at);
CREATE INDEX IF NOT EXISTS rr_meter_type_idx   ON rejected_readings (meter_id, rejection_type);
-- Partial index: the archive job only ever scans rows not yet archived.
CREATE INDEX IF NOT EXISTS rr_unarchived_idx   ON rejected_readings (rejected_at) WHERE NOT archived;

-- ─── Operational tables ──────────────────────────────────────────────────────

-- One row per accepted delivery; monitors feed health.
CREATE TABLE IF NOT EXISTS batch_log (
    delivery_id   VARCHAR      PRIMARY KEY,
    source        VARCHAR,
    received_at   TIMESTAMPTZ,
    reading_count INTEGER,
    status        VARCHAR,
    logged_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- One row per duplicate delivery detected by Redis dedup (Layer 3).
CREATE TABLE IF NOT EXISTS batch_duplicates (
    delivery_id   VARCHAR      PRIMARY KEY,
    detected_at   TIMESTAMPTZ  NOT NULL,
    source        VARCHAR,
    reading_count INTEGER
);

-- ─── Analytics tables ─────────────────────────────────────────────────────────

-- One row per silent meter detected by the scheduler (Layer 5).
CREATE TABLE IF NOT EXISTS meter_silence_log (
    id            BIGSERIAL        PRIMARY KEY,
    meter_id      VARCHAR          NOT NULL,
    postcode_area VARCHAR,
    last_seen     TIMESTAMPTZ,
    silent_hours  DOUBLE PRECISION,
    detected_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- One row per orphan meter (meter in readings but not in homes); upserted daily.
CREATE TABLE IF NOT EXISTS orphan_meters_summary (
    meter_id      VARCHAR      PRIMARY KEY,
    first_seen    TIMESTAMPTZ  NOT NULL,
    last_seen     TIMESTAMPTZ  NOT NULL,
    reading_count INTEGER      NOT NULL DEFAULT 0,
    total_kwh     DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- One row per meter; running avg+max transmission lag.
CREATE TABLE IF NOT EXISTS meter_lag_stats (
    meter_id    VARCHAR      PRIMARY KEY,
    avg_lag_sec DOUBLE PRECISION,
    p95_lag_sec DOUBLE PRECISION,
    max_lag_sec DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Hourly rollup of rejected counts (kept forever; ~4 MB/year).
-- Source of truth for quality trends after rejected_readings rows age out.
CREATE TABLE IF NOT EXISTS rejection_hourly_summary (
    ts_hour          TIMESTAMPTZ NOT NULL,
    rejection_reason VARCHAR     NOT NULL,
    count            INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (ts_hour, rejection_reason)
);
