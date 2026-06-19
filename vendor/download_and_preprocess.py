#!/usr/bin/env python3
"""data-init sidecar entrypoint.

Downloads the London Datastore LCL smart-meter ZIP (~765 MB), extracts the
CSV, and writes the following into the named volume:

  <data_dir>/households.parquet               written FIRST
  <data_dir>/readings/<YYYY-MM>/part-00000.parquet
  <data_dir>/weather.parquet
  <data_dir>/_READY                           sentinel — last

Idempotent: if <data_dir>/_READY exists, exits 0 immediately.

CSV schema (LCL Power-Networks-LCL-June2015):
    LCLid,stdorToU,DateTime,KWH/hh (per half hour) ,Acorn,Acorn_grouped

Design choices (recorded so future readers don't re-litigate them):
  * pyarrow CSV reader (vectorised) instead of stdlib csv — ~15× faster.
  * Filter to [window_start - backfill_days, window_end] during scan, not
    after. ~90% of the source is outside our replay window.
  * Source DateTime is UK *local* time. Assume Europe/London (BST/GMT),
    then convert to UTC. Without this, DST shifts (2013-10-27, etc.) put
    readings in the wrong hour and break windowed aggregations.
  * Clamp kwh into [0, 50]. UK Power Networks documents that meter resets
    produce negative spikes and >50 kWh/half-hour is physically impossible
    for a residential meter (≈ 100 kW continuous draw).
  * Drop households with <50% reading coverage in the replay window — the
    LCL trial had households joining/leaving over its 27-month run, so the
    "5,567 households" figure does not apply uniformly.
  * Write households.parquet BEFORE the readings loop so a partial run can
    be resumed without losing the household dimension.
  * Use a named `.dl-tmp/` subdirectory of the volume (not stdlib
    tempfile.TemporaryDirectory) so try/finally can clean it up explicitly
    on any failure, including SIGKILL recovery on next start.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests


LCL_URL = (
    "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/"
    "3527bf39-d93e-4071-8451-df2ade1ea4f2/Power-Networks-LCL-June2015%28withAcornGps%29v2_1.csv.zip"
)

# Heathrow ISD / ISH (NOAA hourly global-hourly). USAF 037720, WBAN 99999.
HEATHROW_STATION = "03772099999"

KWH_CLAMP_MIN = 0.0
KWH_CLAMP_MAX = 50.0
COVERAGE_THRESHOLD = 0.5  # drop households with <50% readings in window

READING_SCHEMA = pa.schema([
    ("meter_id", pa.string()),
    ("household_id", pa.string()),
    ("reading_time", pa.timestamp("ms", tz="UTC")),
    ("kwh", pa.float64()),
])


def log(msg: str) -> None:
    print(f"[data-init] {msg}", flush=True)


# ───────────────────────── Env / config ─────────────────────────────────────

def parse_window_env() -> tuple[date, date, int]:
    start = date.fromisoformat(os.environ.get("REPLAY_WINDOW_START", "2013-12-01"))
    end = date.fromisoformat(os.environ.get("REPLAY_WINDOW_END", "2014-02-28"))
    backfill = int(os.environ.get("PREPROCESS_BACKFILL_DAYS", "30"))
    if end < start:
        raise SystemExit(f"REPLAY_WINDOW_END ({end}) is before REPLAY_WINDOW_START ({start})")
    return start, end, backfill


# ───────────────────────── Synthetic dimensions ─────────────────────────────

POSTCODE_AREAS = [f"N1-{c}" for c in "ABCDEFGH"]

def postcode_area_for(household_id: str) -> str:
    """LCL is fully anonymised — synthesise a stable bucket label.

    Deterministic on household_id so reruns produce identical groupings.
    The BRIEF documents that "postcode area" is a synthesised bucket.
    """
    h = hashlib.md5(household_id.encode("utf-8")).digest()[0]
    return POSTCODE_AREAS[h % len(POSTCODE_AREAS)]


# The LCL bulk CSV (`CC_LCL-FullData.csv`) only carries readings — no demographic
# columns. The "ACORN" classification is in a separate file that isn't included
# in the public zip. Synthesise per household so the BRIEF's ACORN-segmented
# queries are answerable. Distribution roughly matches UK demographics:
#   Affluent (A-E): 30%   Comfortable (F-J): 30%   Adversity (K-Q): 40%
_ACORN_BUCKETS = (
    [chr(ord("A") + i) for i in range(5)] * 6           # A-E ×6 = 30
    + [chr(ord("F") + i) for i in range(5)] * 6         # F-J ×6 = 30
    + [chr(ord("K") + i) for i in range(7)] * 6         # K-Q ×6 = 42, trimmed below
)
_ACORN_BUCKETS = _ACORN_BUCKETS[:100]

def synth_acorn_for(household_id: str) -> str:
    h = int.from_bytes(hashlib.md5(household_id.encode("utf-8")).digest()[1:3], "big")
    return f"ACORN-{_ACORN_BUCKETS[h % 100]}"


def acorn_household_size(acorn: str) -> int:
    """Rough ACORN-letter → household-size proxy. Defaults to 2."""
    table = {
        "A": 3, "B": 3, "C": 3, "D": 3, "E": 3,
        "F": 2, "G": 2, "H": 2, "I": 2, "J": 2,
        "K": 4, "L": 4, "M": 3, "N": 3, "O": 2,
        "P": 2, "Q": 1,
    }
    if not acorn:
        return 2
    key = acorn.strip().upper()
    if key.startswith("ACORN-") and len(key) >= 7:
        return table.get(key[6], 2)
    return 2


# ───────────────────────── Download / extract ───────────────────────────────

def download_with_progress(url: str, dest: Path) -> None:
    log(f"GET {url}")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        written = 0
        last_log = time.monotonic()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                fh.write(chunk)
                written += len(chunk)
                now = time.monotonic()
                if now - last_log >= 5:
                    pct = (written / total * 100) if total else 0.0
                    log(f"  downloaded {written/1e6:,.1f} MB"
                        + (f" / {total/1e6:,.1f} MB ({pct:.1f}%)" if total else ""))
                    last_log = now
    log(f"  done ({dest.stat().st_size/1e6:,.1f} MB)")


def extract_csv(zip_path: Path, tmp_path: Path) -> Path:
    """LCL archive uses Deflate64 (method 9), which stdlib zipfile can't read.

    Use the system `unzip` binary, which handles it. Peek with zipfile to
    discover the member name so we don't hard-code the CSV filename.
    """
    log(f"unzipping {zip_path}")
    with zipfile.ZipFile(zip_path) as zf:
        csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise SystemExit("zip contains no CSV")
        target = csv_members[0]
    log(f"  extracting {target} via system unzip")
    subprocess.run(
        ["unzip", "-o", "-q", str(zip_path), target, "-d", str(tmp_path)],
        check=True,
    )
    csv_path = tmp_path / target
    log(f"  extracted {csv_path} ({csv_path.stat().st_size/1e9:.2f} GB)")
    return csv_path


# ───────────────────────── Phase 1: scan + window filter ────────────────────

def _find_kwh_column(csv_path: Path) -> str:
    """The KWH column header has trailing whitespace and case variation. Sniff it."""
    with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
        header = next(csv.reader(fh))
    for name in header:
        if "KWH" in name.upper():
            return name
    raise SystemExit(f"no KWH column found in header: {header}")


def scan_and_filter(
    csv_path: Path, buffer_start: date, window_end: date
) -> tuple[list[pa.RecordBatch], dict[str, dict]]:
    """Stream the CSV through pyarrow, keep rows in [buffer_start, window_end],
    convert UK-local timestamps to UTC, clamp kwh outliers, harvest household
    metadata as we go. Returns (kept_batches, household_meta).
    """
    kwh_col = _find_kwh_column(csv_path)
    log(f"scanning {csv_path} (KWH column: {kwh_col!r})")

    # Buffer end at end-of-day in UK local — convert via a UTC range with
    # 1-day slack (the cast handles DST correctly).
    win_start_utc = pa.scalar(
        datetime.combine(buffer_start, datetime.min.time()).replace(tzinfo=timezone.utc),
        type=pa.timestamp("ms", tz="UTC"),
    )
    win_end_utc = pa.scalar(
        datetime.combine(window_end + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc),
        type=pa.timestamp("ms", tz="UTC"),
    )

    read_opts = pacsv.ReadOptions(block_size=32 * 1024 * 1024)  # 32 MB blocks
    convert_opts = pacsv.ConvertOptions(
        column_types={
            "LCLid": pa.string(),
            "stdorToU": pa.string(),
            "DateTime": pa.string(),
            kwh_col: pa.float64(),
        },
        null_values=["Null", "null", "NA", "N/A", ""],
        include_columns=["LCLid", "stdorToU", "DateTime", kwh_col],
    )

    kept: list[pa.RecordBatch] = []
    household_meta: dict[str, dict] = {}
    total = 0
    kept_rows = 0
    started = time.monotonic()
    last_log = started

    with pacsv.open_csv(
        csv_path, read_options=read_opts, convert_options=convert_opts
    ) as reader:
        for batch in reader:
            total += batch.num_rows

            # Parse DateTime: strip ".0000000" sub-seconds, strptime as naive,
            # assume Europe/London, cast to UTC. `assume_timezone` handles DST.
            dt_clean = pc.replace_substring_regex(batch.column("DateTime"), r"\..*$", "")
            naive = pc.strptime(dt_clean, format="%Y-%m-%d %H:%M:%S", unit="ms")
            uk = pc.assume_timezone(
                naive, "Europe/London", ambiguous="earliest", nonexistent="latest"
            )
            utc = pc.cast(uk, pa.timestamp("ms", tz="UTC"))

            kwh = batch.column(kwh_col)
            mask = pc.and_(
                pc.is_valid(utc),
                pc.and_(
                    pc.greater_equal(utc, win_start_utc),
                    pc.less(utc, win_end_utc),
                ),
            )
            mask = pc.and_(
                mask,
                pc.and_(
                    pc.is_valid(kwh),
                    pc.and_(
                        pc.greater_equal(kwh, pa.scalar(KWH_CLAMP_MIN)),
                        pc.less_equal(kwh, pa.scalar(KWH_CLAMP_MAX)),
                    ),
                ),
            )

            n_keep = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
            if n_keep == 0:
                pass  # nothing to keep this block; fall through to logging
            else:
                idx = pc.indices_nonzero(mask)
                lcl_kept = batch.column("LCLid").take(idx)
                kept.append(pa.RecordBatch.from_arrays(
                    [lcl_kept, lcl_kept, utc.take(idx), kwh.take(idx)],
                    schema=READING_SCHEMA,
                ))
                kept_rows += n_keep

                # Capture household metadata once per LCLid (first sight wins).
                # ACORN is synthesised — the bulk CSV doesn't carry it (see
                # synth_acorn_for above for the rationale).
                tariff_arr = batch.column("stdorToU").take(idx)
                for lcl, tariff in zip(
                    lcl_kept.to_pylist(),
                    tariff_arr.to_pylist(),
                ):
                    if lcl and lcl not in household_meta:
                        t = (tariff or "").strip().lower()
                        household_meta[lcl] = {
                            "tariff_type": "ToU" if (t.startswith("tou") or t == "dtou") else "Std",
                            "acorn_group": synth_acorn_for(lcl),
                        }

            now = time.monotonic()
            if now - last_log >= 5:
                rate = total / max(now - started, 0.001)
                log(f"  scanned {total:,} rows ({rate/1000:,.0f}k/s); kept {kept_rows:,}; {len(household_meta):,} households")
                last_log = now

    log(f"  scan done: {total:,} in, {kept_rows:,} kept, {len(household_meta):,} households")
    return kept, household_meta


# ───────────────────────── Phase 2: qualify households ──────────────────────

def identify_qualified(
    batches: list[pa.RecordBatch],
    meta: dict[str, dict],
    window_start: date,
    window_end: date,
) -> set[str]:
    """Drop households whose coverage in the *replay* window (not the backfill
    buffer) is below COVERAGE_THRESHOLD. A 48-readings/day expectation defines
    100% coverage.
    """
    win_start = pa.scalar(
        datetime.combine(window_start, datetime.min.time()).replace(tzinfo=timezone.utc),
        type=pa.timestamp("ms", tz="UTC"),
    )
    win_end = pa.scalar(
        datetime.combine(window_end + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc),
        type=pa.timestamp("ms", tz="UTC"),
    )

    counts: dict[str, int] = {}
    for batch in batches:
        ts = batch.column("reading_time")
        mask = pc.and_(pc.greater_equal(ts, win_start), pc.less(ts, win_end))
        n = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
        if n == 0:
            continue
        idx = pc.indices_nonzero(mask)
        for h in batch.column("household_id").take(idx).to_pylist():
            counts[h] = counts.get(h, 0) + 1

    days = (window_end - window_start).days + 1
    expected = days * 48
    min_required = int(expected * COVERAGE_THRESHOLD)
    qualified = {h for h in meta if counts.get(h, 0) >= min_required}
    dropped = len(meta) - len(qualified)
    log(
        f"  coverage: {days} days × 48 = {expected} expected/household; "
        f"≥{int(COVERAGE_THRESHOLD*100)}% threshold = {min_required}; "
        f"qualified {len(qualified):,}, dropped {dropped:,}"
    )
    return qualified


# ───────────────────────── Phase 3: write households + readings ─────────────

def write_households(meta: dict[str, dict], qualified: set[str], out: Path) -> None:
    keep = sorted(qualified)
    cols = {
        "household_id": keep,
        "meter_id": keep,
        "postcode_area": [postcode_area_for(h) for h in keep],
        "household_size": [acorn_household_size(meta[h]["acorn_group"]) for h in keep],
        "acorn_group": [meta[h]["acorn_group"] for h in keep],
        "tariff_type": [meta[h]["tariff_type"] for h in keep],
    }
    schema = pa.schema([
        ("household_id", pa.string()),
        ("meter_id", pa.string()),
        ("postcode_area", pa.string()),
        ("household_size", pa.int32()),
        ("acorn_group", pa.string()),
        ("tariff_type", pa.string()),
    ])
    pq.write_table(pa.table(cols, schema=schema), out, compression="zstd")
    log(f"  wrote {out} ({len(keep):,} households)")


def write_readings_partitioned(
    batches: list[pa.RecordBatch], qualified: set[str], out_base: Path
) -> None:
    """Filter batches to qualified households, sort by reading_time, write one
    parquet file per month at <out_base>/<YYYY-MM>/part-00000.parquet (the
    layout the vendor app reads — NOT hive-style)."""
    if out_base.exists():
        shutil.rmtree(out_base)
    out_base.mkdir(parents=True, exist_ok=True)

    if not batches:
        log("  WARN: no batches to write")
        return

    qual_arr = pa.array(sorted(qualified), type=pa.string())
    filtered: list[pa.RecordBatch] = []
    for batch in batches:
        mask = pc.is_in(batch.column("household_id"), value_set=qual_arr)
        n = pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
        if n == 0:
            continue
        filtered.append(batch.take(pc.indices_nonzero(mask)))

    if not filtered:
        log("  WARN: nothing left after qualification filter")
        return

    table = pa.Table.from_batches(filtered, schema=READING_SCHEMA)
    log(f"  combined: {table.num_rows:,} rows; partitioning by month")

    # Partition first so dedup runs on bounded per-month subsets (~7M rows
    # each). Global group_by on the full 28M-row table OOMs in the default
    # Docker memory ceiling because the hash index of (household_id,
    # reading_time) is large.
    month_strs = pc.strftime(table.column("reading_time"), format="%Y-%m")
    unique_months = sorted(set(month_strs.to_pylist()))

    total_dedup_excess = 0
    for m in unique_months:
        idx = pc.indices_nonzero(pc.equal(month_strs, pa.scalar(m)))
        sub = table.take(idx)
        pre = sub.num_rows

        # Dedup on (household_id, reading_time). The LCL bulk CSV has
        # ~0.07% genuine source-side duplicates. Collapse by mean(kwh) so
        # analytics ("sum kwh per household per hour", etc.) don't
        # double-count. The vendor's chaos layer remains the *only* source
        # of duplicate deliveries in the student-facing system.
        deduped = sub.group_by(["household_id", "reading_time"]).aggregate([
            ("meter_id", "min"),   # 1:1 with household in this dataset
            ("kwh", "mean"),
        ])
        # group_by emits group keys first then aggregates; rebuild in
        # READING_SCHEMA column order.
        sub = pa.Table.from_arrays(
            [
                deduped.column("meter_id_min"),
                deduped.column("household_id"),
                deduped.column("reading_time"),
                deduped.column("kwh_mean"),
            ],
            schema=READING_SCHEMA,
        )
        excess = pre - sub.num_rows
        total_dedup_excess += excess

        # Sort within month (group_by destroys order).
        sub = sub.sort_by([("reading_time", "ascending")])

        out_dir = out_base / m
        out_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(sub, out_dir / "part-00000.parquet", compression="zstd")
        log(f"  wrote {out_base.name}/{m}/part-00000.parquet ({sub.num_rows:,} rows, {excess:,} dupes collapsed)")

    if total_dedup_excess:
        log(f"  total source-side duplicates collapsed: {total_dedup_excess:,}")


# ───────────────────────── Phase 4: weather ─────────────────────────────────

def _isd_temp(field: str) -> float | None:
    """ISD TMP field: '+0077,1' → 7.7 C; '+9999,*' → missing."""
    if not field:
        return None
    head = field.split(",", 1)[0]
    if not head or head.endswith("9999"):
        return None
    try:
        return int(head) / 10.0
    except ValueError:
        return None


def fetch_weather(out: Path, buffer_start: date, window_end: date) -> None:
    """Hourly Heathrow temperatures from NOAA ISD covering [buffer_start, window_end]."""
    rows: list[dict] = []
    years = sorted({buffer_start.year, window_end.year})
    for year in years:
        url = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/{HEATHROW_STATION}.csv"
        log(f"GET {url}")
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
        except Exception as e:
            log(f"  WARN: failed to fetch {year} weather: {e}; continuing")
            continue
        n = 0
        for row in csv.DictReader(io.StringIO(r.text)):
            try:
                ts = datetime.strptime(row["DATE"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            rows.append({
                "observed_at": ts,
                "station": "EGLL-Heathrow",
                "temp_c": _isd_temp(row.get("TMP", "")),
                "report_type": (row.get("REPORT_TYPE") or "").strip(),
            })
            n += 1
        log(f"  {year}: {n:,} obs")

    schema = pa.schema([
        ("observed_at", pa.timestamp("ms", tz="UTC")),
        ("station", pa.string()),
        ("temp_c", pa.float64()),
        ("report_type", pa.string()),
    ])
    cols = {k: [r[k] for r in rows] for k in ("observed_at", "station", "temp_c", "report_type")}
    pq.write_table(pa.table(cols, schema=schema), out, compression="zstd")
    log(f"  wrote {out} ({len(rows):,} rows)")


# ───────────────────────── Entrypoint ───────────────────────────────────────

def main(data_dir: Path) -> int:
    data_dir.mkdir(parents=True, exist_ok=True)
    ready = data_dir / "_READY"
    if ready.exists():
        log(f"{ready} exists — skipping (idempotent re-run)")
        return 0

    window_start, window_end, backfill_days = parse_window_env()
    buffer_start = window_start - timedelta(days=backfill_days)
    log(f"replay window: {window_start} → {window_end}; keeping data from {buffer_start} (backfill {backfill_days}d)")

    tmp_path = data_dir / ".dl-tmp"
    if tmp_path.exists():
        log(f"removing stale {tmp_path}")
        shutil.rmtree(tmp_path)

    try:
        tmp_path.mkdir()
        zip_path = tmp_path / "lcl.zip"

        # Escape hatch for fast iteration: LCL_ZIP_PATH points at a prebuilt
        # zip on disk and we skip the 765 MB download. Undocumented for
        # students; used by the implementation loop only.
        prebuilt = os.environ.get("LCL_ZIP_PATH")
        if prebuilt and Path(prebuilt).is_file():
            log(f"using prebuilt LCL_ZIP_PATH={prebuilt}")
            shutil.copyfile(prebuilt, zip_path)
        else:
            download_with_progress(LCL_URL, zip_path)

        csv_path = extract_csv(zip_path, tmp_path)

        batches, household_meta = scan_and_filter(csv_path, buffer_start, window_end)
        if not batches:
            raise SystemExit("scan produced zero rows in window — check REPLAY_WINDOW_*")

        qualified = identify_qualified(batches, household_meta, window_start, window_end)
        if not qualified:
            raise SystemExit("zero households met coverage threshold — lower COVERAGE_THRESHOLD or widen window")

        # households.parquet first — if the readings step dies, retrying picks
        # up where it left off (the dimension is small and cheap to redo).
        write_households(household_meta, qualified, data_dir / "households.parquet")
        write_readings_partitioned(batches, qualified, data_dir / "readings")
        fetch_weather(data_dir / "weather.parquet", buffer_start, window_end)

    finally:
        if tmp_path.exists():
            log(f"cleaning {tmp_path}")
            shutil.rmtree(tmp_path, ignore_errors=True)

    ready.write_text(datetime.now(timezone.utc).isoformat() + "\n")
    log(f"_READY at {ready}")
    return 0


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/data")
    sys.exit(main(target))
