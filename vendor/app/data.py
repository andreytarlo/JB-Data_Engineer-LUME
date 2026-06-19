"""Parquet readers for the meter-vendor.

The data-init sidecar emits month-partitioned reading parquet under
<DATA_DIR>/readings/<YYYY-MM>/part-*.parquet, plus households.parquet and
weather.parquet next to them. This module hides the pyarrow.dataset details
behind two iterators used by the FastAPI handlers and the replay loop.
"""
from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.dataset as ds


DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))


def readings_root() -> Path:
    return DATA_DIR / "readings"


def list_months() -> list[str]:
    root = readings_root()
    if not root.exists():
        return []
    return sorted([p.name for p in root.iterdir() if p.is_dir()])


def _month_files(month: str) -> list[Path]:
    d = readings_root() / month
    if not d.exists():
        return []
    return sorted(d.glob("part-*.parquet"))


# Per-month table cache. Each month parquet is ~7M rows / ~80MB uncompressed,
# so caching the configured 4-month window costs ~300MB peak — fine for a
# laptop demo and avoids re-reading the parquet for every 30-minute slice
# (which was capping replay at ~15s/day instead of the configured 3s/day).
_MONTH_CACHE: dict[str, pa.Table] = {}


def _read_month_table(month: str) -> pa.Table | None:
    cached = _MONTH_CACHE.get(month)
    if cached is not None:
        return cached
    files = _month_files(month)
    if not files:
        return None
    dataset = ds.dataset([str(p) for p in files], format="parquet")
    # Sort once at load — iter_readings_for_window then doesn't need to re-sort
    # on every slice call.
    table = dataset.to_table()
    table = table.take(
        pa.compute.sort_indices(table, sort_keys=[("reading_time", "ascending")])
    )
    _MONTH_CACHE[month] = table
    return table


@dataclass
class Cursor:
    month: str       # "YYYY-MM"
    offset: int      # row offset within the month's concatenated table

    def encode(self) -> str:
        return base64.urlsafe_b64encode(
            json.dumps({"m": self.month, "o": self.offset}).encode("utf-8")
        ).decode("ascii")

    @classmethod
    def decode(cls, raw: str) -> "Cursor":
        d = json.loads(base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8"))
        return cls(month=d["m"], offset=int(d["o"]))


def iter_readings_paginated(since: datetime, cursor: str | None, limit: int) -> tuple[list[dict], str | None]:
    """Return up to `limit` reading dicts at-or-after `since`, plus next_cursor.

    Cursor encodes (month, row_offset). On first call (no cursor) we seed from
    the first month at-or-after `since` and skip rows in that month before
    `since` via a filter. Within a month we paginate by offset.
    """
    months = list_months()
    if not months:
        return [], None

    if cursor:
        cur = Cursor.decode(cursor)
        if cur.month not in months:
            return [], None
        start_month_idx = months.index(cur.month)
        start_offset = cur.offset
    else:
        ts_key = f"{since.year:04d}-{since.month:02d}"
        # Skip months strictly before the `since` month.
        start_month_idx = 0
        for i, m in enumerate(months):
            if m >= ts_key:
                start_month_idx = i
                break
        else:
            return [], None
        start_offset = 0

    out: list[dict] = []
    remaining = limit
    next_cursor: str | None = None

    for i in range(start_month_idx, len(months)):
        month = months[i]
        table = _read_month_table(month)
        if table is None or table.num_rows == 0:
            continue

        # On the first month, filter rows before `since` (only on the very
        # first page — once we've handed out a cursor we trust the offset).
        if i == start_month_idx and not cursor:
            mask = pa.compute.greater_equal(table.column("reading_time"), pa.scalar(since, type=table.schema.field("reading_time").type))
            table = table.filter(mask)

        if i == start_month_idx and start_offset > 0:
            if start_offset >= table.num_rows:
                continue
            table = table.slice(start_offset)

        take = min(remaining, table.num_rows)
        slice_ = table.slice(0, take)
        rows = slice_.to_pylist()
        for r in rows:
            r["reading_time"] = r["reading_time"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        out.extend(rows)
        remaining -= take

        if remaining == 0:
            new_offset = (start_offset if i == start_month_idx else 0) + take
            # If we exhausted this month, advance cursor to next month / offset 0.
            full_rows = table.num_rows
            if take >= full_rows:
                if i + 1 < len(months):
                    next_cursor = Cursor(month=months[i + 1], offset=0).encode()
                else:
                    next_cursor = None
            else:
                next_cursor = Cursor(month=month, offset=new_offset).encode()
            break
        # Otherwise we've drained this month; loop into the next.

    return out, next_cursor


def iter_readings_for_window(start: datetime, end: datetime) -> Iterator[dict]:
    """Yield reading dicts with reading_time in [start, end) in time order.

    The implementation is vectorised end-to-end:
      * month tables are cached after first read (see _MONTH_CACHE)
      * filter is a single pyarrow boolean mask, no Python loop
      * reading_time is converted to ISO strings via pyarrow.compute.strftime
        (vectorised C kernel) — per-row Python isoformat() was the dominant
        cost in the replay loop and capped throughput at ~10s/sim-day.
    """
    months = list_months()
    start_key = f"{start.year:04d}-{start.month:02d}"
    end_key = f"{end.year:04d}-{end.month:02d}"
    for month in months:
        if month < start_key or month > end_key:
            continue
        table = _read_month_table(month)
        if table is None:
            continue
        col = table.column("reading_time")
        mask_a = pa.compute.greater_equal(col, pa.scalar(start, type=col.type))
        mask_b = pa.compute.less(col, pa.scalar(end, type=col.type))
        table = table.filter(pa.compute.and_(mask_a, mask_b))
        if table.num_rows == 0:
            continue
        # Vectorised ISO-8601 (UTC) conversion in C.
        iso_times = pa.compute.strftime(
            table.column("reading_time"), format="%Y-%m-%dT%H:%M:%SZ"
        ).to_pylist()
        meter_ids = table.column("meter_id").to_pylist()
        household_ids = table.column("household_id").to_pylist()
        kwhs = table.column("kwh").to_pylist()
        for m, h, t, k in zip(meter_ids, household_ids, iso_times, kwhs):
            yield {"meter_id": m, "household_id": h, "reading_time": t, "kwh": k}
