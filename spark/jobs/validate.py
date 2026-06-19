"""Layer 4 step — kWh range validation.

Splits a flat list of reading dicts into valid and rejected.

Rejection types:
  invalid_reading_time  reading_time absent or unparseable
  missing_kwh           kwh field absent or null
  kwh_out_of_range      kwh < 0 or kwh > 50 or NaN
"""
from __future__ import annotations

from datetime import datetime

KWH_MIN: float = 0.0
KWH_MAX: float = 50.0


def _parsable_ts(value) -> bool:
    """True if value is a usable timestamp (datetime or ISO-8601 string)."""
    if isinstance(value, datetime):
        return True
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
    return False


def validate_readings(
    readings: list[dict],
    delivery_id: str,
    source: str,
) -> tuple[list[dict], list[dict]]:
    """Return (valid_readings, rejected_readings).

    Rejected dicts carry extra keys: delivery_id, source,
    rejection_type, rejection_detail.
    """
    valid:    list[dict] = []
    rejected: list[dict] = []

    for r in readings:
        reading_time = r.get("reading_time")
        kwh          = r.get("kwh")

        reject_type:   str | None = None
        reject_detail: str | None = None

        if not _parsable_ts(reading_time):
            reject_type   = "invalid_reading_time"
            reject_detail = "reading_time absent or unparseable"
        elif kwh is None:
            reject_type   = "missing_kwh"
            reject_detail = "kwh is null"
        else:
            try:
                fkwh = float(kwh)
            except (ValueError, TypeError):
                reject_type   = "missing_kwh"
                reject_detail = f"kwh={kwh!r} not numeric"
                fkwh          = None  # type: ignore[assignment]

            if reject_type is None:
                if fkwh != fkwh or fkwh < KWH_MIN or fkwh > KWH_MAX:  # NaN check via self-compare
                    reject_type   = "kwh_out_of_range"
                    reject_detail = f"kwh={fkwh}, expected [{KWH_MIN},{KWH_MAX}]"

        if reject_type:
            rejected.append({
                **r,
                "delivery_id":      delivery_id,
                "source":           source,
                "rejection_type":   reject_type,
                "rejection_detail": reject_detail,
            })
        else:
            valid.append(r)

    return valid, rejected
