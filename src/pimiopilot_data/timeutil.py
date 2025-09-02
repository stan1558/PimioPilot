from __future__ import annotations
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import re

_REL_RE = re.compile(r"^(\d+)([dwmy])$")

def _to_utc_floor(d: datetime) -> datetime:
    # Normalize to UTC and drop microseconds
    return d.astimezone(timezone.utc).replace(microsecond=0)

def _end_anchor_for_intervals(intervals: list[str] | None) -> datetime:
    """Choose an end anchor consistent with daily vs intraday use.
    - If any interval is daily (endswith 'd'), anchor to today's 00:00:00Z.
    - Else, use the current moment in UTC.
    """
    now = datetime.now(timezone.utc)
    if intervals and any(str(i).endswith("d") for i in intervals):
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    return now.replace(microsecond=0)

def parse_relative_range(relative: str, *, intervals: list[str] | None = None) -> tuple[str, str]:
    """Parse a compact relative range like '30d', '2w', '6m', '1y'.
    Returns (start_iso, end_iso) both as ISO8601 strings with trailing 'Z'.
    """
    m = _REL_RE.match(relative)
    if not m:
        raise ValueError(f"Invalid relative spec: {relative}")
    n = int(m.group(1))
    unit = m.group(2)

    end_dt = _end_anchor_for_intervals(intervals)
    if unit == "d":
        start_dt = end_dt - relativedelta(days=n)
    elif unit == "w":
        start_dt = end_dt - relativedelta(weeks=n)
    elif unit == "m":
        start_dt = end_dt - relativedelta(months=n)
    elif unit == "y":
        start_dt = end_dt - relativedelta(years=n)
    else:
        raise ValueError(f"Unsupported unit in relative spec: {unit}")

    start_iso = _to_utc_floor(start_dt).isoformat().replace("+00:00", "Z")
    end_iso = _to_utc_floor(end_dt).isoformat().replace("+00:00", "Z")
    return start_iso, end_iso
