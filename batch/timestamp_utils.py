"""Shared timestamp parsing utilities for DynamoDB items.

This module centralizes timestamp conversion logic to avoid duplication
between dynamo_fetcher and main modules.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional


def ts_to_int(value: Optional[Any]) -> Optional[int]:
    """Best-effort conversion of *value* to an `int` epoch seconds.

    Handles multiple formats:
    - Direct int/float/Decimal values
    - Numeric strings (with millisecond detection heuristic)
    - ISO-8601 formatted strings (with or without timezone)
    - Custom timezone abbreviations (EST, EDT)

    Args:
        value: The timestamp value to convert (can be int, float, Decimal, str, etc.)

    Returns:
        Integer epoch seconds, or None if conversion fails
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, str):
        try:
            # Fast-path – value might be purely numeric (possibly quoted).
            num = float(value)
            # Heuristic: treat very large values as ms.
            if num > 1e12:
                num /= 1000.0
            return int(num)
        except (ValueError, TypeError):
            # Fall back to ISO-8601 parsing, e.g. "2025-05-19T21:20:00Z".
            from datetime import datetime, timezone

            try:
                # Replace trailing Z with +00:00 so fromisoformat accepts it
                iso_val = value.replace("Z", "+00:00") if value.endswith("Z") else value
                dt = datetime.fromisoformat(iso_val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                # Keep going – we may still be able to parse custom
                # timezone formats further down.
                pass
    # ------------------------------------------------------------------
    # Fallback for common non-ISO representations that include an explicit
    # US/Eastern timezone abbreviation ("EST" or "EDT").  While
    # ``datetime.fromisoformat`` is very forgiving, it deliberately
    # rejects unknown/ambiguous timezone names.  We therefore handle the
    # two most common abbreviations manually so integrators do *not* need
    # to change their existing schema.
    # ------------------------------------------------------------------

    if isinstance(value, str):
        from datetime import datetime, timezone, timedelta

        val = value.strip()

        for abbr, offset_hours in ((" EST", -5), (" EDT", -4)):
            if val.endswith(abbr):
                try:
                    # Remove the trailing abbreviation and attempt to
                    # parse the remaining string using the same relaxed
                    # ISO-8601 rules that ``fromisoformat`` employs
                    # (space *or* 'T' between date and time).
                    cleaned = val[: -len(abbr)].strip()
                    # Accept both "YYYY-mm-dd HH:MM:SS" and
                    # "YYYY-mm-ddTHH:MM:SS".
                    if "T" in cleaned:
                        fmt = "%Y-%m-%dT%H:%M:%S"
                    else:
                        fmt = "%Y-%m-%d %H:%M:%S"

                    dt_naive = datetime.strptime(cleaned, fmt)
                    dt = dt_naive.replace(tzinfo=timezone(timedelta(hours=offset_hours)))
                    return int(dt.timestamp())
                except Exception:
                    # Parsing failed – fall through so the caller can
                    # inspect other alternatives.
                    return None

    return None  # unsupported type
