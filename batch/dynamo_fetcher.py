"""Fetch recent items from DynamoDB tables.

Assumptions
-----------
* The DynamoDB table is located in *us-east-1* (but this can be overridden).
* The items include an integer/number attribute named ``timestamp`` containing
  the UTC epoch seconds when the record was created.
* The text to analyse lives under either ``summary`` (preferred), ``text`` or
  ``content`` – the first non-empty field wins.

If your schema differs, tweak ``_extract_text`` or the filter expression.
"""

from __future__ import annotations


import time
import os
from decimal import Decimal
from typing import List, Dict, Any, Optional, Set

import boto3
# NOTE: We *used* to apply a `timestamp >= cutoff` filter directly in the
# `Scan` request (see git history for the previous implementation).  This only
# works when the attribute is stored as a *Number* in DynamoDB.  Our production
# table, however, stores the value as a **string** (`S`) which caused the scan
# to silently return zero results because DynamoDB does **not** support
# comparing different attribute types in a filter expression.
#
# To make the function robust we now perform the temporal filtering on the
# Python side and therefore remove the filter expression completely.  For most
# workloads the amount of data touched during every run (24 h look-back window)
# is small, so scanning the table is acceptable and avoids the complexity of
# type juggling in the DynamoDB expression language.  If the table ever grows
# to millions of items consider adding a GSI that stores the timestamp as a
# Number and switch to a `Query`.

from boto3.dynamodb.conditions import Attr

from batch.logger import get_logger


log = get_logger(__name__)

# Some tables either lack a timestamp attribute entirely (static reference
# data) or we always want to process the full dataset.  For those tables we
# disable the usual *cutoff* filter so every item is returned.

_NO_TS_FILTER: Set[str] = {
    "GoogleTrendsHistorical",
    "YOUR_TABLE_DATA_SOURCE_HERE",
}

# ---------------------------------------------------------------------------
# Common timestamp attribute names (case-insensitive)
# ---------------------------------------------------------------------------
#
# The timestamp/key detection logic is used by *fetch_recent()* below as well
# as the high-water-mark filtering implemented in *batch.main*.  To keep the
# two call-sites in sync (and to make future additions trivial) we expose the
# *canonical* candidate set as a module-level constant that can be imported
# elsewhere instead of maintaining two divergent, error-prone copies.
# ---------------------------------------------------------------------------

TIMESTAMP_KEYS: Set[str] = {
    # Generic
    "timestamp",
    "ts",
    "time",
    "date",
    "datetime",
    "created",
    "created_at",
    "createdat",  # camelCase → lower-case normalised

    # Published / news specific
    "published",
    "published_at",
    "publishedat",  # publishedAt in camelCase
    "pub_date",

    # Misc historical hold-overs
    "est_timestamp",
}


def _extract_text(item: Dict[str, Any]) -> str | None:
    """Return the summary text from *item* if possible."""

    # DynamoDB tables often use slightly different attribute names for the
    # article body / summary.
    # under slightly different attribute names.  To make the pipeline more
    # forgiving we iterate over a *superset* of likely candidates and return
    # the first non-empty string that we encounter instead of hard-coding a
    # single field.

    # Build a lowercase map so we can match keys case-insensitively without
    # copying the original payload.
    lower_map = {k.lower(): v for k, v in item.items()}

    for key in (
        "summary",
        "text",
        "content",
        "review_summary",
        "review_text",
        "description",
        "body",
        "article",
        "title",
        "headline",
        "selftext",
        "query",
        "keyword",
        "term",
        "trend_name",
        "trend_breakdown",
        "company",
        "symbol",

        # Trend data specific fields
        "percent_increase",
        "search_volume",
        "source_page",
        "started_time_ago",

        # Market data columns
        "avgvolume30",
        "bollingerlo",
        "bollingerup",
        "changepct",
        "changepctstr",
        "highprice",
        "lastprice",
        "lastpricetime",
        "lastupdated",
        "lastvolume",
        "lowprice",
        "prevclose",
        "rsi14",
        "sma20",
        "week52high",
        "week52low",
    ):
        val = lower_map.get(key.lower())
        if isinstance(val, str) and val.strip():
            return val.strip()

        # Convert basic numeric types to strings so we can still generate a
        # non-empty summary rather than skipping the record entirely.  This is
        # primarily useful for trend data where fields like
        # ``percent_increase`` or ``search_volume`` are stored as numbers.
        if isinstance(val, (int, float, Decimal)):
            return str(val)

        # If the value is a list/dict (e.g. ``trend_breakdown``) cast to a
        # compact JSON string to retain structure while remaining within the
        # textual payload constraints imposed by the Chat Completion API.
        if isinstance(val, (list, dict)):
            import json as _json

            try:
                compact = _json.dumps(val, ensure_ascii=False, separators=(",", ":"))
                if compact:
                    return compact
            except Exception:
                pass
    return None


def fetch_recent(
    *,
    hours: int = 12,
    table_name: str = "YOUR_TABLE_DATA_SOURCE_HERE",
    region_name: str | None = None,
) -> List[Dict[str, Any]]:
    """Return items newer than *hours* from *table_name*.

    The function retries transient DynamoDB errors up to three times.
    """

    # Short-circuit when *hours* is zero or negative (useful for dry-runs / unit tests).
    if hours <= 0:
        log.info("Hours <= 0 supplied – skipping DynamoDB fetch and returning empty list")
        return []

    if region_name is None:
        region_name = os.getenv("AWS_REGION", "us-east-1")

    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(table_name)

    cutoff = int(time.time() - hours * 3600)

    # We do **not** pass a FilterExpression – see header comment for details.
    scan_kwargs: dict[str, Any] = {}

    results: List[Dict[str, Any]] = []

    # Reset duplicate tracking per invocation so state does not leak across
    # multiple independent fetches during the same interpreter session.
    if hasattr(fetch_recent, "_seen_keys"):
        delattr(fetch_recent, "_seen_keys")

    def _ts_to_int(value: Optional[Any]) -> Optional[int]:  # type: ignore[override]
        """Best-effort conversion of *value* to an `int` epoch seconds.

        Returns ``None`` when the conversion fails.
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

    attempt = 0
    while attempt < 3:
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key

                response = table.scan(**scan_kwargs)
                items = response.get("Items", [])

                for item in items:
                    # Some tables (e.g. *YOUR_TABLE_DATA_SOURCE_HERE*) use slightly different
                    # attribute names for the epoch timestamp.  We therefore
                    # try a handful of common alternatives before giving up.

                    ts = None

                    # Build a lowercase lookup of candidate timestamp names so we treat
                    # "Timestamp" and "timestamp" equally.
                    for k, v in item.items():
                        if k.lower() not in TIMESTAMP_KEYS:
                            continue

                        ts_candidate = _ts_to_int(v)
                        if ts_candidate is not None:
                            ts = ts_candidate
                            break

                    # For tables that lack a timestamp we include *all* rows.
                    if table_name not in _NO_TS_FILTER:
                        if ts is None or ts < cutoff:
                            continue  # too old or invalid timestamp


                    # --------------------------------------------------
                    # Duplicate elimination – DynamoDB tables (especially
                    # news crawlers) occasionally contain identical rows
                    # when a source re-ingests the same article multiple
                    # times.  We use the *url* (or alternatively the primary
                    # key "id") as a stable de-duplication token **within a
                    # single fetch() call** so downstream stages never
                    # process duplicates.
                    #
                    # The check is intentionally performed *after* the text
                    # extraction so we do not waste cycles on rows that will
                    # be discarded anyway.
                    # --------------------------------------------------

                    if _extract_text(item):
                        dedup_key = None

                        # Prefer canonical article URL when present – this is
                        # globally unique for news articles.
                        for key in ("url", "link", "source_url", "guid"):
                            val = item.get(key) or item.get(key.capitalize())
                            if isinstance(val, str) and val.strip():
                                dedup_key = ("url", val.strip().lower())
                                break

                        # Fallback: use the DynamoDB primary key "id" (or
                        # syntactic variants) when available.
                        if dedup_key is None:
                            for key in ("id", "pk", "record_id", "article_id"):
                                val = item.get(key) or item.get(key.capitalize())
                                if val is not None:
                                    dedup_key = ("id", str(val))
                                    break

                        if dedup_key is not None:
                            _seen: set = getattr(fetch_recent, "_seen_keys", set())
                            if dedup_key in _seen:
                                continue  # duplicate – skip
                            _seen.add(dedup_key)
                            # Store back attribute so subsequent iterations
                            # see the updated set.
                            setattr(fetch_recent, "_seen_keys", _seen)

                        results.append(item)

                start_key = response.get("LastEvaluatedKey")
                done = start_key is None

            break  # success – exit retry loop
        except Exception:  # noqa: BLE001 (broad but re-raised after limit)
            attempt += 1
            log.exception("DynamoDB scan attempt %s failed", attempt)
            if attempt >= 3:
                raise

    log.info("Fetched %s usable records from DynamoDB", len(results))
    return results
