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
from botocore.exceptions import ClientError, BotoCoreError
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
from batch.text_utils import extract_text
from batch.timestamp_utils import ts_to_int


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


# Text extraction logic has been moved to batch.text_utils.extract_text
# to avoid duplication with jsonl_formatter.py
_extract_text = extract_text  # Alias for backward compatibility


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

    # Use shared timestamp conversion function (moved to batch.timestamp_utils)
    _ts_to_int = ts_to_int

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
        except (ClientError, BotoCoreError, ConnectionError) as e:
            # Catch specific AWS/network errors for retry logic
            attempt += 1
            log.exception("DynamoDB scan attempt %s failed: %s", attempt, e)
            if attempt >= 3:
                raise

    log.info("Fetched %s usable records from DynamoDB", len(results))
    return results
