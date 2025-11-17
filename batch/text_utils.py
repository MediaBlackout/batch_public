"""Shared text extraction utilities for DynamoDB items.

This module centralizes text field extraction logic to avoid duplication
between dynamo_fetcher and jsonl_formatter modules.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Text field candidates (case-insensitive)
# ---------------------------------------------------------------------------
# DynamoDB tables often use slightly different attribute names for the
# article body / summary. To make the pipeline more forgiving we iterate
# over a *superset* of likely candidates and return the first non-empty
# value that we encounter instead of hard-coding a single field.
# ---------------------------------------------------------------------------

TEXT_FIELD_CANDIDATES = (
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
)


def extract_text(item: Dict[str, Any]) -> str | None:
    """Return the summary text from *item* if possible.

    This function searches through a comprehensive list of field names
    (case-insensitive) and returns the first non-empty value found.

    Handles:
    - String values (returned with whitespace stripped)
    - Numeric values (int, float, Decimal - converted to strings)
    - Complex types (list, dict - serialized as compact JSON)

    Args:
        item: DynamoDB item dictionary

    Returns:
        Extracted text content, or None if no usable text found
    """
    # Build a lowercase map so we can match keys case-insensitively without
    # copying the original payload.
    lower_map = {k.lower(): v for k, v in item.items()}

    for key in TEXT_FIELD_CANDIDATES:
        val = lower_map.get(key.lower())

        # String values - return if non-empty after stripping
        if isinstance(val, str) and val.strip():
            return val.strip()

        # Convert basic numeric types to strings so we can still generate a
        # non-empty summary rather than skipping the record entirely. This is
        # primarily useful for trend data where fields like
        # ``percent_increase`` or ``search_volume`` are stored as numbers.
        if isinstance(val, (int, float, Decimal)):
            return str(val)

        # If the value is a list/dict (e.g. ``trend_breakdown``) cast to a
        # compact JSON string to retain structure while remaining within the
        # textual payload constraints imposed by the Chat Completion API.
        if isinstance(val, (list, dict)):
            try:
                compact = json.dumps(val, ensure_ascii=False, separators=(",", ":"))
                if compact:
                    return compact
            except Exception:
                pass

    return None
