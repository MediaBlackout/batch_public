"""Convert DynamoDB rows into OpenAI batch-ready JSONL."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from batch.logger import get_logger
from batch.models import resolve


log = get_logger(__name__)


SYSTEM_PROMPT = (
    "You are a senior macroeconomic intelligence analyst. Your job is to clean, verify, and standardize incoming real-time macro and market data into a structured intelligence report for Media Blackout LLC.\n\n"
    "The input may include Reddit sentiment summaries, Google Trends spikes, news headlines, and live market prices.\n\n"
    "Your job is to transform this raw data into a clean, verified, and structured intelligence report in JSON format for later use in batch analytics and dashboards.\n"
)


def _build_payload(summary_text: str, *, model: str, source_id: str | None) -> Dict[str, Any]:
    """Return a JSON-serialisable payload for one summary."""

    payload: Dict[str, Any] = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": summary_text},
        ],
        "model": model,
    }

    # The Chat Completions endpoint allows a *user* field for traceability.
    if source_id is not None:
        payload["user"] = str(source_id)

    return payload


def write_jsonl(
    items: List[Dict[str, Any]],
    *,
    model_key: str = "nano",
    output_dir: str | None = None,
    tag: str | None = None,
) -> tuple[str, int]:
    """Write *items* to a new JSONL file.

    Returns a tuple of ``(path, record_count)`` where *record_count* is the
    number of JSONL records written (i.e. lines in the resulting file).  This
    is useful for downstream bookkeeping so callers do not have to count the
    lines themselves.
    """

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "jsonl")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    if tag:
        safe_tag = "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in tag)[:32]
        base_name = f"batch_{safe_tag}_{timestamp}.jsonl"
    else:
        base_name = f"batch_{timestamp}.jsonl"
    path = os.path.join(output_dir, base_name)

    # Never overwrite – append incremental suffix if necessary.
    counter = 1
    while os.path.exists(path):
        path = os.path.join(output_dir, f"batch_{timestamp}_{counter}.jsonl")
        counter += 1

    model_name = resolve(model_key)

    written = skipped = 0
    with open(path, "w", encoding="utf-8") as fh:
        for item in items:
            # Use the same generous field search that *dynamo_fetcher._extract_text*
            # applies so we do **not** discard rows that were deemed usable
            # during the fetch step.

            summary = None
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

                # GoogleTrendsData5min specific fields
                "percent_increase",
                "search_volume",
                "source_page",
                "started_time_ago",

                # Nasdaq100Constituents and related market data columns
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
                value = lower_map.get(key.lower())

                if isinstance(value, str) and value.strip():
                    summary = value.strip()
                    break

                # Convert simple numeric types to strings so rows that only
                # contain e.g. ``percent_increase`` are not discarded.
                if isinstance(value, (int, float)):
                    summary = str(value)
                    break

                if isinstance(value, (list, dict)):
                    import json as _json
                    try:
                        summary = _json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                        break
                    except Exception:
                        pass
            if not summary:
                skipped += 1
                continue

            payload = _build_payload(summary, model=model_name, source_id=item.get("id"))

            # -----------------------------------------------------------------
            # The OpenAI Batch API (as of 2025-05) requires each JSONL record to
            # include a ``custom_id`` field that uniquely identifies the row
            # **within the batch**.  Without it the entire batch is rejected
            # during validation ("Missing required parameter: 'custom_id'").
            #
            # While the *endpoint* is supplied when the batch is created we
            # still include the "method" and "url" keys for completeness – this
            # makes the file self-contained and keeps future migrations
            # (e.g. mixing endpoints) trivial.
            # -----------------------------------------------------------------

            record = {
                "custom_id": f"row_{written + 1}",  # 1-based, <=64 chars
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": payload,
            }

            try:
                fh.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                written += 1
            except (TypeError, ValueError):
                skipped += 1

    log.info(
        "JSONL prepared: %s (written=%s, skipped=%s)",
        path,
        written,
        skipped,
    )

    return path, written
