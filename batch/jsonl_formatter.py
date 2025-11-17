"""Convert DynamoDB rows into OpenAI batch-ready JSONL."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from batch.logger import get_logger
from batch.models import resolve
from batch.text_utils import extract_text


log = get_logger(__name__)


# Default fallback prompt if no config is provided
DEFAULT_SYSTEM_PROMPT = (
    "You are an AI assistant that processes and analyzes data. "
    "Transform the input data into a clean, structured format suitable for analysis."
)


def _build_payload(
    summary_text: str,
    *,
    model: str,
    source_id: str | None,
    system_prompt: str,
) -> Dict[str, Any]:
    """Return a JSON-serialisable payload for one summary."""

    payload: Dict[str, Any] = {
        "messages": [
            {"role": "system", "content": system_prompt},
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
    config: Dict[str, Any] | None = None,
    table_name: str | None = None,
) -> tuple[str, int]:
    """Write *items* to a new JSONL file.

    Returns a tuple of ``(path, record_count)`` where *record_count* is the
    number of JSONL records written (i.e. lines in the resulting file).  This
    is useful for downstream bookkeeping so callers do not have to count the
    lines themselves.

    Args:
        items: List of dictionaries to convert to JSONL
        model_key: Model identifier (e.g., "nano", "mini")
        output_dir: Directory to write JSONL file to
        tag: Optional tag to include in filename
        config: Batch configuration dictionary (for system prompts)
        table_name: Name of the table being processed (for table-specific prompts)
    """

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "jsonl")
    os.makedirs(output_dir, exist_ok=True)

    # Determine which system prompt to use
    system_prompt = DEFAULT_SYSTEM_PROMPT
    if config:
        # Check for table-specific prompt first
        if table_name and "table_prompts" in config:
            table_prompts = config.get("table_prompts", {})
            if table_name in table_prompts:
                system_prompt = table_prompts[table_name]
                log.debug("Using table-specific prompt for %s", table_name)
            elif "default_system_prompt" in config:
                system_prompt = config["default_system_prompt"]
                log.debug("Using default system prompt from config")
        elif "default_system_prompt" in config:
            system_prompt = config["default_system_prompt"]
            log.debug("Using default system prompt from config")

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
            # Use the shared text extraction function to ensure consistent
            # field handling across the pipeline
            summary = extract_text(item)
            if not summary:
                skipped += 1
                continue

            payload = _build_payload(
                summary,
                model=model_name,
                source_id=item.get("id"),
                system_prompt=system_prompt,
            )

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
