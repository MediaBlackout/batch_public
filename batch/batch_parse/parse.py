"""Parse OpenAI *batch* output JSONL files into structured Python objects.

Each line in a batch output file resembles the following (simplified):

    {
        "id": "batch_req_…",
        "custom_id": "row_123",
        "response": {
            "status_code": 200,
            "body": {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "{ … JSON string … }"
                        }
                    }
                ]
            }
        }
    }

The *content* field is itself a JSON-encoded string that contains the actual
structured intelligence record produced by the LLM.  This helper extracts
that inner JSON, converts it to a Python ``dict`` and returns a list with one
entry per *row_…* regardless of the originating file.

Typical CLI usage::

    python batch_parse/parse.py \
        --output parsed/aggregate.json \
        batch/output/batch_output_*.jsonl

If a *directory* is passed instead of explicit files the script will recurse
into it and read all ``*.jsonl`` files it finds.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _yield_jsonl_lines(path: Path) -> Iterable[Dict[str, Any]]:
    """Yield decoded JSON dicts for each non-blank line in *path*."""

    with path.open("r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except json.JSONDecodeError:
                log.warning("%s:%s not valid JSON – ignored", path, lineno)


def _extract_inner_json(record: Dict[str, Any]) -> Dict[str, Any] | None:  # noqa: ANN401
    """Return the *content* JSON dict inside *record* or **None** on error."""

    # Ensure we have a *response* object with a success status code.
    response = record.get("response")
    if not isinstance(response, dict):
        return None

    if response.get("status_code") != 200:
        return None

    body = response.get("body")
    if not isinstance(body, dict):
        return None

    choices = body.get("choices")
    if not (isinstance(choices, list) and choices):
        return None

    message = choices[0].get("message")
    if not (isinstance(message, dict) and "content" in message):
        return None

    content_raw = message["content"]

    # The model returns a JSON string.  Attempt to decode it.
    if not isinstance(content_raw, str):
        return None

    # The content might contain stray whitespace or be wrapped in markdown
    # code fences.  Trim common wrappers to make parsing more resilient.
    cleaned = content_raw.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        # Remove leading/trailing triple-backticks – also drop optional json
        # language hint.
        cleaned_lines = cleaned.splitlines()
        if cleaned_lines[0].startswith("```"):
            cleaned_lines = cleaned_lines[1:]
        if cleaned_lines and cleaned_lines[-1].startswith("```"):
            cleaned_lines = cleaned_lines[:-1]
        cleaned = "\n".join(cleaned_lines).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # --------------------------------------------------------------------
        # The string often *almost* is valid JSON but may contain a few
        # common issues such as JavaScript style comments, leading plus signs
        # on numbers, or trailing commas.  We try to fix these cases in a
        # best-effort manner before giving up.
        # --------------------------------------------------------------------

        fixed = _loosen_json(cleaned)

        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            # Final fallback – keep the literal text for manual inspection.
            log.warning("Unable to parse inner JSON for %s – keeping raw string", record.get("custom_id"))
            return {"raw_content": cleaned}


# ---------------------------------------------------------------------------
# Fuzzy JSON repair helpers
# ---------------------------------------------------------------------------


_COMMENT_RE = re.compile(r"//.*?(?=[\n\r])")
_PLUS_NUMBER_RE = re.compile(r":\s*\+([0-9\.]+)")
_TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")
_COMMA_NUMBER_RE = re.compile(r"(:\s*)(-?\d{1,3}(?:,\d{3})+(?:\.\d+)?)(?=[,}\]])")


def _loosen_json(text: str) -> str:  # noqa: D401
    """Return *text* with a few non-standard JSON constructs removed.

    The aim is **not** to implement full JSON5 but to quickly address the
    issues frequently produced by LLM answers:

    1. Line comments starting with ``//``
    2. Leading plus signs on numbers (``+0.5``)
    3. Trailing commas before closing ``]`` or ``}``
    """

    # Strip // comments – keep newline so positions remain roughly stable.
    out = _COMMENT_RE.sub("", text)

    # Remove "+" sign before a number that appears after a colon.
    out = _PLUS_NUMBER_RE.sub(r": \1", out)

    # Remove trailing commas in lists / dicts.
    out = _TRAILING_COMMA_RE.sub(r"\1", out)

    # Remove thousands separators from numbers like 1,230,456 → 1230456.
    out = _COMMA_NUMBER_RE.sub(lambda m: m.group(1) + m.group(2).replace(",", ""), out)

    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_files(paths: Iterable[Path]) -> List[Dict[str, Any]]:  # noqa: ANN401
    """Return a list with the decoded *content* of all *paths*."""

    aggregated: List[Dict[str, Any]] = []

    for p in paths:
        log.info("Parsing %s", p)
        for outer in _yield_jsonl_lines(p):
            inner = _extract_inner_json(outer)

            if inner is None:
                # Nothing we could extract – skip silently (a warning has
                # already been logged inside *_extract_inner_json()*).
                continue

            # ----------------------------------------------------------------
            # Normalize the returned structure so that *aggregated* always
            # contains dictionaries.  In practice the assistant may answer
            # either with a single JSON object **or** with a JSON *array* of
            # objects.  The previous implementation assumed the first case
            # only which caused a runtime *TypeError* once an array was
            # encountered.  We now transparently flatten arrays while keeping
            # traceability information for every resulting element.
            # ----------------------------------------------------------------

            def _attach_meta(obj: Any, index: int | None = None) -> Dict[str, Any]:  # noqa: ANN401
                """Return *obj* as dict and add provenance metadata."""

                if isinstance(obj, dict):
                    out = obj
                else:
                    # For primitive types keep the value under *raw_value* so
                    # downstream code can still inspect it.
                    out = {"raw_value": obj}

                out["_source_custom_id"] = outer.get("custom_id")
                if index is not None:
                    out["_source_list_index"] = index
                return out

            if isinstance(inner, list):
                for idx, item in enumerate(inner):
                    aggregated.append(_attach_meta(item, idx))
            else:
                aggregated.append(_attach_meta(inner))

    return aggregated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _collect_jsonl_files(arg: str) -> List[Path]:
    """Return list of *.jsonl* files referenced by *arg* (file or dir)."""

    p = Path(arg)
    if not p.exists():
        raise FileNotFoundError(arg)

    if p.is_file():
        return [p]

    # Directory – collect recursively.
    return sorted(p.rglob("*.jsonl"))


def main(argv: list[str] | None = None) -> None:  # noqa: D401
    """Command-line entry point."""

    parser = argparse.ArgumentParser(
        prog="batch-parse",
        description="Parse OpenAI batch output JSONL files and aggregate the inner JSON records.",
    )

    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more batch output files or directories containing *.jsonl files.",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="aggregate.json",
        help="Path of the aggregated JSON file (default: %(default)s)",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    files: List[Path] = []
    for item in args.inputs:
        files.extend(_collect_jsonl_files(item))

    if not files:
        log.error("No JSONL files found in given inputs")
        sys.exit(1)

    aggregated = parse_files(files)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(aggregated, fh, indent=2)

    log.info("Wrote %s records to %s", len(aggregated), out_path)


if __name__ == "__main__":  # pragma: no cover – CLI usage only.
    main()
