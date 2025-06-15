"""Minimal integration template for a DynamoDB table.

Copy this file, rename *TABLE_NAME* and adjust the field lists if your schema
differs.  The helper demonstrates how to invoke the shared pipeline and does
**not** contain any custom business logic – the heavy lifting (timestamp
parsing, de-duplication, watermarking, JSONL generation, OpenAI submission…)
is handled by *batch.main* and its sub-modules.
"""

from __future__ import annotations

import argparse

from batch.main import orchestrate


# DynamoDB table to process.  Replace with your table name.
TABLE_NAME = "YOUR_TABLE_DATA_SOURCE_HERE"


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"One-off run for {TABLE_NAME}")
    parser.add_argument("--hours", type=int, default=24, help="Look-back window in hours (default: 24)")
    parser.add_argument("--model", choices=["nano", "mini", "full"], default="nano")
    parser.add_argument("--test", action="store_true", help="Stop after JSONL generation (dry-run)")
    return parser


def main() -> None:  # pragma: no cover (tiny helper)
    args = _build_cli().parse_args()

    orchestrate(
        hours=args.hours,
        model_key=args.model,
        test_only=args.test,
        table_name=TABLE_NAME,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
