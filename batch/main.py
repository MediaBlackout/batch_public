"""Entry point that orchestrates the full batch lifecycle.

Example usages:
    python main.py                # regular run (12h window, nano model)
    python main.py --model mini   # choose different base model
    python main.py --test         # generate JSONL but do *not* submit to OpenAI
    python main.py --resume abc   # resume polling & download results for id
"""


from __future__ import annotations

# ---------------------------------------------------------------------------
# Allow this file to be executed directly (`python batch/main.py`) **and** via
# the module form (`python -m batch.main`).  When run directly the parent
# directory (which contains the *batch* package) is *not* on ``sys.path`` and
# therefore absolute imports like ``from batch.dynamo_fetcher import\n+# fetch_recent`` fail with ``ModuleNotFoundError``.  To make the developer
# experience smooth we prepend the parent directory to ``sys.path`` **before**
# resolving any of the internal imports.
#
# This keeps the public import style intact (``batch.*``) while avoiding the
# pitfalls of relative imports and does not interfere when the package is
# installed in editable/regular mode because the parent directory will already
# be discoverable through *site-packages*.
# ---------------------------------------------------------------------------

import os
import sys

_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Ensure the current directory is discoverable when running as a script
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from batch.dynamo_fetcher import fetch_recent
from batch.jsonl_formatter import write_jsonl
from batch.batch_submitter import upload_file, submit_batch
from batch.status_checker import wait_for_completion, download_results
from batch.logger import get_logger
import boto3

# Optional dependency – only needed when OPENAI interaction is required.  We
# try to import it here so *auto_resume_pending()* can use the same retrieval
# logic as *status_checker.wait_for_completion()* without duplicating code in
# a separate module.

try:
    import openai  # type: ignore

    openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:  # pragma: no cover – cli may run in *test* mode only.
    openai = None  # type: ignore


log = get_logger(__name__)

# DynamoDB table that stores bookkeeping information for each OpenAI batch run.
# DynamoDB table used for batch bookkeeping.  The default value matches the
# specification provided by the infrastructure team.
BATCHJOB_TABLE = os.getenv("BATCHJOB_TABLE", "batchjob")


STATUS_FILE = os.path.join(os.path.dirname(__file__), "batch_status.json")
# Store the per-table *high-water-mark* (newest timestamp that has been
# successfully batched) in a lightweight JSON file next to the regular status
# file.  The structure is
#     {
#         "DailySourceReviews": 1716241234,
#         "GoogleTrendsData5min": 1716241111,
#         ...
#     }
# The file is purely advisory – if it is deleted we simply re-process the full
# look-back window once and then resume incremental operation.  This makes the
# mechanism robust against manual clean-ups and cross-machine sync scenarios.
#
# *Only* tables that expose a usable timestamp participate in the watermark
# logic.  For static reference data (e.g. Nasdaq100Constituents) we keep the
# previous behaviour of always sending the full dataset.

WATERMARK_FILE = os.path.join(os.path.dirname(__file__), "batch_watermark.json")


def _load_status() -> dict:
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            log.exception("Unable to read %s", STATUS_FILE)
    return {}


# ---------------------------------------------------------------------------
# High-water-mark helpers
# ---------------------------------------------------------------------------


def _load_watermark() -> dict[str, int]:
    """Return the persisted watermark dict (table -> last_ts).

    Errors are swallowed so the orchestrator never fails due to a corrupted or
    missing file.
    """

    if os.path.exists(WATERMARK_FILE):
        try:
            with open(WATERMARK_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                # Ensure all values are *int* so callers can compare directly
                return {str(k): int(v) for k, v in data.items() if isinstance(v, (int, float, str))}
        except Exception:
            log.exception("Unable to read %s", WATERMARK_FILE)
    return {}


def _save_watermark(data: dict[str, int]) -> None:
    tmp_path = WATERMARK_FILE + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        os.replace(tmp_path, WATERMARK_FILE)
    except Exception:
        log.exception("Unable to persist watermark state to %s", WATERMARK_FILE)


def _save_status(data: dict) -> None:
    tmp_path = STATUS_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    os.replace(tmp_path, STATUS_FILE)


# ----------------------------------------------------------------------------
# Public orchestration helpers
# ----------------------------------------------------------------------------


# Each invocation of *orchestrate()* handles **one** DynamoDB table so it can
# be called in a simple loop when the user passes ``--table A --table B``.


def orchestrate(
    hours: int,
    model_key: str,
    test_only: bool,
    *,
    table_name: str = "DailySourceReviews",
    wait: bool = True,
) -> None:
    # ---------------------------------------------------------------------
    # 1. Fetch from DynamoDB.
    # ---------------------------------------------------------------------

    log.info("Fetching data from DynamoDB table %s (look-back %sh)", table_name, hours)

    items = fetch_recent(hours=hours, table_name=table_name)

    if not items:
        log.info("No new data – exiting")
        return

    # ---------------------------------------------------------------------
    # 1b.  Incremental *watermark* filtering – drop rows we have already sent.
    # ---------------------------------------------------------------------

    from batch.dynamo_fetcher import _NO_TS_FILTER  # local import to avoid cycle

    watermark_data = _load_watermark()
    if table_name not in _NO_TS_FILTER:
        last_ts = watermark_data.get(table_name, 0)

        def _get_ts(record: Dict[str, Any]) -> Optional[int]:  # type: ignore[override]
            """Return epoch seconds extracted from *record* or None."""

# Re-use canonical key set defined in *batch.dynamo_fetcher* so the
# high-water-mark logic stays in sync with the main fetch implementation.
            from batch.dynamo_fetcher import TIMESTAMP_KEYS as _TS_KEYS  # local import to avoid cycle

            value: Any | None = None
            for k, v in record.items():
                if k.lower() not in _TS_KEYS:
                    continue
                value_candidate = v

                # Convert to int epoch seconds using same heuristics
                timestamp: Optional[int] = None
                if isinstance(value_candidate, int):
                    timestamp = value_candidate
                elif isinstance(value_candidate, float):
                    timestamp = int(value_candidate)
                elif isinstance(value_candidate, str):
                    try:
                        num = float(value_candidate)
                        if num > 1e12:
                            num /= 1000.0
                        timestamp = int(num)
                    except Exception:
                        pass
                else:
                    try:
                        from decimal import Decimal

                        if isinstance(value_candidate, Decimal):
                            timestamp = int(value_candidate)
                    except Exception:
                        pass

                if timestamp is not None:
                    return timestamp

            # Either no matching keys or none convertible to epoch seconds.
            return None

        new_items = [rec for rec in items if (_ts := _get_ts(rec)) is not None and _ts > last_ts]

        if not new_items:
            log.info("No new records after watermark filtering (last_ts=%s) – skipping table %s", last_ts, table_name)
            return

        items = new_items  # keep only unseen rows

    # ---------------------------------------------------------------------
    # 2. Build JSONL file and capture written record count.
    # ---------------------------------------------------------------------

    # ---------------------------------------------------------------------
    # Use a dedicated *jsonl_test/* folder when running in *--test* mode so
    # developers can easily differentiate between production-bound files and
    # local dry-run artefacts.  This keeps the main *jsonl/* directory clean
    # and prevents accidental re-submission of test files.
    # ---------------------------------------------------------------------

    if test_only:
        test_dir = os.path.join(os.path.dirname(__file__), "jsonl_test")
        jsonl_path, record_count = write_jsonl(
            items,
            model_key=model_key,
            output_dir=test_dir,
            tag=table_name,
        )
    else:
        jsonl_path, record_count = write_jsonl(items, model_key=model_key, tag=table_name)

    if record_count == 0:
        log.info("All fetched items lacked usable text – skipping table %s", table_name)
        return

    if test_only:
        log.info("--test flag supplied – stopping after JSONL generation (%s)", jsonl_path)
        return

    # ------------------------------------------------------------------
    # Watermark update – persist new maximum timestamp *before* we leave
    # orchestrate so follow-up runs do not resend the same rows even when the
    # OpenAI call fails mid-way.  We only update for tables that participate
    # in the timestamp-based filtering described above.
    # ------------------------------------------------------------------

    if table_name not in _NO_TS_FILTER:
        # Re-use helper defined earlier in this function.
        max_ts = 0
        for rec in items:
            ts_val = _get_ts(rec)
            if ts_val and ts_val > max_ts:
                max_ts = ts_val

        if max_ts:
            watermark_data[table_name] = max_ts
            _save_watermark(watermark_data)

    # 3. Upload file & create batch.
    file_obj = upload_file(jsonl_path)
    batch_obj = submit_batch(file_id=file_obj.id)

    # 4. Persist status (both locally **and** optionally in DynamoDB).
    created_ts = datetime.now(timezone.utc)

    status_data = _load_status()
    status_data[batch_obj.id] = {
        "created_utc": created_ts.isoformat(),
        "status": batch_obj.status,
        "model": model_key,
        "input_jsonl": os.path.basename(jsonl_path),
        "input_file_id": file_obj.id,
        "table_name": table_name,
        "record_count": record_count,
    }
    _save_status(status_data)

    # DynamoDB bookkeeping (best-effort – do *not* abort batch on failure).
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(BATCHJOB_TABLE)
        table.put_item(
            Item={
                "batch_id": batch_obj.id,
                "timestamp": created_ts.isoformat(),
                "table_name": table_name,
                "status": batch_obj.status,
                "model": model_key,
                "input_file_id": file_obj.id,
                "record_count": record_count,
            }
        )
        log.info("Recorded batch %s in DynamoDB table %s", batch_obj.id, BATCHJOB_TABLE)
    except Exception:
        log.exception("Unable to write batch %s to DynamoDB table %s", batch_obj.id, BATCHJOB_TABLE)

    if not wait:
        log.info(
            "--async flag supplied – skipping wait/download for batch %s (status=%s)",
            batch_obj.id,
            batch_obj.status,
        )
        return

    # ---------------------------------------------------------------------
    # 5. Monitor until completion then download results.
    # ---------------------------------------------------------------------

    batch_obj = wait_for_completion(batch_obj.id)

    # Update final status.
    status_data[batch_obj.id].update(
        {
            "final_status": batch_obj.status,
            "output_file_id": getattr(batch_obj, "output_file_id", None),
        }
    )
    _save_status(status_data)

    if batch_obj.status == "completed":
        download_results(batch_obj.output_file_id)  # type: ignore[attr-defined]
    else:
        log.error("Batch %s finished with status %s", batch_obj.id, batch_obj.status)

    # Update DynamoDB record.
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(BATCHJOB_TABLE)
        key: dict[str, str] = {"batch_id": batch_obj.id}
        key["table_name"] = table_name
        key["timestamp"] = created_ts.isoformat()

        try:
            table.update_item(
                Key=key,
                UpdateExpression="SET final_status=:s, output_file_id=:o",
                ExpressionAttributeValues={
                    ":s": batch_obj.status,
                    ":o": getattr(batch_obj, "output_file_id", None),
                },
            )
        except Exception:
            ts_val = key.get("timestamp") or created_ts.isoformat()
            merged = {**key, "timestamp": ts_val, "final_status": batch_obj.status, "output_file_id": getattr(batch_obj, "output_file_id", None)}
            table.put_item(Item=merged)
        log.info("Updated batch %s in DynamoDB table %s", batch_obj.id, BATCHJOB_TABLE)
    except Exception:
        log.exception("Unable to update batch %s in DynamoDB table %s", batch_obj.id, BATCHJOB_TABLE)


def resume(batch_id: str) -> None:
    log.info("Resuming monitoring for batch %s", batch_id)
    batch_obj = wait_for_completion(batch_id)

    if batch_obj.status == "completed":
        download_results(batch_obj.output_file_id)  # type: ignore[attr-defined]
    else:
        log.error("Batch %s finished with status %s", batch_id, batch_obj.status)

    # Update DynamoDB record if present.
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(BATCHJOB_TABLE)
        # We may not know *table_name* at this point (resume may be called
        # manually).  Attempt to retrieve it from the existing item first so
        # we can supply the full composite key if required.

        key: dict[str, str] = {"batch_id": batch_id}
        try:
            response = table.get_item(Key=key)
            item = response.get("Item")
            if item and "table_name" in item:
                key["table_name"] = item["table_name"]
            if "timestamp" in item:
                key["timestamp"] = item["timestamp"]
        except Exception:  # noqa: BLE001
            # Best-effort – fall back to partial key.
            pass

        try:
            table.update_item(
                Key=key,
                UpdateExpression="SET final_status=:s, output_file_id=:o",
                ExpressionAttributeValues={
                    ":s": batch_obj.status,
                    ":o": getattr(batch_obj, "output_file_id", None),
                },
            )
        except Exception:
            ts_val = key.get("timestamp") or status_data.get(batch_id, {}).get("created_utc")
            if ts_val is None:
                from datetime import datetime, timezone
                ts_val = datetime.now(timezone.utc).isoformat()
            table.put_item(
                Item={
                    **key,
                    "timestamp": ts_val,
                    "final_status": batch_obj.status,
                    "output_file_id": getattr(batch_obj, "output_file_id", None),
                }
            )
        log.info("(resume) Updated batch %s in DynamoDB table %s", batch_id, BATCHJOB_TABLE)
    except Exception:
        log.exception("(resume) Unable to update batch %s in DynamoDB table %s", batch_id, BATCHJOB_TABLE)


# ---------------------------------------------------------------------------
# Background resume helpers – used when running in asynchronous (cron) mode.
# ---------------------------------------------------------------------------


def _retrieve_batch(batch_id: str):  # noqa: ANN001 – openai type depends on version
    """Return the current Batch object using whichever path the client exposes."""

    if openai is None or not openai.api_key:
        raise RuntimeError("OpenAI client unavailable – cannot resume pending batches")

    if hasattr(openai, "batches") and hasattr(openai.batches, "retrieve"):
        return openai.batches.retrieve(batch_id)  # type: ignore[attr-defined]
    if (
        hasattr(openai, "beta")
        and hasattr(openai.beta, "batches")
        and hasattr(openai.beta.batches, "retrieve")
    ):
        return openai.beta.batches.retrieve(batch_id)  # type: ignore[attr-defined]
    raise AttributeError("'openai' client missing batches.retrieve method")


def _auto_resume_pending() -> None:
    """Check *batch_status.json* for unfinished batches and finalise them.

    The function does **not** block for hours. It performs a single status
    check per batch. Completed batches are downloaded; still-running batches
    are left untouched and will be checked in the next cron cycle.
    """

    status_data = _load_status()
    if not status_data:
        return

    pending_ids = [bid for bid, info in status_data.items() if "final_status" not in info]
    if not pending_ids:
        return

    log.info("Found %d pending batch(es) – checking status", len(pending_ids))

    for batch_id in pending_ids:
        try:
            batch_obj = _retrieve_batch(batch_id)
            current_status = batch_obj.status
            log.info("(auto-resume) batch %s status=%s", batch_id, current_status)

            if current_status == "completed":
                path = download_results(batch_obj.output_file_id)  # type: ignore[attr-defined]
                status_data[batch_id]["final_status"] = "completed"
                status_data[batch_id]["output_file_id"] = batch_obj.output_file_id
                status_data[batch_id]["output_path"] = os.path.basename(path)
            elif current_status in {"failed", "expired", "cancelled"}:
                status_data[batch_id]["final_status"] = current_status
            else:
                # still running – nothing to do
                continue

            _save_status(status_data)

            # Optionally update DynamoDB record
            try:
                dynamodb = boto3.resource("dynamodb")
                table = dynamodb.Table(BATCHJOB_TABLE)

                key: dict[str, str] = {"batch_id": batch_id}
                # Include *table_name* or *timestamp* when they are part of the
                # composite key so *update_item* succeeds regardless of the
                # concrete schema being used.
                info = status_data[batch_id]
                if "table_name" in info:
                    key["table_name"] = info["table_name"]
                if "timestamp" in info:
                    key["timestamp"] = info["timestamp"]

                try:
                    table.update_item(
                        Key=key,
                        UpdateExpression="SET final_status=:s, output_file_id=:o",
                        ExpressionAttributeValues={
                            ":s": status_data[batch_id].get("final_status"),
                            ":o": status_data[batch_id].get("output_file_id"),
                        },
                    )
                except Exception:
                    # Fallback: perform an *upsert* so we never lose data even
                    # when the key schema is different than expected.
                    ts_val = status_data[batch_id].get("timestamp") or status_data[batch_id].get("created_utc")
                    if ts_val is None:
                        from datetime import datetime, timezone
                        ts_val = datetime.now(timezone.utc).isoformat()

                    merged = {**status_data[batch_id], **key, "timestamp": ts_val}
                    table.put_item(Item=merged)

                log.info("(auto-resume) Updated batch %s in DynamoDB", batch_id)
            except Exception:
                log.exception("(auto-resume) Unable to update batch %s in DynamoDB", batch_id)

        except Exception:
            log.exception("(auto-resume) Error while checking batch %s", batch_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="DailySourceReviews batch processor")
    parser.add_argument("--hours", type=int, default=12, help="Lookback window in hours (default: 12)")
    from batch.models import MODEL_MAP, TEXT_CHAT_MODELS, EMBEDDING_MODELS

    model_choices = list(MODEL_MAP.keys()) + TEXT_CHAT_MODELS + EMBEDDING_MODELS
    parser.add_argument(
        "--model",
        choices=model_choices,
        default="nano",
        help="Model key (nano/mini/full) or actual model name",
    )
    parser.add_argument(
        "--table",
        "-t",
        action="append",
        help=(
            "Name of the DynamoDB table to process. Can be used multiple times. "
            "Default: DailySourceReviews"
        ),
    )
    parser.add_argument("--resume", metavar="ID", help="Resume monitoring an existing batch id")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Stop after JSONL creation – do not contact OpenAI",
    )

    parser.add_argument(
        "--async",
        dest="async_mode",
        action="store_true",
        help=(
            "Submit the batch and exit immediately (do not poll for completion). "
            "Recommended for cron jobs."
        ),
    )

    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List DynamoDB tables in the current AWS account/region and exit",
    )
    parser.add_argument(
        "--list-models",
        action="store_true",
        help="List supported models (text/chat and embedding) and exit",
    )

    # Lightweight helper for cron jobs that only need to download finished
    # results without submitting a new batch.  This performs a single status
    # check against all unfinished batches tracked in *batch_status.json* and
    # exits immediately.
    parser.add_argument(
        "--check-outputs",
        action="store_true",
        help=(
            "Download finished batch outputs (if any) and update bookkeeping "
            "files without submitting new work – suitable for an hourly cron "
            "job."
        ),
    )

    # Optional convenience flag – overrides the OPENAI_API_KEY environment
    # variable and *.env* file for this invocation only.
    parser.add_argument(
        "--openai-key",
        metavar="KEY",
        help="Explicit OpenAI API key (overrides environment/.env)",
    )

    args = parser.parse_args()


    # ---------------------------------------------------------------------
    # Apply explicit API key override *early* so every downstream module uses
    # the correct credentials without requiring callers to touch the process
    # environment or modify their .env file.
    # ---------------------------------------------------------------------

    if args.openai_key:
        try:
            import openai  # type: ignore

            openai.api_key = args.openai_key.strip()
            log.info("Using OpenAI API key supplied via --openai-key (length=%d)", len(args.openai_key))
        except ImportError:
            log.error("--openai-key provided but the 'openai' package is not installed")

    # -------------------------------------------------
    # Early-exit mode: *check outputs only* (must run **after** potential API
    # key override so the OpenAI client is correctly configured).
    # -------------------------------------------------

    if getattr(args, "check_outputs", False):
        _auto_resume_pending()
        raise SystemExit(0)

    if args.list_tables:
        try:
            client = boto3.client("dynamodb")
            response = client.list_tables()
            for name in response.get("TableNames", []):
                print(name)
        except Exception as exc:  # noqa: BLE001
            log.exception("Unable to list DynamoDB tables: %s", exc)
            raise SystemExit(1) from exc

        raise SystemExit(0)
    if args.list_models:
        from batch.models import TEXT_CHAT_MODELS, EMBEDDING_MODELS

        print("Models with Batch Support")
        print("\nText / Chat Models:")
        for m in TEXT_CHAT_MODELS:
            print(m)
        print("\nEmbedding Models:")
        for m in EMBEDDING_MODELS:
            print(m)
        print(
            "\nThe Batch API processes asynchronous jobs (text, chat, image) "
            "and returns them within 24 hours"
        )
        raise SystemExit(0)

    if args.resume:
        resume(args.resume)
    else:
        # First attempt to finalise previous batches (lightweight – no long wait)
        if args.async_mode:
            _auto_resume_pending()

        table_names: list[str]

        if args.table:
            # Explicit --table flags take precedence over the config file.
            table_names = []
            for t in args.table:
                table_names.extend([x.strip() for x in t.split(",") if x.strip()])
        else:
            # Fall back to *batch_config.json*.
            cfg_path = os.path.join(os.path.dirname(__file__), "batch_config.json")
            try:
                with open(cfg_path, "r", encoding="utf-8") as fh:
                    cfg = json.load(fh)
                table_names = cfg.get("enabled_tables", [])
                if not table_names:
                    raise ValueError("'enabled_tables' missing or empty")
            except Exception:
                log.exception(
                    "Unable to load enabled tables from %s – defaulting to DailySourceReviews",
                    cfg_path,
                )
                table_names = ["DailySourceReviews"]

        # If the user did *not* request synchronous execution but multiple
        # tables are being processed we implicitly switch to *async* mode so
        # the script does not wait minutes (or hours) for each batch to
        # finish and therefore risk exceeding cron's time window.

        if len(table_names) > 1 and not args.async_mode:
            log.info(
                "Multiple tables detected (%d) – automatically enabling async mode",
                len(table_names),
            )
            wait_flag = False
        else:
            wait_flag = not args.async_mode

        for tbl in table_names:
            orchestrate(
                args.hours,
                args.model,
                args.test,
                table_name=tbl,
                wait=wait_flag,
            )


if __name__ == "__main__":
    main()
