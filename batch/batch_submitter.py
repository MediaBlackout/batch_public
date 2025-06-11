"""Upload JSONL and create an OpenAI batch job."""

from __future__ import annotations

import os
import time
from typing import Any, Dict

# Optional *python-dotenv* – silently skip if not available so the script can
# run in minimal environments (e.g. cron) where only real env vars are set.
try:
    from dotenv import load_dotenv  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – dev convenience only

    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False

from batch.logger import get_logger


log = get_logger(__name__)


# Load environment variables from a .env file if present so developers can run
# the script locally without exporting variables each time.
load_dotenv()


try:
    import openai

    openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:  # pragma: no cover – the library *should* be installed.
    openai = None  # type: ignore


class OpenAIUnavailable(RuntimeError):
    """Raised if the *openai* library is missing or not configured."""


def _require_openai() -> None:
    if openai is None or not openai.api_key:
        raise OpenAIUnavailable(
            "The openai package is not available or OPENAI_API_KEY is missing."
        )


def upload_file(path: str, *, purpose: str = "batch", max_retries: int = 3) -> Any:  # noqa: ANN401,E501
    """Upload *path* to OpenAI and return the file object."""

    _require_openai()

    attempt = 0
    while attempt < max_retries:
        try:
            with open(path, "rb") as fh:
                file_obj = openai.files.create(file=fh, purpose=purpose)  # type: ignore[attr-defined]
            log.info("Uploaded JSONL (%s) – file_id=%s", path, file_obj.id)
            return file_obj
        except Exception:  # noqa: BLE001
            attempt += 1
            log.exception("Upload attempt %s for %s failed", attempt, path)
            time.sleep(1 + attempt)
    raise RuntimeError(f"Failed to upload {path} after {max_retries} attempts")


def submit_batch(
    *,
    file_id: str,
    endpoint: str = "/v1/chat/completions",
    completion_window: str = "24h",
    max_retries: int = 3,
):
    """Create an OpenAI batch and return the JSON response.

    The *model* is specified inside each JSONL line, so it is **not** passed to
    the batch create endpoint (passing unknown fields will raise an error).
    """

    _require_openai()

    attempt = 0
    while attempt < max_retries:
        try:
            # Prior to openai-python v1.12 the *batches* endpoint lived under
            # the experimental ``beta`` namespace.  In newer versions (e.g.
            # v1.75) it was promoted to the stable, top-level namespace.  To
            # keep compatibility across versions we first attempt the modern
            # location and fall back to the legacy path if needed.

            create_fn = None

            # Newer location (>=1.14-ish)
            if hasattr(openai, "batches") and hasattr(openai.batches, "create"):
                create_fn = openai.batches.create  # type: ignore[attr-defined]
            # Older location (beta namespace)
            elif (
                hasattr(openai, "beta")
                and hasattr(openai.beta, "batches")
                and hasattr(openai.beta.batches, "create")
            ):
                create_fn = openai.beta.batches.create  # type: ignore[attr-defined]

            if create_fn is None:  # pragma: no cover – unexpected client layout.
                raise AttributeError("'openai' client missing batches.create method")

            batch = create_fn(
                input_file_id=file_id,
                endpoint=endpoint,
                completion_window=completion_window,
            )
            log.info("Batch created – id=%s status=%s", batch.id, batch.status)
            return batch
        except Exception:  # noqa: BLE001
            attempt += 1
            log.exception("Batch create attempt %s failed", attempt)
            time.sleep(1 + attempt)
    raise RuntimeError("Unable to create batch after retries")
