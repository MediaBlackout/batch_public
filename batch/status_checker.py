"""Poll a batch until finished and download its output."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

# Optional *python-dotenv* – skip gracefully if absent to support minimal
# execution environments where variables are already exported.
try:
    from dotenv import load_dotenv  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – dev convenience only

    def load_dotenv(*_args, **_kwargs):  # type: ignore
        return False

from batch.logger import get_logger


log = get_logger(__name__)

load_dotenv()


try:
    import openai

    openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:  # pragma: no cover
    openai = None  # type: ignore


def _require_openai() -> None:
    if openai is None or not openai.api_key:
        raise RuntimeError("OpenAI library not available or API key missing")


def wait_for_completion(batch_id: str, *, poll_every: int = 60):
    """Block until batch *batch_id* is completed or failed. Return the batch."""

    _require_openai()

    # Newer openai-python versions moved *batches* out of the experimental
    # ``beta`` namespace.  Support both layouts so the script works regardless
    # of the installed client version.

    def _retrieve(b_id: str):
        if hasattr(openai, "batches") and hasattr(openai.batches, "retrieve"):
            return openai.batches.retrieve(b_id)  # type: ignore[attr-defined]
        if (
            hasattr(openai, "beta")
            and hasattr(openai.beta, "batches")
            and hasattr(openai.beta.batches, "retrieve")
        ):
            return openai.beta.batches.retrieve(b_id)  # type: ignore[attr-defined]
        raise AttributeError("'openai' client missing batches.retrieve method")

    while True:
        batch = _retrieve(batch_id)
        status = batch.status
        log.info("Batch %s status = %s", batch_id, status)

        if status in {"completed", "failed", "expired", "cancelled"}:
            return batch
        time.sleep(poll_every)


def download_results(output_file_id: str, *, output_dir: str | None = None) -> str:
    """Download *output_file_id* and write it to *output_dir*, returning the path."""

    _require_openai()

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    content = openai.files.retrieve_content(output_file_id)  # type: ignore[attr-defined]

    # The OpenAI client may return the file contents as either *bytes* (newer
    # SDK versions) or *str* (older versions / backward-compat path).  Ensure
    # we always write *bytes* to disk so the file size is reported correctly
    # and we avoid encoding issues on non-UTF-8 systems.
    if isinstance(content, str):
        content = content.encode("utf-8")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"batch_output_{timestamp}.jsonl")

    with open(path, "wb") as fh:
        fh.write(content)

    log.info("Batch output saved to %s", path)
    return path
