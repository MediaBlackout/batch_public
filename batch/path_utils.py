"""Path utilities for standardized file path handling.

This module provides centralized path management using pathlib.Path
for consistent, cross-platform file operations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

# Base directory for the batch module
BATCH_DIR = Path(__file__).parent

# Common directories
LOGS_DIR = BATCH_DIR / "logs"
JSONL_DIR = BATCH_DIR / "jsonl"
JSONL_TEST_DIR = BATCH_DIR / "jsonl_test"
OUTPUT_DIR = BATCH_DIR / "output"

# Configuration and state files
CONFIG_FILE = BATCH_DIR / "batch_config.json"
SCHEMA_FILE = BATCH_DIR / "batch_config.schema.json"
STATUS_FILE = BATCH_DIR / "batch_status.json"
WATERMARK_FILE = BATCH_DIR / "batch_watermark.json"


def ensure_dir(path: Path | str, parents: bool = True, exist_ok: bool = True) -> Path:
    """Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path to ensure exists
        parents: Create parent directories if needed
        exist_ok: Don't raise error if directory already exists

    Returns:
        Path object for the directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=parents, exist_ok=exist_ok)
    return path_obj


def get_batch_file_path(
    base_dir: Optional[Path | str] = None,
    tag: Optional[str] = None,
    timestamp: Optional[str] = None,
    extension: str = "jsonl",
    counter: Optional[int] = None,
) -> Path:
    """Generate a batch file path with optional tag and timestamp.

    Args:
        base_dir: Base directory (defaults to JSONL_DIR)
        tag: Optional tag to include in filename
        timestamp: Optional timestamp string (defaults to current time if not provided)
        extension: File extension (default: "jsonl")
        counter: Optional counter for collision avoidance

    Returns:
        Path object for the batch file
    """
    from datetime import datetime, timezone

    if base_dir is None:
        base_dir = JSONL_DIR

    base_path = Path(base_dir)

    if timestamp is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

    if tag:
        # Sanitize tag: only alphanumeric, dash, underscore
        safe_tag = "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in tag)[:32]
        if counter is not None:
            filename = f"batch_{safe_tag}_{timestamp}_{counter}.{extension}"
        else:
            filename = f"batch_{safe_tag}_{timestamp}.{extension}"
    else:
        if counter is not None:
            filename = f"batch_{timestamp}_{counter}.{extension}"
        else:
            filename = f"batch_{timestamp}.{extension}"

    return base_path / filename


def get_unique_path(base_path: Path | str) -> Path:
    """Get a unique file path by adding a counter if the path exists.

    Args:
        base_path: Base file path to check

    Returns:
        Unique Path object (original or with counter suffix)
    """
    path = Path(base_path)

    if not path.exists():
        return path

    # Extract stem and suffix
    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    counter = 1
    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1


def atomic_write(path: Path | str, content: str, encoding: str = "utf-8") -> None:
    """Write content to a file atomically using a temporary file.

    Args:
        path: Destination file path
        content: Content to write
        encoding: File encoding (default: utf-8)
    """
    import os

    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    # Write to temporary file
    tmp_path.write_text(content, encoding=encoding)

    # Atomic replace
    tmp_path.replace(path)
