"""State management abstraction for batch processing.

This module provides a flexible state management interface that supports
both local file-based storage and distributed backends (DynamoDB, Redis).

For distributed deployments, use DynamoDBStateManager to ensure consistency
across multiple workers.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Optional

from batch.logger import get_logger

log = get_logger(__name__)


class StateManager(ABC):
    """Abstract base class for state management backends."""

    @abstractmethod
    def load_status(self) -> Dict[str, Any]:
        """Load batch status data."""
        pass

    @abstractmethod
    def save_status(self, data: Dict[str, Any]) -> None:
        """Save batch status data."""
        pass

    @abstractmethod
    def load_watermark(self) -> Dict[str, int]:
        """Load high-water-mark data."""
        pass

    @abstractmethod
    def save_watermark(self, data: Dict[str, int]) -> None:
        """Save high-water-mark data."""
        pass

    @abstractmethod
    @contextmanager
    def lock(self, key: str, timeout: float = 30.0):
        """Acquire a distributed lock for the given key.

        Args:
            key: Lock identifier
            timeout: Maximum time to wait for lock (seconds)

        Yields:
            None when lock is acquired

        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        pass


class FileStateManager(StateManager):
    """File-based state management with file locking for local deployments.

    Provides atomic writes and file-based locking to prevent race conditions
    when multiple processes access the same state files.
    """

    def __init__(self, status_file: str, watermark_file: str):
        """Initialize file-based state manager.

        Args:
            status_file: Path to batch status JSON file
            watermark_file: Path to watermark JSON file
        """
        self.status_file = status_file
        self.watermark_file = watermark_file

    def load_status(self) -> Dict[str, Any]:
        """Load batch status from file."""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, "r", encoding="utf-8") as fh:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                    try:
                        return json.load(fh)
                    finally:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                log.exception("Unable to read %s", self.status_file)
        return {}

    def save_status(self, data: Dict[str, Any]) -> None:
        """Save batch status to file atomically."""
        tmp_path = self.status_file + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)  # Exclusive lock for writing
                try:
                    json.dump(data, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())  # Ensure written to disk
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            os.replace(tmp_path, self.status_file)
        except Exception:
            log.exception("Unable to persist status to %s", self.status_file)

    def load_watermark(self) -> Dict[str, int]:
        """Load watermark data from file."""
        if os.path.exists(self.watermark_file):
            try:
                with open(self.watermark_file, "r", encoding="utf-8") as fh:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_SH)
                    try:
                        data = json.load(fh)
                        if isinstance(data, dict):
                            return {str(k): int(v) for k, v in data.items() if isinstance(v, (int, float, str))}
                    finally:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except Exception:
                log.exception("Unable to read %s", self.watermark_file)
        return {}

    def save_watermark(self, data: Dict[str, int]) -> None:
        """Save watermark data to file atomically."""
        tmp_path = self.watermark_file + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            os.replace(tmp_path, self.watermark_file)
        except Exception:
            log.exception("Unable to persist watermark to %s", self.watermark_file)

    @contextmanager
    def lock(self, key: str, timeout: float = 30.0):
        """Acquire a file-based lock.

        Uses flock for inter-process locking on the same machine.
        Not suitable for distributed deployments.
        """
        lock_file = f"{self.status_file}.{key}.lock"
        start_time = time.time()

        while True:
            try:
                fh = open(lock_file, "w", encoding="utf-8")
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    yield
                finally:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    fh.close()
                    try:
                        os.remove(lock_file)
                    except Exception:
                        pass
                return
            except BlockingIOError:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Could not acquire lock for '{key}' within {timeout}s")
                time.sleep(0.1)


class DynamoDBStateManager(StateManager):
    """DynamoDB-based state management for distributed deployments.

    Uses DynamoDB for atomic operations and conditional updates to prevent
    race conditions across multiple workers.
    """

    def __init__(
        self,
        table_name: str,
        status_key: str = "batch_status",
        watermark_key: str = "batch_watermark",
        region_name: Optional[str] = None,
    ):
        """Initialize DynamoDB state manager.

        Args:
            table_name: DynamoDB table name for state storage
            status_key: Partition key for status data
            watermark_key: Partition key for watermark data
            region_name: AWS region (defaults to AWS_REGION env var)
        """
        import boto3

        self.table_name = table_name
        self.status_key = status_key
        self.watermark_key = watermark_key

        if region_name is None:
            region_name = os.getenv("AWS_REGION", "us-east-1")

        dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.table = dynamodb.Table(table_name)

    def load_status(self) -> Dict[str, Any]:
        """Load batch status from DynamoDB."""
        try:
            response = self.table.get_item(Key={"id": self.status_key})
            if "Item" in response:
                item = response["Item"]
                # Remove the partition key from returned data
                item.pop("id", None)
                return json.loads(item.get("data", "{}"))
            return {}
        except Exception:
            log.exception("Unable to load status from DynamoDB")
            return {}

    def save_status(self, data: Dict[str, Any]) -> None:
        """Save batch status to DynamoDB."""
        try:
            self.table.put_item(
                Item={
                    "id": self.status_key,
                    "data": json.dumps(data),
                    "updated_at": int(time.time()),
                }
            )
        except Exception:
            log.exception("Unable to save status to DynamoDB")

    def load_watermark(self) -> Dict[str, int]:
        """Load watermark data from DynamoDB."""
        try:
            response = self.table.get_item(Key={"id": self.watermark_key})
            if "Item" in response:
                item = response["Item"]
                data = json.loads(item.get("data", "{}"))
                return {str(k): int(v) for k, v in data.items() if isinstance(v, (int, float, str))}
            return {}
        except Exception:
            log.exception("Unable to load watermark from DynamoDB")
            return {}

    def save_watermark(self, data: Dict[str, int]) -> None:
        """Save watermark data to DynamoDB."""
        try:
            self.table.put_item(
                Item={
                    "id": self.watermark_key,
                    "data": json.dumps(data),
                    "updated_at": int(time.time()),
                }
            )
        except Exception:
            log.exception("Unable to save watermark to DynamoDB")

    @contextmanager
    def lock(self, key: str, timeout: float = 30.0):
        """Acquire a distributed lock using DynamoDB conditional writes.

        Uses a dedicated lock item with TTL for automatic cleanup.
        """
        lock_id = f"lock_{key}"
        lock_token = f"{os.getpid()}_{time.time()}"
        start_time = time.time()

        while True:
            try:
                # Try to acquire lock with conditional write
                self.table.put_item(
                    Item={
                        "id": lock_id,
                        "owner": lock_token,
                        "acquired_at": int(time.time()),
                        "ttl": int(time.time()) + 300,  # Auto-expire after 5 minutes
                    },
                    ConditionExpression="attribute_not_exists(id)",
                )
                break
            except Exception as e:
                if "ConditionalCheckFailedException" in str(e):
                    if time.time() - start_time > timeout:
                        raise TimeoutError(f"Could not acquire lock for '{key}' within {timeout}s")
                    time.sleep(0.1)
                    continue
                raise

        try:
            yield
        finally:
            # Release lock
            try:
                self.table.delete_item(
                    Key={"id": lock_id},
                    ConditionExpression="owner = :token",
                    ExpressionAttributeValues={":token": lock_token},
                )
            except Exception:
                log.warning("Failed to release lock for '%s'", key)


def create_state_manager(
    backend: str = "file",
    status_file: Optional[str] = None,
    watermark_file: Optional[str] = None,
    dynamodb_table: Optional[str] = None,
) -> StateManager:
    """Factory function to create appropriate state manager.

    Args:
        backend: Backend type ("file" or "dynamodb")
        status_file: Path to status file (for file backend)
        watermark_file: Path to watermark file (for file backend)
        dynamodb_table: DynamoDB table name (for dynamodb backend)

    Returns:
        StateManager instance

    Raises:
        ValueError: If required parameters are missing for chosen backend
    """
    if backend == "file":
        if not status_file or not watermark_file:
            raise ValueError("File backend requires status_file and watermark_file")
        return FileStateManager(status_file, watermark_file)

    elif backend == "dynamodb":
        if not dynamodb_table:
            raise ValueError("DynamoDB backend requires dynamodb_table")
        return DynamoDBStateManager(dynamodb_table)

    else:
        raise ValueError(f"Unknown backend: {backend}")
