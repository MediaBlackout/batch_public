"""Pytest configuration and fixtures for the batch processing system."""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock

import pytest
import boto3
from moto import mock_dynamodb


@pytest.fixture
def mock_env(monkeypatch):
    """Mock environment variables for testing."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-api-key-12345")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("BATCHJOB_TABLE", "test-batchjob")


@pytest.fixture
def sample_dynamodb_items() -> List[Dict[str, Any]]:
    """Sample DynamoDB items for testing."""
    return [
        {
            "id": "item-1",
            "timestamp": int(datetime.now(timezone.utc).timestamp()),
            "summary": "This is a test summary about macroeconomic trends.",
            "source": "test-source-1",
        },
        {
            "id": "item-2",
            "timestamp": int(datetime.now(timezone.utc).timestamp()) - 3600,
            "text": "Another test item with text field instead of summary.",
            "source": "test-source-2",
        },
        {
            "id": "item-3",
            "timestamp": int(datetime.now(timezone.utc).timestamp()) - 7200,
            "content": "Third test item using content field.",
            "source": "test-source-3",
        },
    ]


@pytest.fixture
def sample_dynamodb_items_for_embeddings() -> List[Dict[str, Any]]:
    """Sample DynamoDB items specifically for embeddings testing."""
    return [
        {
            "id": "embed-1",
            "timestamp": int(datetime.now(timezone.utc).timestamp()),
            "text": "Short text for embedding",
        },
        {
            "id": "embed-2",
            "timestamp": int(datetime.now(timezone.utc).timestamp()) - 1800,
            "text": "Another piece of text to be embedded",
        },
    ]


@pytest.fixture
def mock_openai_file_response():
    """Mock OpenAI file upload response."""
    mock_file = MagicMock()
    mock_file.id = "file-test123"
    mock_file.object = "file"
    mock_file.purpose = "batch"
    mock_file.created_at = int(datetime.now(timezone.utc).timestamp())
    return mock_file


@pytest.fixture
def mock_openai_batch_response():
    """Mock OpenAI batch creation response."""
    mock_batch = MagicMock()
    mock_batch.id = "batch_test456"
    mock_batch.object = "batch"
    mock_batch.endpoint = "/v1/chat/completions"
    mock_batch.status = "validating"
    mock_batch.input_file_id = "file-test123"
    mock_batch.completion_window = "24h"
    mock_batch.created_at = int(datetime.now(timezone.utc).timestamp())
    mock_batch.output_file_id = None
    mock_batch.error_file_id = None
    mock_batch.request_counts = {
        "total": 0,
        "completed": 0,
        "failed": 0,
    }
    return mock_batch


@pytest.fixture
def mock_completed_batch_response():
    """Mock OpenAI completed batch response."""
    mock_batch = MagicMock()
    mock_batch.id = "batch_test456"
    mock_batch.object = "batch"
    mock_batch.endpoint = "/v1/chat/completions"
    mock_batch.status = "completed"
    mock_batch.input_file_id = "file-test123"
    mock_batch.output_file_id = "file-output789"
    mock_batch.completion_window = "24h"
    mock_batch.created_at = int(datetime.now(timezone.utc).timestamp())
    mock_batch.completed_at = int(datetime.now(timezone.utc).timestamp())
    mock_batch.request_counts = {
        "total": 3,
        "completed": 3,
        "failed": 0,
    }
    return mock_batch


@pytest.fixture
def mock_batch_output_content():
    """Mock batch output file content (JSONL)."""
    return (
        '{"id": "batch_req_1", "custom_id": "row_1", "response": {"status_code": 200, "body": {"id": "chatcmpl-1", "choices": [{"message": {"content": "Analysis of item 1"}}]}}}\n'
        '{"id": "batch_req_2", "custom_id": "row_2", "response": {"status_code": 200, "body": {"id": "chatcmpl-2", "choices": [{"message": {"content": "Analysis of item 2"}}]}}}\n'
        '{"id": "batch_req_3", "custom_id": "row_3", "response": {"status_code": 200, "body": {"id": "chatcmpl-3", "choices": [{"message": {"content": "Analysis of item 3"}}]}}}\n'
    )


@pytest.fixture
def mock_dynamodb_table():
    """Create a mock DynamoDB table for testing."""
    with mock_dynamodb():
        # Create DynamoDB resource
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

        # Create a test table
        table = dynamodb.create_table(
            TableName="test-table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "timestamp-index",
                    "KeySchema": [{"AttributeName": "timestamp", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield table


@pytest.fixture
def mock_batchjob_table():
    """Create a mock batchjob DynamoDB table for testing."""
    with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

        table = dynamodb.create_table(
            TableName="test-batchjob",
            KeySchema=[
                {"AttributeName": "batch_id", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "batch_id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield table


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create a temporary directory for test outputs."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return str(output_dir)


@pytest.fixture
def temp_jsonl_dir(tmp_path):
    """Create a temporary directory for JSONL files."""
    jsonl_dir = tmp_path / "jsonl"
    jsonl_dir.mkdir()
    return str(jsonl_dir)


@pytest.fixture
def mock_openai_client(monkeypatch):
    """Mock the OpenAI client for testing."""
    mock_client = MagicMock()

    # Mock files.create
    mock_file = MagicMock()
    mock_file.id = "file-test123"
    mock_client.files.create.return_value = mock_file

    # Mock files.retrieve_content
    mock_client.files.retrieve_content.return_value = b'{"test": "content"}\n'

    # Mock batches.create
    mock_batch = MagicMock()
    mock_batch.id = "batch_test456"
    mock_batch.status = "validating"
    mock_batch.input_file_id = "file-test123"
    mock_batch.output_file_id = None
    mock_client.batches.create.return_value = mock_batch

    # Mock batches.retrieve
    completed_batch = MagicMock()
    completed_batch.id = "batch_test456"
    completed_batch.status = "completed"
    completed_batch.output_file_id = "file-output789"
    mock_client.batches.retrieve.return_value = completed_batch

    return mock_client


@pytest.fixture(autouse=True)
def reset_logger_handlers():
    """Reset logger handlers between tests to avoid duplicate logging."""
    import logging

    # Clear all handlers from batch loggers
    for name in logging.Logger.manager.loggerDict:
        if name.startswith("batch"):
            logger = logging.getLogger(name)
            logger.handlers = []

    yield

    # Cleanup after test
    for name in logging.Logger.manager.loggerDict:
        if name.startswith("batch"):
            logger = logging.getLogger(name)
            logger.handlers = []
