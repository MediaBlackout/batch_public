"""Pytest configuration and shared fixtures."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sample_dynamodb_items() -> list[Dict[str, Any]]:
    """Return sample DynamoDB items for testing."""
    return [
        {
            "id": "test-item-1",
            "timestamp": 1700000000,
            "summary": "This is a test summary about macroeconomic trends.",
            "url": "https://example.com/article-1",
        },
        {
            "id": "test-item-2",
            "timestamp": 1700001000,
            "text": "Another test article about market analysis.",
            "link": "https://example.com/article-2",
        },
        {
            "id": "test-item-3",
            "timestamp": "1700002000",  # String timestamp
            "content": "Test content with string timestamp.",
            "source_url": "https://example.com/article-3",
        },
        {
            "id": "test-item-4",
            "created_at": "2024-01-15T10:30:00Z",  # ISO timestamp
            "description": "Test item with ISO timestamp.",
        },
    ]


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Return sample batch configuration."""
    return {
        "enabled_tables": ["test_table_1", "test_table_2"],
        "default_system_prompt": "You are a test AI assistant.",
        "table_prompts": {
            "test_table_1": "Custom prompt for table 1",
        },
    }


@pytest.fixture
def mock_dynamodb_table():
    """Return a mock DynamoDB table."""
    mock_table = MagicMock()
    mock_table.scan.return_value = {
        "Items": [],
        "Count": 0,
        "ScannedCount": 0,
    }
    return mock_table


@pytest.fixture
def mock_openai_client():
    """Return a mock OpenAI client."""
    mock_client = MagicMock()
    mock_file = MagicMock()
    mock_file.id = "file-test123"
    mock_client.files.create.return_value = mock_file

    mock_batch = MagicMock()
    mock_batch.id = "batch-test123"
    mock_batch.status = "validating"
    mock_client.batches.create.return_value = mock_batch

    return mock_client


@pytest.fixture
def temp_config_file(tmp_path: Path, sample_config: Dict[str, Any]) -> Path:
    """Create a temporary config file for testing."""
    config_file = tmp_path / "batch_config.json"
    with config_file.open("w") as f:
        json.dump(sample_config, f, indent=2)
    return config_file


@pytest.fixture
def temp_env_file(tmp_path: Path) -> Path:
    """Create a temporary .env file for testing."""
    env_file = tmp_path / ".env"
    env_content = """
OPENAI_API_KEY=test-key-123
AWS_ACCESS_KEY_ID=test-access-key
AWS_SECRET_ACCESS_KEY=test-secret-key
AWS_REGION=us-east-1
"""
    env_file.write_text(env_content.strip())
    return env_file


@pytest.fixture(autouse=True)
def reset_env():
    """Reset environment variables before each test."""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)
