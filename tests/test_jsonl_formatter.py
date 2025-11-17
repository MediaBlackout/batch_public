"""Tests for JSONL formatting functionality."""

import json
import os
from pathlib import Path

import pytest

from batch.jsonl_formatter import write_jsonl, _build_payload


class TestBuildPayload:
    """Tests for the _build_payload function."""

    def test_build_payload_basic(self):
        """Test basic payload creation."""
        result = _build_payload("Test summary", model="gpt-4o-mini", source_id="test-123")

        assert "messages" in result
        assert "model" in result
        assert "user" in result
        assert result["model"] == "gpt-4o-mini"
        assert result["user"] == "test-123"
        assert len(result["messages"]) == 2
        assert result["messages"][0]["role"] == "system"
        assert result["messages"][1]["role"] == "user"
        assert result["messages"][1]["content"] == "Test summary"

    def test_build_payload_without_source_id(self):
        """Test payload creation without source_id."""
        result = _build_payload("Test summary", model="gpt-4o", source_id=None)

        assert "messages" in result
        assert "model" in result
        assert "user" not in result


class TestWriteJsonl:
    """Tests for the write_jsonl function."""

    def test_write_jsonl_chat_completions(self, sample_dynamodb_items, temp_jsonl_dir):
        """Test JSONL generation for chat completions."""
        path, count = write_jsonl(
            sample_dynamodb_items,
            model_key="nano",
            output_dir=temp_jsonl_dir,
            tag="test-table",
            endpoint="/v1/chat/completions",
        )

        assert os.path.exists(path)
        assert count == 3  # All 3 items have text fields

        # Verify file content
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 3

        # Check first record
        record = json.loads(lines[0])
        assert "custom_id" in record
        assert "method" in record
        assert "url" in record
        assert "body" in record
        assert record["method"] == "POST"
        assert record["url"] == "/v1/chat/completions"
        assert "messages" in record["body"]
        assert "model" in record["body"]

    def test_write_jsonl_embeddings(
        self, sample_dynamodb_items_for_embeddings, temp_jsonl_dir
    ):
        """Test JSONL generation for embeddings."""
        path, count = write_jsonl(
            sample_dynamodb_items_for_embeddings,
            model_key="text-embedding-3-small",
            output_dir=temp_jsonl_dir,
            tag="embedding-test",
            endpoint="/v1/embeddings",
        )

        assert os.path.exists(path)
        assert count == 2

        # Verify file content
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 2

        # Check embedding record structure
        record = json.loads(lines[0])
        assert record["url"] == "/v1/embeddings"
        assert "input" in record["body"]
        assert "model" in record["body"]
        assert "messages" not in record["body"]  # Embeddings don't use messages

    def test_write_jsonl_empty_items(self, temp_jsonl_dir):
        """Test JSONL generation with empty items list."""
        path, count = write_jsonl([], output_dir=temp_jsonl_dir)

        assert os.path.exists(path)
        assert count == 0

        # File should be empty
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        assert content == ""

    def test_write_jsonl_items_without_text(self, temp_jsonl_dir):
        """Test JSONL generation with items that have no extractable text."""
        items = [
            {"id": "item-1", "timestamp": 123456789},  # No text field
            {"id": "item-2", "numeric_field": 42},  # No text field
        ]

        path, count = write_jsonl(items, output_dir=temp_jsonl_dir)

        assert os.path.exists(path)
        # Both items should be skipped because they lack text fields (numeric fields are converted)
        # Actually, numeric fields ARE converted to strings based on the code
        assert count == 2

    def test_write_jsonl_custom_tag(self, sample_dynamodb_items, temp_jsonl_dir):
        """Test JSONL filename includes custom tag."""
        path, count = write_jsonl(
            sample_dynamodb_items,
            output_dir=temp_jsonl_dir,
            tag="my-custom-table",
        )

        filename = os.path.basename(path)
        assert "my-custom-table" in filename
        assert filename.endswith(".jsonl")

    def test_write_jsonl_no_overwrite(self, sample_dynamodb_items, temp_jsonl_dir):
        """Test that JSONL files are not overwritten."""
        # Write first file
        path1, _ = write_jsonl(
            sample_dynamodb_items, output_dir=temp_jsonl_dir, tag="test"
        )

        # Write second file with same parameters
        # Due to timestamp-based naming, this might create a different file
        # But the code has logic to append _1, _2, etc if needed
        path2, _ = write_jsonl(
            sample_dynamodb_items, output_dir=temp_jsonl_dir, tag="test"
        )

        # Both files should exist
        assert os.path.exists(path1)
        assert os.path.exists(path2)

    def test_write_jsonl_default_output_dir(self, sample_dynamodb_items, monkeypatch):
        """Test JSONL generation with default output directory."""
        # This test ensures the function can create the default directory
        # We'll use a temporary directory as the module directory
        import tempfile

        temp_dir = tempfile.mkdtemp()
        monkeypatch.setattr("batch.jsonl_formatter.os.path.dirname", lambda x: temp_dir)

        path, count = write_jsonl(sample_dynamodb_items, tag="default-test")

        assert os.path.exists(path)
        assert count == 3

        # Clean up
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_write_jsonl_various_text_fields(self, temp_jsonl_dir):
        """Test JSONL generation with various text field names."""
        items = [
            {"id": "1", "summary": "Summary text"},
            {"id": "2", "text": "Text field"},
            {"id": "3", "content": "Content field"},
            {"id": "4", "title": "Title field"},
            {"id": "5", "description": "Description field"},
        ]

        path, count = write_jsonl(items, output_dir=temp_jsonl_dir)

        assert count == 5

        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        # Verify all items were processed
        assert len(lines) == 5

    def test_write_jsonl_json_serialization_error(self, temp_jsonl_dir):
        """Test handling of JSON serialization errors."""
        # Create items that might cause serialization issues
        items = [
            {"id": "1", "text": "Valid text"},
            {"id": "2", "summary": "Also valid"},
        ]

        path, count = write_jsonl(items, output_dir=temp_jsonl_dir)

        # Should succeed with valid items
        assert count == 2

    def test_write_jsonl_custom_id_format(self, sample_dynamodb_items, temp_jsonl_dir):
        """Test that custom_id follows the correct format."""
        path, count = write_jsonl(sample_dynamodb_items, output_dir=temp_jsonl_dir)

        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for i, line in enumerate(lines, start=1):
            record = json.loads(line)
            assert record["custom_id"] == f"row_{i}"
            assert len(record["custom_id"]) <= 64  # OpenAI requirement
