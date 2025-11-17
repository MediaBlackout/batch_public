"""Tests for batch submission functionality."""

import os
from unittest.mock import MagicMock, patch, mock_open

import pytest

from batch.batch_submitter import upload_file, submit_batch, OpenAIUnavailable


class TestUploadFile:
    """Tests for the upload_file function."""

    @patch("batch.batch_submitter.openai")
    def test_upload_file_success(self, mock_openai, temp_jsonl_dir, mock_env):
        """Test successful file upload."""
        # Create a test file
        test_file = os.path.join(temp_jsonl_dir, "test_batch.jsonl")
        with open(test_file, "w") as f:
            f.write('{"custom_id": "test-1", "method": "POST", "url": "/v1/chat/completions", "body": {}}\n')

        # Mock the OpenAI response
        mock_file_obj = MagicMock()
        mock_file_obj.id = "file-test123"
        mock_openai.files.create.return_value = mock_file_obj
        mock_openai.api_key = "test-key"

        result = upload_file(test_file)

        assert result.id == "file-test123"
        mock_openai.files.create.assert_called_once()

    @patch("batch.batch_submitter.openai", None)
    def test_upload_file_openai_unavailable(self, temp_jsonl_dir):
        """Test upload_file when OpenAI is unavailable."""
        test_file = os.path.join(temp_jsonl_dir, "test_batch.jsonl")
        with open(test_file, "w") as f:
            f.write("{}\n")

        with pytest.raises(OpenAIUnavailable):
            upload_file(test_file)

    @patch("batch.batch_submitter.openai")
    def test_upload_file_with_retries(self, mock_openai, temp_jsonl_dir, mock_env):
        """Test file upload with retries on failure."""
        test_file = os.path.join(temp_jsonl_dir, "test_batch.jsonl")
        with open(test_file, "w") as f:
            f.write("{}\n")

        # Mock failure then success
        mock_file_obj = MagicMock()
        mock_file_obj.id = "file-test123"
        mock_openai.api_key = "test-key"
        mock_openai.files.create.side_effect = [
            Exception("Network error"),
            mock_file_obj,
        ]

        result = upload_file(test_file, max_retries=3)

        assert result.id == "file-test123"
        assert mock_openai.files.create.call_count == 2

    @patch("batch.batch_submitter.openai")
    def test_upload_file_max_retries_exceeded(
        self, mock_openai, temp_jsonl_dir, mock_env
    ):
        """Test upload_file when max retries are exceeded."""
        test_file = os.path.join(temp_jsonl_dir, "test_batch.jsonl")
        with open(test_file, "w") as f:
            f.write("{}\n")

        mock_openai.api_key = "test-key"
        mock_openai.files.create.side_effect = Exception("Network error")

        with pytest.raises(RuntimeError, match="Failed to upload"):
            upload_file(test_file, max_retries=2)

        assert mock_openai.files.create.call_count == 2


class TestSubmitBatch:
    """Tests for the submit_batch function."""

    @patch("batch.batch_submitter.openai")
    def test_submit_batch_success(self, mock_openai, mock_env):
        """Test successful batch submission."""
        mock_batch = MagicMock()
        mock_batch.id = "batch_test123"
        mock_batch.status = "validating"
        mock_openai.api_key = "test-key"
        mock_openai.batches.create.return_value = mock_batch

        result = submit_batch(file_id="file-test123")

        assert result.id == "batch_test123"
        assert result.status == "validating"
        mock_openai.batches.create.assert_called_once_with(
            input_file_id="file-test123",
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )

    @patch("batch.batch_submitter.openai")
    def test_submit_batch_custom_endpoint(self, mock_openai, mock_env):
        """Test batch submission with custom endpoint."""
        mock_batch = MagicMock()
        mock_batch.id = "batch_embed123"
        mock_batch.status = "validating"
        mock_openai.api_key = "test-key"
        mock_openai.batches.create.return_value = mock_batch

        result = submit_batch(file_id="file-test123", endpoint="/v1/embeddings")

        mock_openai.batches.create.assert_called_once_with(
            input_file_id="file-test123",
            endpoint="/v1/embeddings",
            completion_window="24h",
        )

    @patch("batch.batch_submitter.openai", None)
    def test_submit_batch_openai_unavailable(self):
        """Test submit_batch when OpenAI is unavailable."""
        with pytest.raises(OpenAIUnavailable):
            submit_batch(file_id="file-test123")

    @patch("batch.batch_submitter.openai")
    def test_submit_batch_with_retries(self, mock_openai, mock_env):
        """Test batch submission with retries."""
        mock_batch = MagicMock()
        mock_batch.id = "batch_test123"
        mock_batch.status = "validating"
        mock_openai.api_key = "test-key"
        mock_openai.batches.create.side_effect = [
            Exception("API error"),
            mock_batch,
        ]

        result = submit_batch(file_id="file-test123", max_retries=3)

        assert result.id == "batch_test123"
        assert mock_openai.batches.create.call_count == 2

    @patch("batch.batch_submitter.openai")
    def test_submit_batch_fallback_to_beta_namespace(self, mock_openai, mock_env):
        """Test batch submission falls back to beta namespace."""
        # Simulate older SDK version without stable batches namespace
        del mock_openai.batches

        mock_batch = MagicMock()
        mock_batch.id = "batch_test123"
        mock_batch.status = "validating"
        mock_openai.api_key = "test-key"
        mock_openai.beta.batches.create.return_value = mock_batch

        result = submit_batch(file_id="file-test123")

        assert result.id == "batch_test123"
        mock_openai.beta.batches.create.assert_called_once()

    @patch("batch.batch_submitter.openai")
    def test_submit_batch_max_retries_exceeded(self, mock_openai, mock_env):
        """Test submit_batch when max retries are exceeded."""
        mock_openai.api_key = "test-key"
        mock_openai.batches.create.side_effect = Exception("API error")

        with pytest.raises(RuntimeError, match="Unable to create batch"):
            submit_batch(file_id="file-test123", max_retries=2)

        assert mock_openai.batches.create.call_count == 2
