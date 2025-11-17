"""Tests for dynamo_fetcher module."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from batch.dynamo_fetcher import fetch_recent


class TestFetchRecent:
    """Test DynamoDB data fetching."""

    def test_fetch_with_zero_hours(self):
        """Test that zero hours returns empty list without querying."""
        result = fetch_recent(hours=0, table_name="test_table")
        assert result == []

    def test_fetch_with_negative_hours(self):
        """Test that negative hours returns empty list."""
        result = fetch_recent(hours=-1, table_name="test_table")
        assert result == []

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_basic(self, mock_boto3, sample_dynamodb_items):
        """Test basic fetch operation with mocked DynamoDB."""
        # Setup mock
        mock_table = MagicMock()
        mock_table.scan.return_value = {
            "Items": sample_dynamodb_items[:2],  # First 2 items
            "Count": 2,
        }
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        # Call function
        result = fetch_recent(hours=24, table_name="test_table")

        # Verify
        assert len(result) == 2
        assert result[0]["id"] == "test-item-1"
        mock_boto3.resource.assert_called_once_with("dynamodb", region_name="us-east-1")

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_with_pagination(self, mock_boto3, sample_dynamodb_items):
        """Test fetch with paginated results."""
        mock_table = MagicMock()
        # First scan returns items with LastEvaluatedKey
        mock_table.scan.side_effect = [
            {
                "Items": sample_dynamodb_items[:2],
                "LastEvaluatedKey": {"id": "test-item-2"},
            },
            {
                "Items": sample_dynamodb_items[2:],
                "Count": 2,
            },
        ]
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        # Call function
        result = fetch_recent(hours=24, table_name="test_table")

        # Verify pagination occurred
        assert mock_table.scan.call_count == 2
        assert len(result) == 4

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_filters_old_items(self, mock_boto3, sample_dynamodb_items):
        """Test that items older than cutoff are filtered."""
        current_time = int(time.time())
        old_timestamp = current_time - (25 * 3600)  # 25 hours ago

        items = [
            {
                "id": "new-item",
                "timestamp": current_time - 3600,  # 1 hour ago
                "summary": "Recent item",
            },
            {
                "id": "old-item",
                "timestamp": old_timestamp,
                "summary": "Old item",
            },
        ]

        mock_table = MagicMock()
        mock_table.scan.return_value = {"Items": items}
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        # Fetch items from last 12 hours
        result = fetch_recent(hours=12, table_name="test_table")

        # Should only return the new item
        assert len(result) == 1
        assert result[0]["id"] == "new-item"

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_deduplication_by_url(self, mock_boto3):
        """Test deduplication using URL field."""
        duplicate_items = [
            {
                "id": "item-1",
                "timestamp": 1700000000,
                "summary": "Article 1",
                "url": "https://example.com/article",
            },
            {
                "id": "item-2",
                "timestamp": 1700000100,
                "summary": "Article 2 (duplicate URL)",
                "url": "https://example.com/article",  # Same URL
            },
            {
                "id": "item-3",
                "timestamp": 1700000200,
                "summary": "Article 3",
                "url": "https://example.com/different-article",
            },
        ]

        mock_table = MagicMock()
        mock_table.scan.return_value = {"Items": duplicate_items}
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        result = fetch_recent(hours=24, table_name="test_table")

        # Should deduplicate based on URL
        assert len(result) == 2
        urls = [item["url"] for item in result]
        assert len(set(urls)) == 2  # All unique URLs

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_filters_items_without_text(self, mock_boto3):
        """Test that items without extractable text are filtered."""
        items = [
            {
                "id": "has-text",
                "timestamp": 1700000000,
                "summary": "This item has text",
            },
            {
                "id": "no-text",
                "timestamp": 1700000100,
                # No text fields
            },
            {
                "id": "empty-text",
                "timestamp": 1700000200,
                "summary": "",  # Empty text
            },
        ]

        mock_table = MagicMock()
        mock_table.scan.return_value = {"Items": items}
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        result = fetch_recent(hours=24, table_name="test_table")

        # Should only return item with text
        assert len(result) == 1
        assert result[0]["id"] == "has-text"

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_retry_on_failure(self, mock_boto3):
        """Test retry logic on DynamoDB errors."""
        from botocore.exceptions import ClientError

        mock_table = MagicMock()
        # First two calls fail, third succeeds
        mock_table.scan.side_effect = [
            ClientError({"Error": {"Code": "500", "Message": "Server error"}}, "Scan"),
            ClientError({"Error": {"Code": "500", "Message": "Server error"}}, "Scan"),
            {"Items": [{"id": "item-1", "timestamp": 1700000000, "summary": "Test"}]},
        ]
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        result = fetch_recent(hours=24, table_name="test_table")

        # Should succeed after retries
        assert len(result) == 1
        assert mock_table.scan.call_count == 3

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_fails_after_max_retries(self, mock_boto3):
        """Test that fetch fails after max retries."""
        from botocore.exceptions import ClientError

        mock_table = MagicMock()
        # All calls fail
        mock_table.scan.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Server error"}}, "Scan"
        )
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        with pytest.raises(ClientError):
            fetch_recent(hours=24, table_name="test_table")

        # Should have tried 3 times
        assert mock_table.scan.call_count == 3

    @patch("batch.dynamo_fetcher.boto3")
    def test_fetch_with_custom_region(self, mock_boto3):
        """Test fetch with custom AWS region."""
        mock_table = MagicMock()
        mock_table.scan.return_value = {"Items": []}
        mock_dynamodb = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_boto3.resource.return_value = mock_dynamodb

        fetch_recent(hours=24, table_name="test_table", region_name="us-west-2")

        mock_boto3.resource.assert_called_with("dynamodb", region_name="us-west-2")
