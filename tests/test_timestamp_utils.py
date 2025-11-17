"""Tests for timestamp_utils module."""

from __future__ import annotations

from decimal import Decimal

import pytest

from batch.timestamp_utils import ts_to_int


class TestTimestampConversion:
    """Test timestamp parsing and conversion functions."""

    def test_int_timestamp(self):
        """Test conversion of integer timestamps."""
        assert ts_to_int(1700000000) == 1700000000
        assert ts_to_int(0) == 0

    def test_float_timestamp(self):
        """Test conversion of float timestamps."""
        assert ts_to_int(1700000000.5) == 1700000000
        assert ts_to_int(1700000000.9) == 1700000000

    def test_decimal_timestamp(self):
        """Test conversion of Decimal timestamps (from DynamoDB)."""
        assert ts_to_int(Decimal("1700000000")) == 1700000000
        assert ts_to_int(Decimal("1700000000.5")) == 1700000000

    def test_string_timestamp_seconds(self):
        """Test conversion of string timestamps in seconds."""
        assert ts_to_int("1700000000") == 1700000000
        assert ts_to_int("  1700000000  ") == 1700000000  # With whitespace

    def test_string_timestamp_milliseconds(self):
        """Test conversion of string timestamps in milliseconds."""
        # Milliseconds should be converted to seconds
        assert ts_to_int("1700000000000") == 1700000000
        assert ts_to_int("1700000000999") == 1700000000

    def test_iso_timestamp(self):
        """Test conversion of ISO-8601 formatted timestamps."""
        assert ts_to_int("2024-01-15T10:30:00Z") is not None
        assert ts_to_int("2024-01-15T10:30:00+00:00") is not None
        assert ts_to_int("2024-01-15 10:30:00") is not None

    def test_est_timezone(self):
        """Test conversion of timestamps with EST/EDT timezone."""
        # Should handle EST/EDT abbreviations
        result = ts_to_int("2024-01-15 10:30:00 EST")
        assert result is not None
        assert isinstance(result, int)

    def test_invalid_timestamp(self):
        """Test handling of invalid timestamps."""
        assert ts_to_int(None) is None
        assert ts_to_int("") is None
        assert ts_to_int("invalid") is None
        assert ts_to_int("not-a-date") is None

    def test_negative_timestamp(self):
        """Test handling of negative timestamps."""
        # Negative timestamps are valid (before epoch)
        assert ts_to_int(-1) == -1
        assert ts_to_int("-100") == -100

    def test_dict_timestamp(self):
        """Test handling of invalid types (dict)."""
        assert ts_to_int({"time": 1700000000}) is None

    def test_list_timestamp(self):
        """Test handling of invalid types (list)."""
        assert ts_to_int([1700000000]) is None
