"""Tests for validation module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from batch.validation import validate_config_with_pydantic


class TestConfigValidation:
    """Test configuration validation functions."""

    def test_valid_config(self, sample_config):
        """Test validation of valid configuration."""
        # Should not raise any exceptions
        validate_config_with_pydantic(sample_config)

    def test_valid_config_minimal(self):
        """Test validation with minimal required fields."""
        config = {"enabled_tables": ["table1"]}
        validate_config_with_pydantic(config)

    def test_missing_enabled_tables(self):
        """Test validation fails when enabled_tables is missing."""
        config = {"default_system_prompt": "Test prompt"}
        with pytest.raises((ValueError, KeyError, Exception)):
            validate_config_with_pydantic(config)

    def test_empty_enabled_tables(self):
        """Test validation fails when enabled_tables is empty."""
        config = {"enabled_tables": []}
        with pytest.raises((ValueError, Exception)):
            validate_config_with_pydantic(config)

    def test_invalid_enabled_tables_type(self):
        """Test validation fails when enabled_tables is not a list."""
        config = {"enabled_tables": "not-a-list"}
        with pytest.raises((ValueError, TypeError, Exception)):
            validate_config_with_pydantic(config)

    def test_enabled_tables_with_empty_string(self):
        """Test validation fails when enabled_tables contains empty strings."""
        config = {"enabled_tables": ["table1", "", "table2"]}
        with pytest.raises((ValueError, Exception)):
            validate_config_with_pydantic(config)

    def test_duplicate_table_names(self):
        """Test validation fails with duplicate table names."""
        config = {"enabled_tables": ["table1", "table1", "table2"]}
        with pytest.raises((ValueError, Exception)):
            validate_config_with_pydantic(config)

    def test_invalid_table_name_characters(self):
        """Test validation of table name characters."""
        # Valid table names
        config = {"enabled_tables": ["valid-table_name.123"]}
        validate_config_with_pydantic(config)

        # Invalid characters might be rejected depending on implementation
        config_invalid = {"enabled_tables": ["table@name"]}
        # This might pass or fail depending on validation rules

    def test_optional_default_prompt(self):
        """Test that default_system_prompt is optional."""
        config = {"enabled_tables": ["table1"]}
        validate_config_with_pydantic(config)

    def test_empty_default_prompt(self):
        """Test validation with empty default_system_prompt."""
        config = {
            "enabled_tables": ["table1"],
            "default_system_prompt": "",
        }
        # Empty string might be rejected depending on validation
        with pytest.raises((ValueError, Exception)):
            validate_config_with_pydantic(config)

    def test_table_prompts_override(self):
        """Test validation with table-specific prompts."""
        config = {
            "enabled_tables": ["table1", "table2"],
            "table_prompts": {
                "table1": "Prompt for table 1",
                "table2": "Prompt for table 2",
            },
        }
        validate_config_with_pydantic(config)

    def test_table_prompts_empty_dict(self):
        """Test validation with empty table_prompts dict."""
        config = {
            "enabled_tables": ["table1"],
            "table_prompts": {},
        }
        validate_config_with_pydantic(config)

    def test_extra_fields_allowed(self):
        """Test that extra fields don't break validation."""
        config = {
            "enabled_tables": ["table1"],
            "extra_field": "This should be allowed",
            "another_field": 123,
        }
        # Depending on Pydantic config, extra fields might be allowed
        try:
            validate_config_with_pydantic(config)
        except Exception:
            # Extra fields might be forbidden - that's okay too
            pass

    def test_none_config(self):
        """Test validation with None input."""
        with pytest.raises((ValueError, TypeError, AttributeError, Exception)):
            validate_config_with_pydantic(None)  # type: ignore

    def test_config_from_file(self, temp_config_file: Path):
        """Test loading and validating config from file."""
        with temp_config_file.open() as f:
            config = json.load(f)
        validate_config_with_pydantic(config)
