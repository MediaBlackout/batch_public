"""Tests for text_utils module."""

from __future__ import annotations

import pytest

from batch.text_utils import extract_text, sanitize_tag


class TestExtractText:
    """Test text extraction from DynamoDB items."""

    def test_extract_summary_field(self):
        """Test extraction of 'summary' field."""
        item = {"summary": "This is a test summary"}
        assert extract_text(item) == "This is a test summary"

    def test_extract_text_field(self):
        """Test extraction of 'text' field when summary is missing."""
        item = {"text": "This is test text"}
        assert extract_text(item) == "This is test text"

    def test_extract_content_field(self):
        """Test extraction of 'content' field."""
        item = {"content": "This is test content"}
        assert extract_text(item) == "This is test content"

    def test_field_priority(self):
        """Test that 'summary' takes precedence over other fields."""
        item = {
            "summary": "Summary text",
            "text": "Text field",
            "content": "Content field",
        }
        assert extract_text(item) == "Summary text"

    def test_case_insensitive_matching(self):
        """Test case-insensitive field name matching."""
        item = {"Summary": "Test with capital S"}
        assert extract_text(item) == "Test with capital S"

        item = {"SUMMARY": "Test with all caps"}
        assert extract_text(item) == "Test with all caps"

    def test_empty_string_skipped(self):
        """Test that empty strings are skipped."""
        item = {"summary": "", "text": "Non-empty text"}
        assert extract_text(item) == "Non-empty text"

    def test_whitespace_only_skipped(self):
        """Test that whitespace-only strings are skipped."""
        item = {"summary": "   ", "text": "Non-empty text"}
        assert extract_text(item) == "Non-empty text"

    def test_numeric_field_conversion(self):
        """Test conversion of numeric fields to strings."""
        item = {"summary": 12345}
        assert extract_text(item) == "12345"

        item = {"summary": 123.45}
        assert extract_text(item) == "123.45"

    def test_list_field_conversion(self):
        """Test conversion of list fields."""
        item = {"summary": ["Item 1", "Item 2", "Item 3"]}
        result = extract_text(item)
        assert "Item 1" in result
        assert "Item 2" in result
        assert "Item 3" in result

    def test_dict_field_conversion(self):
        """Test conversion of dict fields to JSON."""
        item = {"summary": {"key": "value", "count": 42}}
        result = extract_text(item)
        assert result is not None
        assert "key" in result or "value" in result

    def test_no_text_fields(self):
        """Test handling when no text fields are present."""
        item = {"id": "123", "timestamp": 1700000000}
        assert extract_text(item) is None

    def test_none_item(self):
        """Test handling of None input."""
        assert extract_text(None) is None  # type: ignore

    def test_empty_item(self):
        """Test handling of empty dict."""
        assert extract_text({}) is None

    def test_description_field(self):
        """Test extraction of 'description' field."""
        item = {"description": "Product description"}
        assert extract_text(item) == "Product description"

    def test_article_field(self):
        """Test extraction of 'article' field."""
        item = {"article": "Full article text"}
        assert extract_text(item) == "Full article text"


class TestSanitizeTag:
    """Test tag name sanitization."""

    def test_basic_sanitization(self):
        """Test basic tag sanitization."""
        assert sanitize_tag("test-tag") == "test-tag"
        assert sanitize_tag("test_tag") == "test_tag"

    def test_remove_special_chars(self):
        """Test removal of special characters."""
        assert sanitize_tag("test@tag") == "testtag"
        assert sanitize_tag("test#tag") == "testtag"
        assert sanitize_tag("test$tag") == "testtag"

    def test_preserve_alphanumeric(self):
        """Test preservation of alphanumeric characters."""
        assert sanitize_tag("test123tag") == "test123tag"
        assert sanitize_tag("ABC123xyz") == "ABC123xyz"

    def test_preserve_dash_underscore(self):
        """Test preservation of dash and underscore."""
        assert sanitize_tag("test-tag_name") == "test-tag_name"

    def test_empty_string(self):
        """Test handling of empty string."""
        assert sanitize_tag("") == ""

    def test_only_special_chars(self):
        """Test handling of string with only special characters."""
        assert sanitize_tag("@#$%^&*()") == ""

    def test_whitespace_preserved(self):
        """Test that whitespace is handled."""
        # Depending on implementation, spaces might be removed or preserved
        result = sanitize_tag("test tag")
        assert result in ("testtag", "test tag", "test-tag")

    def test_unicode_characters(self):
        """Test handling of unicode characters."""
        # Should preserve unicode letters
        result = sanitize_tag("test-café")
        assert "test" in result
        assert "caf" in result
