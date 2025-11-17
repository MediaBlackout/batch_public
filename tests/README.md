# Test Suite

This directory contains the test suite for the batch processing system.

## Structure

```
tests/
├── __init__.py                  # Package marker
├── conftest.py                  # Pytest configuration and shared fixtures
├── test_timestamp_utils.py      # Tests for timestamp parsing utilities
├── test_text_utils.py           # Tests for text extraction utilities
├── test_validation.py           # Tests for configuration validation
├── test_dynamo_fetcher.py       # Tests for DynamoDB fetching (with mocks)
├── fixtures/                    # Test data and fixtures
│   └── sample_dynamodb_records.json
└── integration/                 # Integration tests
    └── test_full_pipeline.py
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=batch --cov-report=html --cov-report=term
```

### Run specific test file
```bash
pytest tests/test_timestamp_utils.py
```

### Run specific test
```bash
pytest tests/test_timestamp_utils.py::TestTimestampConversion::test_int_timestamp
```

### Run with verbose output
```bash
pytest -v
```

### Run with detailed output (including print statements)
```bash
pytest -s
```

## Test Coverage Goals

- **Unit tests**: 80%+ coverage of individual modules
- **Integration tests**: Cover critical paths through the system
- **Mock external dependencies**: AWS DynamoDB, OpenAI API

## Writing Tests

### Use fixtures from conftest.py

```python
def test_example(sample_dynamodb_items, mock_openai_client):
    # Your test here
    pass
```

### Mock AWS services

```python
from unittest.mock import patch

@patch("batch.dynamo_fetcher.boto3")
def test_aws_operation(mock_boto3):
    # Configure mock
    mock_table = MagicMock()
    mock_boto3.resource.return_value.Table.return_value = mock_table
    # Run test
```

### Test edge cases

- Empty inputs
- Invalid inputs
- Error conditions
- Boundary values
- Type mismatches

## Continuous Integration

Tests should be run automatically on:
- Pull requests
- Commits to main branch
- Pre-commit hooks (optional)

See `.github/workflows/` for CI configuration.
