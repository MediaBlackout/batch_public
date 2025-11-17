# Contributing to Batch Processing System

Thank you for your interest in contributing to the Batch Processing System! This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Report Bugs](#how-to-report-bugs)
- [How to Suggest Features](#how-to-suggest-features)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing](#testing)

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing.

## How to Report Bugs

If you find a bug in the project, please create an issue on GitHub with the following information:

1. **Clear, descriptive title** - Summarize the problem in the title
2. **Detailed description** - Describe what you expected to happen and what actually happened
3. **Steps to reproduce** - Provide step-by-step instructions to reproduce the issue
4. **Environment details** - Include:
   - Python version (`python --version`)
   - Operating system
   - Relevant dependency versions
   - AWS region (if applicable)
5. **Code samples** - If possible, include a minimal code example that reproduces the issue
6. **Logs** - Attach relevant log output from `batch/logs/batch.log`

**Example:**
```
Title: Batch submission fails with embeddings endpoint

Description:
When using the --endpoint /v1/embeddings flag, the batch submission fails
with a validation error.

Steps to reproduce:
1. Run: python -m batch.main --table my_table --model text-embedding-3-small --endpoint /v1/embeddings
2. Observe error: "Invalid endpoint specified"

Environment:
- Python 3.11.4
- openai==1.12.0
- boto3==1.34.0
- OS: Ubuntu 22.04
```

## How to Suggest Features

We welcome feature suggestions! To suggest a new feature:

1. **Check existing issues** - Search to see if someone has already suggested it
2. **Create a new issue** with:
   - Clear, descriptive title
   - Detailed description of the feature
   - Use cases explaining why this would be valuable
   - Potential implementation approach (if you have ideas)
   - Examples of how the feature would be used

## Development Setup

### Prerequisites

- Python 3.8 or higher
- AWS account with DynamoDB access
- OpenAI API key
- Git

### Setup Steps

1. **Fork and clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/batch_public.git
   cd batch_public
   ```

2. **Create a virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies (including dev dependencies):**
   ```bash
   pip install -e .
   pip install -r requirements-dev.txt
   ```

4. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

5. **Run tests to verify setup:**
   ```bash
   pytest
   ```

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes:**
   - Write clean, well-documented code
   - Follow the coding standards below
   - Add tests for new functionality
   - Update documentation as needed

3. **Run the test suite:**
   ```bash
   # Run all tests
   pytest

   # Run with coverage
   pytest --cov=batch --cov-report=html

   # Run linting
   ruff check .
   black --check .
   ```

4. **Commit your changes:**
   ```bash
   git add .
   git commit -m "Add feature: brief description"
   ```

   Use clear, descriptive commit messages:
   - Start with a verb (Add, Fix, Update, Remove, etc.)
   - Keep the first line under 72 characters
   - Add a detailed description if needed

5. **Push to your fork:**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request:**
   - Go to the original repository on GitHub
   - Click "New Pull Request"
   - Select your branch
   - Fill out the PR template with:
     - Description of changes
     - Related issue numbers (if any)
     - Testing performed
     - Screenshots (if applicable)

7. **Address review feedback:**
   - Respond to comments
   - Make requested changes
   - Push updates to your branch

## Coding Standards

### Code Style

We use **Black** for code formatting and **Ruff** for linting.

- **Black:** Automatic code formatting
  ```bash
  black batch/
  ```

- **Ruff:** Fast Python linter
  ```bash
  ruff check batch/
  ```

### Style Guidelines

1. **Type hints:** Use type hints for function parameters and return values
   ```python
   def fetch_recent(hours: int, table_name: str) -> List[Dict[str, Any]]:
       ...
   ```

2. **Docstrings:** All public functions should have docstrings
   ```python
   def process_batch(batch_id: str) -> None:
       """Process a batch by ID and download results.

       Args:
           batch_id: The OpenAI batch ID to process

       Raises:
           RuntimeError: If the batch processing fails
       """
   ```

3. **Error handling:** Use specific exceptions and provide helpful error messages
   ```python
   try:
       result = fetch_data()
   except ClientError as e:
       log.exception("Failed to fetch from DynamoDB: %s", e)
       raise
   ```

4. **Logging:** Use the project's logging system
   ```python
   from batch.logger import get_logger
   log = get_logger(__name__)
   log.info("Processing %d records", len(records))
   ```

5. **Imports:** Group imports in this order:
   - Standard library
   - Third-party packages
   - Local modules

### File Organization

- **batch/**: Main package code
- **tests/**: Test files (mirror the structure of batch/)
- **docs/**: Documentation
- **.github/**: GitHub-specific files (workflows, templates)

## Testing

### Writing Tests

1. **Test files:** Create test files in the `tests/` directory matching the module structure
   ```
   batch/dynamo_fetcher.py  →  tests/test_dynamo_fetcher.py
   ```

2. **Test functions:** Name test functions descriptively
   ```python
   def test_fetch_recent_returns_items_within_timeframe():
       ...

   def test_fetch_recent_handles_empty_table():
       ...
   ```

3. **Use fixtures:** Create reusable fixtures in `tests/conftest.py`
   ```python
   @pytest.fixture
   def mock_dynamodb():
       with mock_dynamodb():
           yield boto3.resource("dynamodb")
   ```

4. **Mock external services:** Mock AWS and OpenAI API calls
   ```python
   @mock_s3
   def test_with_mocked_s3():
       ...
   ```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_dynamo_fetcher.py

# Run specific test
pytest tests/test_dynamo_fetcher.py::test_fetch_recent

# Run with coverage
pytest --cov=batch --cov-report=html
```

## Questions?

If you have questions about contributing, feel free to:
- Open an issue with the `question` label
- Contact us at [contact@mediablackout.ai](mailto:contact@mediablackout.ai)

Thank you for contributing!
