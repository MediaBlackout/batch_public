# Batch Processing System

[![CI](https://github.com/MediaBlackout/batch_public/workflows/CI/badge.svg)](https://github.com/MediaBlackout/batch_public/actions)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A fault-tolerant, modular batch processing pipeline that integrates AWS DynamoDB data sources with the OpenAI Batch API. The system automates:

- **Incremental data ingestion** via high-water-mark filtering
- **JSONL generation** for OpenAI Batch jobs (Chat Completions & Embeddings)
- **Asynchronous submission** and polling of batch jobs
- **Automated result retrieval** and lightweight parsing utilities
- **Cost optimization** with 50% savings using OpenAI's Batch API

> **Who is this for?** Data teams processing large volumes of text through OpenAI's API at scale—perfect for sentiment analysis, content classification, embeddings generation, and batch evaluations.

## Features

- **Incremental Processing**: High-water-mark tracking prevents reprocessing of data
- **Multi-Endpoint Support**: Works with Chat Completions and Embeddings APIs
- **Fault Tolerance**: Exponential backoff retry logic for network failures
- **Async & Sync Modes**: Run synchronously or submit jobs for cron processing
- **Structured Logging**: Comprehensive logs for debugging and monitoring
- **Template System**: Rapid integration of new DynamoDB tables
- **Automated Testing**: Comprehensive test suite with CI/CD
- **Cost Efficient**: 50% cost savings using OpenAI Batch API vs real-time endpoints

## How It Works

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│  DynamoDB   │────>│ Fetch Recent │────>│   Generate  │────>│   Upload to  │
│   Tables    │     │     Data     │     │    JSONL    │     │    OpenAI    │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
                                                                      │
                                                                      v
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Parse &   │<────│   Download   │<────│Poll Batch   │<────│Create Batch  │
│   Analyze   │     │   Results    │     │   Status    │     │     Job      │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
```

### Workflow Steps

1. **Data Ingestion**: Fetch records from DynamoDB tables using time-based filtering
2. **High-Water Mark**: Track the latest processed timestamp to avoid duplicates
3. **JSONL Generation**: Convert records to OpenAI Batch API format
4. **File Upload**: Upload JSONL to OpenAI's file storage
5. **Batch Submission**: Create a batch job with 24-hour completion window
6. **Status Monitoring**: Poll batch status with exponential backoff
7. **Result Download**: Retrieve completed results as JSONL
8. **Parsing**: Extract and structure the AI-generated insights

### Data Flow Example

**Input (DynamoDB):**
```json
{
  "id": "news-001",
  "timestamp": 1704067200,
  "summary": "Fed signals potential rate cuts in 2024"
}
```

**Generated JSONL:**
```json
{
  "custom_id": "row_1",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {
    "model": "gpt-4o-mini",
    "messages": [
      {"role": "system", "content": "You are a macroeconomic analyst..."},
      {"role": "user", "content": "Fed signals potential rate cuts in 2024"}
    ]
  }
}
```

**Output (Batch Result):**
```json
{
  "custom_id": "row_1",
  "response": {
    "status_code": 200,
    "body": {
      "choices": [{
        "message": {
          "content": "Analysis: This indicates a dovish policy shift..."
        }
      }]
    }
  }
}
```

## Prerequisites

- Python 3.8 or higher
- AWS account with permissions to read DynamoDB tables
- OpenAI account with Batch API access

## Installation

```bash
# Clone this repository
git clone https://github.com/MediaBlackout/batch_public.git
cd batch_public

# (Optional) Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -e .
```

## Configuration

Copy and customize the example environment file:
The `.env.example` file is located in the project root.

```bash
cp .env.example .env
```

Edit `.env` to add your credentials:

```bash
OPENAI_API_KEY=YOUR_OPENAI_API_KEY
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
AWS_REGION=us-east-1
```

## Usage

### Run a batch job (synchronous)

```bash
python -m batch.main --table YOUR_TABLE_DATA_SOURCE_HERE --hours 24
```

### Dry-run only (generate JSONL)

```bash
python -m batch.main --table YOUR_TABLE_DATA_SOURCE_HERE --hours 1 --test
```

### Submit and exit (asynchronous)

```bash
python -m batch.main --table YOUR_TABLE_DATA_SOURCE_HERE --async
```

### Resume polling for an existing batch

```bash
python -m batch.main --resume <BATCH_ID>
```

### List available DynamoDB tables

```bash
python -m batch.main --list-tables
```

### Download results only

```bash
python -m batch.main --check-outputs
```

### Parse raw batch outputs

```bash
python -m batch.batch_parse.parse output/batch_output_YYYYMMDD_HHMMSS.jsonl -o parsed/results.json
```

## Quick Start

### 1. Set Up a Test Table

Create a DynamoDB table with sample data:

```bash
# Create table with sample news and market data
python examples/setup_dynamodb_example.py --table-name my-test-table --count 20

# This creates:
# - 20 sample news items
# - 20 sample market data points
# - All timestamped within the last 24 hours
```

### 2. Run a Test Batch (Dry Run)

Generate JSONL without submitting to OpenAI:

```bash
python -m batch.main --table my-test-table --hours 24 --test
```

This creates a file in `batch/jsonl_test/` that you can inspect.

### 3. Run a Full Batch Job

Process the data through OpenAI:

```bash
# Using the nano model (fastest, lowest cost)
python -m batch.main --table my-test-table --hours 24 --model nano

# Or use mini for better quality
python -m batch.main --table my-test-table --hours 24 --model mini
```

The system will:
1. Fetch records from the last 24 hours
2. Generate JSONL in the correct Batch API format
3. Upload to OpenAI
4. Create and monitor the batch job
5. Download results when complete
6. Save output to `batch/output/batch_output_YYYYMMDD_HHMMSS.jsonl`

### 4. Parse the Results

Extract structured data from the batch output:

```bash
python -m batch.batch_parse.parse batch/output/batch_output_*.jsonl -o parsed/results.json
```

### 5. Generate Embeddings (Optional)

Use the Embeddings API instead of Chat Completions:

```bash
python -m batch.main \
  --table my-test-table \
  --model text-embedding-3-small \
  --hours 24
```

## Real-World Example: News Sentiment Analysis

**Scenario:** Analyze 1,000 news headlines for macroeconomic sentiment.

```bash
# 1. Create table with news data
python examples/setup_dynamodb_example.py \
  --table-name news-sentiment \
  --data-type news \
  --count 1000

# 2. Process through OpenAI Batch API
python -m batch.main \
  --table news-sentiment \
  --model mini \
  --hours 24

# 3. Parse results
python -m batch.batch_parse.parse \
  batch/output/batch_output_*.jsonl \
  -o analysis/news_sentiment.json

# 4. Resume if interrupted
python -m batch.main --resume <BATCH_ID>
```

**Cost Comparison:**
- **Real-time API**: 1,000 requests × $0.60/1M tokens ≈ $0.60
- **Batch API**: 1,000 requests × $0.30/1M tokens ≈ $0.30 (50% savings!)
- **Processing Time**: Within 24 hours (usually faster)

## Agent Wrapper (`batch.agent_api`)

Automation frameworks&mdash;including MCP&mdash;can trigger the pipeline
programmatically using the lightweight wrapper in `batch.agent_api`.
These helpers block until the batch completes and the output file has been
downloaded.

```python
from batch.agent_api import run_batch, resume_batch

# Start a new batch run and wait for the results
run_batch("YOUR_TABLE_DATA_SOURCE_HERE", hours=24, model="mini")

# Resume a previously created batch
resume_batch("batch_123abc")
```

`run_batch` mirrors the CLI behaviour of `batch.main` and blocks until the job
finishes and the output file is downloaded. `resume_batch` continues monitoring
an existing batch. Ensure the environment variables listed in the
[Configuration](#configuration) section are available when calling these
functions. Additional examples are provided in [AGENT.md](AGENT.md).

## Templates

Use the helpers in [templates/news_table.py](templates/news_table.py) as a starting point for new tables. See [templates/README.md](templates/README.md) for details.

## Development & Testing

### Running Tests

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run all tests
pytest

# Run with coverage report
pytest --cov=batch --cov-report=html

# Run specific test file
pytest tests/test_jsonl_formatter.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code with Black
black batch/

# Lint with Ruff
ruff check batch/

# Type checking with MyPy
mypy batch/ --ignore-missing-imports
```

### CI/CD

This project uses GitHub Actions for continuous integration:
- Automated testing on Python 3.8, 3.9, 3.10, 3.11, 3.12
- Code linting with Ruff and Black
- Type checking with MyPy
- Security scanning with Bandit
- Coverage reporting to Codecov

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:
- Reporting bugs
- Suggesting features
- Setting up your development environment
- Submitting pull requests
- Code style and standards

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## OpenAI Batch API Reference

This project uses the [OpenAI Batch API](https://platform.openai.com/docs/api-reference/batch):
- **Cost**: 50% discount compared to synchronous APIs
- **Rate Limits**: Separate, higher limits than real-time endpoints
- **Completion Time**: 24-hour completion window (often much faster)
- **Supported Endpoints**:
  - `/v1/chat/completions` (Chat Completions)
  - `/v1/embeddings` (Embeddings)
  - `/v1/completions` (Legacy Completions)
  - `/v1/moderations` (Content Moderation)

See the [Batch API Cookbook](https://cookbook.openai.com/examples/batch_processing) for more examples.

## Project Structure

```
batch_public/
├── batch/                      # Main package
│   ├── main.py                # CLI entry point
│   ├── dynamo_fetcher.py      # DynamoDB data retrieval
│   ├── jsonl_formatter.py     # JSONL generation
│   ├── batch_submitter.py     # OpenAI file upload & batch creation
│   ├── status_checker.py      # Batch monitoring & result download
│   ├── models.py              # Model configuration
│   ├── logger.py              # Structured logging
│   ├── agent_api.py           # Programmatic API
│   ├── batch_parse/           # Result parsing utilities
│   └── templates/             # Table integration templates
├── tests/                     # Test suite
│   ├── conftest.py           # Pytest fixtures
│   ├── test_jsonl_formatter.py
│   └── test_batch_submitter.py
├── examples/                  # Example scripts
│   └── setup_dynamodb_example.py
├── .github/workflows/        # CI/CD configuration
├── CONTRIBUTING.md           # Contribution guidelines
├── CODE_OF_CONDUCT.md        # Code of conduct
└── README.md                 # This file
```

## Troubleshooting

### Common Issues

**Issue**: `OpenAI API key missing`
- **Solution**: Ensure `OPENAI_API_KEY` is set in your `.env` file or environment

**Issue**: `AWS credentials not found`
- **Solution**: Configure AWS credentials via `aws configure` or environment variables

**Issue**: `Batch failed with status 'expired'`
- **Solution**: Batches timeout after 24 hours. Resubmit with: `python -m batch.main --table <TABLE> --hours 24`

**Issue**: `No items returned from DynamoDB`
- **Solution**: Check your `--hours` parameter and verify table contains recent data

**Issue**: Rate limit errors
- **Solution**: The Batch API has separate rate limits. Check your [Platform Settings](https://platform.openai.com/settings)

### Logs

All operations are logged to `batch/logs/batch.log`:
```bash
# View recent logs
tail -f batch/logs/batch.log

# Search for errors
grep ERROR batch/logs/batch.log
```

## Roadmap

- [ ] Add support for streaming results
- [ ] Implement result caching layer
- [ ] Add Datadog/CloudWatch integration
- [ ] Support for custom prompt templates
- [ ] Web dashboard for monitoring batches
- [ ] Support for additional data sources (S3, PostgreSQL)

## Contact

For questions or support:
- Email: [contact@mediablackout.ai](mailto:contact@mediablackout.ai)
- Issues: [GitHub Issues](https://github.com/MediaBlackout/batch_public/issues)
- Website: [https://mediablackout.co](https://mediablackout.co)

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.

---

**Built with ❤️ by Media Blackout LLC**
