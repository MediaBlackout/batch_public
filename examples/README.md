# Examples

This directory contains example scripts and configurations to help you get started with the batch processing system.

## Setup DynamoDB Example

The `setup_dynamodb_example.py` script creates a sample DynamoDB table and populates it with test data.

### Usage

**Create a table with sample news and market data:**
```bash
python examples/setup_dynamodb_example.py --table-name my-test-table
```

**Create a table with only news data:**
```bash
python examples/setup_dynamodb_example.py --table-name news-table --data-type news --count 20
```

**Populate an existing table:**
```bash
python examples/setup_dynamodb_example.py --table-name existing-table --skip-create
```

### Options

- `--table-name`: Name of the DynamoDB table (default: `batch-test-table`)
- `--region`: AWS region (default: `us-east-1`)
- `--data-type`: Type of data to generate: `news`, `market`, or `both` (default: `both`)
- `--count`: Number of items per data type (default: 10)
- `--skip-create`: Skip table creation and only populate data

### Running a Test Batch

After setting up your table, you can test the batch processing system:

```bash
# Dry run (generate JSONL only, don't submit to OpenAI)
python -m batch.main --table my-test-table --hours 24 --test

# Full run with chat completions
python -m batch.main --table my-test-table --hours 24

# Generate embeddings instead
python -m batch.main --table my-test-table --hours 24 --model text-embedding-3-small
```

## Example Workflows

### 1. News Analysis Pipeline

```bash
# Create table with news data
python examples/setup_dynamodb_example.py --table-name news-analysis --data-type news --count 50

# Process with GPT-4o-mini
python -m batch.main --table news-analysis --model mini --hours 24
```

### 2. Market Data Processing

```bash
# Create table with market data
python examples/setup_dynamodb_example.py --table-name market-data --data-type market --count 100

# Process with nano model for cost efficiency
python -m batch.main --table market-data --model nano --hours 12
```

### 3. Embeddings Generation

```bash
# Create table with mixed data
python examples/setup_dynamodb_example.py --table-name embedding-data --count 200

# Generate embeddings
python -m batch.main --table embedding-data --model text-embedding-3-small --hours 24
```

## Cleanup

To delete the test table:

```bash
aws dynamodb delete-table --table-name my-test-table --region us-east-1
```
