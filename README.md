# Batch Processing System

A fault-tolerant, modular batch processing pipeline that integrates AWS DynamoDB data sources with the OpenAI Batch API. The system automates:

- Incremental data ingestion via high-water-mark filtering
- JSONL prompt generation for OpenAI Batch jobs
- Asynchronous submission and polling of batch jobs
- Automated result retrieval and lightweight parsing utilities

> **Who is this for?** Media Blackout LLC engineers or any team needing a reliable, scalable workflow for batch-processing DynamoDB tables with OpenAI.

## Features

- High-water-mark support for incremental updates
- Flexible prompt formatting and JSONL file generation
- Submit, monitor, and download OpenAI Batch jobs
- Lightweight JSON parsing utility for downstream analytics
- Template helpers for rapid integration of new tables

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
pip install -r requirements.txt
```

## Configuration

Copy and customize the example environment file:

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
python -m batch.main --table RSSLinkhash --hours 24
```

### Dry-run only (generate JSONL)

```bash
python -m batch.main --table RSSLinkhash --hours 1 --test
```

### Submit and exit (asynchronous)

```bash
python -m batch.main --table RSSLinkhash --async
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

## Quick Start: Testing with `RSSLinkhash`

A minimal example to get started with the `RSSLinkhash` table:

```bash
# Dry-run JSONL generation
python -m batch.main --table RSSLinkhash --hours 1 --test

# Parse the generated JSONL
python -m batch.batch_parse.parse jsonl_test/batch_RSSLinkhash_*.jsonl -o parsed/rsslinkhash_test.json
```

Inspect `jsonl_test/` and `parsed/` to review input payloads and parsed results.

## Templates

Use the helpers in [templates/news_table.py](templates/news_table.py) as a starting point for new tables. See [templates/README.md](templates/README.md) for details.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.
