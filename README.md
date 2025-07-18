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

## Quick Start: Testing with `YOUR_TABLE_DATA_SOURCE_HERE`

A minimal example to get started with the `YOUR_TABLE_DATA_SOURCE_HERE` table:

```bash
# Dry-run JSONL generation
python -m batch.main --table YOUR_TABLE_DATA_SOURCE_HERE --hours 1 --test

# Parse the generated JSONL
python -m batch.batch_parse.parse jsonl_test/batch_YOUR_TABLE_DATA_SOURCE_HERE_*.jsonl -o parsed/YOUR_TABLE_DATA_SOURCE_HERE_test.json
```

Inspect `jsonl_test/` and `parsed/` to review input payloads and parsed results.

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

## Contact

For questions or support, email [contact@mediablackout.ai](mailto:contact@mediablackout.ai).

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.
