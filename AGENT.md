# Agent Integration

This repository exposes a simple wrapper for launching and monitoring batch jobs
programmatically. It lives in `batch.agent_api` and can be used by MCP or any
other automation system.

## Starting a Batch

```python
from batch.agent_api import run_batch

# Launch a job and wait for completion
run_batch("YOUR_TABLE_DATA_SOURCE_HERE", hours=24, model="mini")
```

`run_batch` performs the same steps as running `python -m batch.main` but
returns only when the job has finished and the results have been downloaded.

## Resuming a Batch

```python
from batch.agent_api import resume_batch

resume_batch("batch_123abc")
```

This call resumes polling for an existing batch and downloads the output file if
available.

Ensure the environment variables shown in `.env.example` are configured before
invoking these functions.
