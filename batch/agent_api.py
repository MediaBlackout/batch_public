"""Simplified entrypoints for external agents.

This module exposes small helper functions that wrap the public orchestration
routines in :mod:`batch.main`.  They provide an easy programmatic API for
triggering a batch run and resuming an existing batch without interacting with
the CLI.
"""

from batch.main import orchestrate, resume


def run_batch(table: str, hours: int, model: str = "nano", test_only: bool = False) -> None:
    """Run a batch job synchronously for *table*.

    Parameters
    ----------
    table:
        Name of the DynamoDB table to process.
    hours:
        Look-back window specifying how many hours of data to fetch.
    model:
        Logical model key (``nano``, ``mini``, or ``full``) or a concrete model
        name supported by the batch system.
    test_only:
        If ``True`` generate the JSONL input but do not submit the job to
        OpenAI.
    """

    orchestrate(hours, model, test_only, table_name=table, wait=True)


def resume_batch(batch_id: str) -> None:
    """Resume monitoring for an existing batch job."""

    resume(batch_id)
