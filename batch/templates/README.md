Reusable Pipeline Template
==========================

This folder contains **minimal, self-contained skeletons** that showcase the
changes required to integrate a new DynamoDB table into the batch processing
pipeline.  Copy the template that best matches your use-case, adjust the
constants (table name, timestamp attribute, text fields) and you are ready to
run `python -m batch.main --table YourNewTable`.

Template structure
------------------

```
templates/
│
├── __init__.py          # Makes the folder import-able (no content necessary)
└── news_table.py        # End-to-end example for an RSS/News style feed
```

Key sections worth tweaking:

1. `TIMESTAMP_KEYS` – add your custom attribute if it is not already covered
   (e.g. `"ingestedAt"`).
2. `_extract_text()` – extend the ordered list of candidate field names when
   the summary lives under a novel column.

Once the file is adapted you can **immediately** orchestrate the table:

```bash
python -m batch.main --table MyNewTable --hours 24
```

The high-water-mark and de-duplication logic of the shared pipeline will kick
in automatically because it is implemented at the *framework* level and
therefore applies to **all** tables.
