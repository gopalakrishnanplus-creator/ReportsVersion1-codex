# ReportsVersion1 — Reporting v2.1 Implementation

This repository now contains a full Django + PostgreSQL medallion implementation for the In-Clinic Sharing System reporting pipeline.

## Implemented architecture

- **RAW**: exact source replication in `raw_server1`/`raw_server2` with all source columns as text + ingestion metadata.
- **BRONZE**: deduplicated and exclusion-filtered tables in `bronze`.
- **SILVER**: conformed dimensions/facts, parsing and identity logic in `silver`.
- **GOLD**: campaign-scoped schemas `gold_campaign_*` with KPI wide tables and global benchmark tables in `gold_global`.
- **CONTROL**: ETL run/watermark/DQ log tables in `control`.
- **OPS**: exclusion rules + thresholds in `ops`.

## Commands

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python manage.py run_etl
python manage.py runserver
```

## Main entry points

- ETL command: `etl.management.commands.run_etl`
- Dashboard page: `/` or `/campaign/<brand_campaign_id>/`

## Quick local bootstrap

1. Update values in `.env` for your local PostgreSQL/MySQL credentials.
2. Run bootstrap script:

```bash
./setup_local.sh
```

The script creates `.venv`, installs dependencies, optionally starts `reports-postgres` Docker container, runs `python manage.py check`, and executes `python manage.py run_etl`.

### Windows quick notes

- Create venv: `python -m venv reports`
- Activate venv (CMD): `reports\Scripts\activate`
- Activate venv (PowerShell): `.\reports\Scripts\Activate.ps1`

If you see `The system cannot find the path specified` for activation, verify the folder name (`reports` vs `.venv`) and use `Scripts` (not `scripts`) on Windows.

If you hit `IndexError: list index out of range` during `run_etl`, pull latest code and rerun: this was caused by executing raw SQL with `%...%` patterns using an empty params list in the DB helper.

If you hit `ProgrammingError: INSERT has more expressions than target columns` during `run_etl`, pull latest code and rerun: silver tables are now rebuilt with `CREATE TABLE AS SELECT` each run to keep schema aligned with selected columns.
