# ReportsVersion1 — Reporting v2.1 Implementation



This repository now contains a full Django + PostgreSQL medallion implementation for the In-Clinic Sharing System, SAPA Growth Dashboard , PE Reports System reporting pipeline.
## SAPA / ESAPA dashboard

The SAPA dashboard is implemented as a separate app and ETL path. See [docs/ESAPA_DASHBOARD.md](docs/ESAPA_DASHBOARD.md) for:

- runtime source boundaries
- MySQL Server 1 configuration behavior
- SAPA schema isolation from the legacy Inclinic report

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


Source extraction reads directly from configured MySQL source tables (`MYSQL_SERVER1_*` and `MYSQL_SERVER2_*` in `.env`). CSV files in the repository are reference samples only and are not used by ETL ingestion.

Settings now auto-load variables from a local `.env` file at startup, so `python manage.py run_etl` uses those credentials even if your shell session has not exported them.

## Main entry points

- ETL command: `etl.management.commands.run_etl`
- Dashboard page: `/` or `/campaign/<brand_campaign_id>/`

### MySQL extraction scope

- **Server 1** (`MYSQL_SERVER1_*`, DB: `healthcare_forms_2`)
  - `campaign_fieldrep`
  - `campaign_campaignfieldrep`
  - `campaign_campaign`
- **Server 2** (`MYSQL_SERVER2_*`, DB: `myproject_dev`)
  - `campaign_management_campaign`
  - `collateral_management_campaigncollateral`
  - `collateral_management_collateral`
  - `sharing_management_collateraltransaction`
  - `doctor_viewer_doctor`

## Quick local bootstrap

1. Update values in `.env` for your local PostgreSQL/MySQL credentials.
2. Run bootstrap script:

```bash
./setup_local.sh
```

The script creates `.venv`, initializes a local PostgreSQL instance, installs dependencies, seeds realistic SQLite dummy source databases, runs `python manage.py check`, resets reporting schemas, and executes `python manage.py run_etl`.

For a Docker-free local setup using Postgres.app and SQLite sample sources, see [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md). The current `setup_local.sh` follows that native flow.

### Windows quick notes

- Create venv: `python -m venv reports`
- Activate venv (CMD): `reports\Scripts\activate`
- Activate venv (PowerShell): `.\reports\Scripts\Activate.ps1`

If you see `The system cannot find the path specified` for activation, verify the folder name (`reports` vs `.venv`) and use `Scripts` (not `scripts`) on Windows.

If you hit `IndexError: list index out of range` during `run_etl`, pull latest code and rerun: this was caused by executing raw SQL with `%...%` patterns using an empty params list in the DB helper.

If you hit `ProgrammingError: INSERT has more expressions than target columns` during `run_etl`, pull latest code and rerun: silver tables are now rebuilt with `CREATE TABLE AS SELECT` each run to keep schema aligned with selected columns.

If you hit `ProgrammingError: column "_dq_status" specified more than once` during `run_etl`, pull latest code and rerun: Silver `CREATE TABLE AS SELECT` statements were updated to avoid duplicating audit columns when using `*` from Bronze tables.

If you hit `DataError: invalid input syntax for type date: "NULL"` during `run_etl`, pull latest code and rerun: Silver schedule/date parsing now treats literal `NULL`/blank strings as null before casting.

If you hit `DataError: invalid input syntax for type date: "NULL"` in GOLD (`kpi_weekly_summary`), pull latest code and rerun: weekly aggregation now normalizes literal `NULL`/blank timestamp strings before date casts.

Weekly GOLD buckets are now anchored to the latest observed campaign event week (Saturday-ending), not fixed to the current week, so historical campaign activity appears in the dashboard KPIs.


If `run_etl` fails with MySQL timeout/access errors (for example `OperationalError(2003)` / `OperationalError(1045)`), verify network access to each MySQL host (security group / firewall / VPC), credentials, and optional SSL settings. The app supports:

If one source table is temporarily unreachable (for example MySQL `1045` on one server), RAW ingestion now skips only that table and continues the ETL for other sources; skipped-table errors are captured in the ETL run notes.

- `MYSQL_SERVER1_CONNECT_TIMEOUT`, `MYSQL_SERVER1_READ_TIMEOUT`, `MYSQL_SERVER1_WRITE_TIMEOUT`
- `MYSQL_SERVER2_CONNECT_TIMEOUT`, `MYSQL_SERVER2_READ_TIMEOUT`, `MYSQL_SERVER2_WRITE_TIMEOUT`
- `MYSQL_SERVER1_SSL_MODE`, `MYSQL_SERVER1_SSL_CA`
- `MYSQL_SERVER2_SSL_MODE`, `MYSQL_SERVER2_SSL_CA`

Use SSL mode `required`, `verify_ca`, or `verify_identity` when your managed MySQL/RDS setup enforces TLS.



### Deployment env file path

In EC2 deployment, settings now load dotenv from the first existing path in this order:

1. `DJANGO_ENV_FILE` (explicit override)
2. `/var/www/secrets/.env`
3. `<repo>/.env`

For your server (`13.126.7.118`), keep production credentials in `/var/www/secrets/.env` or set `DJANGO_ENV_FILE=/var/www/secrets/.env` in the deploy shell.

The settings loader also accepts common DB env aliases in addition to `POSTGRES_*`: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (and `PG*` variants). This helps CI/CD pipelines that export generic DB variable names.

### Session/Auth note

This project uses signed-cookie sessions (`SESSION_ENGINE=django.contrib.sessions.backends.signed_cookies`) for local campaign login flow, so report auth works without requiring `django_session` table migrations.


### EC2 deployment script (`deploy.sh`)

The repository includes `deploy.sh` for server deployments.

The script now prints a short runtime summary (settings module + resolved DB host/port) before migrations to make pipeline debugging easier from logs.

Key behavior:
- Uses production settings by default: `DJANGO_SETTINGS_MODULE=config.settings.prod`
- Loads env vars from the first available file:
  1. `ENV_FILE` (default `/var/www/secrets/.env`)
  2. `<project>/.env.prod`
  3. `<project>/.env`
- Runs migrations, collects static files, and restarts `gunicorn`.
- Runs ETL by default (`python manage.py run_etl`) to create/update RAW→GOLD tables required by the dashboard (`gold_global.campaign_registry`).
- `RUN_ETL_CONTINUE_ON_ERROR` defaults to `1` so deploy does not fail hard when source MySQL credentials/network are temporarily unavailable.
  - Disable with `RUN_ETL_ON_DEPLOY=0`
  - Continue despite ETL failure with `RUN_ETL_CONTINUE_ON_ERROR=1` (not recommended for production)

You can override runtime values, for example:

```bash
PROJECT_DIR=/var/www/ReportsVersion1 VENV_DIR=/var/www/venv ENV_FILE=/var/www/secrets/.env DJANGO_SETTINGS_MODULE=config.settings.prod RUN_ETL_ON_DEPLOY=1 RUN_ETL_CONTINUE_ON_ERROR=1 GUNICORN_SERVICE=gunicorn ./deploy.sh
```
