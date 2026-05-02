# ESAPA Dashboard

This document describes the isolated SAPA/ESAPA dashboard implementation inside the existing Django project.

## Runtime source policy

The SAPA dashboard must not fetch application data from CSV files at runtime.

Runtime inputs are:

- MySQL source tables from the SAPA application/source MySQL database
- RFA campaign tables from the same MySQL source, including `campaign_campaign`, `campaign_doctorcampaignenrollment`, `campaign_fieldrep`, and `campaign_campaignfieldrep`
- PostgreSQL reporting tables in SAPA-specific schemas
- WordPress/LearnDash API for webinar and course data when the SAPA ETL is run against live upstreams

CSV files are not part of the runtime path for SAPA MySQL extraction.

## Database isolation

The SAPA dashboard is isolated from the legacy Inclinic report by schema and route separation.

- Legacy report routes remain under `/` and `/campaign/...`
- SAPA routes remain under `/sapa-growth/`
- `/sapa-growth/` is the SAPA campaign launcher
- SAPA campaign dashboards use `/sapa-growth/campaign/<campaign_key>/`
- Legacy reporting tables remain in their existing schemas
- SAPA reporting tables remain in:
  - `raw_sapa_mysql`
  - `raw_sapa_api`
  - `bronze_sapa`
  - `silver_sapa`
  - `gold_sapa`
  - `gold_sapa_stage`
  - `control` tables prefixed with `sapa_`

No Django migrations were added for the SAPA app.

## Source configuration

SAPA MySQL extraction first uses explicit `SAPA_MYSQL_*` credentials when set.

If those variables are absent, SAPA falls back in this order:

- `MYSQL_SERVER1_*`
- `MYSQL_SERVER2_*`

This keeps SAPA pointed at `healthcare_forms_2` by default while preserving an explicit SAPA override path when a deployment needs something different.

The SAPA/RFA campaign separation is built from live MySQL source tables, not from CSVs or manually seeded data:

- `campaign_campaign` supplies the campaign id/name and is filtered to `system_rfa` campaigns when that flag is available.
- `campaign_doctorcampaignenrollment` maps source doctors to campaigns.
- `campaign_fieldrep` supplies field-rep identity, names, external ids, and state.
- `campaign_campaignfieldrep` maps field reps to campaigns for redflags-side doctor records where direct campaign enrollment is unavailable.
- Screening, reminder, follow-up, video, webinar, and course facts inherit the campaign-specific doctor key so dashboard tiles, drill-downs, CSV exports, and PDF exports remain filter-consistent.
- When a doctor appears in multiple RFA campaigns because of transfer, each dated fact is attributed to one campaign only. The ETL chooses the latest campaign enrollment/start date applicable to the event date, instead of copying the same source event into every campaign.

SAPA WordPress extraction first uses explicit `SAPA_WORDPRESS_*` variables.

If those variables are absent, it also accepts the helper-style names:

- `WORDPRESS_URL`
- `WORDPRESS_API_SECRET`
- `API_SECRET`
- `WORDPRESS_TIMEOUT`

Relevant settings are in:

- `config/settings/base.py`
- `etl/sapa_growth/mysql.py`

This means SAPA can share the same source server as the Inclinic extractor without reusing the legacy ETL code path.

## Live SAPA ETL

The live SAPA ETL command is:

```bash
.venv/bin/python manage.py run_sapa_growth_etl
```

This command:

- extracts SAPA application tables from MySQL
- extracts webinar/course data from WordPress/LearnDash
- resolves YouTube/Vimeo video titles into SAPA reporting tables without changing source tables
- builds SAPA bronze, silver, and gold layers
- publishes SAPA gold tables only

## Legacy safety boundary

Shared project touches are intentionally limited to:

- app registration in `config/settings/base.py`
- route inclusion in `config/urls.py`

All SAPA UI, ETL, and export logic is namespaced under `sapa_growth` and `etl/sapa_growth`.

## Optional stronger isolation

If you want SAPA to use a separate PostgreSQL database such as `rfareports`, that can be done later by pointing Django to that database for the SAPA deployment target. It is not required for code-level isolation because SAPA already uses separate schemas and does not modify legacy reporting tables.

Current isolation guarantees are logical, not physical:

- SAPA writes only to SAPA-specific schemas and `control.sapa_*` tables
- SAPA routes are isolated under `/sapa-growth/`
- the SAPA ETL uses its own management command and pipeline package

A separate PostgreSQL database is still the stronger defense-in-depth option if you want operational isolation from accidental manual edits or future misconfiguration.
