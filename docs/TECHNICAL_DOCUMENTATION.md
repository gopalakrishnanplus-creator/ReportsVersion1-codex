# ReportsVersion1 Technical Documentation

## 1. Project Overview

### Purpose
ReportsVersion1 is a Django-based reporting platform for the In-Clinic Sharing System. It combines ETL pipelines and a reporting UI to transform operational campaign activity into weekly and campaign-level KPIs.

### Problem It Solves
The project standardizes fragmented source exports and renders campaign analytics through a dashboard, enabling stakeholders to assess reach, open, and consumption performance by campaign and week.

### Target Users
- Data/analytics engineers running and maintaining ETL jobs.
- Business/reporting users reviewing campaign health dashboards.
- Application developers extending Django views/templates and ETL logic.

### High-Level Architecture
- **Web layer**: Django app with menu, brand login, campaign report, and export routes.
- **ETL layer**: RAW → BRONZE → SILVER → GOLD transformations in PostgreSQL.
- **Presentation layer**: Server-rendered HTML with custom CSS/JS grouped-bar chart.

---

## 2. Technology Stack

### Languages
- Python
- SQL (PostgreSQL dialect)
- HTML/CSS/JavaScript

### Frameworks and Libraries
- Django 5.x
- psycopg2-binary

### Databases
- PostgreSQL (default Django database and analytics warehouse schemas)

### External Services
- Optional Dockerized PostgreSQL local runtime via setup script.
- CSV-based source extraction fallback for local development.

### DevOps / Local Ops
- `setup_local.sh` for environment bootstrap, dependency install, checks, and ETL run.

---

## 3. Repository Structure

```text
.
├── config/                         # Django project settings + URL entry points
│   ├── settings/
│   │   ├── base.py                 # Core settings (apps, DB, sessions, middleware)
│   │   ├── dev.py                  # DEBUG=True
│   │   └── prod.py                 # DEBUG=False
│   ├── urls.py                     # Route declarations
│   ├── asgi.py                     # ASGI entry point
│   └── wsgi.py                     # WSGI entry point
├── dashboard/                      # Reporting UI app
│   ├── views.py                    # Menu/login/report/export controllers + report context assembly
│   ├── templates/dashboard/
│   │   ├── menu.html               # Campaign listing page
│   │   ├── login.html              # Brand credential gate
│   │   └── overview.html           # Main report page
│   └── static/dashboard/
│       ├── css/overview.css        # Dashboard styles
│       └── js/overview.js          # Grouped-bar chart renderer
├── etl/                            # Medallion ETL implementation
│   ├── connectors/                 # Postgres execution + CSV extractors
│   ├── control/                    # ETL log/control table management
│   ├── management/commands/
│   │   └── run_etl.py              # ETL orchestration command
│   ├── pipelines/                  # raw_ingestion, bronze_transform, silver_transform, gold_aggregations
│   └── utils/                      # Specs + normalization helpers
├── docs/                           # Project documentation
│   └── TECHNICAL_DOCUMENTATION.md  # This file
├── manage.py                       # Django CLI entry point
├── requirements.txt                # Python dependencies
├── setup_local.sh                  # Local setup automation
└── *.csv                           # Source input datasets used by CSV fallback connectors
```

---

## 4. Architecture and Design Patterns

### Architectural Pattern
The project is a **modular monolith**:
- Django serves UI and request handling.
- ETL logic lives in Python modules executed by a Django management command.
- PostgreSQL stores operational analytics layers and powers dashboard queries.

### Data-Layer Pattern: Medallion
- **RAW**: schema-preserving source capture with audit metadata.
- **BRONZE**: deduplication and exclusion filtering.
- **SILVER**: conformed dimensions/facts and normalized event semantics.
- **GOLD**: campaign marts + global benchmarks/history.

### Key Design Decisions
- SQL-heavy transforms maximize pushdown to database.
- Campaign-specific GOLD schemas isolate report data by brand campaign.
- Signed-cookie sessions avoid dependency on `django_session` table migrations for local auth flow.

### Dependency Flow
```text
CSV source files
  -> etl.connectors.mysql_server1/mysql_server2
  -> etl.pipelines.raw_ingestion
  -> etl.pipelines.bronze_transform
  -> etl.pipelines.silver_transform
  -> etl.pipelines.gold_aggregations
  -> dashboard.views SQL queries
  -> templates + CSS + JS rendering
```

---

## 5. Detailed Code Explanation

## 5.1 Django Application Layer

### URL Routing (`config/urls.py`)
- `/` → campaign menu page.
- `/campaign/<brand_campaign_id>/login/` → brand credential login.
- `/campaign/<brand_campaign_id>/` → authenticated campaign report.
- `/campaign/<brand_campaign_id>/export/` → print-oriented report export page.
- `/admin/` → Django admin.

### Dashboard Views (`dashboard/views.py`)
Core responsibilities:
1. Pull campaign choices for menu from `gold_global.campaign_registry`.
2. Validate brand credentials and set campaign auth session key.
3. Build full report context:
   - Weekly summary table from campaign GOLD schema.
   - KPI cards, health cards, benchmark labels, WoW deltas.
   - State attention panel (bottom-3 states).
   - Action panel from weakest metric.
   - Comparison cards (current/best/benchmark).
   - Trend chart series arrays.
4. Support week-level filtering (`?week=`).
5. Render export mode that calls browser print for PDF workflow.

### Templates
- `menu.html`: lists campaign IDs and names with “View Report”.
- `login.html`: campaign-specific credential gate.
- `overview.html`: report sections (header, controls, health, KPI tiles, state panel, action panel, comparison, trend, weekly table).

### Front-End JavaScript
`overview.js` draws a grouped-bar chart on `<canvas>` using supplied JSON script tags:
- Doctors Opened %
- Doctors Reached %
- PDF Downloads %
- Video Viewed (>50%) %

### Styling
`overview.css` defines:
- Card-based layout and grid sections.
- Score color states (red/yellow/green).
- KPI card hierarchy and responsive breakpoints.

---

## 5.2 ETL Layer

### ETL Orchestration (`etl/management/commands/run_etl.py`)
Execution order:
1. `ensure_control_tables()`
2. `ingest_raw(run_id)`
3. `build_bronze()`
4. `build_silver(run_id)`
5. `build_gold(run_id)`
6. `log_run(...SUCCESS/FAIL...)`

### Connectors
- `postgres.py`: `execute` / `fetchall` helper wrappers around Django DB cursor.
- `mysql_server1.py`, `mysql_server2.py`: currently read CSV files (`<table>.csv`) for local extraction fallback.

### RAW Pipeline (`raw_ingestion.py`)
- Creates RAW schemas and tables from canonical source specs.
- Loads rows with audit metadata (`_ingestion_run_id`, `_record_hash`, etc.).

### BRONZE Pipeline (`bronze_transform.py`)
- Rebuilds deduplicated rows using `ROW_NUMBER()` partitioning.
- Applies campaign exclusion rules for test/blank IDs.

### SILVER Pipeline (`silver_transform.py`)
Builds conformed entities:
- `dim_field_rep`
- `dim_doctor`
- `dim_collateral`
- `bridge_campaign_collateral_schedule`
- `fact_collateral_transaction`
- `map_brand_campaign_to_campaign`
- `bridge_brand_campaign_doctor_base`
- `doctor_action_first_seen`

Includes normalization rules for booleans, percentages, and string dates (including literal `'null'` / blank handling in date fields).

### GOLD Pipeline (`gold_aggregations.py`)
For each campaign:
- Resolves campaign GOLD schema name (`gold_campaign_*`).
- Builds `fact_doctor_collateral_latest` with unique-doctor first-seen events.
- Builds `kpi_weekly_summary` using a 4-week, event-anchored Saturday-ending window.
- Computes weekly reach/open/consumption percentages and health score.
- Updates global history and benchmark tables:
  - `gold_global.campaign_health_history`
  - `gold_global.benchmark_last_10_campaigns`

---

## 6. Database Documentation

## 6.1 Schemas
- `raw_server1`, `raw_server2`
- `bronze`
- `silver`
- `gold_campaign_*`
- `gold_global`
- `control`
- `ops`

## 6.2 Core Tables

### Control
- `control.etl_run_log`
- `control.etl_step_log`
- `control.etl_watermark`
- `control.dq_issue_log`

### SILVER
- `silver.fact_collateral_transaction` (normalized interaction facts)
- `silver.doctor_action_first_seen` (first event timestamps per doctor/collateral)
- `silver.bridge_brand_campaign_doctor_base` (doctor denominator base)
- Additional dimension and bridge tables listed above.

### GOLD
Per-campaign schema tables:
- `fact_doctor_collateral_latest`
- `kpi_weekly_summary`
- `weekly_action_items` (placeholder scaffold)

Global tables:
- `gold_global.campaign_registry`
- `gold_global.campaign_health_history`
- `gold_global.benchmark_last_10_campaigns`

## 6.3 Relationship Overview (Text ER)
```text
silver.fact_collateral_transaction
  -> silver.doctor_action_first_seen
  -> gold_campaign_<id>.fact_doctor_collateral_latest
  -> gold_campaign_<id>.kpi_weekly_summary

gold_global.campaign_registry
  -> maps brand_campaign_id to campaign gold schema name

gold_global.campaign_health_history
  -> feeds benchmark_last_10_campaigns
```

---

## 7. HTTP Interface Documentation

## 7.1 Endpoints

### `GET /`
Lists all campaign IDs and campaign names with View Report actions.

### `GET /campaign/<id>/login/`
Renders brand login form.

### `POST /campaign/<id>/login/`
Validates credentials. On success sets `request.session[f"auth_{id}"] = True`.

### `GET /campaign/<id>/`
Renders campaign report; requires campaign auth session key.

### `GET /campaign/<id>/export/`
Renders report with `export_mode` and invokes `window.print()`.

## 7.2 Authentication
- Brand credential check is deterministic based on campaign ID suffix/prefix.
- Session backend: signed cookies.

## 7.3 Errors and Redirects
- Unknown campaign → redirect to menu.
- Unauthenticated report/export access → redirect to campaign login.
- Report query errors are surfaced in-template through `error_message`.

---

## 8. Configuration and Environment Setup

## 8.1 Required Configuration
Django settings consume env variables such as:
- `DJANGO_SECRET_KEY`, `DJANGO_DEBUG`
- `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`
- `MYSQL_SERVER1_*`, `MYSQL_SERVER2_*`

## 8.2 Local Setup
Recommended commands:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python manage.py run_etl
python manage.py runserver
```

Or run bootstrap script:
```bash
./setup_local.sh
```

---

## 9. Testing and Validation

Current repository emphasizes operational checks over automated tests.

### Existing Validation Practices
- `python manage.py check` for Django system validation.
- `python manage.py run_etl` for ETL functional run validation.
- Manual browser verification for report rendering and export.

### Current Gap
No dedicated automated unit/integration test suite is present.

---

## 10. Deployment

## 10.1 Deployment Shape
- Django app process + PostgreSQL database.
- Static asset serving configured via Django static settings.

## 10.2 CI/CD
No CI/CD workflows are included in this repository snapshot.

## 10.3 Environment Profiles
- `config.settings.dev`: `DEBUG=True`
- `config.settings.prod`: `DEBUG=False`

---

## 11. Security Considerations

### Implemented
- CSRF middleware enabled.
- Session middleware enabled.
- Signed-cookie session backend avoids DB session-table dependency.

### Risks / Improvement Areas
- `ALLOWED_HOSTS = ["*"]` is permissive for production.
- Fallback secret key is development-oriented.
- Credential generation approach is not enterprise-grade authentication.
- Extensive dynamic SQL requires careful validation and governance.

---

## 12. Performance Considerations

### Existing Performance Practices
- Aggregations are SQL-native (COUNT DISTINCT + FILTER).
- GOLD precomputes weekly KPIs for dashboard reads.

### Potential Bottlenecks
- Full-table rebuild strategy in SILVER/GOLD at each run.
- Row-by-row INSERT in RAW ingestion rather than bulk COPY.
- View-layer SQL composition with campaign schema interpolation.

### Opportunities
- Add indexes on high-use keys (`brand_campaign_id`, `doctor_identity_key`, week fields).
- Move to incremental ETL and partitioning as data volume grows.

---

## 13. Scalability and Future Improvements

1. Replace CSV fallback connectors with production-grade MySQL extraction and watermarking.
2. Add robust authentication and role-based authorization.
3. Introduce automated testing (ETL assertions, view tests, integration tests).
4. Add CI/CD pipeline for lint/check/test/deploy stages.
5. Model benchmark and collateral comparison metrics fully in database (reduce heuristic view logic).
6. Improve export path with server-side PDF rendering for deterministic output parity.

---

## 14. Code Quality Assessment

### Strengths
- Clear modular separation of ETL and dashboard concerns.
- Readable SQL-first transformations aligned with medallion architecture.
- Practical operational tooling for local setup and ETL execution.

### Technical Debt
- Limited automated tests.
- Some report logic lives in a large context-construction function.
- Security posture and auth model should be hardened for production.
- Dynamic SQL strategy can become hard to maintain at scale.

---

## 15. Quick Runbook

### Refresh data and run app
```bash
python manage.py run_etl
python manage.py runserver
```

### Open pages
- Menu: `http://127.0.0.1:8000/`
- Campaign login: `http://127.0.0.1:8000/campaign/<brand_campaign_id>/login/`
- Campaign report: `http://127.0.0.1:8000/campaign/<brand_campaign_id>/`
- Admin: `http://127.0.0.1:8000/admin/`

