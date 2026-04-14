# Weekly Transfer and Cleanup Plan

## Goal

Run a once-a-week archive workflow that:

1. extracts eligible rows from source operational databases,
2. stores them in the central PostgreSQL reporting database,
3. verifies the copy,
4. deletes only the rows that were successfully copied,
5. never touches source master/reference data outside the approved scope.

This plan is aligned to the current Django ETL structure already in this repo.

## Current Repo Fit

The repository already has:

- central PostgreSQL RAW landing layers for Inclinic and related reporting pipelines
- control tables / watermark patterns in the ETL code
- Django management commands that are already used on EC2
- GitHub Actions only for deployment, not for recurring data movement

Relevant files:

- `etl/management/commands/run_etl.py`
- `etl/management/commands/run_sapa_growth_etl.py`
- `etl/management/commands/run_pe_reports_etl.py`
- `etl/utils/specs.py`
- `etl/sapa_growth/specs.py`
- `.github/workflows/deploy.yml`
- `deploy.sh`

## Recommendation

### Preferred architecture

Use a dedicated Django management command on the EC2 host and schedule it with an AWS-managed scheduler.

Recommended runtime flow:

`EventBridge schedule -> Systems Manager Run Command (or Automation) -> EC2 -> Django management command -> PostgreSQL control tables / logs`

Why this is the best fit here:

- the repo already runs Django management commands on EC2
- the source databases may be private to the VPC or only reachable from EC2
- cleanup should be decoupled from deployment
- AWS scheduling gives better observability and retry options than bare cron
- GitHub Actions is better kept for code delivery, not as the primary scheduler for destructive data jobs

### Practical fallback

If you want the fastest implementation with the fewest AWS changes:

- run the same Django management command from EC2 using `systemd` timer
- use cron only if you want the simplest possible scheduler and accept weaker observability

## Scheduling Strategy

### Recommended run time

Run the weekly transfer + cleanup on **Friday at 11:35 PM IST**.

Why:

- it finishes before the Saturday brand report window
- it leaves Saturday morning buffer for investigation or a manual rerun
- it avoids running exactly on the hour, which is useful if a GitHub or cloud scheduler is ever used

If the reporting team normally sends the report very early on Saturday, move the run earlier to **Friday 10:30 PM IST**.

### Reporting buffer

Recommended operating window:

- Transfer + cleanup job: Friday 11:35 PM IST
- Data validation / dashboard refresh check: Saturday 6:00 AM IST
- Brand report generation/send: Saturday business hours

## Why Not Tie This To Deploy

`deploy.sh` already runs ETL during deployments. That is useful for freshness after code changes, but **weekly archive deletion should not be coupled to push/deploy**.

Reasons:

- a deployment is not the same thing as a scheduled retention run
- destructive cleanup must have its own logs, approvals, and alerting
- a failed deployment should not block the scheduled archive process, and vice versa

## Option Comparison

### Option 1: Cron job on EC2

Good for:

- quickest rollout
- minimal change from the current EC2-centric setup

Pros:

- simplest to implement
- runs where DB connectivity already exists
- no GitHub scheduler dependency

Cons:

- weaker retry handling
- weaker alerting unless you add CloudWatch or log scraping
- harder auditability than AWS-managed scheduling

Verdict:

- acceptable
- better if implemented as `systemd` timer instead of plain cron

### Option 2: Scheduled GitHub Actions

Good for:

- light orchestration where the runner already has private DB access

Pros:

- easy to version in repo
- familiar to the team

Cons:

- not ideal for destructive jobs that depend on private DB connectivity
- if using GitHub-hosted runners, source DBs may need public exposure or SSH indirection
- scheduler reliability is weaker than EC2/AWS-native scheduling for this use case
- should not perform deletes directly from the hosted runner

Verdict:

- not recommended as the primary scheduler
- acceptable only if the workflow simply SSHes into EC2 and runs the same management command there

### Option 3: AWS-managed scheduler

Good for:

- production-grade scheduling
- retries, monitoring, IAM-based execution, centralized control

Pros:

- scheduler is separate from GitHub and separate from deployment
- works well with EC2 via Systems Manager
- cleaner operational model for weekly data movement + cleanup

Cons:

- small amount of AWS setup required

Verdict:

- **recommended**

### Lambda + EventBridge note

This is possible, but it is not my first recommendation here.

Why:

- the workflow may touch multiple databases and run longer than a lightweight function should
- packaging DB drivers and VPC networking adds operational overhead
- Lambda is a weaker fit if the job grows into a longer ETL + verification + cleanup sequence

Verdict:

- acceptable for a very small weekly job
- not preferred for this repo compared with EC2 execution

### Airflow note

Use Airflow only if this weekly job is expected to grow into a larger orchestrated data platform with many DAGs, SLAs, backfills, and lineage requirements.

Verdict:

- powerful but likely overkill for the current requirement

## Safe Transfer-and-Delete Pattern

### Important repo caveat

Do **not** enable source deletion directly off the current `ingest_raw()` / raw append logic as-is.

Why:

- `etl/pipelines/raw_ingestion.py` inserts rows into RAW without a per-source delete receipt
- `etl/pe_reports/raw.py` and `etl/sapa_growth/raw.py` are incremental, but they still do not maintain a source-side deletion manifest
- without a manifest keyed to source rows, you cannot prove exactly which records are safe to delete

### Required design

Add a new archive/cleanup control layer:

- `control.transfer_cleanup_run`
- `control.transfer_cleanup_manifest`
- `control.transfer_cleanup_step_log`

Recommended manifest fields:

- `run_id`
- `source_system`
- `source_table`
- `source_pk`
- `source_watermark`
- `source_row_hash`
- `copied_to_schema`
- `copied_to_table`
- `copy_status`
- `verify_status`
- `delete_status`
- `delete_attempted_at`
- `delete_error`

Recommended uniqueness:

- unique on `(source_system, source_table, source_pk, source_row_hash)`

That uniqueness gives idempotency on reruns.

## Approved Delete Scope

### Inclinic

Delete from source:

- `sharing_management_collateraltransaction`

Do not delete from source:

- `doctor_viewer_doctor`
- `campaign_fieldrep`
- `campaign_campaignfieldrep`
- `campaign_campaign`
- `campaign_management_campaign`
- `collateral_management_campaigncollateral`
- `collateral_management_collateral`
- `sharing_management_sharelog`

### RFA / patient-submission flow

Safe phase-1 delete scope from source:

- `redflags_patientsubmission`
- `gnd_gndpatientsubmission`
- `redflags_submissionredflag`
- `gnd_gndsubmissionredflag`

Do not delete from source:

- `redflags_doctor`
- `campaign_doctor`
- `campaign_fieldrep`
- `campaign_campaignfieldrep`
- `redflags_redflag`
- `gnd_gndredflag`
- `redflags_patientvideo`
- `gnd_gndpatientvideo`

Keep out of phase 1 until explicitly confirmed:

- `redflags_followupreminder`
- `redflags_metricevent`

Those two tables contain patient-linked activity, but they are also used by the reporting pipeline and may still be operationally relevant. They should only be added after business confirmation that source-side retention is no longer needed.

## Idempotent Workflow

For each configured source table:

1. acquire a Postgres advisory lock so only one weekly cleanup run executes
2. read the last successful watermark
3. extract candidate rows using a lookback window
4. upsert rows into the central Postgres landing table
5. write one manifest row per copied source record
6. verify copied row count and checksum/hash coverage
7. delete only manifest rows with `copy_status='SUCCESS'` and `verify_status='SUCCESS'`
8. verify source delete count matches manifest delete count
9. mark manifest rows as `DELETED`
10. advance the cleanup watermark only after delete verification succeeds

If any step fails:

- do not advance watermark
- do not delete unmatched rows
- stop processing that table
- continue to the next table only if the business wants partial success behavior

## Deletion Guardrails

Use all of the following:

- chunk deletes in small batches such as 500 or 1000 rows
- delete by source primary key, not by date-only filters
- for mutable tables, also guard by `updated_at <= extracted_high_watermark`
- for immutable event tables, `id` plus manifest hash is enough
- commit copy and manifest creation before any delete starts
- log every batch result

## Sample Command Shape

Suggested new command:

```bash
python manage.py run_weekly_transfer_cleanup --domains=inclinic,rfa
```

Suggested internal structure:

```python
def handle(self, *args, **options):
    run_id = new_run_id()
    acquire_lock("weekly_transfer_cleanup")
    for spec in ENABLED_ARCHIVE_SPECS:
        rows = extract_with_lookback(spec)
        receipts = upsert_into_central_store(spec, rows, run_id)
        verification = verify_copy(spec, receipts, run_id)
        if not verification.ok:
            mark_failed(spec, run_id, verification.error)
            continue

        deletable = [r for r in receipts if r.copy_ok and r.verify_ok]
        for batch in chunked(deletable, spec.delete_batch_size):
            delete_result = delete_from_source(spec, batch)
            record_delete_result(spec, run_id, batch, delete_result)
            if not delete_result.ok:
                raise CleanupError(spec.name, delete_result.error)

        advance_watermark(spec, run_id, verification.max_watermark)
        mark_success(spec, run_id)
```

## Pseudocode For Safe Copy + Delete

```python
def transfer_and_cleanup(spec, run_id):
    source_rows = spec.connector.fetch_rows(
        watermark_start=load_watermark(spec),
        lookback_days=spec.lookback_days,
    )

    copied_receipts = []
    for row in source_rows:
        source_pk = row[spec.pk_column]
        source_hash = hash_row(row, spec.columns)

        upsert_central_row(
            schema=spec.target_schema,
            table=spec.target_table,
            row=row,
            source_system=spec.source_system,
            source_table=spec.source_table,
            source_pk=source_pk,
            source_hash=source_hash,
        )

        copied_receipts.append(
            {
                "source_pk": source_pk,
                "source_hash": source_hash,
                "watermark": row.get(spec.watermark_column),
            }
        )

    verify_manifest_against_target(spec, copied_receipts)

    for batch in chunked(copied_receipts, 500):
        spec.connector.delete_rows(
            table=spec.source_table,
            rows=batch,
            guard_watermark=True,
        )

    mark_watermark_after_verified_delete(spec, copied_receipts)
```

### Example guarded delete for Inclinic

Use the source PK and `updated_at` guard:

```sql
DELETE FROM sharing_management_collateraltransaction
WHERE id = %s
  AND updated_at <= %s;
```

If the row changed after extraction, `updated_at` will be newer and the delete will be skipped, which is what we want.

### Example guarded delete for immutable submission tables

```sql
DELETE FROM redflags_patientsubmission
WHERE record_id = %s;
```

Use this only where the source row is effectively append-only.

## Multi-Database Support

Introduce a generic source adapter layer:

- `MySQLSourceAdapter`
- `PostgresSourceAdapter`

Each adapter should support:

- `fetch_rows`
- `delete_rows`
- `healthcheck`

Each table spec should define:

- connection name
- source table
- target schema/table
- primary key column(s)
- watermark column
- mutability
- delete enabled flag
- batch size

That lets you mix:

- RDS MySQL
- RDS PostgreSQL
- EC2-hosted MySQL
- EC2-hosted PostgreSQL

under one weekly orchestrator.

## Logging, Monitoring, Failure Handling

### Logging

- write structured JSON logs to stdout
- persist run/step/manifest status in PostgreSQL control tables
- include source system, source table, run id, batch number, row counts, and error text

### Monitoring

- send EC2 command output to CloudWatch Logs
- create an alarm on non-zero exit or `FAIL` status
- send notification through SNS or email

### Failure handling

- no watermark advancement on failure
- no delete without successful copy verification
- chunked retries only for failed delete batches
- manual rerun should be safe because of manifest uniqueness and target upsert behavior

## Security

- keep DB credentials out of the repo
- prefer AWS Secrets Manager or SSM Parameter Store
- if staying with the current env-file pattern, store the file only on EC2 at `/var/www/secrets/.env`
- use least-privilege DB users
- use read-only credentials for extract-only sources where deletion is disabled
- use a separate credential or role for tables that are allowed to delete
- enforce TLS/SSL for RDS/MySQL/Postgres connections where available

## Sample Scheduler Snippets

### EC2 cron

```cron
35 23 * * 5 cd /var/www/ReportsVersion1 && /var/www/venv/bin/python manage.py run_weekly_transfer_cleanup --domains=inclinic,rfa >> /var/log/reports/weekly-transfer-cleanup.log 2>&1
```

### GitHub Actions sample

If you still choose GitHub Actions, keep the actual data job on EC2:

```yaml
name: Weekly Transfer Cleanup

on:
  workflow_dispatch:
  schedule:
    - cron: '35 23 * * 5'
      timezone: 'Asia/Kolkata'

jobs:
  run-cleanup:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run command on EC2
        run: |
          ssh -i ~/.ssh/id_rsa ${{ secrets.EC2_USER }}@${{ secrets.EC2_HOST }} \
            "cd ${{ secrets.EC2_TARGET_DIR }} && /var/www/venv/bin/python manage.py run_weekly_transfer_cleanup --domains=inclinic,rfa"
```

### AWS-managed schedule

Preferred target:

- weekly EventBridge schedule
- target: Systems Manager Run Command or Automation
- command executed on the EC2 instance:

```bash
cd /var/www/ReportsVersion1 && /var/www/venv/bin/python manage.py run_weekly_transfer_cleanup --domains=inclinic,rfa
```

## Final Recommendation

Use this rollout order:

1. implement a dedicated `run_weekly_transfer_cleanup` management command
2. add manifest/receipt tables before enabling any delete
3. enable delete scope only for:
   - `sharing_management_collateraltransaction`
   - `redflags_patientsubmission`
   - `gnd_gndpatientsubmission`
   - `redflags_submissionredflag`
   - `gnd_gndsubmissionredflag`
4. schedule it for Friday 11:35 PM IST
5. run it from EC2, preferably triggered by AWS-managed scheduling
6. keep GitHub Actions for deploy only, or use it only as a thin SSH wrapper if needed

That gives you the safest version of weekly transfer + cleanup without mixing deployment, reporting, and destructive retention logic into one path.
