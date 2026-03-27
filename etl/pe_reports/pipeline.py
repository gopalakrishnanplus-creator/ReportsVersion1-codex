from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from etl.pe_reports.bronze import build_bronze
from etl.pe_reports.control import ensure_control_tables, log_dq_issue, log_run, log_step, pipeline_lock, record_refresh
from etl.pe_reports.gold import build_gold
from etl.pe_reports.raw import ensure_raw_tables, ingest_master_sources, ingest_portal_sources
from etl.pe_reports.silver import build_silver


def run_pipeline(run_id: str | None = None, trigger_type: str = "manual", skip_raw_extraction: bool = False) -> dict[str, Any]:
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    extracted_at = datetime.now(timezone.utc).isoformat()
    ensure_control_tables()
    log_run(run_id, "RUNNING", trigger_type=trigger_type, notes="{}")

    with pipeline_lock():
        try:
            if skip_raw_extraction:
                ensure_raw_tables()
                raw_portal = {"counts": {}, "errors": {}, "max_watermarks": {}}
                raw_master = {"counts": {}, "errors": {}, "max_watermarks": {}}
                log_step(run_id, "extract_portal", "seeded_raw_pe_portal", "SKIPPED")
                log_step(run_id, "extract_master", "seeded_raw_pe_master", "SKIPPED")
            else:
                raw_portal = ingest_portal_sources(run_id, extracted_at)
                raw_master = ingest_master_sources(run_id, extracted_at)
            raw_errors = {}
            raw_errors.update(raw_portal.get("errors", {}))
            raw_errors.update(raw_master.get("errors", {}))
            if raw_errors:
                error_summary = json.dumps({"raw_errors": raw_errors}, default=str)
                log_run(run_id, "FAIL", trigger_type=trigger_type, notes=error_summary)
                raise RuntimeError(f"PE raw extraction failed: {error_summary}")

            bronze_counts = build_bronze()
            log_step(run_id, "build_bronze", "bronze_pe", "SUCCESS", rows_written=sum(bronze_counts.values()))

            silver_result = build_silver(run_id)
            silver_counts = silver_result.get("counts", {})
            silver_issues = silver_result.get("issues", {})
            log_step(run_id, "build_silver", "silver_pe", "SUCCESS", rows_written=sum(silver_counts.values()))
            for issue_name, issue_count in silver_issues.items():
                if int(issue_count or 0) > 0:
                    log_dq_issue(run_id, "silver", "silver_pe", issue_name, int(issue_count))

            gold_result = build_gold(run_id, source_status="SUCCESS", notes="")
            log_step(run_id, "build_gold", "gold_pe", "SUCCESS", rows_written=len(gold_result.get("campaign_schemas", [])))
            record_refresh(run_id, gold_result["as_of_date"], "SUCCESS", json.dumps({"campaign_schemas": gold_result.get("campaign_schemas", [])}))

            notes = {
                "raw_portal_counts": raw_portal.get("counts", {}),
                "raw_master_counts": raw_master.get("counts", {}),
                "bronze_counts": bronze_counts,
                "silver_counts": silver_counts,
                "silver_issues": silver_issues,
                "campaign_schemas": gold_result.get("campaign_schemas", []),
                "campaigns_built": gold_result.get("campaigns_built", 0),
                "benchmark_population_count": gold_result.get("benchmark_population_count", 0),
                "as_of_date": gold_result.get("as_of_date"),
            }
            log_run(run_id, "SUCCESS", trigger_type=trigger_type, notes=json.dumps(notes, default=str))
            return {"run_id": run_id, **notes}
        except Exception as exc:
            log_run(run_id, "FAIL", trigger_type=trigger_type, notes=str(exc))
            raise
