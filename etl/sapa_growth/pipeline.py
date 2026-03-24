from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from etl.sapa_growth.bronze import build_bronze
from etl.sapa_growth.control import ensure_control_tables, log_dq_issue, log_run, log_step, pipeline_lock, record_refresh
from etl.sapa_growth.gold import build_gold
from etl.sapa_growth.raw import ingest_api_sources, ingest_mysql_sources
from etl.sapa_growth.silver import build_silver


def run_pipeline(run_id: str | None = None, trigger_type: str = "manual") -> dict[str, Any]:
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    extracted_at = datetime.now(timezone.utc).isoformat()
    ensure_control_tables()
    log_run(run_id, "RUNNING", trigger_type=trigger_type, notes="{}")

    with pipeline_lock():
        try:
            raw_mysql = ingest_mysql_sources(run_id, extracted_at)
            raw_api = ingest_api_sources(run_id, extracted_at)

            raw_errors = {}
            raw_errors.update(raw_mysql.get("errors", {}))
            raw_errors.update(raw_api.get("errors", {}))
            if raw_errors:
                error_summary = json.dumps({"raw_errors": raw_errors}, default=str)
                log_run(run_id, "FAIL", trigger_type=trigger_type, notes=error_summary)
                raise RuntimeError(f"SAPA raw extraction failed: {error_summary}")

            bronze_counts = build_bronze()
            log_step(run_id, "build_bronze", "bronze_sapa", "SUCCESS", rows_written=sum(bronze_counts.values()))

            silver_result = build_silver(run_id)
            log_step(
                run_id,
                "build_silver",
                "silver_sapa",
                "SUCCESS",
                rows_written=sum(silver_result.get("counts", {}).values()),
            )

            invalid_course_status = silver_result.get("issues", {}).get("invalid_course_status", {})
            if invalid_course_status:
                log_dq_issue(
                    run_id,
                    "silver",
                    "fact_course_user_progress",
                    "invalid_progress_status",
                    sum(int(value) for value in invalid_course_status.values()),
                    json.dumps(invalid_course_status, default=str),
                )

            stale_sources = raw_api.get("stale_sources", [])
            gold_result = build_gold(
                run_id,
                source_status="SUCCESS",
                stale_source_flags=",".join(stale_sources),
                notes="",
            )
            log_step(run_id, "build_gold", "gold_sapa", "SUCCESS", rows_written=len(gold_result.get("tables", [])))
            record_refresh(run_id, gold_result["as_of_date"], "SUCCESS", ",".join(stale_sources), "")

            notes = {
                "raw_mysql_counts": raw_mysql.get("counts", {}),
                "raw_api_counts": raw_api.get("counts", {}),
                "bronze_counts": bronze_counts,
                "silver_counts": silver_result.get("counts", {}),
                "gold_tables": gold_result.get("tables", []),
                "as_of_date": gold_result.get("as_of_date"),
            }
            log_run(run_id, "SUCCESS", trigger_type=trigger_type, notes=json.dumps(notes, default=str))
            return {"run_id": run_id, **notes}
        except Exception as exc:
            log_run(run_id, "FAIL", trigger_type=trigger_type, notes=str(exc))
            raise
