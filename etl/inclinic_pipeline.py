from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from etl.control.repository import ensure_control_tables, log_run
from etl.pipelines.bronze_transform import build_bronze
from etl.pipelines.gold_aggregations import build_gold
from etl.pipelines.raw_ingestion import ingest_raw
from etl.pipelines.silver_transform import build_silver
from etl.pipelines.v2_reporting import _load_source_from_mysql_v2, build_v2_reporting, refresh_raw_v2_from_source
from etl.utils.specs import SOURCE_TABLE_SPECS


def run_pipeline(run_id: str | None = None, trigger_type: str = "manual") -> dict[str, Any]:
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ensure_control_tables()

    try:
        source_mode = os.environ.get("INCLINIC_REPORTING_SOURCE_MODE", "v2").strip().lower()
        if source_mode == "v2":
            refresh_counts = {}
            refresh_source_v2 = os.environ.get("INCLINIC_REPORTING_REFRESH_RAW_V2_FROM_SOURCE", "1").strip().lower()
            source_v2 = None
            if refresh_source_v2 in {"1", "true", "yes", "y", "on"}:
                source_v2 = _load_source_from_mysql_v2()
                refresh_counts = refresh_raw_v2_from_source(run_id, source=source_v2)
            silver_result = build_v2_reporting(run_id, source=source_v2)
            build_gold(run_id)
            counts = silver_result.get("counts", {})
            issues = silver_result.get("issues", {})
            preservation_counts = silver_result.get("preservation_counts", {})
            preserved_rows = sum(int(row.get("archived", 0)) for row in preservation_counts.values())
            notes = {
                "summary": {
                    "source_mode": "v2",
                    "loaded_tables": len(counts),
                    "loaded_rows": sum(int(v) for v in counts.values()),
                    "dq_issue_types": len(issues),
                    "dq_issue_rows": sum(int(v) for v in issues.values()),
                    "preserved_reporting_tables": len(preservation_counts),
                    "preserved_reporting_rows": preserved_rows,
                    "source_v2_refresh_tables": len(refresh_counts),
                    "source_v2_refresh_rows": sum(int(v) for v in refresh_counts.values()),
                },
                "source_v2_refresh_counts": refresh_counts,
                "counts": counts,
                "preservation_counts": preservation_counts,
                "issues": issues,
                "errors": {},
            }
            log_run(run_id, "SUCCESS", trigger_type=trigger_type, notes=json.dumps(notes, default=str))
            return {
                "run_id": run_id,
                "status": "SUCCESS",
                "counts": counts,
                "errors": {},
                "preservation_counts": preservation_counts,
                "summary": notes["summary"],
            }

        raw_result = ingest_raw(run_id)
        counts = raw_result.get("counts", {})
        skipped_counts = raw_result.get("skipped_counts", {})
        extracted_counts = raw_result.get("extracted_counts", {})
        errors = raw_result.get("errors", {})

        build_bronze()
        build_silver(run_id)
        build_gold(run_id)

        total_tables = sum(len(tables) for tables in SOURCE_TABLE_SPECS.values())
        failed_tables = len(errors)
        loaded_tables = total_tables - failed_tables
        loaded_rows = sum(int(v) for v in counts.values()) if counts else 0
        skipped_rows = sum(int(v) for v in skipped_counts.values()) if skipped_counts else 0
        extracted_rows = sum(int(v) for v in extracted_counts.values()) if extracted_counts else loaded_rows + skipped_rows

        if failed_tables == 0:
            status = "SUCCESS"
        elif failed_tables == total_tables:
            status = "FAIL"
        else:
            status = "PARTIAL_SUCCESS"

        notes = {
            "summary": {
                "total_tables": total_tables,
                "loaded_tables": loaded_tables,
                "failed_tables": failed_tables,
                "extracted_rows": extracted_rows,
                "loaded_rows": loaded_rows,
                "skipped_existing_rows": skipped_rows,
            },
            "counts": counts,
            "skipped_counts": skipped_counts,
            "extracted_counts": extracted_counts,
            "errors": errors,
        }
        log_run(run_id, status, trigger_type=trigger_type, notes=json.dumps(notes, default=str))
        return {
            "run_id": run_id,
            "status": status,
            "counts": counts,
            "errors": errors,
            "summary": notes["summary"],
        }
    except Exception as exc:
        log_run(run_id, "FAIL", trigger_type=trigger_type, notes=str(exc))
        raise
