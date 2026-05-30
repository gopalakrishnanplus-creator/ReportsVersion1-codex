from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from etl.control.repository import ensure_control_tables, log_run
from etl.pipelines.bronze_transform import build_bronze
from etl.pipelines.gold_aggregations import build_gold
from etl.pipelines.raw_ingestion import ingest_raw
from etl.pipelines.silver_transform import build_silver
from etl.utils.specs import SOURCE_TABLE_SPECS


def run_pipeline(run_id: str | None = None, trigger_type: str = "manual") -> dict[str, Any]:
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ensure_control_tables()

    try:
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
