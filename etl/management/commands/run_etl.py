import json
from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from etl.control.repository import ensure_control_tables, log_run
from etl.pipelines.bronze_transform import build_bronze
from etl.pipelines.gold_aggregations import build_gold
from etl.pipelines.raw_ingestion import ingest_raw
from etl.pipelines.silver_transform import build_silver
from etl.utils.specs import SOURCE_TABLE_SPECS


class Command(BaseCommand):
    help = "Run RAW->BRONZE->SILVER->GOLD ETL pipeline"

    def add_arguments(self, parser):
        parser.add_argument("--run-id", default=None)

    def handle(self, *args, **options):
        run_id = options["run_id"] or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        ensure_control_tables()

        try:
            raw_result = ingest_raw(run_id)
            counts = raw_result.get("counts", {})
            errors = raw_result.get("errors", {})

            build_bronze()
            build_silver(run_id)
            build_gold(run_id)

            total_tables = sum(len(tables) for tables in SOURCE_TABLE_SPECS.values())
            failed_tables = len(errors)
            loaded_tables = total_tables - failed_tables
            loaded_rows = sum(int(v) for v in counts.values()) if counts else 0

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
                    "loaded_rows": loaded_rows,
                },
                "counts": counts,
                "errors": errors,
            }
            log_run(run_id, status, notes=json.dumps(notes, default=str))

            if status == "SUCCESS":
                self.stdout.write(self.style.SUCCESS(f"ETL completed for run_id={run_id}"))
            elif status == "PARTIAL_SUCCESS":
                self.stdout.write(
                    self.style.WARNING(
                        f"ETL completed with partial source failures for run_id={run_id} "
                        f"({failed_tables}/{total_tables} source tables failed)"
                    )
                )
            else:
                self.stdout.write(
                    self.style.ERROR(
                        f"ETL completed with no successful source extractions for run_id={run_id}; "
                        f"all {total_tables} source tables failed"
                    )
                )
        except Exception as exc:
            log_run(run_id, "FAIL", notes=str(exc))
            raise
