from datetime import datetime, timezone
from django.core.management.base import BaseCommand

from etl.control.repository import ensure_control_tables, log_run
from etl.pipelines.raw_ingestion import ingest_raw
from etl.pipelines.bronze_transform import build_bronze
from etl.pipelines.silver_transform import build_silver
from etl.pipelines.gold_aggregations import build_gold


class Command(BaseCommand):
    help = "Run RAW->BRONZE->SILVER->GOLD ETL pipeline"

    def add_arguments(self, parser):
        parser.add_argument("--run-id", default=None)

    def handle(self, *args, **options):
        run_id = options["run_id"] or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        ensure_control_tables()
        try:
            counts = ingest_raw(run_id)
            build_bronze()
            build_silver(run_id)
            build_gold(run_id)
            log_run(run_id, "SUCCESS", notes=f"RAW tables loaded: {counts}")
            self.stdout.write(self.style.SUCCESS(f"ETL completed for run_id={run_id}"))
        except Exception as exc:
            log_run(run_id, "FAIL", notes=str(exc))
            raise
