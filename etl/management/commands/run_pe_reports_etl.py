from __future__ import annotations

from django.core.management.base import BaseCommand

from etl.pe_reports.pipeline import run_pipeline


class Command(BaseCommand):
    help = "Run the Patient Education RAW->BRONZE->SILVER->GOLD ETL pipeline"

    def add_arguments(self, parser):
        parser.add_argument("--run-id", default=None)
        parser.add_argument(
            "--skip-raw",
            action="store_true",
            help="Skip live MySQL extraction and build bronze/silver/gold from existing seeded raw tables.",
        )

    def handle(self, *args, **options):
        result = run_pipeline(
            run_id=options["run_id"],
            trigger_type="manual",
            skip_raw_extraction=bool(options["skip_raw"]),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"PE Reports ETL completed for run_id={result['run_id']} with as_of_date={result.get('as_of_date')}"
            )
        )
