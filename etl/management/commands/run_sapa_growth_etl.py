from __future__ import annotations

from django.core.management.base import BaseCommand

from etl.sapa_growth.pipeline import run_pipeline


class Command(BaseCommand):
    help = "Run the SAPA Growth Clinic RAW->BRONZE->SILVER->GOLD ETL pipeline"

    def add_arguments(self, parser):
        parser.add_argument("--run-id", default=None)

    def handle(self, *args, **options):
        result = run_pipeline(run_id=options["run_id"], trigger_type="manual")
        self.stdout.write(
            self.style.SUCCESS(
                f"SAPA ETL completed for run_id={result['run_id']} with as_of_date={result.get('as_of_date')}"
            )
        )
