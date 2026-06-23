from __future__ import annotations

import os

from django.core.management.base import BaseCommand, CommandError

from etl.weekly_transfer_cleanup import run_weekly_transfer_cleanup


class Command(BaseCommand):
    help = "Run weekly transfer pipelines and delete approved source transaction data after successful reporting ingestion"

    def add_arguments(self, parser):
        parser.add_argument(
            "--domains",
            default="inclinic,rfa",
            help="Comma-separated cleanup domains to run. Supported values: inclinic, rfa.",
        )
        parser.add_argument(
            "--source-tables",
            default="",
            help="Optional comma-separated protected source tables to clean. Omit to clean every approved table in the selected domains.",
        )

    def handle(self, *args, **options):
        enabled = os.environ.get("ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP", "0").strip().lower()
        if enabled not in {"1", "true", "yes", "y", "on"}:
            raise CommandError(
                "Source transfer cleanup is disabled. Set ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP=1 only after "
                "source deletion has been explicitly approved and reporting preservation has been verified."
            )

        raw_domains = [item.strip() for item in str(options["domains"]).split(",") if item.strip()]
        if not raw_domains:
            raise CommandError("At least one cleanup domain must be provided.")

        raw_source_tables = [item.strip() for item in str(options["source_tables"]).split(",") if item.strip()]
        result = run_weekly_transfer_cleanup(raw_domains, source_tables=raw_source_tables or None)
        cleanup_run_id = result["cleanup_run_id"]
        status = result["status"]

        if status == "SUCCESS":
            self.stdout.write(self.style.SUCCESS(f"Weekly transfer cleanup completed for cleanup_run_id={cleanup_run_id}"))
            return

        if status == "PARTIAL_SUCCESS":
            self.stdout.write(
                self.style.WARNING(
                    f"Weekly transfer cleanup completed with partial success for cleanup_run_id={cleanup_run_id}"
                )
            )
            return

        raise CommandError(f"Weekly transfer cleanup failed for cleanup_run_id={cleanup_run_id}")
