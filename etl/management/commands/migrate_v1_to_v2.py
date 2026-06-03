from __future__ import annotations

import traceback
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from etl.v1_to_v2_migration import (
    build_migration_plan,
    failed_global_row,
    rebuild_reporting_from_v2,
    write_reports,
    write_startup_failure_reports,
    write_v2_tables,
)


class Command(BaseCommand):
    help = (
        "Migrate reporting V1 raw tables into canonical V2 raw tables, "
        "write audit reports, validate the result, and rebuild reporting from V2."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--run-id",
            default="",
            help="Optional stable run id. Defaults to v1_to_v2_<UTC timestamp>.",
        )
        parser.add_argument(
            "--report-dir",
            default="",
            help="Directory for CSV/TXT migration reports. Defaults to var/v1_to_v2_migrations/<run-id>.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Build and validate the migration plan and reports without writing V2 tables.",
        )
        parser.add_argument(
            "--skip-reporting-rebuild",
            action="store_true",
            help="Write validated V2 tables but do not rebuild silver/gold reporting tables.",
        )

    def handle(self, *args, **options):
        run_id = options["run_id"] or f"v1_to_v2_{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
        dry_run = bool(options["dry_run"])
        skip_reporting_rebuild = bool(options["skip_reporting_rebuild"])
        report_dir = Path(options["report_dir"] or Path("var") / "v1_to_v2_migrations" / run_id).resolve()

        def progress(message: str) -> None:
            self.stdout.write(message)

        self.stdout.write(self.style.NOTICE(f"Starting V1 to V2 migration: {run_id}"))
        self.stdout.write(f"Report directory: {report_dir}")

        plan = None
        extra_traceback = ""
        try:
            plan = build_migration_plan(run_id, progress=progress)
            if plan.validation_status != "PASS" or plan.failed_rows:
                write_reports(
                    report_dir,
                    plan,
                    dry_run=dry_run,
                    skipped_rebuild=True,
                    extra_traceback="Validation failed before database write.",
                )
                raise CommandError(
                    f"Validation failed. V2 tables were not switched. See reports in {report_dir}"
                )

            if dry_run:
                self.stdout.write(self.style.WARNING("Dry run only. V2 tables were not written."))
            else:
                write_v2_tables(plan, progress=progress)
                if skip_reporting_rebuild:
                    self.stdout.write(
                        self.style.WARNING("Skipped reporting rebuild. Application reporting was not switched.")
                    )
                else:
                    plan.reporting_result = rebuild_reporting_from_v2(run_id, progress=progress)

            report_paths = write_reports(
                report_dir,
                plan,
                dry_run=dry_run,
                skipped_rebuild=dry_run or skip_reporting_rebuild,
                extra_traceback=extra_traceback,
            )

        except Exception as exc:
            extra_traceback = traceback.format_exc()
            if plan is not None:
                plan.failed_rows.append(failed_global_row(run_id, exc))
                plan.validation_status = "FAIL"
                plan.validation_rows.append(
                    {
                        "check_name": "global_migration_error",
                        "scope": "command_execution",
                        "source_count": "",
                        "target_count": "1",
                        "status": "FAIL",
                        "details": str(exc),
                    }
                )
                write_reports(
                    report_dir,
                    plan,
                    dry_run=dry_run,
                    skipped_rebuild=True,
                    extra_traceback=extra_traceback,
                )
            else:
                write_startup_failure_reports(report_dir, run_id, exc, dry_run=dry_run)
            raise CommandError(f"V1 to V2 migration failed. See reports in {report_dir}: {exc}") from exc

        self.stdout.write(self.style.SUCCESS("V1 to V2 migration completed successfully."))
        self.stdout.write(self.style.SUCCESS(f"Overall validation: {plan.validation_status}"))
        self.stdout.write("Generated reports:")
        for label, path in report_paths.items():
            self.stdout.write(f"  {label}: {path}")
