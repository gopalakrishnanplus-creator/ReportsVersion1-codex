from __future__ import annotations

import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from etl.reporting_corrections import (
    RULE_KEEP_DOCTOR_WITH_REP,
    deactivate_seeded_reporting_correction_rules,
    upsert_reporting_correction_rule,
)


PRESET_FILES = {
    "apex-83ce-week5": "inclinic_apex_83ce_week5_manual_roster_corrections.csv",
}


class Command(BaseCommand):
    help = "Seed reviewed InClinic reporting correction rules without changing source tables."

    def add_arguments(self, parser):
        parser.add_argument(
            "--preset",
            choices=sorted(PRESET_FILES),
            default="apex-83ce-week5",
            help="Reviewed correction preset to seed.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report the preset rows without inserting/updating correction rules.",
        )

    def handle(self, *args, **options):
        preset = options["preset"]
        preset_path = Path(__file__).resolve().parents[2] / "correction_presets" / PRESET_FILES[preset]
        if not preset_path.exists():
            raise CommandError(f"Correction preset not found: {preset_path}")

        created = 0
        updated = 0
        skipped = 0
        active_correction_ids: set[str] = set()
        with preset_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                campaign_id = (row.get("campaign_id") or "").strip()
                doctor_phone = (row.get("doctor_phone") or "").strip()
                expected_rep = (row.get("expected_field_rep_brand_supplied_id") or "").strip()
                if not campaign_id or not doctor_phone or not expected_rep:
                    skipped += 1
                    continue
                if options["dry_run"]:
                    updated += 1
                    continue
                correction_id, was_created = upsert_reporting_correction_rule(
                    rule_type=RULE_KEEP_DOCTOR_WITH_REP,
                    system_name="inclinic",
                    campaign_id=campaign_id,
                    doctor_phone=doctor_phone,
                    doctor_name=(row.get("doctor_name") or "").strip(),
                    expected_field_rep_brand_supplied_id=expected_rep,
                    affected_field_rep_brand_supplied_ids=(row.get("affected_field_rep_brand_supplied_ids") or "").strip(),
                    reason=(row.get("reason") or "").strip(),
                    created_by=f"seed:{preset}:row:{(row.get('source_row') or '').strip()}",
                )
                active_correction_ids.add(correction_id)
                if was_created:
                    created += 1
                else:
                    updated += 1

        if options["dry_run"]:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Validated correction preset {preset}: {updated} importable row(s), {skipped} skipped row(s)."
                )
            )
            return
        deactivated = deactivate_seeded_reporting_correction_rules(
            created_by_prefix=f"seed:{preset}:row:",
            keep_correction_ids=active_correction_ids,
        )
        self.stdout.write(
            self.style.SUCCESS(
                (
                    f"Seeded correction preset {preset}: {created} created, {updated} updated/reactivated, "
                    f"{deactivated} stale deactivated, {skipped} skipped."
                )
            )
        )
