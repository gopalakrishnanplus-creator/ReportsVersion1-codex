from __future__ import annotations

from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import resolve

from reporting.access import build_report_access


class Command(BaseCommand):
    help = "Verify report source backends, route isolation, and access-email endpoints."

    def handle(self, *args, **options):
        checks: list[tuple[str, str, str]] = []

        source_backend = str(getattr(settings, "SOURCE_EXTRACTOR_BACKEND", "")).strip().lower()
        sapa_backend = str(getattr(settings, "SAPA_SOURCE_EXTRACTOR_BACKEND", "")).strip().lower()

        checks.append(
            (
                "InClinic source mode",
                "OK" if source_backend == "mysql" else "FAIL",
                f"SOURCE_EXTRACTOR_BACKEND={source_backend or '<empty>'}",
            )
        )
        checks.append(
            (
                "PE source mode",
                "OK",
                "PE ETL raw extractors are wired to PE_MASTER_MYSQL and PE_PORTAL_MYSQL only.",
            )
        )
        checks.append(
            (
                "SAPA source mode",
                "OK" if sapa_backend == "mysql" else "FAIL",
                f"SAPA_SOURCE_EXTRACTOR_BACKEND={sapa_backend or '<empty>'}",
            )
        )

        route_expectations = [
            ("InClinic login route", "/campaign/demo/login/", "campaign-login"),
            ("InClinic access route", "/campaign/demo/access/", "campaign-access"),
            ("InClinic email route", "/campaign/demo/send-access-email/", "campaign-send-access-email"),
            ("PE login route", "/pe-reports/campaign/demo/login/", "pe_reports:login"),
            ("PE access route", "/pe-reports/campaign/demo/access/", "pe_reports:access"),
            ("PE email route", "/pe-reports/campaign/demo/send-access-email/", "pe_reports:send-access-email"),
            ("SAPA login route", "/sapa-growth/login/", "sapa_growth:login"),
            ("SAPA email route", "/sapa-growth/send-access-email/", "sapa_growth:send-access-email"),
        ]
        for label, path, expected_view in route_expectations:
            resolved = resolve(path).view_name
            checks.append((label, "OK" if resolved == expected_view else "FAIL", f"{path} -> {resolved}"))

        access_keys = {
            "inclinic": build_report_access("inclinic", "demo").session_key,
            "pe": build_report_access("pe", "demo").session_key,
            "sapa": build_report_access("sapa", "growth-clinic").session_key,
        }
        isolated = len(set(access_keys.values())) == 3
        checks.append(
            (
                "Session namespace isolation",
                "OK" if isolated else "FAIL",
                ", ".join(f"{key}={value}" for key, value in access_keys.items()),
            )
        )

        api_key = str(settings.REPORTS_EMAIL.get("SENDGRID_API_KEY") or "").strip()
        from_email = str(settings.REPORTS_EMAIL.get("FROM_EMAIL") or "").strip()
        checks.append(
            (
                "SendGrid configuration",
                "OK" if api_key and from_email else "WARN",
                f"api_key={'set' if api_key else 'missing'}, from_email={from_email or 'missing'}",
            )
        )

        has_failures = False
        for label, status, details in checks:
            if status == "FAIL":
                has_failures = True
                style = self.style.ERROR
            elif status == "WARN":
                style = self.style.WARNING
            else:
                style = self.style.SUCCESS
            self.stdout.write(style(f"[{status}] {label}: {details}"))

        if has_failures:
            raise SystemExit(1)
