from __future__ import annotations

import base64
import json
from io import BytesIO
from unittest.mock import patch
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve, reverse

from sapa_growth.logic import classify_metric_event, explode_followup_schedule, map_course_status, normalize_phone, webinar_effective_date
from sapa_growth.services import _derived_certified_rows, _enrich_video_rows, dashboard_context, export_dashboard_pdf
from sapa_growth.video_metadata import resolve_video_metadata


class SapaGrowthLogicTests(SimpleTestCase):
    def test_map_course_status(self):
        self.assertEqual(map_course_status("In Progress"), "Started")
        self.assertEqual(map_course_status("Completed"), "Completed")
        self.assertEqual(map_course_status("Not Started"), "Pending")
        self.assertIsNone(map_course_status("Archived"))

    def test_webinar_effective_date_prefers_registration_timestamp(self):
        row = {
            "registration_created_at": "2026-03-15 09:00:00",
            "start_date": "2026-03-18 11:00:00",
        }
        self.assertEqual(str(webinar_effective_date(row)), "2026-03-15")

    def test_followup_schedule_explodes_explicit_and_derived_without_duplicates(self):
        row = {
            "followup_date1": "2026-03-10",
            "followup_date2": "2026-03-25",
            "followup_date3": "",
            "first_followup_date": "2026-03-10",
            "frequency_unit": "d",
            "frequency": "15",
            "num_followups": "3",
        }
        dates = [item["scheduled_followup_date"] for item in explode_followup_schedule(row)]
        self.assertEqual(dates, ["2026-03-10", "2026-03-25", "2026-04-09"])

    def test_metric_event_classification(self):
        self.assertTrue(classify_metric_event("action_click", "reminder_sent")["is_reminder_sent"])
        self.assertTrue(classify_metric_event("action_click", "patient_edu")["is_patient_education"])
        self.assertTrue(classify_metric_event("doctor_edu_click", "doctor_video_click")["is_doctor_education"])

    def test_phone_normalization(self):
        self.assertEqual(normalize_phone("+91 98765 43210"), "9876543210")

    def test_video_rows_keep_only_supported_external_links(self):
        with patch(
            "sapa_growth.services.resolve_video_metadata",
            side_effect=[
                {
                    "video_url": "https://youtu.be/GUtzx5PH7mo?feature=shared",
                    "video_title": "Actual Patient Title",
                    "preferred_display_label": "Actual Patient Title",
                },
                {
                    "video_url": "https://vimeo.com/1096825313/c9c304c088?share=copy",
                    "video_title": "Actual Doctor Title",
                    "preferred_display_label": "Actual Doctor Title",
                },
            ],
        ):
            rows = _enrich_video_rows(
                [
                    {
                        "audience": "patient",
                        "content_identifier": "https://youtu.be/GUtzx5PH7mo?feature=shared",
                        "preferred_display_label": "Placeholder",
                    },
                    {"audience": "patient", "content_identifier": "/videos/patient/RF0052/en/"},
                    {"audience": "patient", "content_identifier": "https://drive.google.com/file/d/abc/view"},
                    {
                        "audience": "doctor",
                        "content_identifier": "https://vimeo.com/1096825313/c9c304c088?share=copy",
                        "video_title": "Placeholder",
                    },
                ]
            )
        self.assertEqual(
            rows,
            [
                {
                    "audience": "patient",
                    "content_identifier": "https://youtu.be/GUtzx5PH7mo?feature=shared",
                    "video_url": "https://youtu.be/GUtzx5PH7mo?feature=shared",
                    "video_title": "Actual Patient Title",
                    "preferred_display_label": "Actual Patient Title",
                },
                {
                    "audience": "doctor",
                    "content_identifier": "https://vimeo.com/1096825313/c9c304c088?share=copy",
                    "video_url": "https://vimeo.com/1096825313/c9c304c088?share=copy",
                    "video_title": "Actual Doctor Title",
                    "preferred_display_label": "Actual Doctor Title",
                },
            ],
        )

    def test_video_metadata_resolves_title_from_oembed(self):
        resolve_video_metadata.cache_clear()

        class DummyResponse(BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch(
            "sapa_growth.video_metadata.urlopen",
            return_value=DummyResponse(json.dumps({"title": "Example Video Title"}).encode("utf-8")),
        ):
            metadata = resolve_video_metadata("https://youtu.be/GUtzx5PH7mo?feature=shared")
        self.assertEqual(metadata["video_title"], "Example Video Title")
        self.assertEqual(metadata["preferred_display_label"], "Example Video Title")
        self.assertEqual(metadata["video_url"], "https://youtu.be/GUtzx5PH7mo?feature=shared")

    def test_certified_rows_require_active_clinic_and_doctor_course_enrollment(self):
        rows = _derived_certified_rows(
            {"state": None, "field_rep_id": None, "doctor_key": None},
            [
                {"doctor_key": "DOC-1", "doctor_display_name": "Dr A", "active_flag": "true", "city": "Pune", "state": "MH", "field_rep_id": "FR1"},
                {"doctor_key": "DOC-2", "doctor_display_name": "Dr B", "active_flag": "false", "city": "Delhi", "state": "DL", "field_rep_id": "FR2"},
            ],
            [
                {"doctor_key": "DOC-1", "course_audience": "doctor", "enrolled_at": "2026-03-01"},
                {"doctor_key": "DOC-2", "course_audience": "doctor", "enrolled_at": "2026-03-02"},
                {"doctor_key": "DOC-3", "course_audience": "paramedic", "enrolled_at": "2026-03-03"},
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doctor_key"], "DOC-1")
        self.assertEqual(rows[0]["certification_status"], "enrolled")

    def test_dashboard_pdf_export_returns_pdf_attachment(self):
        request = RequestFactory().post(
            "/sapa-growth/export/dashboard.pdf",
            {
                "snapshot": SimpleUploadedFile(
                    "dashboard.png",
                    base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z4ZcAAAAASUVORK5CYII="),
                    content_type="image/png",
                )
            },
        )

        class DummySession:
            session_key = "test-session"

        request.session = DummySession()
        with patch("sapa_growth.services.log_export") as log_export_mock, patch("sapa_growth.services._latest_refresh", return_value={"as_of_date": "2026-03-24"}):
            response = export_dashboard_pdf({"state": None, "field_rep_id": None, "doctor_key": None}, request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/pdf")
        self.assertIn("attachment; filename=", response["Content-Disposition"])
        self.assertTrue(response.content.startswith(b"%PDF"))
        log_export_mock.assert_called_once()

    def test_dashboard_context_without_refresh_sets_safe_export_filename(self):
        with patch("sapa_growth.services._latest_refresh", return_value=None), patch(
            "sapa_growth.services.filter_options",
            return_value={"states": [], "field_reps": [], "doctors": [], "cities": []},
        ):
            context = dashboard_context({"state": None, "field_rep_id": None, "doctor_key": None})
        self.assertFalse(context["ready"])
        self.assertEqual(context["export_filename"], "sapa-growth-dashboard-report.pdf")


class SapaGrowthRoutingTests(SimpleTestCase):
    def test_dashboard_route_registered(self):
        self.assertEqual(reverse("sapa_growth:dashboard"), "/sapa-growth/")
        self.assertEqual(resolve("/sapa-growth/").view_name, "sapa_growth:dashboard")
