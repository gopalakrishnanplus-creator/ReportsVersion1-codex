from __future__ import annotations

import json
from unittest.mock import patch

from django.test import SimpleTestCase
from django.urls import resolve

from reporting.campaign_performance import CampaignPerformanceNotFound


class UnifiedReportingApiRoutingTests(SimpleTestCase):
    def test_routes_registered(self):
        self.assertEqual(resolve("/reporting/api/red_flag_alert/").view_name, "reporting-api-red-flag-alert")
        self.assertEqual(resolve("/reporting/api/in_clinic/").view_name, "reporting-api-in-clinic")
        self.assertEqual(resolve("/reporting/api/patient_education/").view_name, "reporting-api-patient-education")
        self.assertEqual(resolve("/reporting/api/campaign-performance/").view_name, "reporting-api-campaign-performance")
        self.assertEqual(resolve("/reporting/api/campaign-performance/demo/").view_name, "reporting-api-campaign-performance-specific")
        self.assertEqual(resolve("/reporting/api/campaign-performance-page/demo/").view_name, "reporting-api-campaign-performance-page")


class UnifiedReportingApiViewTests(SimpleTestCase):
    @patch(
        "reporting.api_views.build_red_flag_alert_rows",
        return_value=[
            {
                "campaign": "growth-clinic",
                "clinic_group": "Pune",
                "clinic": "Sunrise Clinic",
                "period_start": "2026-03-21",
                "period_end": "2026-03-25",
                "form_fills": 4,
                "red_flags_total": 3,
                "patient_video_views": 2,
                "reports_emailed_to_doctors": 4,
                "form_shares": 2,
                "patient_scans": 2,
                "follow_ups_scheduled": 1,
                "reminders_sent": 1,
            }
        ],
    )
    def test_red_flag_alert_returns_json_envelope(self, _mock_builder):
        response = self.client.get("/reporting/api/red_flag_alert/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["subsystem"], "red_flag_alert")
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["id"], 1)
        self.assertEqual(payload["results"][0]["clinic"], "Sunrise Clinic")

    @patch(
        "reporting.api_views.build_patient_education_rows",
        return_value=[
            {
                "campaign": "pe-alpha-2026",
                "clinic_group": "Pune",
                "clinic": "Sunrise Clinic",
                "period_start": "2026-03-02",
                "period_end": "2026-03-24",
                "video_views": 3,
                "video_completions": 1,
                "cluster_shares": 3,
                "patient_scans": 3,
                "banner_clicks": 2,
            }
        ],
    )
    def test_patient_education_returns_contract_shape(self, _mock_builder):
        response = self.client.get("/reporting/api/patient_education/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["subsystem"], "patient_education")
        self.assertEqual(payload["results"][0]["video_views"], 3)
        self.assertEqual(payload["results"][0]["banner_clicks"], 2)

    @patch("reporting.api_views.build_in_clinic_rows", side_effect=RuntimeError("boom"))
    def test_in_clinic_returns_safe_error_envelope(self, _mock_builder):
        response = self.client.get("/reporting/api/in_clinic/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(payload["subsystem"], "in_clinic")
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["results"], [])
        self.assertIn("detail", payload)

    @patch(
        "reporting.api_views.build_campaign_performance_payload",
        return_value={
            "campaign": {
                "campaign_id": "demo",
                "campaign_name": "Demo Campaign",
                "brand_name": "Brand A",
                "identifiers": {"brand_campaign_id": "demo", "resolved_campaign_id": "camp-42", "pe_campaign_id": "camp-42"},
            },
            "system_count": 2,
            "available_systems": [
                {"key": "in_clinic", "label": "InClinic (In-Clinic Sharing)"},
                {"key": "patient_education", "label": "PE (Patient Education)"},
            ],
            "sections": [
                {"key": "in_clinic", "type": "system", "label": "InClinic (In-Clinic Sharing)", "metrics": [], "meta": [], "trend": None},
                {"key": "adoption_by_clinics", "type": "adoption", "label": "Adoption by Clinics", "metrics": [], "meta": [], "trend": None, "breakdown": []},
            ],
            "generated_at": "2026-04-20T10:00:00+00:00",
        },
    )
    def test_campaign_performance_returns_dynamic_sections(self, _mock_builder):
        response = self.client.get("/reporting/api/campaign-performance/demo/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["campaign"]["campaign_name"], "Demo Campaign")
        self.assertEqual(payload["system_count"], 2)
        self.assertEqual(payload["available_systems"][0]["key"], "in_clinic")
        self.assertEqual(payload["available_systems"][0]["system_report_url"], "http://testserver/campaign/demo/")
        self.assertEqual(payload["available_systems"][1]["system_report_url"], "http://testserver/pe-reports/campaign/camp-42/")
        self.assertEqual(payload["sections"][0]["system_report_url"], "http://testserver/campaign/demo/")
        self.assertEqual(payload["campaign"]["system_report_links"]["in_clinic"], "http://testserver/campaign/demo/")
        self.assertEqual(payload["campaign"]["system_report_links"]["patient_education"], "http://testserver/pe-reports/campaign/camp-42/")
        self.assertEqual(payload["sections"][-1]["key"], "adoption_by_clinics")
        self.assertEqual(payload["requested_campaign_id"], "demo")

    @patch(
        "reporting.api_views.build_campaign_performance_page_payload",
        return_value={
            "campaign": {
                "campaign_id": "demo",
                "campaign_name": "Demo Campaign",
                "brand_name": "Brand A",
                "identifiers": {"brand_campaign_id": "demo", "resolved_campaign_id": "camp-42", "pe_campaign_id": "camp-42"},
            },
            "system_count": 1,
            "available_systems": [
                {"key": "in_clinic", "label": "InClinic (In-Clinic Sharing)"},
            ],
            "sections": [
                {"key": "in_clinic", "label": "InClinic (In-Clinic Sharing)", "metrics": [], "meta": [], "trend": None, "bar_chart": None, "table": None},
                {"key": "adoption_by_clinics", "label": "Adoption by Clinics", "metrics": [], "meta": [], "trend": None, "bar_chart": None, "table": None, "breakdown": []},
            ],
            "generated_at": "2026-04-21T10:00:00+00:00",
        },
    )
    def test_campaign_performance_page_api_uses_page_payload_builder(self, _mock_builder):
        response = self.client.get("/reporting/api/campaign-performance-page/demo/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["campaign"]["campaign_name"], "Demo Campaign")
        self.assertEqual(payload["available_systems"][0]["key"], "in_clinic")
        self.assertEqual(payload["requested_campaign_id"], "demo")

    def test_campaign_performance_requires_campaign_id(self):
        response = self.client.get("/reporting/api/campaign-performance/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["system_count"], 0)
        self.assertIn("detail", payload)

    @patch("reporting.api_views.build_campaign_performance_payload", side_effect=CampaignPerformanceNotFound("missing"))
    def test_campaign_performance_returns_not_found_envelope(self, _mock_builder):
        response = self.client.get("/reporting/api/campaign-performance/missing/")
        payload = json.loads(response.content)

        self.assertEqual(response.status_code, 404)
        self.assertEqual(payload["requested_campaign_id"], "missing")
        self.assertEqual(payload["sections"], [])
