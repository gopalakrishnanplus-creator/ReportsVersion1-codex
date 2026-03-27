from __future__ import annotations

from unittest.mock import patch

from django.http import HttpResponse
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve, reverse

from etl.pe_reports.gold import build_benchmark_row, compute_health_components
from etl.pe_reports.silver import attribute_share_row, match_campaign_doctors, rollup_share_funnel
from etl.pe_reports.utils import week_end_saturday


class PeReportsLogicTests(SimpleTestCase):
    def test_campaign_doctor_mapping_prefers_logical_id_then_email_then_phone(self):
        mapped = match_campaign_doctors(
            [
                {"id": "10", "doctor_id": "DOC-1", "email": "wrong@example.com", "phone": "99999"},
                {"id": "11", "doctor_id": "", "email": "doctor.two@example.com", "phone": "88888"},
                {"id": "12", "doctor_id": "", "email": "", "phone": "+91 77777 66666"},
            ],
            [
                {"doctor_id": "DOC-1", "email": "doctor.one@example.com", "whatsapp_no": "9999999999"},
                {"doctor_id": "DOC-2", "email": "doctor.two@example.com", "whatsapp_no": "8888888888"},
                {"doctor_id": "DOC-3", "email": "doctor.three@example.com", "whatsapp_no": "7777766666"},
            ],
        )
        self.assertEqual(mapped[0]["doctor_key"], "DOC-1")
        self.assertEqual(mapped[0]["match_method"], "logical_doctor_id")
        self.assertEqual(mapped[1]["doctor_key"], "DOC-2")
        self.assertEqual(mapped[1]["match_method"], "email")
        self.assertEqual(mapped[2]["doctor_key"], "DOC-3")
        self.assertEqual(mapped[2]["match_method"], "phone")

    def test_ambiguous_standalone_video_share_stays_unattributed(self):
        result = attribute_share_row(
            {
                "shared_item_type": "video",
                "shared_item_code": "VID-1",
                "doctor_key": "DOC-1",
                "shared_at_ts": "2026-03-20 10:00:00",
            },
            campaigns_by_doctor={"DOC-1": ["camp-a", "camp-b"]},
            campaign_by_id={
                "camp-a": {"campaign_id_original": "camp-a", "campaign_id_normalized": "camp-a", "start_date": "2026-03-01", "end_date": "2026-04-01"},
                "camp-b": {"campaign_id_original": "camp-b", "campaign_id_normalized": "camp-b", "start_date": "2026-03-01", "end_date": "2026-04-01"},
            },
            campaign_by_cluster_code={},
            campaign_videos_by_campaign={"camp-a": {"VID-1"}, "camp-b": {"VID-1"}},
        )
        self.assertEqual(result["campaign_attribution_method"], "ambiguous_video")
        self.assertEqual(result["is_campaign_attributed_flag"], "false")

    def test_rollup_keeps_orphan_playback_outside_share_funnel_and_counts_any_video_threshold_once(self):
        rolled = rollup_share_funnel(
            [
                {
                    "share_public_id": "SHARE-1",
                    "campaign_id_original": "camp-a",
                    "campaign_id_normalized": "camp-a",
                    "doctor_key": "DOC-1",
                    "doctor_id": "DOC-1",
                    "shared_item_type": "cluster",
                    "shared_item_code": "BUNDLE-1",
                    "shared_item_name": "Bundle",
                    "language_code": "en",
                    "recipient_reference": "R1",
                    "shared_at_ts": "2026-03-20 09:00:00",
                    "video_cluster_code": "BUNDLE-1",
                    "therapy_area_name": "Growth",
                    "trigger_name": "Nutrition",
                    "state": "MH",
                }
            ],
            [
                {"share_public_id": "SHARE-1", "event_type": "play", "occurred_at_ts": "2026-03-20 09:05:00"},
                {"share_public_id": "SHARE-1", "event_type": "progress", "milestone_percent_num": "50", "occurred_at_ts": "2026-03-20 09:06:00", "video_code": "VID-1"},
                {"share_public_id": "ORPHAN", "event_type": "progress", "milestone_percent_num": "100", "occurred_at_ts": "2026-03-20 09:07:00", "video_code": "VID-2"},
            ],
        )
        self.assertEqual(len(rolled), 1)
        self.assertEqual(rolled[0]["is_played"], "true")
        self.assertEqual(rolled[0]["is_viewed_50"], "true")
        self.assertEqual(rolled[0]["is_viewed_100"], "false")

    def test_week_bucket_ends_on_saturday(self):
        self.assertEqual(str(week_end_saturday("2026-03-25")), "2026-03-28")

    def test_zero_enrollment_health_marks_insufficient(self):
        health = compute_health_components(
            enrolled_doctors_current=0,
            doctors_sharing_unique=0,
            shares_total=0,
            shares_played=0,
            shares_viewed_50=0,
            shares_viewed_100=0,
        )
        self.assertEqual(health["campaign_health_score"], 0.0)
        self.assertTrue(health["insufficient_data_flag"])

    def test_benchmark_generation_uses_last_eligible_rows(self):
        row = build_benchmark_row(
            [
                {"campaign_health_score": "70", "activation_pct": "60", "play_rate_pct": "80", "engagement_50_pct": "75", "completion_pct": "65", "insufficient_data_flag": "false"},
                {"campaign_health_score": "50", "activation_pct": "45", "play_rate_pct": "55", "engagement_50_pct": "52", "completion_pct": "48", "insufficient_data_flag": "false"},
                {"campaign_health_score": "0", "activation_pct": "0", "play_rate_pct": "0", "engagement_50_pct": "0", "completion_pct": "0", "insufficient_data_flag": "true"},
            ],
            "2026-03-25",
            "2026-03-25T10:00:00Z",
        )
        self.assertEqual(row["campaign_count"], 2)
        self.assertEqual(row["avg_campaign_health_score"], 60.0)


class PeReportsRoutingTests(SimpleTestCase):
    def test_routes_registered(self):
        self.assertEqual(reverse("pe_reports:menu"), "/pe-reports/")
        self.assertEqual(resolve("/pe-reports/").view_name, "pe_reports:menu")
        self.assertEqual(resolve("/pe-reports/campaign/abc/login/").view_name, "pe_reports:login")
        self.assertEqual(resolve("/pe-reports/campaign/abc/access/").view_name, "pe_reports:access")
        self.assertEqual(resolve("/pe-reports/campaign/abc/send-access-email/").view_name, "pe_reports:send-access-email")
        self.assertEqual(resolve("/pe-reports/campaign/abc/").view_name, "pe_reports:dashboard")
        self.assertEqual(resolve("/pe-reports/campaign/abc/details/total_shares/").view_name, "pe_reports:detail")
        self.assertEqual(resolve("/pe-reports/campaign/abc/details/total_shares/export/").view_name, "pe_reports:detail-export")
        self.assertEqual(resolve("/pe-reports/campaign/abc/export/dashboard.pdf").view_name, "pe_reports:dashboard-export")

    def test_existing_inclinic_and_sapa_routes_still_resolve(self):
        self.assertEqual(resolve("/campaign/demo/login/").view_name, "campaign-login")
        self.assertEqual(resolve("/sapa-growth/").view_name, "sapa_growth:dashboard")


class PeReportsViewTests(SimpleTestCase):
    def test_menu_page_renders(self):
        with patch("pe_reports.views.menu_context", return_value={"ready": True, "campaigns": [], "refresh": {}}):
            response = self.client.get("/pe-reports/")
        self.assertEqual(response.status_code, 200)

    def test_dashboard_page_renders(self):
        fake_context = {
            "ready": False,
            "campaign_id": "camp-1",
            "filters": {},
            "filter_options": {"weeks": [], "states": [], "field_reps": [], "doctors": [], "languages": [], "share_types": [], "therapy_areas": [], "triggers": [], "bundles": []},
            "registry": {"campaign_name": "Campaign"},
            "refresh": {},
            "filters_query": "",
            "export_filename": "pe.pdf",
        }
        with patch("pe_reports.views._campaign_registry_row", return_value={"campaign_id_original": "camp-1"}), patch(
            "pe_reports.views.is_authenticated",
            return_value=True,
        ), patch("pe_reports.views.dashboard_context", return_value=fake_context):
            response = self.client.get("/pe-reports/campaign/camp-1/")
        self.assertEqual(response.status_code, 200)

    def test_detail_page_renders(self):
        fake_context = {
            "metric": "total_shares",
            "title": "Total Shares",
            "campaign_id": "camp-1",
            "campaign_name": "Campaign",
            "columns": ["share_public_id"],
            "rows": [{"share_public_id": "SHARE-1"}],
            "row_count": 1,
            "page": 1,
            "page_count": 1,
            "filters": {},
            "filters_query": "",
            "last_updated": "2026-03-25",
            "as_of_date": "2026-03-25",
            "back_href": "/pe-reports/campaign/camp-1/",
        }
        with patch("pe_reports.views._campaign_registry_row", return_value={"campaign_id_original": "camp-1"}), patch(
            "pe_reports.views.is_authenticated",
            return_value=True,
        ), patch("pe_reports.views.detail_context", return_value=fake_context):
            response = self.client.get("/pe-reports/campaign/camp-1/details/total_shares/")
        self.assertEqual(response.status_code, 200)

    def test_detail_export_route_returns_response(self):
        with patch("pe_reports.views._campaign_registry_row", return_value={"campaign_id_original": "camp-1"}), patch(
            "pe_reports.views.is_authenticated",
            return_value=True,
        ), patch("pe_reports.views.export_detail_csv", return_value=HttpResponse("ok", content_type="text/csv")):
            response = self.client.get("/pe-reports/campaign/camp-1/details/total_shares/export/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")

    def test_dashboard_pdf_export_route_returns_response(self):
        factory = RequestFactory()
        request = factory.post("/pe-reports/campaign/camp-1/export/dashboard.pdf")
        with patch("pe_reports.views._campaign_registry_row", return_value={"campaign_id_original": "camp-1"}), patch(
            "pe_reports.views.is_authenticated",
            return_value=True,
        ), patch("pe_reports.views.export_dashboard_pdf", return_value=HttpResponse(b"%PDF-test", content_type="application/pdf")):
            response = resolve("/pe-reports/campaign/camp-1/export/dashboard.pdf").func(request, campaign_id="camp-1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/pdf")

    def test_dashboard_redirects_to_login_when_unauthenticated(self):
        with patch("pe_reports.views._campaign_registry_row", return_value={"campaign_id_original": "camp-1"}), patch(
            "pe_reports.views.is_authenticated",
            return_value=False,
        ):
            response = self.client.get("/pe-reports/campaign/camp-1/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/pe-reports/campaign/camp-1/login/")

    def test_login_page_renders(self):
        with patch(
            "pe_reports.views._campaign_registry_row",
            return_value={"campaign_id_original": "camp-1", "campaign_name": "PE Campaign", "brand_name": "Brand A"},
        ):
            response = self.client.get("/pe-reports/campaign/camp-1/login/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Patient Education Login")

    def test_send_access_email_route_redirects_and_calls_mailer(self):
        with patch(
            "pe_reports.views._campaign_registry_row",
            return_value={"campaign_id_original": "camp-1", "campaign_name": "PE Campaign", "brand_name": "Brand A"},
        ), patch("pe_reports.views.send_access_email") as send_email_mock:
            response = self.client.post(
                "/pe-reports/campaign/camp-1/send-access-email/",
                {"recipient_email": "team@example.com"},
            )
        self.assertEqual(response.status_code, 302)
        send_email_mock.assert_called_once()

    def test_access_page_renders_history(self):
        with patch(
            "pe_reports.views._campaign_registry_row",
            return_value={"campaign_id_original": "camp-1", "campaign_name": "PE Campaign", "brand_name": "Brand A"},
        ), patch(
            "pe_reports.views.access_email_history",
            return_value=[{"recipient_email": "team@example.com", "sent_at": "2026-03-27T09:00:00Z"}],
        ):
            response = self.client.get("/pe-reports/campaign/camp-1/access/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Previous Recipients")
