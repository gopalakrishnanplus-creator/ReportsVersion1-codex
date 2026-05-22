from __future__ import annotations

from unittest.mock import MagicMock, patch

from django.conf import settings
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve

import dashboard.views
from dashboard.internal_data_admin import (
    ColumnInfo,
    TableInfo,
    _batch_cleanup_confirmation_phrase,
    _cleanup_candidate_columns,
    _cleanup_inverse_match_condition,
    _is_relevant_schema,
    _layer_key_for_schema,
    _parse_campaign_ids,
    _selected_cleanup_systems,
    _system_key_for_schema,
)


class DashboardRoutingTests(SimpleTestCase):
    def test_access_routes_registered(self):
        self.assertEqual(resolve("/").view_name, "reports-home")
        self.assertEqual(resolve("/inclinic/").view_name, "menu")
        self.assertEqual(resolve("/campaign/demo/login/").view_name, "campaign-login")
        self.assertEqual(resolve("/campaign/demo/access/").view_name, "campaign-access")
        self.assertEqual(resolve("/campaign/demo/send-access-email/").view_name, "campaign-send-access-email")
        self.assertEqual(resolve("/campaign-performance/links/").view_name, "campaign-performance-links")
        self.assertEqual(resolve("/campaign-performance/demo/").view_name, "campaign-performance-page")
        self.assertEqual(resolve("/campaign/demo/performance/").view_name, "campaign-performance-page-legacy")
        self.assertEqual(resolve("/_internal/data-admin/").view_name, "internal-data-admin-home")
        self.assertEqual(resolve("/_internal/data-admin/login/").view_name, "internal-data-admin-login")
        self.assertEqual(resolve("/_internal/data-admin/cleanup/").view_name, "internal-data-admin-cleanup")
        self.assertEqual(resolve("/_internal/data-admin/bronze/campaign_campaign/").view_name, "internal-data-admin-table")
        self.assertEqual(resolve("/_internal/data-admin/bronze/campaign_campaign/bulk-delete/").view_name, "internal-data-admin-bulk-delete")

    def test_internal_data_admin_schema_filter(self):
        self.assertTrue(_is_relevant_schema("bronze"))
        self.assertTrue(_is_relevant_schema("bronze_pe"))
        self.assertTrue(_is_relevant_schema("gold_campaign_cardio_2026_q1"))
        self.assertTrue(_is_relevant_schema("silver_sapa"))
        self.assertFalse(_is_relevant_schema("public"))
        self.assertFalse(_is_relevant_schema("information_schema"))

    def test_internal_data_admin_system_mapping(self):
        self.assertEqual(_system_key_for_schema("raw_server1"), "inclinic")
        self.assertEqual(_system_key_for_schema("gold_campaign_demo"), "inclinic")
        self.assertEqual(_system_key_for_schema("gold_sapa"), "sapa")
        self.assertEqual(_system_key_for_schema("raw_pe_portal"), "pe")
        self.assertEqual(_system_key_for_schema("control"), "shared")

    def test_internal_data_admin_layer_mapping(self):
        self.assertEqual(_layer_key_for_schema("raw_server2"), "raw")
        self.assertEqual(_layer_key_for_schema("bronze_pe"), "bronze")
        self.assertEqual(_layer_key_for_schema("silver_sapa"), "silver")
        self.assertEqual(_layer_key_for_schema("gold_pe_campaign_demo"), "gold")
        self.assertEqual(_layer_key_for_schema("ops"), "other")

    def test_hierarchy_cleanup_uses_campaign_identity_columns_only(self):
        columns = [
            ColumnInfo("id", "integer", False, 1, None, False, False),
            ColumnInfo("campaign_name", "text", True, 2, None, False, False),
            ColumnInfo("brand_campaign_id", "text", True, 3, None, False, False),
            ColumnInfo("doctor_name", "text", True, 4, None, False, False),
            ColumnInfo("campaign_key", "text", True, 5, None, False, False),
        ]
        info = TableInfo("silver", "campaign_summary", columns, ["id"])

        self.assertEqual(
            _cleanup_candidate_columns(info),
            ["brand_campaign_id", "campaign_key", "id"],
        )

    def test_batch_campaign_id_parser_dedupes_common_separators(self):
        self.assertEqual(
            _parse_campaign_ids("CARDIO-1\nDERMA-2, CARDIO-1;PE-3\t"),
            ["CARDIO-1", "DERMA-2", "PE-3"],
        )

    def test_batch_cleanup_system_selection_supports_multiple_systems(self):
        self.assertEqual(_selected_cleanup_systems({"systems": ["pe", "inclinic"]}), ["inclinic", "pe"])
        self.assertEqual(_selected_cleanup_systems({}), ["inclinic", "sapa", "pe"])

    def test_batch_keep_mode_protects_matching_campaign_gold_schema(self):
        scope = {"scoped_gold_schemas": ["gold_pe_campaign_keep"], "match_values": ["keep"]}
        protected = TableInfo("gold_pe_campaign_keep", "dashboard_summary", [], [])
        stale = TableInfo("gold_pe_campaign_stale", "dashboard_summary", [], [])

        self.assertIsNone(_cleanup_inverse_match_condition(protected, scope))
        self.assertEqual(
            _cleanup_inverse_match_condition(stale, scope)["scope_note"],
            "entire campaign GOLD schema not in keep list",
        )

    def test_batch_cleanup_confirmation_phrases_are_explicit(self):
        self.assertEqual(_batch_cleanup_confirmation_phrase("delete_listed", 2), "DELETE 2 LISTED CAMPAIGNS")
        self.assertEqual(_batch_cleanup_confirmation_phrase("keep_listed", 3), "KEEP 3 CAMPAIGNS DELETE REST")


class DashboardAccessViewTests(SimpleTestCase):
    def test_engagement_health_uses_actual_campaign_denominator(self):
        score = dashboard.views._engagement_health_score(reached=5, opened=5, consumed=5, total_doctors=1000)
        self.assertAlmostEqual(score, 0.5, places=1)

    def test_field_rep_insights_select_brand_supplied_rep_id_for_display(self):
        with patch("dashboard.views._fetch_dicts", return_value=[]) as fetch_mock:
            dashboard.views._field_rep_insight_rows("brand-1", ["brand-1"], ["9"])

        sql = fetch_mock.call_args.args[0]
        self.assertIn("brand_supplied_field_rep_id", sql)
        self.assertIn("field_rep_display_id", sql)
        self.assertIn("AS field_rep_id", sql)
        self.assertIn("d.source", sql)
        self.assertNotIn("CASE WHEN COALESCE(ad.total_doctors_assigned, 0) > 0", sql)
        self.assertIn("activity_for_rep AS", sql)
        self.assertNotIn("GREATEST(", sql)
        self.assertIn("assignment_note", sql)

    def test_reports_home_renders(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reports Home")
        self.assertNotContains(response, "_internal/data-admin")

    def test_login_page_does_not_render_credential_hint(self):
        with patch(
            "dashboard.views._campaign_list",
            return_value=[{"brand_campaign_id": "demo", "campaign_name": "Demo Campaign"}],
        ):
            response = self.client.get("/campaign/demo/login/")
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Credential hint")

    def test_send_access_email_route_redirects_and_calls_mailer(self):
        with patch(
            "dashboard.views._campaign_list",
            return_value=[{"brand_campaign_id": "demo", "campaign_name": "Demo Campaign"}],
        ), patch("dashboard.views.send_access_email") as send_email_mock:
            response = self.client.post(
                "/campaign/demo/send-access-email/",
                {"recipient_email": "team@example.com"},
            )
        self.assertEqual(response.status_code, 302)
        send_email_mock.assert_called_once()

    def test_access_page_renders_history(self):
        with patch(
            "dashboard.views._campaign_list",
            return_value=[{"brand_campaign_id": "demo", "campaign_name": "Demo Campaign"}],
        ), patch(
            "dashboard.views.access_email_history",
            return_value=[{"recipient_email": "team@example.com", "sent_at": "2026-03-27T09:00:00Z"}],
        ):
            response = self.client.get("/campaign/demo/access/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Previous Recipients")

    def test_menu_page_keeps_existing_actions_only(self):
        with patch(
            "dashboard.views._campaign_list",
            return_value=[{"brand_campaign_id": "demo", "campaign_name": "Demo Campaign"}],
        ):
            response = self.client.get("/inclinic/")
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "/campaign/demo/performance/")
        self.assertNotContains(response, "_internal/data-admin")

    def test_internal_data_admin_requires_login(self):
        response = self.client.get("/_internal/data-admin/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/_internal/data-admin/login/", response["Location"])

    def test_campaign_performance_page_renders_bootstrap_data(self):
        with patch(
            "dashboard.views._campaign_list",
            return_value=[{"brand_campaign_id": "demo", "campaign_name": "Demo Campaign"}],
        ):
            response = self.client.get("/campaign-performance/demo/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Campaign Performance")
        self.assertContains(response, "/reporting/api/campaign-performance-page/demo/")

    def test_campaign_performance_link_library_renders_copy_targets(self):
        with patch(
            "dashboard.views._campaign_performance_link_rows",
            return_value=[
                {
                    "campaign_id": "camp-1",
                    "campaign_name": "Campaign One",
                    "selected_systems": ["RFA", "InClinic"],
                    "performance_page_url": "https://reports.test/campaign-performance/camp-1/",
                    "performance_api_url": "https://reports.test/reporting/api/campaign-performance/camp-1/",
                    "system_report_links": [
                        {"label": "InClinic Report", "url": "https://reports.test/campaign/brand-1/", "status": ""},
                        {"label": "RFA Report", "url": "", "status": "Not available yet"},
                    ],
                    "legacy_brand_route_url": "https://reports.test/campaign/brand-1/performance/",
                    "brand_manager_login_link": "",
                }
            ],
        ):
            response = self.client.get("/campaign-performance/links/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Campaign Performance Embed Links")
        self.assertContains(response, "Copy Page URL")
        self.assertContains(response, "https://reports.test/reporting/api/campaign-performance/camp-1/")
        self.assertContains(response, "Copy InClinic Report")
        self.assertContains(response, "RFA Report: Not available yet")

    def test_campaign_performance_link_rows_ignore_navigation_metadata_for_selected_systems(self):
        request = RequestFactory().get("/campaign-performance/links/")
        cursor = MagicMock()
        cursor.fetchone.side_effect = [("bronze.campaign_campaign",), ("silver.map_brand_campaign_to_campaign",)]
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor
        cursor_context.__exit__.return_value = False

        fake_connection = MagicMock()
        fake_connection.cursor.return_value = cursor_context

        with patch.object(dashboard.views, "connection", fake_connection), patch(
            "dashboard.views._fetch_dicts",
            return_value=[
                {
                    "campaign_id": "camp-9",
                    "campaign_name": "Apex Demo",
                    "system_rfa": False,
                    "system_ic": True,
                    "system_pe": False,
                    "system_entry_navigation": True,
                    "brand_manager_login_link": "",
                    "brand_campaign_id": "",
                }
            ],
        ), patch(
            "dashboard.views._resolve_campaign_reference",
            return_value=type(
                "Ref",
                (),
                {"brand_campaign_id": "brand-9", "pe_campaign_id": None},
            )(),
        ):
            rows = dashboard.views._campaign_performance_link_rows(request)

        self.assertEqual(rows[0]["selected_systems"], ["InClinic"])
        self.assertEqual(rows[0]["system_report_links"][0]["url"], "http://testserver/campaign/brand-9/")

    def test_campaign_overview_renders_field_rep_insights_instead_of_action_tile(self):
        session = self.client.session
        session["auth_demo"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

        context = {
            "selected_campaign": "demo",
            "brand_name": "Demo Brand",
            "brand_logo_text": "Demo",
            "company_logo_url": None,
            "selected_schema": "gold_campaign_demo",
            "weekly_rows": [],
            "error_message": None,
            "schedule_text": "May 01, 2026 - May 31, 2026",
            "collateral_name": "Current Collateral",
            "state_attention": [],
            "field_rep_summary": {
                "total_reps": 1,
                "total_doctors_assigned": 120,
                "doctors_sent": 30,
                "doctors_viewed": 20,
                "doctors_video_played": 12,
                "doctors_pdf_downloaded": 8,
            },
            "field_rep_insights": [
                {
                    "field_rep_id": "FR-101",
                    "field_rep_name": "Asha Mehta",
                    "state_normalized": "Maharashtra",
                    "total_doctors_assigned": 120,
                    "doctors_sent": 30,
                    "doctors_viewed": 20,
                    "doctors_video_played": 12,
                    "doctors_pdf_downloaded": 8,
                }
            ],
            "collateral_cards": {
                "current": {"title": "Current Collateral", "reached": 30, "opened": 20, "video": 12, "pdf": 8, "reached_pct": 25, "opened_pct": 66.7, "video_pct": 60, "pdf_pct": 40},
                "best": {"title": "Week 1 Best", "reached": 30, "opened": 20, "video": 12, "pdf": 8, "reached_pct": 25, "opened_pct": 66.7, "video_pct": 60, "pdf_pct": 40},
                "benchmark": {"reached": 40, "opened": 30, "video": 20, "pdf": 10, "reached_pct": 30, "opened_pct": 75, "video_pct": 66.7, "pdf_pct": 33.3},
            },
            "trend_labels": [],
            "reached_pct_series": [],
            "opened_pct_series": [],
            "pdf_pct_series": [],
            "video_pct_series": [],
            "week_options": [],
            "selected_week": None,
            "campaign_health": 45,
            "campaign_wow": 0,
            "campaign_benchmark_label": "Below Average",
            "campaign_color": "yellow",
            "weekly_health": 45,
            "weekly_wow": 0,
            "weekly_benchmark_label": "Average",
            "weekly_color": "yellow",
            "kpi_reached": 30,
            "kpi_opened": 20,
            "kpi_video": 12,
            "kpi_pdf": 8,
            "kpi_reached_pct": 25,
            "kpi_opened_pct": 66.7,
            "kpi_video_pct": 60,
            "kpi_pdf_pct": 40,
            "week_of": "Week 1",
        }

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=context):
            response = self.client.get("/campaign/demo/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '<div class="brand-logo">Demo</div>', html=True)
        self.assertContains(response, "Field Representative Insights")
        self.assertContains(response, 'role="dialog"')
        self.assertContains(response, "Field Rep ID")
        self.assertContains(response, "Download Excel")
        self.assertContains(response, "Asha Mehta")
        self.assertContains(response, "FR-101")
        self.assertNotContains(response, "Action Required This Week")
        self.assertNotContains(response, "Weekly KPI Table")
        self.assertNotContains(response, "Back to Menu")
