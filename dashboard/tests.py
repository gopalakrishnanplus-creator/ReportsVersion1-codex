from __future__ import annotations

from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve

import dashboard.views
from dashboard.internal_data_admin import (
    ColumnInfo,
    TableInfo,
    _cleanup_candidate_columns,
    _is_relevant_schema,
    _layer_key_for_schema,
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


class DashboardAccessViewTests(SimpleTestCase):
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
