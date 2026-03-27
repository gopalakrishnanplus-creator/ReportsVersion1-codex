from __future__ import annotations

from unittest.mock import patch

from django.test import SimpleTestCase
from django.urls import resolve


class DashboardRoutingTests(SimpleTestCase):
    def test_access_routes_registered(self):
        self.assertEqual(resolve("/").view_name, "reports-home")
        self.assertEqual(resolve("/inclinic/").view_name, "menu")
        self.assertEqual(resolve("/campaign/demo/login/").view_name, "campaign-login")
        self.assertEqual(resolve("/campaign/demo/access/").view_name, "campaign-access")
        self.assertEqual(resolve("/campaign/demo/send-access-email/").view_name, "campaign-send-access-email")


class DashboardAccessViewTests(SimpleTestCase):
    def test_reports_home_renders(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reports Home")

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
