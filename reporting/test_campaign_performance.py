from __future__ import annotations

from unittest.mock import patch

from django.db import DatabaseError
from django.test import SimpleTestCase

from reporting.campaign_performance import (
    CampaignConfig,
    CampaignReference,
    RfaAttributionContext,
    _build_in_clinic_summary_section,
    _build_rfa_summary_section,
    _configured_system_keys,
    _query_schema_rows,
    build_campaign_performance_page_payload,
    build_campaign_performance_payload,
)


def _reference(config: CampaignConfig) -> CampaignReference:
    return CampaignReference(
        requested_id="camp-1",
        lookup_key="camp1",
        brand_campaign_id="brand-1",
        brand_campaign_name="Campaign One",
        brand_name="Brand One",
        in_clinic_schema="gold_campaign_one",
        resolved_campaign_id="camp-1",
        pe_campaign_id="camp-1",
        pe_campaign_normalized="camp1",
        pe_campaign_name="Campaign One",
        pe_schema="gold_pe_campaign_one",
        pe_dim_campaign=None,
        campaign_config=config,
    )


class CampaignPerformanceSelectionTests(SimpleTestCase):
    def test_configured_system_keys_do_not_treat_navigation_as_standalone_system(self):
        reference = _reference(
            CampaignConfig(
                campaign_id="camp-1",
                campaign_name="Campaign One",
                system_rfa=False,
                system_ic=True,
                system_pe=False,
                has_entry_navigation=True,
                banner_target_url="https://example.com/banner",
            )
        )

        self.assertEqual(_configured_system_keys(reference), ["in_clinic"])

    @patch(
        "reporting.campaign_performance._build_in_clinic_summary_section",
        return_value=(
            {
                "key": "in_clinic",
                "label": "InClinic (In-Clinic Sharing)",
                "metrics": [],
                "meta": [],
            },
            {"system_key": "in_clinic", "label": "In Clinic", "clinics_added": 4, "active_records": 4, "clinics_with_activity": 2},
        ),
    )
    @patch("reporting.campaign_performance._resolve_campaign_reference")
    def test_payload_system_count_uses_core_selected_systems_only(self, resolve_reference_mock, _build_in_clinic_section_mock):
        resolve_reference_mock.return_value = _reference(
            CampaignConfig(
                campaign_id="camp-1",
                campaign_name="Campaign One",
                system_rfa=False,
                system_ic=True,
                system_pe=False,
                has_entry_navigation=True,
                banner_target_url="https://example.com/banner",
            )
        )

        payload = build_campaign_performance_payload("camp-1")

        self.assertEqual(payload["system_count"], 1)
        self.assertEqual([item["key"] for item in payload["available_systems"]], ["in_clinic"])
        self.assertEqual(payload["sections"][-1]["key"], "adoption_by_clinics")

    @patch(
        "reporting.campaign_performance._in_clinic_summary_counts",
        return_value={
            "shares": 5,
            "link_opens": 4,
            "pdf_reads": 3,
            "video_views": 2,
            "video_completions": 1,
            "pdf_downloads": 4,
            "clinics_added": 5,
            "active_records": 4,
            "clinics_with_activity": 3,
        },
    )
    def test_in_clinic_summary_section_exposes_main_report_path(self, _counts_mock):
        section, adoption_row = _build_in_clinic_summary_section(
            _reference(
                CampaignConfig(
                    campaign_id="camp-1",
                    campaign_name="Campaign One",
                    system_rfa=False,
                    system_ic=True,
                    system_pe=False,
                    has_entry_navigation=False,
                )
            )
        )

        self.assertEqual(section["system_report_path"], "/campaign/brand-1/")
        self.assertEqual(adoption_row["label"], "In Clinic")

    @patch(
        "reporting.campaign_performance._build_in_clinic_section",
        return_value={
            "key": "in_clinic",
            "label": "InClinic (In-Clinic Sharing)",
            "metrics": [],
            "trend": None,
            "bar_chart": None,
            "table": None,
            "meta": [],
            "adoption": {"eligible_clinics": 4, "active_records": 4, "participating_clinics": 2, "adoption_rate": 50.0},
        },
    )
    @patch("reporting.campaign_performance._resolve_campaign_reference")
    def test_page_payload_keeps_page_sections(self, resolve_reference_mock, _build_in_clinic_section_mock):
        resolve_reference_mock.return_value = _reference(
            CampaignConfig(
                campaign_id="camp-1",
                campaign_name="Campaign One",
                system_rfa=False,
                system_ic=True,
                system_pe=False,
                has_entry_navigation=True,
                banner_target_url="https://example.com/banner",
            )
        )

        payload = build_campaign_performance_page_payload("camp-1")

        self.assertEqual(payload["system_count"], 1)
        self.assertEqual([item["key"] for item in payload["available_systems"]], ["in_clinic"])
        self.assertEqual(payload["sections"][-1]["key"], "adoption_by_clinics")

    @patch("reporting.campaign_performance._schema_table_columns", return_value=frozenset({"available_col"}))
    @patch("reporting.campaign_performance._fetch_rows", return_value=[])
    def test_query_schema_rows_backfills_missing_columns(self, fetch_rows_mock, _columns_mock):
        _query_schema_rows("demo_schema", "demo_table", ["available_col", "missing_col"], order_by="available_col")

        executed_sql = fetch_rows_mock.call_args.args[0]
        self.assertIn("available_col", executed_sql)
        self.assertIn("NULL AS missing_col", executed_sql)

    @patch("reporting.api_services.build_red_flag_alert_rows")
    @patch(
        "reporting.campaign_performance._rfa_attribution_context",
        return_value=RfaAttributionContext(
            mode="field_rep",
            rep_keys=("rep123",),
            assigned_rep_count=1,
            states=("Maharashtra",),
        ),
    )
    def test_rfa_summary_section_uses_field_rep_attribution(self, _context_mock, build_rows_mock):
        build_rows_mock.return_value = [
            {
                "clinic_group": "Mumbai",
                "clinic": "Sunrise Clinic",
                "form_fills": 7,
                "red_flags_total": 2,
                "patient_video_views": 3,
                "reports_emailed_to_doctors": 7,
                "form_shares": 1,
                "patient_scans": 4,
                "follow_ups_scheduled": 2,
                "reminders_sent": 1,
            }
        ]
        section, adoption_row = _build_rfa_summary_section(
            _reference(
                CampaignConfig(
                    campaign_id="camp-1",
                    campaign_name="Campaign One",
                    system_rfa=True,
                    system_ic=False,
                    system_pe=False,
                    has_entry_navigation=False,
                )
            )
        )

        build_rows_mock.assert_called_once_with({"rep123"})
        self.assertEqual(section["data_status"], "ready")
        self.assertEqual(section["metrics"][0]["value"], 7)
        self.assertEqual(adoption_row["clinics_with_activity"], 1)

    @patch(
        "reporting.campaign_performance._build_in_clinic_summary_section",
        return_value=(
            {"key": "in_clinic", "label": "InClinic (In-Clinic Sharing)", "metrics": [], "meta": [], "data_status": "ready"},
            {"system_key": "in_clinic", "label": "In Clinic", "clinics_added": 4, "active_records": 4, "clinics_with_activity": 3},
        ),
    )
    @patch("reporting.campaign_performance._build_rfa_summary_section", side_effect=DatabaseError("boom"))
    @patch("reporting.campaign_performance._resolve_campaign_reference")
    def test_summary_payload_returns_partial_sections_when_one_system_fails(self, resolve_reference_mock, _rfa_mock, _ic_mock):
        resolve_reference_mock.return_value = _reference(
            CampaignConfig(
                campaign_id="camp-1",
                campaign_name="Campaign One",
                system_rfa=True,
                system_ic=True,
                system_pe=False,
                has_entry_navigation=False,
            )
        )

        payload = build_campaign_performance_payload("camp-1")

        self.assertEqual(payload["system_count"], 2)
        self.assertEqual([section["key"] for section in payload["sections"][:-1]], ["rfa", "in_clinic"])
        self.assertEqual(payload["sections"][0]["data_status"], "unavailable")

    @patch(
        "reporting.campaign_performance._build_in_clinic_section",
        return_value={
            "key": "in_clinic",
            "label": "InClinic (In-Clinic Sharing)",
            "metrics": [],
            "trend": None,
            "bar_chart": None,
            "table": None,
            "meta": [],
            "adoption": {"eligible_clinics": 4, "active_records": 4, "participating_clinics": 2, "adoption_rate": 50.0},
            "data_status": "ready",
        },
    )
    @patch("reporting.campaign_performance._build_patient_education_section", side_effect=DatabaseError("boom"))
    @patch("reporting.campaign_performance._resolve_campaign_reference")
    def test_page_payload_returns_partial_sections_when_one_system_fails(self, resolve_reference_mock, _pe_mock, _ic_mock):
        resolve_reference_mock.return_value = _reference(
            CampaignConfig(
                campaign_id="camp-1",
                campaign_name="Campaign One",
                system_rfa=False,
                system_ic=True,
                system_pe=True,
                has_entry_navigation=False,
            )
        )

        payload = build_campaign_performance_page_payload("camp-1")

        self.assertEqual(payload["system_count"], 2)
        self.assertEqual([section["key"] for section in payload["sections"][:-1]], ["in_clinic", "patient_education"])
        self.assertEqual(payload["sections"][1]["data_status"], "unavailable")
