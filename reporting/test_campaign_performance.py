from __future__ import annotations

from unittest.mock import patch

from django.test import SimpleTestCase

from reporting.campaign_performance import (
    CampaignConfig,
    CampaignReference,
    _configured_system_keys,
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
