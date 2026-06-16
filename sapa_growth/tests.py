from __future__ import annotations

import base64
import json
from datetime import date
from io import BytesIO
from unittest.mock import MagicMock, patch
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve, reverse

from etl.sapa_growth import bronze as sapa_bronze
from etl.sapa_growth import pipeline as sapa_pipeline
from etl.sapa_growth import raw as sapa_raw
from etl.sapa_growth import silver as sapa_silver
from etl.sapa_growth import storage as sapa_storage
from etl.sapa_growth import gold as sapa_gold
from etl.sapa_growth.mysql import extract_rows
from etl.sapa_growth.silver import (
    _activity_events_as_legacy_rows,
    _best_dim_for_event,
    _campaign_ids_for_field_rep_login_event,
    _doctor_indexes,
    _doctor_matches_for_api,
    _merge_legacy_rows,
)
from etl.sapa_growth.specs import MYSQL_TABLE_SPECS, RAW_AUDIT_COLUMNS
from sapa_growth import services as sapa_services
from sapa_growth.logic import classify_metric_event, explode_followup_schedule, map_course_status, normalize_phone, webinar_effective_date
from sapa_growth.reporting import filter_rows
from sapa_growth.services import _derived_certified_rows, _enrich_video_rows, dashboard_context, detail_context, export_dashboard_pdf
from sapa_growth.video_metadata import resolve_video_metadata


class SapaGrowthLogicTests(SimpleTestCase):
    def test_raw_source_insert_uses_payload_hash_guard(self):
        cursor_mock = MagicMock()
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor_mock
        cursor_context.__exit__.return_value = False
        connection_mock = MagicMock()
        connection_mock.cursor.return_value = cursor_context

        with patch("etl.sapa_growth.storage.ensure_text_table") as ensure_table, patch(
            "etl.sapa_growth.storage.ensure_source_payload_hash"
        ) as ensure_hash, patch("etl.sapa_growth.storage.connection", connection_mock), patch(
            "etl.sapa_growth.storage.execute_values",
            return_value=[(1,)],
        ) as execute_values_mock:
            inserted = sapa_storage.insert_new_source_rows(
                "raw_sapa_mysql",
                "campaign_fieldrep_raw",
                ["id", "full_name"],
                ["_record_hash", "_source_payload_hash"],
                [{"id": "1", "full_name": "Asha", "_record_hash": "row-hash"}],
            )

        self.assertEqual(inserted, 1)
        self.assertIn("_source_payload_hash", RAW_AUDIT_COLUMNS)
        ensure_table.assert_called_once()
        ensure_hash.assert_called_once_with("raw_sapa_mysql", "campaign_fieldrep_raw", ["id", "full_name"])
        _, query, values = execute_values_mock.call_args.args
        self.assertIn("ROW_NUMBER() OVER (PARTITION BY source_payload_hash", query)
        self.assertIn('existing."_source_payload_hash" = deduped.source_payload_hash', query)
        self.assertEqual(values, [["1", "Asha", "row-hash"]])

    def test_raw_source_insert_can_fingerprint_v2_source_table(self):
        cursor_mock = MagicMock()
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor_mock
        cursor_context.__exit__.return_value = False
        connection_mock = MagicMock()
        connection_mock.cursor.return_value = cursor_context

        with patch("etl.sapa_growth.storage.ensure_text_table"), patch(
            "etl.sapa_growth.storage.ensure_source_payload_hash"
        ) as ensure_hash, patch("etl.sapa_growth.storage.connection", connection_mock), patch(
            "etl.sapa_growth.storage.execute_values",
            return_value=[(1,)],
        ) as execute_values_mock:
            sapa_storage.insert_new_source_rows(
                "raw_sapa_mysql",
                "campaign_fieldrep_raw",
                ["id", "full_name"],
                ["_source_table", "_record_hash", "_source_payload_hash"],
                [{"id": "1", "full_name": "Asha", "_source_table": "field_rep_v2", "_record_hash": "row-hash"}],
                fingerprint_columns=["id", "full_name", "_source_table"],
            )

        ensure_hash.assert_called_once_with("raw_sapa_mysql", "campaign_fieldrep_raw", ["id", "full_name", "_source_table"])
        _, query, values = execute_values_mock.call_args.args
        self.assertIn('"incoming"."_source_table"::text', query)
        self.assertEqual(values, [["1", "Asha", "field_rep_v2", "row-hash"]])

    def test_bronze_uses_only_v2_rows_for_v2_source_specs(self):
        rows = [
            {"id": "1", "full_name": "Legacy", "_source_table": "campaign_fieldrep"},
            {"id": "1", "full_name": "V2 old", "_source_table": "field_rep_v2", "_ingestion_run_id": "run-1", "_ingested_at": "2026-06-02T00:00:00+00:00"},
            {"id": "1", "full_name": "V2", "_source_table": "field_rep_v2", "_ingestion_run_id": "run-2", "_ingested_at": "2026-06-03T00:00:00+00:00"},
            {"id": "2", "full_name": "Old inactive V2", "_source_table": "field_rep_v2", "_ingestion_run_id": "run-1", "_ingested_at": "2026-06-02T00:00:00+00:00"},
        ]

        self.assertEqual(
            sapa_bronze._active_source_rows(rows, "field_rep_v2", ["id"], {"1"}),
            [
                {"id": "1", "full_name": "V2 old", "_source_table": "field_rep_v2", "_ingestion_run_id": "run-1", "_ingested_at": "2026-06-02T00:00:00+00:00"},
                {"id": "1", "full_name": "V2", "_source_table": "field_rep_v2", "_ingestion_run_id": "run-2", "_ingested_at": "2026-06-03T00:00:00+00:00"},
            ],
        )
        self.assertEqual(sapa_bronze._active_source_rows(rows, "redflags_doctor", ["id"]), rows)

    def test_sapa_roster_specs_prefer_redflag_admin_tables(self):
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_doctor"].source_table, "campaign_doctor")
        self.assertIn("doctor_v2", MYSQL_TABLE_SPECS["campaign_doctor"].fallback_source_tables)
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_doctor"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_doctorcampaignenrollment"].source_table, "campaign_doctorcampaignenrollment")
        self.assertIn(
            "doctor_campaign_enrollment_v2",
            MYSQL_TABLE_SPECS["campaign_doctorcampaignenrollment"].fallback_source_tables,
        )
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_doctorcampaignenrollment"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_campaign"].source_table, "campaign_campaign")
        self.assertIn("campaign_v2", MYSQL_TABLE_SPECS["campaign_campaign"].fallback_source_tables)
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_campaign"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_brand"].source_table, "campaign_brand")
        self.assertIn("brand_v2", MYSQL_TABLE_SPECS["campaign_brand"].fallback_source_tables)
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_brand"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_fieldrep"].source_table, "campaign_fieldrep")
        self.assertIn("field_rep_v2", MYSQL_TABLE_SPECS["campaign_fieldrep"].fallback_source_tables)
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_fieldrep"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["campaign_campaignfieldrep"].source_table, "campaign_campaignfieldrep")
        self.assertIn(
            "campaign_field_rep_assignment_v2",
            MYSQL_TABLE_SPECS["campaign_campaignfieldrep"].fallback_source_tables,
        )
        self.assertTrue(MYSQL_TABLE_SPECS["campaign_campaignfieldrep"].current_snapshot)
        self.assertFalse(MYSQL_TABLE_SPECS["rfa_activity_event"].current_snapshot)
        self.assertEqual(MYSQL_TABLE_SPECS["rfa_activity_event"].lookback_days, 45)
        self.assertEqual(MYSQL_TABLE_SPECS["redflags_patientsubmission"].lookback_days, 45)
        self.assertEqual(MYSQL_TABLE_SPECS["gnd_gndpatientsubmission"].lookback_days, 45)

    def test_bronze_prefers_admin_source_rows_over_v2_fallback_rows(self):
        spec = MYSQL_TABLE_SPECS["campaign_campaignfieldrep"]
        rows = [
            {
                "id": "old-v2",
                "field_rep_id": "15",
                "campaign_id": "camp-1",
                "_source_table": "campaign_field_rep_assignment_v2",
            },
            {
                "id": "admin-1",
                "field_rep_id": "142",
                "campaign_id": "camp-1",
                "_source_table": "campaign_campaignfieldrep",
            },
            {
                "id": "admin-stale",
                "field_rep_id": "stale",
                "campaign_id": "camp-1",
                "_source_table": "campaign_campaignfieldrep",
            },
        ]

        with patch("etl.sapa_growth.bronze.current_v2_snapshot_keys", return_value={"admin-1"}):
            self.assertEqual(sapa_bronze._active_source_rows_for_spec(rows, spec), [rows[1]])

    def test_bronze_uses_fallback_when_primary_snapshot_has_no_current_rows(self):
        spec = MYSQL_TABLE_SPECS["campaign_campaignfieldrep"]
        rows = [
            {
                "id": "admin-stale",
                "field_rep_id": "stale",
                "campaign_id": "camp-1",
                "_source_table": "campaign_campaignfieldrep",
            },
            {
                "id": "fallback-v2",
                "field_rep_id": "142",
                "campaign_id": "camp-1",
                "_source_table": "campaign_field_rep_assignment_v2",
            },
        ]

        with patch("etl.sapa_growth.bronze.current_v2_snapshot_keys", side_effect=[set(), {"fallback-v2"}]):
            self.assertEqual(sapa_bronze._active_source_rows_for_spec(rows, spec), [rows[1]])

    def test_current_snapshot_admin_sources_skip_incremental_watermark_and_fallback_on_empty(self):
        spec = MYSQL_TABLE_SPECS["campaign_doctor"]
        fallback_rows = [{"id": "doctor-v2-1"}]
        with patch("etl.sapa_growth.raw.extract_rows", side_effect=[[], fallback_rows]) as extract_mock:
            source_table, rows = sapa_raw._extract_spec_rows("campaign_doctor", spec, "2026-06-01 00:00:00")

        self.assertEqual(source_table, "doctor_v2")
        self.assertEqual(rows, fallback_rows)
        self.assertEqual([call.kwargs["watermark_start"] for call in extract_mock.call_args_list], [None, None])

    def test_current_snapshot_admin_sources_keep_non_empty_primary_rows(self):
        spec = MYSQL_TABLE_SPECS["campaign_doctor"]
        primary_rows = [{"id": "campaign-doctor-1"}]
        with patch("etl.sapa_growth.raw.extract_rows", return_value=primary_rows) as extract_mock:
            source_table, rows = sapa_raw._extract_spec_rows("campaign_doctor", spec, "2026-06-01 00:00:00")

        self.assertEqual(source_table, "campaign_doctor")
        self.assertEqual(rows, primary_rows)
        self.assertEqual(extract_mock.call_count, 1)
        self.assertIsNone(extract_mock.call_args.kwargs["watermark_start"])

    def test_incremental_event_sources_keep_watermark(self):
        spec = MYSQL_TABLE_SPECS["rfa_activity_event"]
        with patch("etl.sapa_growth.raw.extract_rows", return_value=[]) as extract_mock:
            source_table, rows = sapa_raw._extract_spec_rows("rfa_activity_event", spec, "2026-06-01 00:00:00")

        self.assertEqual(source_table, "rfa_activity_event_v2")
        self.assertEqual(rows, [])
        self.assertEqual(extract_mock.call_args.kwargs["watermark_start"], "2026-06-01 00:00:00")

    def test_pipeline_refuses_empty_required_v2_source_counts(self):
        raw_mysql = {
            "extracted_counts": {
                "campaign_doctor": 10,
                "campaign_doctorcampaignenrollment": 10,
                "campaign_campaign": 1,
                "campaign_brand": 1,
                "campaign_fieldrep": 0,
                "campaign_campaignfieldrep": 10,
            }
        }

        with self.assertRaisesRegex(RuntimeError, "campaign_fieldrep"):
            sapa_pipeline._validate_required_v2_source_counts(raw_mysql)

    def test_pipeline_allows_empty_incremental_activity_source_count(self):
        raw_mysql = {
            "extracted_counts": {
                "campaign_doctor": 10,
                "campaign_doctorcampaignenrollment": 10,
                "campaign_campaign": 1,
                "campaign_brand": 1,
                "campaign_fieldrep": 10,
                "campaign_campaignfieldrep": 10,
                "rfa_activity_event": 0,
            }
        }

        sapa_pipeline._validate_required_v2_source_counts(raw_mysql)

    def test_empty_incremental_activity_pull_does_not_replace_current_snapshot(self):
        with patch("etl.sapa_growth.raw.ensure_raw_tables"), patch(
            "etl.sapa_growth.raw.extract_rows",
            return_value=[],
        ), patch("etl.sapa_growth.raw.insert_new_source_rows", return_value=0), patch(
            "etl.sapa_growth.raw.record_v2_current_snapshot"
        ) as snapshot_mock, patch("etl.sapa_growth.raw.get_watermark", return_value=None), patch(
            "etl.sapa_growth.raw.upsert_watermark"
        ), patch("etl.sapa_growth.raw.log_step"):
            sapa_raw.ingest_mysql_sources("run-1", "2026-06-03T00:00:00+00:00")

        self.assertFalse(
            any(call.kwargs["source_table"] == "rfa_activity_event_v2" for call in snapshot_mock.call_args_list)
        )

    def test_rfa_activity_v2_payloads_convert_to_legacy_activity_rows(self):
        converted = _activity_events_as_legacy_rows(
            [
                {
                    "source_table": "redflags_patientsubmission",
                    "source_pk_value": "fallback-id",
                    "event_at": "2026-06-03 10:00:00",
                    "event_payload_json": json.dumps(
                        {
                            "record_id": "sub-1",
                            "doctor_id": "DOC001",
                            "patient_id": "PAT001",
                            "form_id": "FRM001",
                            "language_code": "en",
                            "overall_flag_code": "red",
                            "submitted_at": "2026-06-03 09:55:00",
                        }
                    ),
                },
                {
                    "source_table": "redflags_submissionredflag",
                    "source_pk_value": "7",
                    "event_payload_json": json.dumps({"id": 7, "red_flag_id": "RF001", "submission_id": "sub-1"}),
                },
            ]
        )

        self.assertEqual(converted["redflags_patientsubmission"][0]["record_id"], "sub-1")
        self.assertEqual(converted["redflags_patientsubmission"][0]["doctor_id"], "DOC001")
        self.assertEqual(converted["redflags_submissionredflag"][0]["red_flag_id"], "RF001")

    def test_native_rfa_patient_submission_activity_converts_without_legacy_source_table(self):
        converted = _activity_events_as_legacy_rows(
            [
                {
                    "activity_type": "patient_submission",
                    "source_event_id": "sub-native-1",
                    "event_at": "2026-06-13 09:55:00",
                    "doctor_uuid": "DOC001",
                    "campaign_uuid": "599a2023-3ab9-4227-b82c-5f0a1bc36579",
                    "field_rep_uuid_at_event_time": "rep-599",
                    "patient_id_raw": "PAT001",
                    "form_id_raw": "FORM001",
                    "overall_flag_code": "RED",
                }
            ]
        )

        submission = converted["redflags_patientsubmission"][0]
        self.assertEqual(submission["record_id"], "sub-native-1")
        self.assertEqual(submission["doctor_id"], "DOC001")
        self.assertEqual(submission["campaign_id"], "599a2023-3ab9-4227-b82c-5f0a1bc36579")
        self.assertEqual(submission["overall_flag_code"], "RED")

    def test_rfa_activity_v2_payloads_fall_back_to_bridge_columns(self):
        converted = _activity_events_as_legacy_rows(
            [
                {
                    "source_table": "redflags_metricevent",
                    "source_pk_value": "login-1",
                    "event_at": "2026-06-11 10:51:11",
                    "doctor_uuid": "",
                    "campaign_uuid": "1151a492-947b-4c91-83ac-5a224b2d07b1",
                    "field_rep_uuid_at_event_time": "44228",
                    "event_payload_json": json.dumps({"event_type": "field_rep_login", "meta": {"device_type": "desktop"}}),
                }
            ]
        )

        login = converted["redflags_metricevent"][0]
        self.assertEqual(login["event_type"], "field_rep_login")
        self.assertEqual(login["action_key"], "44228")
        self.assertEqual(login["campaign_id"], "1151a492-947b-4c91-83ac-5a224b2d07b1")
        self.assertEqual(login["field_rep_id"], "44228")

    def test_native_rfa_field_rep_login_activity_converts_without_legacy_source_table(self):
        converted = _activity_events_as_legacy_rows(
            [
                {
                    "activity_type": "field_rep_login",
                    "activity_event_uuid": "activity-1",
                    "source_pk_value": "activity-1",
                    "event_at": "2026-06-13 10:51:11",
                    "campaign_uuid": "599a2023-3ab9-4227-b82c-5f0a1bc36579",
                    "field_rep_uuid_at_event_time": "rep-599",
                    "event_payload_json": json.dumps({"meta": {"device_type": "mobile"}}),
                }
            ]
        )

        login = converted["redflags_metricevent"][0]
        self.assertEqual(login["event_type"], "field_rep_login")
        self.assertEqual(login["action_key"], "rep-599")
        self.assertEqual(login["campaign_id"], "599a2023-3ab9-4227-b82c-5f0a1bc36579")
        self.assertEqual(login["field_rep_id"], "rep-599")

    def test_activity_bridge_rows_merge_with_direct_metric_source_rows(self):
        merged = _merge_legacy_rows(
            [{"id": "login-1", "event_type": "field_rep_login", "action_key": "44228", "ts": "2026-06-11 10:51:11"}],
            [{"id": "login-1", "campaign_id": "1151a492-947b-4c91-83ac-5a224b2d07b1", "field_rep_id": "44228"}],
            ("id",),
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["event_type"], "field_rep_login")
        self.assertEqual(merged[0]["campaign_id"], "1151a492-947b-4c91-83ac-5a224b2d07b1")

    def test_filter_rows_matches_normalized_campaign_route_keys(self):
        rows = [
            {"campaign_key": "1151a492-947b-4c91-83ac-5a224b2d07b1", "value": "match"},
            {"campaign_key": "599a2023-3ab9-4227-b82c-5f0a1bc36579", "value": "skip"},
        ]

        filtered = filter_rows(rows, {"campaign_key": "1151a492947b4c9183ac5a224b2d07b1"})

        self.assertEqual([row["value"] for row in filtered], ["match"])

    def test_field_rep_login_campaign_hint_is_not_expanded_to_other_campaigns(self):
        campaigns = {
            "1151a492-947b-4c91-83ac-5a224b2d07b1": {"id": "1151a492-947b-4c91-83ac-5a224b2d07b1", "name": "Portal"},
            "599a2023-3ab9-4227-b82c-5f0a1bc36579": {"id": "599a2023-3ab9-4227-b82c-5f0a1bc36579", "name": "Abbott"},
        }

        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-1"},
            row={"campaign_id": "599a20233ab94227b82c5f0a1bc36579"},
            rfa_campaigns=campaigns,
            rep_campaign_ids={"rep-1": set(campaigns)},
        )

        self.assertEqual(matched, ["599a2023-3ab9-4227-b82c-5f0a1bc36579"])

    def test_field_rep_login_campaign_hint_can_come_from_metric_meta(self):
        campaigns = {
            "1151a492-947b-4c91-83ac-5a224b2d07b1": {"id": "1151a492-947b-4c91-83ac-5a224b2d07b1", "name": "Portal"},
            "599a2023-3ab9-4227-b82c-5f0a1bc36579": {"id": "599a2023-3ab9-4227-b82c-5f0a1bc36579", "name": "Abbott"},
        }

        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-599"},
            row={
                "event_type": "field_rep_login",
                "action_key": "rep-599",
                "meta": json.dumps(
                    {
                        "campaign_id": "599a20233ab94227b82c5f0a1bc36579",
                        "brand_supplied_field_rep_id": "FR599",
                        "field_rep_name": "Rep 599",
                    }
                ),
            },
            rfa_campaigns=campaigns,
            rep_campaign_ids={"rep-599": set(campaigns)},
            campaign_rep_ids={
                "1151a492-947b-4c91-83ac-5a224b2d07b1": {"other-rep"},
                "599a2023-3ab9-4227-b82c-5f0a1bc36579": {"rep-599"},
            },
        )

        self.assertEqual(matched, ["599a2023-3ab9-4227-b82c-5f0a1bc36579"])

    def test_field_rep_login_meta_campaign_accepts_assigned_brand_supplied_id(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep=None,
            row={
                "event_type": "field_rep_login",
                "action_key": "44228",
                "meta": json.dumps(
                    {
                        "campaign_id": "1151a492947b4c9183ac5a224b2d07b1",
                        "brand_supplied_field_rep_id": "44228",
                    }
                ),
            },
            rfa_campaigns={
                "1151a492-947b-4c91-83ac-5a224b2d07b1": {
                    "id": "1151a492-947b-4c91-83ac-5a224b2d07b1",
                    "name": "Portal",
                }
            },
            rep_campaign_ids={},
            campaign_rep_ids={"1151a492-947b-4c91-83ac-5a224b2d07b1": {"internal-fr-1"}},
            campaign_rep_membership_ids={"1151a492-947b-4c91-83ac-5a224b2d07b1": {"internal-fr-1", "44228"}},
        )

        self.assertEqual(matched, ["1151a492-947b-4c91-83ac-5a224b2d07b1"])

    def test_field_rep_login_meta_campaign_requires_campaign_assignment(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-599"},
            row={
                "event_type": "field_rep_login",
                "action_key": "rep-599",
                "meta": json.dumps({"campaign_id": "campaign-a"}),
            },
            rfa_campaigns={"campaign-a": {"id": "campaign-a", "name": "Campaign A"}},
            rep_campaign_ids={"rep-599": {"campaign-a"}},
            campaign_rep_ids={"campaign-a": {"different-rep"}},
            campaign_rep_membership_ids={"campaign-a": {"different-rep", "FR-DIFFERENT"}},
        )

        self.assertEqual(matched, [])

    def test_field_rep_login_meta_campaign_is_used_when_membership_source_is_empty(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep=None,
            row={
                "event_type": "field_rep_login",
                "action_key": "44228",
                "meta": json.dumps(
                    {
                        "campaign_id": "1151a492947b4c9183ac5a224b2d07b1",
                        "brand_supplied_field_rep_id": "44228",
                    }
                ),
            },
            rfa_campaigns={
                "1151a492-947b-4c91-83ac-5a224b2d07b1": {
                    "id": "1151a492-947b-4c91-83ac-5a224b2d07b1",
                    "name": "Portal",
                }
            },
            rep_campaign_ids={},
            campaign_rep_ids={},
            campaign_rep_membership_ids={},
        )

        self.assertEqual(matched, ["1151a492-947b-4c91-83ac-5a224b2d07b1"])

    def test_field_rep_login_without_campaign_is_not_copied_to_multiple_campaigns(self):
        campaigns = {
            "1151a492-947b-4c91-83ac-5a224b2d07b1": {"id": "1151a492-947b-4c91-83ac-5a224b2d07b1", "name": "Portal"},
            "599a2023-3ab9-4227-b82c-5f0a1bc36579": {"id": "599a2023-3ab9-4227-b82c-5f0a1bc36579", "name": "Abbott"},
        }

        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-1"},
            row={},
            rfa_campaigns=campaigns,
            rep_campaign_ids={"rep-1": set(campaigns)},
        )

        self.assertEqual(matched, [])

    def test_field_rep_login_without_campaign_can_use_single_campaign_assignment(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-1"},
            row={},
            rfa_campaigns={"campaign-a": {"id": "campaign-a", "name": "Only Campaign"}},
            rep_campaign_ids={"rep-1": {"campaign-a"}},
        )

        self.assertEqual(matched, ["campaign-a"])

    def test_field_rep_login_without_campaign_can_use_single_active_assignment(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-1"},
            row={"ts": "2026-06-13 10:00:00"},
            rfa_campaigns={
                "old-campaign": {
                    "id": "old-campaign",
                    "name": "Old Campaign",
                    "start_date": "2026-05-01",
                    "end_date": "2026-06-01",
                },
                "current-campaign": {
                    "id": "current-campaign",
                    "name": "Current Campaign",
                    "start_date": "2026-06-01",
                    "end_date": "2026-06-30",
                },
            },
            rep_campaign_ids={"rep-1": {"old-campaign", "current-campaign"}},
            rep_campaign_assignments={
                "rep-1": [
                    {"campaign_id": "old-campaign", "assigned_at": "2026-05-01"},
                    {"campaign_id": "current-campaign", "assigned_at": "2026-06-01"},
                ]
            },
        )

        self.assertEqual(matched, ["current-campaign"])

    def test_field_rep_login_without_campaign_can_use_brand_supplied_assignment_key(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep=None,
            row={"event_type": "field_rep_login", "action_key": "44228", "ts": "2026-06-13 10:00:00"},
            rfa_campaigns={
                "1151a492-947b-4c91-83ac-5a224b2d07b1": {
                    "id": "1151a492-947b-4c91-83ac-5a224b2d07b1",
                    "name": "Portal",
                    "start_date": "2026-06-01",
                    "end_date": "2026-06-30",
                }
            },
            rep_campaign_ids={"44228": {"1151a492-947b-4c91-83ac-5a224b2d07b1"}},
            rep_campaign_assignments={
                "44228": [
                    {
                        "campaign_id": "1151a492-947b-4c91-83ac-5a224b2d07b1",
                        "assigned_at": "2026-06-01",
                    }
                ]
            },
        )

        self.assertEqual(matched, ["1151a492-947b-4c91-83ac-5a224b2d07b1"])

    def test_field_rep_login_without_campaign_uses_all_active_assignments_when_metadata_is_available(self):
        matched = _campaign_ids_for_field_rep_login_event(
            rep={"id": "rep-1"},
            row={"ts": "2026-06-13 10:00:00"},
            rfa_campaigns={
                "current-a": {
                    "id": "current-a",
                    "name": "Current A",
                    "start_date": "2026-06-01",
                    "end_date": "2026-06-30",
                },
                "current-b": {
                    "id": "current-b",
                    "name": "Current B",
                    "start_date": "2026-06-01",
                    "end_date": "2026-06-30",
                },
                "old-campaign": {
                    "id": "old-campaign",
                    "name": "Old Campaign",
                    "start_date": "2026-05-01",
                    "end_date": "2026-06-01",
                },
            },
            rep_campaign_ids={"rep-1": {"current-a", "current-b", "old-campaign"}},
            rep_campaign_assignments={
                "rep-1": [
                    {"campaign_id": "current-a", "assigned_at": "2026-06-01"},
                    {"campaign_id": "current-b", "assigned_at": "2026-06-01"},
                    {"campaign_id": "old-campaign", "assigned_at": "2026-05-01"},
                ]
            },
        )

        self.assertEqual(matched, ["current-a", "current-b"])

    def test_course_user_can_match_clinic_staff_email_to_doctor_campaign(self):
        dim_rows = [
            {
                "doctor_key": "doc-1",
                "source_doctor_id": "DOC001",
                "canonical_email": "doctor@example.com",
                "canonical_phone": "",
                "canonical_whatsapp_no": "",
                "clinic_user1_email": "staff@example.com",
                "campaign_key": "campaign-a",
            }
        ]
        _by_doctor_id, by_email, by_phone = _doctor_indexes(dim_rows)

        matches = _doctor_matches_for_api({"user_email": "staff@example.com"}, by_email, by_phone, "2026-06-13")

        self.assertEqual(matches[0][0]["doctor_key"], "doc-1")

    def test_course_user_can_match_receptionist_phone_to_doctor_campaign(self):
        dim_rows = [
            {
                "doctor_key": "doc-1",
                "source_doctor_id": "DOC001",
                "canonical_email": "doctor@example.com",
                "canonical_phone": "",
                "canonical_whatsapp_no": "",
                "receptionist_whatsapp_number": "98765 43210",
                "campaign_key": "campaign-a",
            }
        ]
        _by_doctor_id, by_email, by_phone = _doctor_indexes(dim_rows)

        matches = _doctor_matches_for_api({"phone": "+91 98765 43210"}, by_email, by_phone, "2026-06-13")

        self.assertEqual(matches[0][0]["doctor_key"], "doc-1")

    def test_map_course_status(self):
        self.assertEqual(map_course_status("In Progress"), "In Progress")
        self.assertEqual(map_course_status("Completed"), "Completed")
        self.assertEqual(map_course_status("Not Started"), "Not Started")
        self.assertEqual(map_course_status("course_completed"), "Completed")
        self.assertEqual(map_course_status("not-started"), "Not Started")
        self.assertEqual(map_course_status("Started"), "In Progress")
        self.assertEqual(map_course_status("Pending"), "Not Started")
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
            {"campaign_key": None, "state": None, "field_rep_id": None, "doctor_key": None},
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

    def test_transferred_doctor_events_are_attributed_to_one_campaign(self):
        dim_rows = [
            {
                "doctor_key": "DOC-1::campaign:old",
                "source_doctor_id": "DOC-1",
                "canonical_email": "doctor@example.com",
                "campaign_key": "old",
                "campaign_registered_at": "2026-01-01 00:00:00",
                "campaign_start_date": "2026-01-01",
                "campaign_end_date": "",
            },
            {
                "doctor_key": "DOC-1::campaign:new",
                "source_doctor_id": "DOC-1",
                "canonical_email": "doctor@example.com",
                "campaign_key": "new",
                "campaign_registered_at": "2026-04-15 00:00:00",
                "campaign_start_date": "2026-04-01",
                "campaign_end_date": "",
            },
        ]

        self.assertEqual(_best_dim_for_event(dim_rows, "2026-03-15")["campaign_key"], "old")
        self.assertEqual(_best_dim_for_event(dim_rows, "2026-04-20")["campaign_key"], "new")

        _, by_email, by_phone = _doctor_indexes(dim_rows)
        matches = _doctor_matches_for_api({"email": "doctor@example.com"}, by_email, by_phone, "2026-04-20")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0][0]["campaign_key"], "new")

    def test_redflags_only_doctor_counts_as_enrolled_doctor(self):
        rows_by_table = {
            "campaign_campaign": [
                {
                    "id": "camp-a",
                    "name": "Campaign A",
                    "brand_id": "brand-a",
                    "system_rfa": "true",
                    "start_date": "2026-05-01",
                    "end_date": "",
                }
            ],
            "campaign_fieldrep": [
                {
                    "id": "fieldrep-1",
                    "brand_supplied_field_rep_id": "FR1",
                    "full_name": "Rep One",
                    "state": "Karnataka",
                }
            ],
            "campaign_campaignfieldrep": [{"campaign_id": "camp-a", "field_rep_id": "fieldrep-1"}],
            "redflags_doctor": [
                {
                    "doctor_id": "DOC-RF-1",
                    "first_name": "Self",
                    "last_name": "Doctor",
                    "email": "self@example.com",
                    "field_rep_id": "fieldrep-1",
                    "created_at": "2026-05-20 10:00:00",
                    "state": "Karnataka",
                }
            ],
            "redflags_metricevent": [
                {
                    "id": "evt-1",
                    "doctor_id": "DOC-RF-1",
                    "campaign_id": "camp-a",
                    "event_type": "clinic_login",
                    "action_key": "doctor",
                    "ts": "2026-05-20 10:30:00",
                }
            ],
        }
        replace_calls = []

        with patch("etl.sapa_growth.silver.fetch_table", side_effect=lambda _schema, table: rows_by_table.get(table, [])), patch(
            "etl.sapa_growth.silver.replace_table",
            side_effect=lambda schema, table, columns, rows: replace_calls.append((schema, table, columns, list(rows))),
        ), patch(
            "etl.sapa_growth.silver.active_campaign_privacy_allowlist",
            return_value=set(),
        ), patch(
            "etl.sapa_growth.silver.active_person_privacy_rules",
            return_value=[],
        ), patch(
            "etl.sapa_growth.silver.active_raw_visibility_rules",
            return_value=[],
        ):
            sapa_silver.build_silver("run-1")

        dim_call = next(call for call in replace_calls if call[1] == "dim_doctor_clinic")
        dim_rows = dim_call[3]
        self.assertEqual(len(dim_rows), 1)
        self.assertEqual(dim_rows[0]["campaign_key"], "camp-a")
        self.assertEqual(dim_rows[0]["is_user_created_doctor"], "true")
        self.assertEqual(dim_rows[0]["has_campaign_source"], "false")
        self.assertEqual(dim_rows[0]["has_redflags_source"], "true")

    def test_redflags_only_doctor_without_campaign_activity_is_not_copied_by_field_rep_assignment(self):
        rows_by_table = {
            "campaign_campaign": [
                {"id": "camp-a", "name": "Campaign A", "brand_id": "brand-a", "system_rfa": "true"},
                {"id": "camp-b", "name": "Campaign B", "brand_id": "brand-a", "system_rfa": "true"},
            ],
            "campaign_fieldrep": [{"id": "fieldrep-1", "brand_supplied_field_rep_id": "FR1", "full_name": "Rep One"}],
            "campaign_campaignfieldrep": [
                {"campaign_id": "camp-a", "field_rep_id": "fieldrep-1"},
                {"campaign_id": "camp-b", "field_rep_id": "fieldrep-1"},
            ],
            "redflags_doctor": [
                {
                    "doctor_id": "DOC-RF-1",
                    "first_name": "Self",
                    "last_name": "Doctor",
                    "field_rep_id": "fieldrep-1",
                    "created_at": "2026-05-20 10:00:00",
                }
            ],
        }
        replace_calls = []

        with patch("etl.sapa_growth.silver.fetch_table", side_effect=lambda _schema, table: rows_by_table.get(table, [])), patch(
            "etl.sapa_growth.silver.replace_table",
            side_effect=lambda schema, table, columns, rows: replace_calls.append((schema, table, columns, list(rows))),
        ), patch("etl.sapa_growth.silver.active_campaign_privacy_allowlist", return_value=set()), patch(
            "etl.sapa_growth.silver.active_person_privacy_rules",
            return_value=[],
        ), patch("etl.sapa_growth.silver.active_raw_visibility_rules", return_value=[]):
            sapa_silver.build_silver("run-1")

        dim_call = next(call for call in replace_calls if call[1] == "dim_doctor_clinic")
        self.assertEqual(dim_call[3], [])

    def test_campaign_doctor_enrollment_can_match_logical_doctor_id(self):
        rows_by_table = {
            "campaign_campaign": [{"id": "camp-a", "name": "Campaign A", "brand_id": "brand-a", "system_rfa": "true"}],
            "campaign_brand": [{"id": "brand-a", "name": "Brand A"}],
            "campaign_fieldrep": [{"id": "fieldrep-1", "brand_supplied_field_rep_id": "FR1", "full_name": "Rep One"}],
            "campaign_campaignfieldrep": [{"campaign_id": "camp-a", "field_rep_id": "fieldrep-1"}],
            "campaign_doctor": [
                {
                    "id": "campaign-row-1",
                    "doctor_id": "DOC-1",
                    "full_name": "Doctor One",
                    "created_at": "2026-06-01 08:00:00",
                }
            ],
            "campaign_doctorcampaignenrollment": [
                {
                    "campaign_id": "camp-a",
                    "doctor_id": "DOC-1",
                    "registered_by_id": "fieldrep-1",
                    "registered_at": "2026-06-01 09:00:00",
                }
            ],
        }
        replace_calls = []

        with patch("etl.sapa_growth.silver.fetch_table", side_effect=lambda _schema, table: rows_by_table.get(table, [])), patch(
            "etl.sapa_growth.silver.replace_table",
            side_effect=lambda schema, table, columns, rows: replace_calls.append((schema, table, columns, list(rows))),
        ), patch("etl.sapa_growth.silver.active_campaign_privacy_allowlist", return_value=set()), patch(
            "etl.sapa_growth.silver.active_person_privacy_rules",
            return_value=[],
        ), patch("etl.sapa_growth.silver.active_raw_visibility_rules", return_value=[]):
            sapa_silver.build_silver("run-1")

        dim_call = next(call for call in replace_calls if call[1] == "dim_doctor_clinic")
        dim_rows = dim_call[3]
        self.assertEqual(len(dim_rows), 1)
        self.assertEqual(dim_rows[0]["source_doctor_id"], "DOC-1")
        self.assertEqual(dim_rows[0]["campaign_key"], "camp-a")
        self.assertEqual(dim_rows[0]["campaign_label"], "Campaign A (Brand A)")
        self.assertEqual(dim_rows[0]["field_rep_id"], "FR1")

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
            return_value={"campaigns": [{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}], "states": [], "field_reps": [], "doctors": [], "cities": []},
        ):
            context = dashboard_context({"campaign_key": None, "state": None, "field_rep_id": None, "doctor_key": None})
        self.assertFalse(context["ready"])
        self.assertEqual(context["export_filename"], "sapa-growth-dashboard-report.pdf")

    def test_detail_context_includes_window_summary_cards(self):
        with patch("sapa_growth.services._gold_rows") as gold_rows_mock, patch(
            "sapa_growth.services._latest_refresh",
            return_value={"as_of_date": "2026-04-27", "published_at": "2026-04-27T10:00:00Z"},
        ):
            def fake_gold_rows(table: str, scope=None):
                if table == "rpt_screening_detail":
                    return [
                        {"submitted_at": "2026-04-27", "doctor_key": "DOC-1", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-04-25", "doctor_key": "DOC-2", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-04-05", "doctor_key": "DOC-3", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-03-01", "doctor_key": "DOC-4", "campaign_key": "growth-clinic"},
                    ]
                return []

            gold_rows_mock.side_effect = fake_gold_rows
            context = detail_context("total_screenings", {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None})
        self.assertEqual([card["label"] for card in context["summary_cards"]], ["Last 24 Hours", "Last Week", "Last Month", "Cumulative"])
        self.assertEqual([card["count"] for card in context["summary_cards"]], [1, 2, 3, 4])
        self.assertFalse(context["has_selected_window"])
        self.assertEqual(context["rows"], [])
        self.assertEqual(context["route_base"], "/sapa-growth/campaign/growth-clinic/")

    def test_detail_context_loads_table_for_selected_window(self):
        with patch("sapa_growth.services._gold_rows") as gold_rows_mock, patch(
            "sapa_growth.services._latest_refresh",
            return_value={"as_of_date": "2026-04-27", "published_at": "2026-04-27T10:00:00Z"},
        ):
            def fake_gold_rows(table: str, scope=None):
                if table == "rpt_screening_detail":
                    return [
                        {"submitted_at": "2026-04-27", "doctor_key": "DOC-1", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-04-25", "doctor_key": "DOC-2", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-04-05", "doctor_key": "DOC-3", "campaign_key": "growth-clinic"},
                        {"submitted_at": "2026-03-01", "doctor_key": "DOC-4", "campaign_key": "growth-clinic"},
                    ]
                return []

            gold_rows_mock.side_effect = fake_gold_rows
            context = detail_context(
                "total_screenings",
                {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None},
                selected_window="last_week",
            )

        self.assertTrue(context["has_selected_window"])
        self.assertEqual(context["selected_window_label"], "Last Week")
        self.assertEqual(context["row_count"], 2)
        self.assertEqual(len(context["rows"]), 2)
        self.assertEqual([card["selected"] for card in context["summary_cards"]], [False, True, False, False])
        self.assertIn("window=last_week", context["export_href"])

    def test_course_detail_cumulative_includes_undated_snapshot_rows(self):
        with patch("sapa_growth.services._gold_rows") as gold_rows_mock, patch(
            "sapa_growth.services._latest_refresh",
            return_value={"as_of_date": "2026-04-27", "published_at": "2026-04-27T10:00:00Z"},
        ):
            def fake_gold_rows(table: str, scope=None):
                if table == "rpt_course_detail":
                    return [
                        {
                            "campaign_key": "growth-clinic",
                            "course_audience": "paramedic",
                            "dashboard_status": "Not Started",
                            "doctor_key": "DOC-1",
                            "enrolled_at": "",
                        }
                    ]
                return []

            gold_rows_mock.side_effect = fake_gold_rows
            context = detail_context(
                "paramedic_course_pending",
                {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None},
                selected_window="cumulative",
            )

        self.assertEqual([card["count"] for card in context["summary_cards"]], [0, 0, 0, 1])
        self.assertEqual(context["row_count"], 1)
        self.assertEqual(context["rows"][0]["doctor_key"], "DOC-1")

    def test_course_card_links_open_cumulative_detail(self):
        cards = sapa_services._course_cards(
            [
                {
                    "campaign_key": "growth-clinic",
                    "course_audience": "doctor",
                    "dashboard_status": "Completed",
                }
            ],
            {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None},
        )

        completed_row = next(row for row in cards[0]["rows"] if row["label"] == "Completed")
        self.assertIn("details/doctor_course_completed/", completed_row["href"])
        self.assertIn("window=cumulative", completed_row["href"])
        self.assertEqual([row["label"] for row in cards[0]["rows"]], ["Not Started", "In Progress", "Completed"])

    def test_onboarded_doctors_detail_uses_business_columns(self):
        columns = sapa_services.DETAIL_SPECS["onboarded_doctors"]["columns"]

        self.assertEqual(sapa_services.DETAIL_SPECS["onboarded_doctors"]["title"], "Onboarded Doctors")
        self.assertNotIn("doctor_key", columns)
        self.assertNotIn("first_seen_at", columns)
        for column in [
            "campaign_label",
            "doctor_display_name",
            "city",
            "state",
            "field_rep_id",
            "doctor_has_logged_in",
            "doctor_has_updated_special_instructions",
            "doctor_has_added_clinic_staff",
            "clinic_staff_has_logged_in",
            "clinic_staff_forms_shared_count",
            "forms_filled_count",
            "red_tagged_patients_count",
            "yellow_tagged_patients_count",
            "registered_at",
        ]:
            self.assertIn(column, columns)

    def test_dashboard_context_adds_doctor_login_tile_and_state_performance(self):
        filters = {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None}

        with patch(
            "sapa_growth.services.filter_options",
            return_value={
                "campaigns": [{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
                "states": [],
                "field_reps": [],
                "doctors": [],
                "cities": [],
            },
        ), patch("sapa_growth.services._latest_refresh", return_value={"as_of_date": "2026-04-27", "published_at": "2026-04-27T10:00:00Z"}), patch(
            "sapa_growth.services._gold_rows"
        ) as gold_rows_mock:
            def fake_gold_rows(table: str, scope=None):
                if table == "dashboard_summary_snapshot":
                    return [{"as_of_date": "2026-04-27", "published_at": "2026-04-27T10:00:00Z", "certified_clinics_supported": "true"}]
                if table == "rpt_doctor_status_current":
                    return [
                        {
                            "campaign_key": "growth-clinic",
                            "doctor_key": "DOC-1",
                            "state": "Delhi",
                            "onboarding_flag": "true",
                            "doctor_has_logged_in": "Yes",
                        },
                        {
                            "campaign_key": "growth-clinic",
                            "doctor_key": "DOC-2",
                            "state": "Maharashtra",
                            "onboarding_flag": "true",
                            "doctor_has_logged_in": "No",
                        },
                    ]
                if table == "rpt_screening_detail":
                    return [
                        {"campaign_key": "growth-clinic", "doctor_key": "DOC-1", "state": "Delhi"},
                        {"campaign_key": "growth-clinic", "doctor_key": "DOC-1", "state": "Delhi"},
                        {"campaign_key": "growth-clinic", "doctor_key": "DOC-2", "state": "Maharashtra"},
                    ]
                if table == "rpt_field_rep_login_detail":
                    return [
                        {
                            "campaign_key": "growth-clinic",
                            "source_field_rep_id": "FR-1",
                            "field_rep_id": "FR001",
                            "field_rep_name": "Rep One",
                            "state": "Delhi",
                            "login_ts": "2026-04-27 09:00:00",
                        }
                    ]
                return []

            gold_rows_mock.side_effect = fake_gold_rows
            context = dashboard_context(filters)

        doctor_login_tile = next(tile for tile in context["tiles"]["clinic"] if tile["title"] == "Doctor Logins")
        self.assertEqual(doctor_login_tile["value"], 1)
        self.assertIn("window=cumulative", doctor_login_tile["href"])
        field_rep_login_tile = next(tile for tile in context["tiles"]["field_rep"] if tile["title"] == "Field Rep Logins")
        self.assertEqual(field_rep_login_tile["value"], 1)
        self.assertEqual(
            [(row["state"], row["onboarded_doctors"], row["screenings"], row["field_rep_logins"]) for row in context["state_performance"]],
            [("Delhi", 1, 2, 1), ("Maharashtra", 1, 1, 0)],
        )

    def test_dashboard_context_recomputes_when_summary_snapshot_is_stale(self):
        filters = {"campaign_key": "growth-clinic", "state": None, "field_rep_id": None, "doctor_key": None}

        with patch(
            "sapa_growth.services.filter_options",
            return_value={
                "campaigns": [{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
                "states": [],
                "field_reps": [],
                "doctors": [],
                "cities": [],
            },
        ), patch("sapa_growth.services._latest_refresh", return_value={"as_of_date": "2026-06-05", "published_at": "2026-06-05T10:00:00Z"}), patch(
            "sapa_growth.services._gold_rows"
        ) as gold_rows_mock:
            def fake_gold_rows(table: str, scope=None):
                if table == "dashboard_summary_snapshot":
                    return [
                        {
                            "as_of_date": "2026-06-01",
                            "published_at": "2026-06-01T10:00:00Z",
                            "onboarded_doctors_cumulative": "0",
                            "total_screenings_cumulative": "0",
                        }
                    ]
                if table == "rpt_doctor_status_current":
                    return [
                        {
                            "campaign_key": "growth-clinic",
                            "doctor_key": "DOC-1",
                            "onboarding_flag": "true",
                            "first_seen_at": "2026-06-03 09:00:00",
                            "active_flag": "true",
                        }
                    ]
                if table == "rpt_doctor_status_history":
                    return [{"campaign_key": "growth-clinic", "doctor_key": "DOC-1", "as_of_date": "2026-06-05", "is_active": "true"}]
                if table == "rpt_screening_detail":
                    return [{"campaign_key": "growth-clinic", "doctor_key": "DOC-1", "submitted_at": "2026-06-05"}]
                return []

            gold_rows_mock.side_effect = fake_gold_rows
            context = dashboard_context(filters)

        self.assertEqual(context["summary"]["as_of_date"], "2026-06-05")
        self.assertEqual(context["summary"]["onboarded_doctors_cumulative"], 1)
        self.assertEqual(context["summary"]["total_screenings_cumulative"], 1)

    def test_campaign_rows_are_read_from_registered_campaign_schema(self):
        with patch(
            "sapa_growth.services._global_rows",
            return_value=[
                {
                    "campaign_key": "camp-a",
                    "campaign_id_normalized": "campa",
                    "campaign_label": "Campaign A",
                    "gold_schema_name": "gold_sapa_campaign_campa",
                }
            ],
        ), patch("sapa_growth.services.table_exists", return_value=True), patch(
            "sapa_growth.services.fetch_table",
            return_value=[{"doctor_key": "doc-1", "campaign_key": "camp-a"}],
        ) as fetch_table_mock:
            rows = sapa_services._gold_rows("rpt_doctor_status_current", "camp-a")

        self.assertEqual(rows, [{"doctor_key": "doc-1", "campaign_key": "camp-a"}])
        fetch_table_mock.assert_called_once_with("gold_sapa_campaign_campa", "rpt_doctor_status_current")

    def test_gold_publisher_splits_campaign_specific_schemas(self):
        table_names = [
            "refresh_status",
            "dashboard_summary_snapshot",
            "dashboard_summary_state_rep",
            "rpt_doctor_status_current",
            "rpt_doctor_status_history",
            "rpt_screening_detail",
            "rpt_followup_schedule_detail",
            "rpt_reminder_sent_detail",
            "rpt_webinar_registration_detail",
            "rpt_course_detail",
            "rpt_video_view_detail",
            "rpt_submission_redflag_detail",
            "rpt_course_summary",
            "rpt_video_rankings",
            "rpt_red_flag_rankings",
            "rpt_condition_rankings",
            "dim_filter_campaign",
            "dim_filter_state",
            "dim_filter_field_rep",
            "dim_filter_doctor",
            "dim_filter_city",
        ]
        rows_by_table = {
            "dim_filter_campaign": [
                {"underlying_key": "camp-a", "display_label": "Campaign A"},
                {"underlying_key": "camp-b", "display_label": "Campaign B"},
            ],
            "rpt_doctor_status_current": [
                {
                    "doctor_key": "doc-a",
                    "campaign_key": "camp-a",
                    "campaign_label": "Campaign A",
                    "doctor_display_name": "Doctor A",
                    "city": "Mumbai",
                    "state": "Maharashtra",
                    "field_rep_id": "rep-a",
                    "field_rep_name": "Rep A",
                    "active_flag": "true",
                    "inactive_flag": "false",
                    "onboarding_flag": "true",
                    "first_seen_at": "2026-04-01",
                    "latest_seen_at": "2026-04-20",
                },
                {
                    "doctor_key": "doc-b",
                    "campaign_key": "camp-b",
                    "campaign_label": "Campaign B",
                    "doctor_display_name": "Doctor B",
                    "city": "Pune",
                    "state": "Maharashtra",
                    "field_rep_id": "rep-b",
                    "field_rep_name": "Rep B",
                    "active_flag": "false",
                    "inactive_flag": "true",
                    "onboarding_flag": "true",
                    "first_seen_at": "2026-04-02",
                    "latest_seen_at": "2026-04-21",
                },
            ],
            "rpt_doctor_status_history": [
                {"doctor_key": "doc-a", "campaign_key": "camp-a", "as_of_date": "2026-04-20", "is_active": "true", "is_inactive": "false"},
                {"doctor_key": "doc-b", "campaign_key": "camp-b", "as_of_date": "2026-04-20", "is_active": "false", "is_inactive": "true"},
            ],
            "rpt_screening_detail": [
                {"submission_key": "sub-a", "doctor_key": "doc-a", "campaign_key": "camp-a", "submitted_at": "2026-04-20"},
                {"submission_key": "sub-b", "doctor_key": "doc-b", "campaign_key": "camp-b", "submitted_at": "2026-04-20"},
            ],
        }
        default_columns = {
            "refresh_status": ["publish_id", "as_of_date", "published_at", "source_completeness_status", "stale_source_flags", "notes"],
            "dashboard_summary_snapshot": ["as_of_date", "published_at"],
            "dashboard_summary_state_rep": ["campaign_key", "campaign_label", "state", "field_rep_id"],
            "rpt_followup_schedule_detail": ["campaign_key"],
            "rpt_reminder_sent_detail": ["campaign_key"],
            "rpt_webinar_registration_detail": ["campaign_key"],
            "rpt_course_detail": ["campaign_key", "course_audience", "dashboard_status", "doctor_key"],
            "rpt_video_view_detail": ["campaign_key", "audience"],
            "rpt_submission_redflag_detail": ["campaign_key", "red_flag"],
            "rpt_course_summary": ["as_of_date", "course_id", "course_audience"],
            "rpt_video_rankings": ["audience", "content_identifier", "rank"],
            "rpt_red_flag_rankings": ["red_flag", "rank"],
            "rpt_condition_rankings": ["condition_name", "rank"],
            "dim_filter_state": ["display_label", "underlying_key"],
            "dim_filter_field_rep": ["display_label", "underlying_key"],
            "dim_filter_doctor": ["display_label", "underlying_key"],
            "dim_filter_city": ["display_label", "underlying_key"],
        }
        columns_by_table = {
            table: list(rows_by_table[table][0].keys()) if rows_by_table.get(table) else default_columns.get(table, ["campaign_key"])
            for table in table_names
        }
        replace_calls = []

        cursor_context = MagicMock()
        cursor_context.__enter__.return_value.fetchall.return_value = []
        cursor_context.__exit__.return_value = False
        connection_mock = MagicMock()
        connection_mock.cursor.return_value = cursor_context

        with patch("etl.sapa_growth.gold.fetch_table", side_effect=lambda _schema, table: rows_by_table.get(table, [])), patch(
            "etl.sapa_growth.gold._table_columns", side_effect=lambda _schema, table: columns_by_table[table]
        ), patch("etl.sapa_growth.gold.replace_table", side_effect=lambda schema, table, columns, rows: replace_calls.append((schema, table, columns, list(rows)))), patch(
            "etl.sapa_growth.gold.ensure_schema"
        ), patch("etl.sapa_growth.gold.connection", connection_mock):
            schemas = sapa_gold._publish_campaign_schemas(
                table_names=table_names,
                run_id="run-1",
                as_of_date=date(2026, 4, 20),
                published_at="2026-04-20T10:00:00Z",
                source_status="SUCCESS",
                stale_source_flags="",
                notes="",
            )

        self.assertEqual(schemas, ["gold_sapa_campaign_campa", "gold_sapa_campaign_campb"])
        registry_call = next(call for call in replace_calls if call[0] == "gold_sapa_global" and call[1] == "campaign_registry")
        self.assertEqual([row["campaign_key"] for row in registry_call[3]], ["camp-a", "camp-b"])
        camp_a_doctor_call = next(call for call in replace_calls if call[0] == "gold_sapa_campaign_campa" and call[1] == "rpt_doctor_status_current")
        self.assertEqual([row["doctor_key"] for row in camp_a_doctor_call[3]], ["doc-a"])

    def test_gold_current_doctor_status_includes_onboarded_detail_metrics(self):
        today = date.today().isoformat()
        rows_by_table = {
            "dim_doctor_clinic": [
                {
                    "doctor_key": "doc-a",
                    "campaign_key": "camp-a",
                    "campaign_label": "Campaign A",
                    "canonical_display_name": "Doctor A",
                    "city": "Mumbai",
                    "district": "",
                    "state": "Maharashtra",
                    "field_rep_id": "rep-a",
                    "field_rep_name": "Rep A",
                    "is_user_created_doctor": "true",
                    "campaign_registered_at": "2026-06-01 10:00:00",
                    "clinic_password_set_at": "2026-06-01 11:00:00",
                    "special_instructions_uploaded_at": "",
                    "special_instructions_removed_at": "2026-06-02 10:00:00",
                    "clinic_user1_email": "staff@example.com",
                    "clinic_user2_email": "",
                    "first_seen_at": "2026-06-01 10:00:00",
                    "latest_seen_at": "2026-06-02 10:00:00",
                }
            ],
            "fact_doctor_status_daily": [
                {
                    "doctor_key": "doc-a",
                    "campaign_key": "camp-a",
                    "as_of_date": today,
                    "screenings_last_15d": "0",
                    "is_active": "false",
                    "is_inactive": "true",
                    "last_screening_at": "",
                }
            ],
            "fact_metric_event": [
                {"doctor_key": "doc-a", "event_type": "clinic_login", "action_key": "doctor"},
                {"doctor_key": "doc-a", "event_type": "clinic_staff", "action_key": "added"},
                {"doctor_key": "doc-a", "event_type": "clinic_login", "action_key": "clinic_staff"},
                {"doctor_key": "doc-a", "event_type": "clinic_form_share", "action_key": "clinic_staff"},
                {"doctor_key": "doc-a", "event_type": "clinic_form_share", "action_key": "clinic_staff"},
            ],
            "fact_screening_submission": [
                {
                    "doctor_key": "doc-a",
                    "source_table": "redflags_patientsubmission",
                    "source_submission_id": "sub-1",
                    "patient_id": "patient-1",
                    "overall_flag_code": "RED",
                },
                {
                    "doctor_key": "doc-a",
                    "source_table": "redflags_patientsubmission",
                    "source_submission_id": "sub-2",
                    "patient_id": "patient-1",
                    "overall_flag_code": "RED",
                },
                {
                    "doctor_key": "doc-a",
                    "source_table": "redflags_patientsubmission",
                    "source_submission_id": "sub-3",
                    "patient_id": "patient-2",
                    "overall_flag_code": "YELLOW",
                },
                {"doctor_key": "doc-a", "source_table": "gnd_gndpatientsubmission", "overall_flag_code": "RED"},
            ],
        }
        replace_calls = []

        with patch("etl.sapa_growth.gold.fetch_table", side_effect=lambda _schema, table: rows_by_table.get(table, [])), patch(
            "etl.sapa_growth.gold.replace_table",
            side_effect=lambda schema, table, columns, rows: replace_calls.append((schema, table, columns, list(rows))),
        ), patch("etl.sapa_growth.gold._publish_stage_tables"), patch(
            "etl.sapa_growth.gold._publish_campaign_schemas",
            return_value=[],
        ):
            sapa_gold.build_gold("run-1")

        status_call = next(call for call in replace_calls if call[1] == "rpt_doctor_status_current")
        row = status_call[3][0]
        self.assertEqual(row["doctor_has_logged_in"], "Yes")
        self.assertEqual(row["doctor_has_updated_special_instructions"], "Yes")
        self.assertEqual(row["doctor_has_added_clinic_staff"], "Yes")
        self.assertEqual(row["clinic_staff_has_logged_in"], "Yes")
        self.assertEqual(row["clinic_staff_forms_shared_count"], "2")
        self.assertEqual(row["forms_filled_count"], "3")
        self.assertEqual(row["red_tagged_patients_count"], "1")
        self.assertEqual(row["yellow_tagged_patients_count"], "1")
        self.assertEqual(row["registered_at"], "2026-06-01 10:00:00")


class SapaGrowthRoutingTests(SimpleTestCase):
    def test_dashboard_route_registered(self):
        self.assertEqual(reverse("sapa_growth:menu"), "/sapa-growth/")
        self.assertEqual(reverse("sapa_growth:dashboard"), "/sapa-growth/dashboard/")
        self.assertEqual(resolve("/sapa-growth/").view_name, "sapa_growth:menu")
        self.assertEqual(resolve("/sapa-growth/campaign/growth-clinic/").view_name, "sapa_growth:campaign-dashboard")
        self.assertEqual(resolve("/sapa-growth/menu/").view_name, "sapa_growth:menu-legacy")
        self.assertEqual(resolve("/sapa-growth/login/").view_name, "sapa_growth:login")
        self.assertEqual(resolve("/sapa-growth/campaign/growth-clinic/login/").view_name, "sapa_growth:campaign-login")
        self.assertEqual(resolve("/sapa-growth/access/").view_name, "sapa_growth:access")
        self.assertEqual(resolve("/sapa-growth/send-access-email/").view_name, "sapa_growth:send-access-email")


class SapaGrowthAccessViewTests(SimpleTestCase):
    def test_menu_page_renders(self):
        with patch("sapa_growth.views._latest_refresh", return_value=None), patch(
            "sapa_growth.views.campaign_options",
            return_value=[{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
        ):
            response = self.client.get("/sapa-growth/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "SAPA Growth Clinic Program")

    def test_dashboard_redirects_to_login_when_unauthenticated(self):
        response = self.client.get("/sapa-growth/dashboard/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/sapa-growth/")

    def test_legacy_login_route_redirects_to_menu(self):
        with patch(
            "sapa_growth.views.campaign_options",
            return_value=[{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
        ):
            response = self.client.get("/sapa-growth/login/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/sapa-growth/")

    def test_campaign_login_page_renders(self):
        with patch(
            "sapa_growth.views.campaign_options",
            return_value=[{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
        ):
            response = self.client.get("/sapa-growth/campaign/growth-clinic/login/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "SAPA Growth Clinic Program")

    def test_send_access_email_route_redirects_and_calls_mailer(self):
        with patch("sapa_growth.views.send_access_email") as send_email_mock, patch(
            "sapa_growth.views.campaign_options",
            return_value=[{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
        ):
            response = self.client.post(
                "/sapa-growth/send-access-email/",
                {"recipient_email": "team@example.com"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/sapa-growth/")
        send_email_mock.assert_not_called()

    def test_campaign_send_access_email_route_redirects_and_calls_mailer(self):
        with patch("sapa_growth.views.send_access_email") as send_email_mock, patch(
            "sapa_growth.views.campaign_options",
            return_value=[{"underlying_key": "growth-clinic", "display_label": "SAPA Growth Clinic Program"}],
        ):
            response = self.client.post(
                "/sapa-growth/campaign/growth-clinic/send-access-email/",
                {"recipient_email": "team@example.com"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/sapa-growth/campaign/growth-clinic/access/")
        send_email_mock.assert_called_once()


class SapaGrowthMySQLExtractionTests(SimpleTestCase):
    def test_extract_rows_tolerates_missing_optional_columns(self):
        executed_queries: list[tuple[str, list[str]]] = []

        class DummyCursor:
            def __init__(self) -> None:
                self._mode = "columns"
                self._select_batches = [
                    [
                        {
                            "id": 1,
                            "event_type": "action_click",
                            "action_key": "reminder_sent",
                            "share_code": "ABC",
                            "form_id": "FORM-1",
                            "language_code": "en",
                            "video_url": "https://youtu.be/example",
                            "meta": "{}",
                            "ts": "2026-03-25 09:00:00",
                            "doctor_id": "DOC-1",
                            "red_flag_id": "RF-1",
                            "overall_flag_code": "red",
                        }
                    ],
                    [],
                ]

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query: str, params: list[str] | None = None) -> None:
                executed_queries.append((query, list(params or [])))
                if query.startswith("SHOW COLUMNS"):
                    self._mode = "columns"
                else:
                    self._mode = "select"

            def fetchall(self):
                return [
                    {"Field": "id"},
                    {"Field": "event_type"},
                    {"Field": "action_key"},
                    {"Field": "share_code"},
                    {"Field": "form_id"},
                    {"Field": "language_code"},
                    {"Field": "video_url"},
                    {"Field": "meta"},
                    {"Field": "ts"},
                    {"Field": "doctor_id"},
                    {"Field": "red_flag_id"},
                    {"Field": "overall_flag_code"},
                ]

            def fetchmany(self, size: int):
                if self._mode != "select":
                    return []
                return self._select_batches.pop(0)

        class DummyConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self):
                return DummyCursor()

        with patch("etl.sapa_growth.mysql.pymysql.connect", return_value=DummyConnection()):
            rows = extract_rows(
                "redflags_metricevent",
                [
                    "id",
                    "event_type",
                    "action_key",
                    "share_code",
                    "form_id",
                    "language_code",
                    "video_url",
                    "meta",
                    "ts",
                    "doctor_id",
                    "patient_id",
                    "red_flag_id",
                    "overall_flag_code",
                ],
                watermark_field="ts",
                watermark_start="2026-03-01 00:00:00",
            )

        self.assertEqual(rows[0]["patient_id"], None)
        self.assertEqual(len(executed_queries), 2)
        self.assertIn("WHERE `ts` >= %s", executed_queries[1][0])
        self.assertEqual(executed_queries[1][1], ["2026-03-01 00:00:00"])
