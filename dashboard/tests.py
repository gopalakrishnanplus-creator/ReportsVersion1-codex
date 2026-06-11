from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from django.conf import settings
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve

import dashboard.views
from etl import inclinic_pipeline
from etl.pipelines import bronze_transform, raw_ingestion, silver_transform, v2_reporting
from etl.reporting_privacy import (
    campaign_allowed_by_allowlist,
    filter_rows_by_campaign_fields,
    normalize_campaign_id,
    row_visible_by_person_privacy,
)
from dashboard.internal_data_admin import (
    ColumnInfo,
    RAW_AUDIT_COLUMN_NAMES,
    RAW_DEDUPE_BATCH_SIZE,
    TableInfo,
    _batch_cleanup_confirmation_phrase,
    _cleanup_candidate_columns,
    _cleanup_inverse_match_condition,
    _raw_dedupe_confirmation_phrase,
    _raw_dedupe_order_sql,
    _raw_dedupe_report_snapshot,
    _raw_dedupe_validation,
    _raw_fingerprint_sql,
    _raw_payload_sql,
    _is_raw_table_ref,
    _is_relevant_schema,
    _layer_key_for_schema,
    _parse_campaign_ids,
    _selected_cleanup_systems,
    _source_fingerprint_columns,
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
        self.assertEqual(resolve("/campaign/demo/export/").view_name, "campaign-export")
        self.assertEqual(resolve("/campaign/demo/export/field-rep-insights/").view_name, "campaign-field-rep-insights-export")
        self.assertEqual(resolve("/campaign/demo/export/unmapped-doctors/").view_name, "campaign-unmapped-doctors-export")
        self.assertEqual(resolve("/campaign/demo/field-rep-insights/details/").view_name, "campaign-field-rep-insights-detail")
        self.assertEqual(
            resolve("/campaign/demo/collateral/11/field-rep-insights/export/").view_name,
            "campaign-field-rep-insights-collateral-export",
        )
        self.assertEqual(resolve("/_internal/data-admin/").view_name, "internal-data-admin-home")
        self.assertEqual(resolve("/_internal/data-admin/login/").view_name, "internal-data-admin-login")
        self.assertEqual(resolve("/_internal/data-admin/cleanup/").view_name, "internal-data-admin-cleanup")
        self.assertEqual(resolve("/_internal/data-admin/raw-downloads/").view_name, "internal-data-admin-raw-downloads")
        self.assertEqual(resolve("/_internal/data-admin/raw-dedupe/").view_name, "internal-data-admin-raw-dedupe")
        self.assertEqual(resolve("/_internal/data-admin/privacy/").view_name, "internal-data-admin-privacy")
        self.assertEqual(
            resolve("/_internal/data-admin/raw-downloads/raw_server1/campaign_fieldrep/download/").view_name,
            "internal-data-admin-raw-download",
        )
        self.assertEqual(resolve("/_internal/data-admin/bronze/campaign_campaign/").view_name, "internal-data-admin-table")
        self.assertEqual(resolve("/_internal/data-admin/bronze/campaign_campaign/bulk-delete/").view_name, "internal-data-admin-bulk-delete")

    def test_new_delhi_displays_as_delhi(self):
        self.assertEqual(dashboard.views._display_state_name("New Delhi"), "Delhi")

    def test_collateral_options_link_to_main_dashboard_and_use_collateral_schedule_dates(self):
        rows = [
            {
                "collateral_id": "11",
                "collateral_title": "MINI CME POST IMMUNE LAG",
                "schedule_start_date": "2026-05-10",
                "schedule_end_date": "2026-06-10",
                "campaign_start_date": "2026-05-10",
                "campaign_end_date": "2099-06-15",
            },
            {
                "collateral_id": "12",
                "collateral_title": "Current Collateral",
                "schedule_start_date": "2026-06-01",
                "schedule_end_date": "2099-06-30",
                "campaign_start_date": "2026-06-01",
                "campaign_end_date": "2099-06-30",
            },
        ]

        options = dashboard.views._format_collateral_options(rows, "demo", "11", selected_week=2)

        selected = options[0]
        self.assertEqual(selected["name"], "MINI CME POST IMMUNE LAG")
        self.assertEqual(selected["schedule_text"], "May 10, 2026 - Jun 10, 2026")
        self.assertEqual(selected["url"], "/campaign/demo/?collateral_id=11&week=2")
        self.assertEqual(selected["status_label"], "Selected")

    def test_collateral_options_dedupe_exact_duplicate_title_and_schedule(self):
        rows = [
            {
                "collateral_id": "9",
                "collateral_title": "Mini CME Post-COVID Immune Lag in Children",
                "schedule_start_date": "2026-04-08",
                "schedule_end_date": "2026-05-09",
            },
            {
                "collateral_id": "10",
                "collateral_title": "Mini CME Post-COVID Immune Lag in Children",
                "schedule_start_date": "2026-04-08",
                "schedule_end_date": "2026-05-09",
            },
            {
                "collateral_id": "13",
                "collateral_title": "Mini CME Post-COVID Immune Lag in Children",
                "schedule_start_date": "2026-05-10",
                "schedule_end_date": "2026-06-10",
            },
        ]

        options = dashboard.views._format_collateral_options(rows, "demo", "13")

        self.assertEqual(len(options), 2)
        self.assertEqual(
            [option["schedule_text"] for option in options],
            ["May 10, 2026 - Jun 10, 2026", "Apr 08, 2026 - May 09, 2026"],
        )

    def test_internal_data_admin_schema_filter(self):
        self.assertTrue(_is_relevant_schema("bronze"))
        self.assertTrue(_is_relevant_schema("bronze_pe"))
        self.assertTrue(_is_relevant_schema("gold_campaign_cardio_2026_q1"))
        self.assertTrue(_is_relevant_schema("silver_sapa"))
        self.assertTrue(_is_relevant_schema("archive"))
        self.assertFalse(_is_relevant_schema("public"))
        self.assertFalse(_is_relevant_schema("information_schema"))

    def test_internal_data_admin_system_mapping(self):
        self.assertEqual(_system_key_for_schema("raw_server1"), "inclinic")
        self.assertEqual(_system_key_for_schema("gold_campaign_demo"), "inclinic")
        self.assertEqual(_system_key_for_schema("gold_sapa"), "sapa")
        self.assertEqual(_system_key_for_schema("raw_pe_portal"), "pe")
        self.assertEqual(_system_key_for_schema("control"), "shared")
        self.assertEqual(_system_key_for_schema("archive"), "shared")

    def test_internal_data_admin_layer_mapping(self):
        self.assertEqual(_layer_key_for_schema("raw_server2"), "raw")
        self.assertEqual(_layer_key_for_schema("bronze_pe"), "bronze")
        self.assertEqual(_layer_key_for_schema("silver_sapa"), "silver")
        self.assertEqual(_layer_key_for_schema("gold_pe_campaign_demo"), "gold")
        self.assertEqual(_layer_key_for_schema("ops"), "other")

    def test_raw_download_helpers_only_include_raw_source_columns(self):
        columns = [
            ColumnInfo("id", "text", False, 1, None, False, False),
            ColumnInfo("campaign_id", "text", True, 2, None, False, False),
            ColumnInfo("_record_hash", "text", True, 3, None, False, False),
            ColumnInfo("_ingested_at", "text", True, 4, None, False, False),
            ColumnInfo("_source_payload_hash", "text", True, 5, None, False, False),
        ]
        info = TableInfo("raw_server2", "sharing_management_sharelog", columns, [])

        self.assertTrue(_is_raw_table_ref("raw_server2", "sharing_management_sharelog"))
        self.assertFalse(_is_raw_table_ref("bronze", "sharing_management_sharelog"))
        self.assertIn("_record_hash", RAW_AUDIT_COLUMN_NAMES)
        self.assertEqual(_source_fingerprint_columns(info), ["id", "campaign_id"])

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

    def test_raw_dedupe_confirmation_phrase_is_scope_specific(self):
        self.assertEqual(_raw_dedupe_confirmation_phrase("inclinic"), "ARCHIVE RAW DUPLICATES INCLINIC")
        self.assertEqual(_raw_dedupe_confirmation_phrase("all"), "ARCHIVE RAW DUPLICATES ALL")

    def test_raw_dedupe_validation_protects_unique_and_report_counts(self):
        before_plan = {
            "rows": [
                {
                    "schema": "raw_server1",
                    "table": "campaign_campaignfieldrep",
                    "total_rows": 12,
                    "unique_rows": 10,
                    "duplicate_rows": 2,
                }
            ]
        }
        after_plan = {
            "rows": [
                {
                    "schema": "raw_server1",
                    "table": "campaign_campaignfieldrep",
                    "total_rows": 10,
                    "unique_rows": 10,
                    "duplicate_rows": 0,
                }
            ]
        }
        report_snapshot = {
            "rows": [{"schema": "gold_global", "table": "campaign_registry", "row_count": 8, "error": None}],
            "table_count": 1,
            "row_count": 8,
        }

        validation = _raw_dedupe_validation(
            before_plan,
            after_plan,
            report_snapshot,
            report_snapshot,
            [{"schema": "raw_server1", "table": "campaign_campaignfieldrep", "deleted_count": 2}],
        )

        self.assertTrue(validation["passed"])
        self.assertEqual(validation["raw_unique_changed_count"], 0)
        self.assertEqual(validation["report_changed_count"], 0)

    def test_raw_dedupe_report_snapshot_keeps_all_report_rows(self):
        with patch(
            "dashboard.internal_data_admin._raw_dedupe_report_refs",
            return_value=[
                {"schema": "bronze", "name": "campaign_campaignfieldrep"},
                {"schema": "gold_global", "name": "campaign_registry"},
            ],
        ), patch(
            "dashboard.internal_data_admin._fetch_dicts",
            side_effect=[[{"row_count": 4}], [{"row_count": 6}]],
        ):
            snapshot = _raw_dedupe_report_snapshot("inclinic")

        self.assertEqual(snapshot["table_count"], 2)
        self.assertEqual(snapshot["row_count"], 10)
        self.assertEqual([row["table"] for row in snapshot["rows"]], ["campaign_campaignfieldrep", "campaign_registry"])

    def test_raw_dedupe_sql_qualifies_ctid_in_join_scope(self):
        info = TableInfo(
            "raw_pe_master",
            "catalog_videolanguage_raw",
            [
                ColumnInfo("id", "text", True, 1, None, False, False),
                ColumnInfo("_ingested_at", "text", True, 2, None, False, False),
                ColumnInfo("_source_payload_hash", "text", True, 3, None, False, False),
            ],
            [],
        )

        payload_sql = repr(_raw_payload_sql(info, relation="src"))
        fingerprint_sql = repr(_raw_fingerprint_sql(info, relation="src"))
        order_sql = repr(_raw_dedupe_order_sql(info, relation="src"))

        self.assertIn("Identifier('src')", payload_sql)
        self.assertIn("Identifier('src')", fingerprint_sql)
        self.assertIn("Identifier('src')", order_sql)
        self.assertIn("SQL('.ctid DESC')", order_sql)

    def test_raw_ingestion_insert_skips_existing_exact_payload_hash(self):
        cursor_mock = MagicMock()
        cursor_mock.rowcount = 0
        cursor_context = MagicMock()
        cursor_context.__enter__.return_value = cursor_mock
        cursor_context.__exit__.return_value = False
        metadata = raw_ingestion._metadata_values("run-1", "2026-05-30T10:00:00+00:00", "mysql_server_1", "campaign_fieldrep", ["1", "Asha"])

        with patch("etl.pipelines.raw_ingestion.cursor", return_value=cursor_context):
            inserted = raw_ingestion._insert_raw_row(
                "raw_server1",
                "campaign_fieldrep",
                ["id", "full_name"],
                ["1", "Asha"],
                metadata,
            )

        self.assertFalse(inserted)
        query, params = cursor_mock.execute.call_args.args
        self.assertIn("_source_payload_hash", query)
        self.assertIn("WHERE NOT EXISTS", query)
        self.assertIn("md5(jsonb_build_array(%s::text, %s::text)::text)", query)
        self.assertEqual(params[:2], ["1", "Asha"])

    def test_raw_ingestion_counts_inserted_and_skipped_rows(self):
        rows = [{"id": "1", "full_name": "Asha"}, {"id": "1", "full_name": "Asha"}]
        specs = {"mysql_server_1": {"campaign_fieldrep": ["id", "full_name"]}}

        with patch("etl.pipelines.raw_ingestion.SOURCE_TABLE_SPECS", specs), patch(
            "etl.pipelines.raw_ingestion.ensure_raw_tables",
        ), patch(
            "etl.pipelines.raw_ingestion._extract",
            return_value=rows,
        ), patch(
            "etl.pipelines.raw_ingestion._insert_raw_row",
            side_effect=[True, False],
        ):
            result = raw_ingestion.ingest_raw("run-1")

        self.assertEqual(result["counts"]["raw_server1.campaign_fieldrep"], 1)
        self.assertEqual(result["skipped_counts"]["raw_server1.campaign_fieldrep"], 1)
        self.assertEqual(result["extracted_counts"]["raw_server1.campaign_fieldrep"], 2)

    def test_campaign_privacy_helpers_normalize_and_filter_campaign_rows(self):
        allowlist = {normalize_campaign_id("83ce-7fc7 c965")}

        self.assertEqual(normalize_campaign_id(" 83CE-7fc7 c965 "), "83ce7fc7c965")
        self.assertTrue(campaign_allowed_by_allowlist("83ce7fc7c965", allowlist))
        self.assertTrue(campaign_allowed_by_allowlist("83CE-7FC7-C965", allowlist))
        self.assertFalse(campaign_allowed_by_allowlist("other-campaign", allowlist))

        rows = [
            {"row_id": "keep", "campaign_id": "83CE-7FC7-C965"},
            {"row_id": "drop", "campaign_id": "other-campaign"},
            {"row_id": "blank", "campaign_id": ""},
        ]

        filtered = filter_rows_by_campaign_fields(rows, allowlist, ("campaign_id",))

        self.assertEqual([row["row_id"] for row in filtered], ["keep"])

    def test_person_privacy_helper_hides_matching_person_outside_allowed_campaign(self):
        rules = [
            {
                "email_normalized": "test.user@example.com",
                "phone_normalized": "9876543210",
                "allowed_campaign_id_normalized": normalize_campaign_id("Allowed Campaign"),
            }
        ]

        self.assertTrue(
            row_visible_by_person_privacy(
                {"campaign_id": "Allowed-Campaign", "email": "TEST.USER@example.com"},
                rules,
                campaign_fields=("campaign_id",),
                email_fields=("email",),
                phone_fields=("phone",),
            )
        )
        self.assertFalse(
            row_visible_by_person_privacy(
                {"campaign_id": "Other Campaign", "phone": "+91 98765 43210"},
                rules,
                campaign_fields=("campaign_id",),
                email_fields=("email",),
                phone_fields=("phone",),
            )
        )
        self.assertTrue(
            row_visible_by_person_privacy(
                {"campaign_id": "Other Campaign", "email": "real.user@example.com"},
                rules,
                campaign_fields=("campaign_id",),
                email_fields=("email",),
                phone_fields=("phone",),
            )
        )


class DashboardAccessViewTests(SimpleTestCase):
    def _authenticated_campaign_session(self, campaign_id: str = "demo") -> None:
        session = self.client.session
        session[f"auth_{campaign_id}"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

    def _download_context(self) -> dict:
        return {
            "selected_campaign": "demo",
            "selected_week": None,
            "brand_name": "Demo Brand",
            "brand_logo_text": "Demo",
            "company_logo_url": None,
            "collateral_name": "Current Collateral",
            "schedule_text": "May 01, 2026 - May 31, 2026",
            "week_of": "All Weeks",
            "campaign_health": 45,
            "weekly_health": 45,
            "kpi_reached": 30,
            "kpi_opened": 20,
            "kpi_video": 12,
            "kpi_pdf": 8,
            "kpi_reached_pct": 25,
            "kpi_opened_pct": 66.7,
            "kpi_video_pct": 60,
            "kpi_pdf_pct": 40,
            "field_rep_summary": {
                "total_reps": 1,
                "total_doctors_assigned": 2,
                "doctors_sent": 2,
                "doctors_viewed": 1,
                "doctors_video_played": 1,
                "doctors_pdf_downloaded": 1,
                "assignment_issue_count": 0,
            },
            "field_rep_insights": [
                {
                    "field_rep_id": "FR-101",
                    "field_rep_name": "Asha Mehta",
                    "total_doctors_assigned": 1,
                    "doctors_sent": 2,
                    "doctors_viewed": 1,
                    "doctors_video_played": 1,
                    "doctors_pdf_downloaded": 1,
                    "assigned_doctors_json": '[{"name":"Dr Meera Rao","phone":"9999999999","doctor_key":"doc-1"}]',
                    "sent_doctors_json": '[{"name":"Dr Meera Rao","phone":"9999999999","doctor_key":"doc-1"},{"name":"","phone":"8888888888","doctor_key":"doc-2"}]',
                    "viewed_doctors_json": '[{"name":"Dr Meera Rao","phone":"9999999999","doctor_key":"doc-1"}]',
                    "video_doctors_json": '[{"name":"Dr Meera Rao","phone":"9999999999","doctor_key":"doc-1"}]',
                    "pdf_doctors_json": '[{"name":"Dr Meera Rao","phone":"9999999999","doctor_key":"doc-1"}]',
                }
            ],
            "error_message": None,
        }

    def test_engagement_health_uses_actual_campaign_denominator(self):
        score = dashboard.views._engagement_health_score(reached=5, opened=5, consumed=5, total_doctors=1000)
        self.assertAlmostEqual(score, 66.8, places=1)

    def test_weekly_health_uses_four_week_base_and_caps_reach(self):
        score = dashboard.views._weekly_engagement_health_score(reached=80, opened=40, consumed=20, total_doctors=100)
        self.assertAlmostEqual(score, 66.7, places=1)

    def test_field_rep_insights_select_brand_supplied_rep_id_for_display(self):
        with patch("dashboard.views._fetch_dicts", return_value=[]) as fetch_mock, patch(
            "dashboard.views._table_exists",
            return_value=False,
        ):
            dashboard.views._field_rep_insight_rows("brand-1", ["brand-1"], ["9"])

        sql = fetch_mock.call_args.args[0]
        self.assertIn("brand_supplied_field_rep_id", sql)
        self.assertIn("field_rep_display_id", sql)
        self.assertIn("AS field_rep_id", sql)
        self.assertNotIn("AND lower(regexp_replace(COALESCE(btrim(uu.id::text)", sql)
        self.assertNotIn("d.source", sql)
        self.assertNotIn("CASE WHEN COALESCE(ad.total_doctors_assigned, 0) > 0", sql)
        self.assertIn("activity_for_rep AS", sql)
        self.assertNotIn("GREATEST(", sql)
        self.assertIn("assignment_note", sql)
        self.assertIn("raw_assigned_reps AS", sql)
        self.assertIn("GROUP BY field_rep_id", sql)
        self.assertIn("auth_user_id_key", sql)
        self.assertIn("silver.doctor_action_first_seen", sql)
        self.assertIn("activity_period AS", sql)
        self.assertIn("action_dates AS", sql)
        self.assertIn("campaign_roster_matches AS", sql)
        self.assertIn("silver.bridge_brand_campaign_doctor_base", sql)
        self.assertIn("ark.key_type = 'campaign_fieldrep_id'", sql)
        self.assertIn("activity_key_candidates AS", sql)
        self.assertIn("activity_candidate_matches AS", sql)
        self.assertIn("unambiguous_activity_matches AS", sql)
        self.assertIn("HAVING COUNT(DISTINCT m.field_rep_id) = 1", sql)
        self.assertIn("matched_activity AS", sql)
        self.assertIn("unmatched_activity AS", sql)
        self.assertIn("reporting_reps AS", sql)
        self.assertIn("assigned_doctor_rows AS", sql)
        self.assertIn("assigned_doctors_json", sql)
        self.assertIn("activity_doctor_rows AS", sql)
        self.assertIn("sent_doctors_json", sql)
        self.assertIn("viewed_doctors_json", sql)
        self.assertIn("video_doctors_json", sql)
        self.assertIn("pdf_doctors_json", sql)
        self.assertIn("'email'::text AS key_type", sql)
        self.assertNotIn("canonical_activity_rep AS", sql)
        self.assertIn("COALESCE(NULLIF(a.doctor_identity_key, ''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key", sql)
        self.assertIn("COUNT(*) FILTER (WHERE sent_flag = 1)", sql)
        self.assertIn("video_gt_50_first_date", sql)
        self.assertIn("pdf_download_first_date", sql)
        self.assertIn("is_unmapped_activity", sql)
        self.assertNotIn("share_rep_id_email_map AS", sql)
        self.assertNotIn("linked_share.field_rep_email", sql)
        self.assertIn("silver.fact_collateral_transaction tx", sql)
        self.assertIn("silver.fact_share_log s", sql)
        self.assertIn("rep_evidence", sql)
        self.assertIn("field_rep_master_id_resolved", sql)
        self.assertIn("lower(btrim(COALESCE(tx._dq_errors, ''))) NOT IN ('missing', 'conflict', 'ambiguous')", sql)
        self.assertIn("s.field_rep_id::text", sql)
        self.assertIn("AS master_rep_key", sql)
        self.assertIn("''::text AS numeric_rep_key", sql)
        self.assertIn("s.field_rep_id::text", sql)
        self.assertNotIn("SELECT DISTINCT ON (src.activity_row_id)\n                ad.field_rep_id", sql)
        self.assertNotIn("lower(COALESCE(tx.pdf_completed", sql)
        self.assertNotIn("tx.pdf_completed_flag", sql)
        self.assertNotIn("COALESCE(tx.last_video_percentage_num, 0)", sql)
        self.assertNotIn("COALESCE(tx.video_watch_percentage_num, 0)", sql)
        self.assertNotIn("tx.last_video_percentage_num::numeric", sql)
        self.assertNotIn("tx.video_watch_percentage_num::numeric", sql)

    def test_field_rep_summary_keeps_unmapped_activity_out_of_rep_count(self):
        summary = dashboard.views._format_field_rep_summary(
            [
                {"field_rep_id": "5763", "total_doctors_assigned": 2, "doctors_sent": 2},
                {
                    "field_rep_id": "UNMAPPED_ACTIVITY",
                    "is_unmapped_activity": True,
                    "total_doctors_assigned": 0,
                    "doctors_sent": 1,
                    "assignment_note": "No roster match",
                },
            ]
        )

        self.assertEqual(summary["total_reps"], 1)
        self.assertEqual(summary["total_doctors_assigned"], 2)
        self.assertEqual(summary["doctors_sent"], 3)

    def test_field_rep_insights_export_downloads_server_workbook(self):
        self._authenticated_campaign_session()

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=self._download_context()):
            response = self.client.get("/campaign/demo/export/field-rep-insights/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.ms-excel", response["Content-Type"])
        self.assertIn("attachment", response["Content-Disposition"])
        self.assertIn("field_rep_insights_demo_all_weeks_", response["Content-Disposition"])
        workbook = response.content.decode("utf-8")
        self.assertIn("Field Representative Summary", workbook)
        self.assertIn("Doctor Details", workbook)
        self.assertIn("FR-101", workbook)
        self.assertIn("Dr Meera Rao", workbook)
        self.assertIn("doc-1", workbook)

    def test_field_rep_doctor_details_returns_metric_doctors(self):
        self._authenticated_campaign_session()

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=self._download_context()) as context_mock:
            response = self.client.get("/campaign/demo/field-rep-insights/details/?rep_id=FR-101&metric=sent")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["metric_label"], "Collateral Sent")
        self.assertEqual(payload["doctor_count"], 2)
        self.assertEqual(payload["doctors"][0]["name"], "Dr Meera Rao")
        self.assertEqual(payload["doctors"][0]["phone"], "9999999999")
        context_mock.assert_called_once_with(
            "demo",
            None,
            include_field_rep_doctor_details=True,
            include_state_attention=False,
        )

    def test_unmapped_doctors_export_downloads_manual_mapping_workbook(self):
        self._authenticated_campaign_session()

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=self._download_context()):
            response = self.client.get("/campaign/demo/export/unmapped-doctors/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.ms-excel", response["Content-Type"])
        workbook = response.content.decode("utf-8")
        self.assertIn("Doctors Requiring Manual Mapping", workbook)
        self.assertIn("Activity doctor is not in assigned roster for this rep", workbook)
        self.assertIn("8888888888", workbook)

    def test_campaign_pdf_export_downloads_server_pdf(self):
        self._authenticated_campaign_session()

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=self._download_context()):
            response = self.client.get("/campaign/demo/export/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/pdf")
        self.assertIn("attachment", response["Content-Disposition"])
        self.assertTrue(response.content.startswith(b"%PDF-1.4"))

    def test_collateral_field_rep_export_downloads_server_workbook(self):
        self._authenticated_campaign_session()
        context = {**self._download_context(), "selected_collateral_id": "11"}

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_collateral_field_rep_context", return_value=context):
            response = self.client.get("/campaign/demo/collateral/11/field-rep-insights/export/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.ms-excel", response["Content-Type"])
        self.assertIn("field_rep_insights_collateral_demo_collateral_11_all_weeks_", response["Content-Disposition"])
        self.assertIn("Dr Meera Rao", response.content.decode("utf-8"))

    def test_state_attention_uses_rep_aliases_and_effective_reach(self):
        latest_week = {"week_start_date": "2026-04-10", "week_end_date": "2026-04-16"}
        with patch("dashboard.views._optional_table_exists", return_value=True), patch(
            "dashboard.views._fetch_dicts_with_timeout",
            return_value=[],
        ) as fetch_mock:
            dashboard.views._state_attention_source_rows(
                "brand-1",
                ["brand-1"],
                "gold_campaign_brand_1",
                latest_week,
                bridge_base_exists=True,
                current_collateral_ids=["9"],
            )

        sql = fetch_mock.call_args.args[0]
        self.assertIn("raw_rep_state_campaign AS", sql)
        self.assertIn("LEFT JOIN bronze.user_management_user uu", sql)
        self.assertIn("LEFT JOIN bronze.sharing_management_fieldrepresentative sfr", sql)
        self.assertIn("silver.doctor_action_first_seen", sql)
        self.assertNotIn(".fact_doctor_collateral_latest", sql)
        self.assertIn("a.collateral_id::text IN", sql)
        self.assertIn("effective_reached_date", sql)
        self.assertIn("consumed", sql)
        self.assertIn("video_gt_50_first_ts", sql)
        self.assertIn("pdf_download_first_ts", sql)
        self.assertIn("effective_reached_date BETWEEN", sql)
        self.assertIn("roster_base AS", sql)
        self.assertIn("event_enriched AS", sql)
        self.assertIn("share_rep_id_email_map AS", sql)
        self.assertIn("linked_share.field_rep_email", sql)
        self.assertIn("COALESCE(NULLIF(btrim(s.field_rep_email), ''), s.field_rep_id::text)", sql)
        self.assertIn("state_normalized IS NOT NULL", sql)
        self.assertIn("WHEN lower(regexp_replace(COALESCE(btrim(d.state_normalized)", sql)
        self.assertIn("THEN 'Uttar Pradesh'", sql)
        self.assertNotIn("THEN 'Aligarh'", sql)
        self.assertNotIn("state_normalized <> 'UNKNOWN'", sql)
        self.assertNotIn("total_state,0)/4.0", sql)
        self.assertEqual(fetch_mock.call_args.kwargs.get("timeout_ms"), 12000)

    def test_inclinic_silver_uses_strict_rep_mapping_and_backfilled_transaction_ids(self):
        with patch("etl.pipelines.silver_transform.execute") as execute_mock:
            silver_transform.build_silver("run-1")

        sql = "\n".join(str(call.args[0]) for call in execute_mock.call_args_list)
        self.assertIn("CREATE TABLE silver.map_field_rep_identity AS", sql)
        self.assertIn("strict_local_users AS", sql)
        self.assertNotIn("uu.id::text = ccf.field_rep_id::text", sql)
        self.assertIn("source_transaction_id", sql)
        self.assertIn("transaction_identity_key", sql)
        self.assertIn("field_rep_master_id_resolved", sql)
        self.assertIn("latest_transaction_rows AS", sql)
        self.assertIn("COALESCE(t.field_rep_master_id_resolved, t.field_rep_id) AS field_rep_id_resolved", sql)

    def test_inclinic_bronze_preserves_blank_campaign_transactions_for_resolution(self):
        minimal_specs = {"mysql_server_2": {"sharing_management_collateraltransaction": ["id", "brand_campaign_id"]}}
        with patch("etl.pipelines.bronze_transform.SOURCE_TABLE_SPECS", minimal_specs), patch(
            "etl.pipelines.bronze_transform.execute"
        ) as execute_mock:
            bronze_transform.build_bronze()

        sql = "\n".join(str(call.args[0]) for call in execute_mock.call_args_list)
        self.assertIn("LIKE '%test%'", sql)
        self.assertNotIn("COALESCE(\"brand_campaign_id\", '') = ''", sql)

    def test_state_attention_ranks_by_weekly_health_and_cards_show_bottom_three(self):
        state_attention = [
            {"state": "Gujarat", "open_pct": 0, "reached_pct": 0, "health_score": 30, "label": "Low"},
            {"state": "Unknown", "open_pct": 20, "reached_pct": 35, "health_score": 10, "label": "Low"},
            {"state": "Andhra Pradesh", "open_pct": 0, "reached_pct": 0, "health_score": 1, "label": "Low"},
            {"state": "Bihar", "open_pct": 0, "reached_pct": 0, "health_score": 2, "label": "Low"},
            {"state": "Delhi", "open_pct": 0, "reached_pct": 0, "health_score": 3, "label": "Low"},
            {"state": "Karnataka", "open_pct": 0, "reached_pct": 0, "health_score": 4, "label": "Low"},
        ]

        state_attention.sort(key=dashboard.views._state_attention_rank_key)
        card_rows = dashboard.views._state_attention_card_rows(state_attention)

        self.assertEqual(len(card_rows), 3)
        self.assertEqual([row["state"] for row in card_rows], ["Andhra Pradesh", "Bihar", "Delhi"])
        self.assertNotIn("Karnataka", [row["state"] for row in card_rows])

    def test_united_kingdom_state_is_grouped_as_unknown(self):
        self.assertEqual(dashboard.views._display_state_name("United Kingdom"), "Unknown")
        self.assertEqual(dashboard.views._display_state_name("U.K"), "Unknown")
        self.assertEqual(dashboard.views._display_state_name("Aligarh"), "Unknown")
        self.assertEqual(dashboard.views._display_state_name("U.P."), "Uttar Pradesh")
        self.assertEqual(dashboard.views._display_state_name("Delhi NCR"), "Delhi")

    def test_all_weeks_metrics_are_aggregated_not_latest_week_only(self):
        rows = [
            {
                "brand_campaign_id": "demo",
                "week_index": 1,
                "week_start_date": "2026-05-10",
                "week_end_date": "2026-05-16",
                "doctors_reached_unique": 5,
                "doctors_opened_unique": 2,
                "video_viewed_50_unique": 1,
                "pdf_download_unique": 1,
                "doctors_consumed_unique": 1,
                "total_doctors_in_campaign": 100,
            },
            {
                "brand_campaign_id": "demo",
                "week_index": 2,
                "week_start_date": "2026-05-17",
                "week_end_date": "2026-05-23",
                "doctors_reached_unique": 15,
                "doctors_opened_unique": 4,
                "video_viewed_50_unique": 2,
                "pdf_download_unique": 3,
                "doctors_consumed_unique": 4,
                "total_doctors_in_campaign": 100,
            },
        ]

        aggregated = dashboard.views._aggregate_weekly_metric_rows(rows, total_doctors=100)

        self.assertEqual(aggregated["week_index"], 0)
        self.assertEqual(aggregated["doctors_reached_unique"], 20)
        self.assertEqual(aggregated["doctors_opened_unique"], 6)
        self.assertEqual(aggregated["pdf_download_unique"], 4)
        self.assertEqual(aggregated["week_start_date"], "2026-05-10")
        self.assertEqual(aggregated["week_end_date"], "2026-05-23")

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

    def test_internal_data_admin_privacy_page_renders_allowlist(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin.ensure_campaign_privacy_table",
        ), patch(
            "dashboard.internal_data_admin.list_person_privacy_rules",
            return_value=[
                {
                    "rule_id": "person-rule-1",
                    "person_label": "Test user 1",
                    "email": "test.user@example.com",
                    "email_normalized": "test.user@example.com",
                    "phone": "9876543210",
                    "phone_normalized": "9876543210",
                    "allowed_campaign_id": "83ce7fc7c965433ab2b9717394abe3c1",
                    "allowed_campaign_id_normalized": "83ce7fc7c965433ab2b9717394abe3c1",
                    "reason": "Testing user should appear only in approved campaign",
                    "is_active": True,
                    "created_by": "internal_admin",
                    "created_at": "2026-06-05 10:00:00+00",
                }
            ],
        ), patch(
            "dashboard.internal_data_admin.list_campaign_privacy_allowlist_rules",
            return_value=[
                {
                    "rule_id": "rule-1",
                    "campaign_id": "83ce7fc7c965433ab2b9717394abe3c1",
                    "campaign_id_normalized": "83ce7fc7c965433ab2b9717394abe3c1",
                    "reason": "Approved campaign for restricted PII visibility",
                    "is_active": True,
                    "created_by": "internal_admin",
                    "created_at": "2026-06-05 10:00:00+00",
                }
            ],
        ):
            response = self.client.get("/_internal/data-admin/privacy/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reporting Privacy Controls")
        self.assertContains(response, "1 active person visibility rule")
        self.assertContains(response, "test.user@example.com")
        self.assertContains(response, "83ce7fc7c965433ab2b9717394abe3c1")

    def test_internal_data_admin_privacy_post_adds_person_rule(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin.ensure_campaign_privacy_table",
        ), patch(
            "dashboard.internal_data_admin.create_person_privacy_rule",
            return_value="person-rule-1",
        ) as create_rule:
            response = self.client.post(
                "/_internal/data-admin/privacy/",
                {
                    "privacy_action": "add_person",
                    "person_label": "Test user 1",
                    "email": "test.user@example.com",
                    "phone": "9876543210",
                    "allowed_campaign_id": "83ce7fc7c965433ab2b9717394abe3c1",
                    "reason": "Testing user should appear only in approved campaign",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/_internal/data-admin/privacy/")
        create_rule.assert_called_once_with(
            person_label="Test user 1",
            email="test.user@example.com",
            phone="9876543210",
            allowed_campaign_id="83ce7fc7c965433ab2b9717394abe3c1",
            reason="Testing user should appear only in approved campaign",
            created_by="internal_admin",
        )

    def test_internal_data_admin_privacy_post_deactivates_person_rule(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin.ensure_campaign_privacy_table",
        ), patch(
            "dashboard.internal_data_admin.deactivate_person_privacy_rule",
            return_value=True,
        ) as deactivate_rule:
            response = self.client.post(
                "/_internal/data-admin/privacy/",
                {"privacy_action": "deactivate_person", "rule_id": "person-rule-1"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/_internal/data-admin/privacy/")
        deactivate_rule.assert_called_once_with("person-rule-1")

    def test_internal_data_admin_privacy_post_adds_campaign_rule(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin.ensure_campaign_privacy_table",
        ), patch(
            "dashboard.internal_data_admin.create_campaign_privacy_allowlist_rule",
            return_value="rule-1",
        ) as create_rule:
            response = self.client.post(
                "/_internal/data-admin/privacy/",
                {
                    "privacy_action": "add",
                    "campaign_id": "83ce7fc7c965433ab2b9717394abe3c1",
                    "reason": "Approved campaign for restricted PII visibility",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/_internal/data-admin/privacy/")
        create_rule.assert_called_once_with(
            campaign_id="83ce7fc7c965433ab2b9717394abe3c1",
            reason="Approved campaign for restricted PII visibility",
            created_by="internal_admin",
        )

    def test_internal_data_admin_privacy_post_deactivates_campaign_rule(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin.ensure_campaign_privacy_table",
        ), patch(
            "dashboard.internal_data_admin.deactivate_campaign_privacy_allowlist_rule",
            return_value=True,
        ) as deactivate_rule:
            response = self.client.post(
                "/_internal/data-admin/privacy/",
                {"privacy_action": "deactivate", "rule_id": "rule-1"},
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/_internal/data-admin/privacy/")
        deactivate_rule.assert_called_once_with("rule-1")

    def test_internal_data_admin_raw_downloads_render_read_only_summary(self):
        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_summary_cards",
            return_value=[
                {
                    "schema": "raw_server2",
                    "name": "sharing_management_sharelog",
                    "system_key": "inclinic",
                    "system_label": "Inclinic",
                    "layer": "RAW source copy",
                    "view_href": "/_internal/data-admin/raw_server2/sharing_management_sharelog/",
                    "download_href": "/_internal/data-admin/raw-downloads/raw_server2/sharing_management_sharelog/download/",
                    "error": None,
                    "total_rows": 12,
                    "unique_rows": 10,
                    "duplicate_rows": 2,
                    "duplicate_groups": 1,
                    "largest_duplicate_group": 3,
                    "latest_ingested_at": "2026-05-26T09:30:00+00:00",
                    "has_duplicates": True,
                }
            ],
        ):
            response = self.client.get("/_internal/data-admin/raw-downloads/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "RAW Data Downloads")
        self.assertContains(response, "Duplicate rows")
        self.assertContains(response, "sharing_management_sharelog")
        self.assertContains(response, "Download CSV")

    def test_internal_data_admin_raw_download_streams_csv(self):
        info = TableInfo(
            "raw_server1",
            "campaign_fieldrep",
            [ColumnInfo("id", "text", False, 1, None, False, False)],
            [],
        )

        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_table_info",
            return_value=info,
        ), patch(
            "dashboard.internal_data_admin._stream_table_csv",
            return_value=iter(["id\r\n", "1\r\n"]),
        ):
            response = self.client.get("/_internal/data-admin/raw-downloads/raw_server1/campaign_fieldrep/download/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv; charset=utf-8")
        self.assertIn("raw_server1.campaign_fieldrep.csv", response["Content-Disposition"])
        self.assertEqual(b"".join(response.streaming_content), b"id\r\n1\r\n")

    def test_internal_data_admin_raw_dedupe_renders_dry_run(self):
        plan = {
            "selected_system": "inclinic",
            "rows": [
                {
                    "schema": "raw_server1",
                    "table": "campaign_campaignfieldrep",
                    "system_label": "Inclinic",
                    "source_column_count": 4,
                    "view_href": "/_internal/data-admin/raw_server1/campaign_campaignfieldrep/",
                    "error": None,
                    "total_rows": 12,
                    "unique_rows": 10,
                    "duplicate_rows": 2,
                    "duplicate_groups": 1,
                    "has_duplicates": True,
                }
            ],
            "duplicate_rows": [],
            "table_count": 1,
            "duplicate_table_count": 1,
            "total_rows": 12,
            "unique_rows": 10,
            "duplicate_row_count": 2,
            "duplicate_group_count": 1,
            "has_errors": False,
        }
        snapshot = {"rows": [], "table_count": 4, "row_count": 99, "error_count": 0}

        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_dedupe_plan",
            return_value=plan,
        ), patch(
            "dashboard.internal_data_admin._raw_dedupe_report_snapshot",
            return_value=snapshot,
        ):
            response = self.client.get("/_internal/data-admin/raw-dedupe/?system=inclinic")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "RAW Dedupe Workflow")
        self.assertContains(response, "Rows to archive/delete")
        self.assertContains(response, "campaign_campaignfieldrep")
        self.assertContains(response, "ARCHIVE RAW DUPLICATES INCLINIC")
        self.assertContains(response, "Start Auto Cleanup")

    def test_internal_data_admin_raw_dedupe_execute_requires_confirmation(self):
        plan = {
            "selected_system": "inclinic",
            "rows": [],
            "duplicate_rows": [{"schema": "raw_server1", "table": "campaign_campaignfieldrep"}],
            "table_count": 1,
            "duplicate_table_count": 1,
            "total_rows": 12,
            "unique_rows": 10,
            "duplicate_row_count": 2,
            "duplicate_group_count": 1,
            "has_errors": False,
        }
        snapshot = {"rows": [], "table_count": 4, "row_count": 99, "error_count": 0}

        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_dedupe_plan",
            return_value=plan,
        ), patch(
            "dashboard.internal_data_admin._raw_dedupe_report_snapshot",
            return_value=snapshot,
        ), patch("dashboard.internal_data_admin._execute_raw_dedupe") as execute_mock:
            response = self.client.post(
                "/_internal/data-admin/raw-dedupe/",
                {
                    "dedupe_action": "execute",
                    "system": "inclinic",
                    "reason": "cleanup repeated source ingests",
                    "confirmation": "WRONG",
                },
            )

        self.assertEqual(response.status_code, 200)
        execute_mock.assert_not_called()

    def test_internal_data_admin_raw_dedupe_executes_one_table_batch(self):
        plan = {
            "selected_system": "inclinic",
            "rows": [],
            "duplicate_rows": [{"schema": "raw_server1", "table": "campaign_campaignfieldrep"}],
            "table_count": 1,
            "duplicate_table_count": 1,
            "total_rows": 12,
            "unique_rows": 10,
            "duplicate_row_count": 2,
            "duplicate_group_count": 1,
            "has_errors": False,
        }
        snapshot = {"rows": [], "table_count": 4, "row_count": 99, "error_count": 0}
        result = {
            "run_id": "raw-dedupe-test",
            "archive_table": "ops.raw_duplicate_archive",
            "deleted_count": 2,
            "table_count": 1,
            "validation": {
                "passed": True,
                "raw_unique_changed_count": 0,
                "raw_total_mismatch_count": 0,
                "report_row_count_before": 99,
                "report_row_count_after": 99,
                "report_table_count": 4,
                "report_error_count": 0,
                "report_changed_rows": [],
            },
        }

        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_dedupe_plan",
            return_value=plan,
        ), patch(
            "dashboard.internal_data_admin._raw_dedupe_report_snapshot",
            return_value=snapshot,
        ), patch(
            "dashboard.internal_data_admin._raw_dedupe_target_allowed",
            return_value=True,
        ), patch(
            "dashboard.internal_data_admin._execute_raw_dedupe",
            return_value=result,
        ) as execute_mock:
            response = self.client.post(
                "/_internal/data-admin/raw-dedupe/",
                {
                    "dedupe_action": "execute_table",
                    "system": "inclinic",
                    "target_table_ref": "raw_server1.campaign_campaignfieldrep",
                    "reason": "cleanup repeated source ingests",
                    "confirmation": "ARCHIVE RAW DUPLICATES INCLINIC",
                },
            )

        self.assertEqual(response.status_code, 200)
        execute_mock.assert_called_once_with(
            "inclinic",
            "cleanup repeated source ingests",
            "internal_admin",
            target=("raw_server1", "campaign_campaignfieldrep"),
            max_rows=RAW_DEDUPE_BATCH_SIZE,
        )
        self.assertContains(response, "raw-dedupe-test")

    def test_internal_data_admin_raw_dedupe_auto_executes_next_largest_batch_json(self):
        plan = {
            "selected_system": "inclinic",
            "rows": [],
            "duplicate_rows": [
                {"schema": "raw_server1", "table": "small_table", "duplicate_rows": 2},
                {"schema": "raw_server2", "table": "large_table", "duplicate_rows": 200},
            ],
            "table_count": 2,
            "duplicate_table_count": 2,
            "total_rows": 250,
            "unique_rows": 48,
            "duplicate_row_count": 202,
            "duplicate_group_count": 2,
            "has_errors": False,
        }
        after_plan = {**plan, "duplicate_rows": [], "duplicate_row_count": 0, "duplicate_table_count": 0}
        snapshot = {"rows": [], "table_count": 4, "row_count": 99, "error_count": 0}
        result = {
            "run_id": "raw-dedupe-auto-test",
            "archive_table": "ops.raw_duplicate_archive",
            "deleted_count": 200,
            "table_count": 1,
            "validation": {
                "passed": True,
                "raw_unique_changed_count": 0,
                "raw_total_mismatch_count": 0,
                "report_row_count_before": 99,
                "report_row_count_after": 99,
                "report_table_count": 4,
                "report_error_count": 0,
                "report_changed_rows": [],
            },
        }

        with patch("dashboard.internal_data_admin._require_auth", return_value=None), patch(
            "dashboard.internal_data_admin._raw_dedupe_plan",
            side_effect=[plan, after_plan],
        ), patch(
            "dashboard.internal_data_admin._raw_dedupe_report_snapshot",
            return_value=snapshot,
        ), patch(
            "dashboard.internal_data_admin._execute_raw_dedupe",
            return_value=result,
        ) as execute_mock:
            response = self.client.post(
                "/_internal/data-admin/raw-dedupe/",
                {
                    "dedupe_action": "execute_next_batch",
                    "system": "inclinic",
                    "reason": "cleanup repeated source ingests",
                    "confirmation": "ARCHIVE RAW DUPLICATES INCLINIC",
                },
                HTTP_X_REQUESTED_WITH="XMLHttpRequest",
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["table"], "raw_server2.large_table")
        execute_mock.assert_called_once_with(
            "inclinic",
            "cleanup repeated source ingests",
            "internal_admin",
            target=("raw_server2", "large_table"),
            max_rows=RAW_DEDUPE_BATCH_SIZE,
        )

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
        with patch("dashboard.views._table_exists", side_effect=[False, True, False, False, False, False]), patch(
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
                    "source_priority": 10,
                }
            ],
        ), patch(
            "dashboard.views._resolve_campaign_reference",
            return_value=type(
                "Ref",
                (),
                {"brand_campaign_id": "brand-9", "pe_campaign_id": None},
            )(),
        ), patch(
            "dashboard.views._configured_system_keys",
            return_value=["in_clinic"],
        ), patch(
            "dashboard.views._system_report_path",
            return_value="/campaign/brand-9/",
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
            "state_attention": [
                {"state": "Andhra Pradesh", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 1, "label": "Low"},
                {"state": "Bihar", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 2, "label": "Low"},
                {"state": "Delhi", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 3, "label": "Low"},
                {"state": "Gujarat", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 4, "label": "Low"},
                {"state": "Karnataka", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 5, "label": "Low"},
                {"state": "Maharashtra", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 6, "label": "Low"},
                {"state": "Unknown", "open_pct": 20, "reached_pct": 35, "consumed_pct": 0, "health_score": 10, "label": "Low"},
            ],
            "state_attention_card": [
                {"state": "Andhra Pradesh", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 1, "label": "Low"},
                {"state": "Bihar", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 2, "label": "Low"},
                {"state": "Delhi", "open_pct": 1, "reached_pct": 2, "consumed_pct": 0, "health_score": 3, "label": "Low"},
            ],
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
                    "assigned_doctors_json": '[{"name":"Dr Meera Rao","phone":"+919999999999"}]',
                    "sent_doctors_json": '[{"name":"Dr Sent","phone":"+911111111111"}]',
                    "viewed_doctors_json": '[{"name":"Dr Viewed","phone":"+912222222222"}]',
                    "video_doctors_json": '[{"name":"Dr Video","phone":"+913333333333"}]',
                    "pdf_doctors_json": '[{"name":"Dr PDF","phone":"+914444444444"}]',
                    "assignment_note": "Hidden diagnostic note",
                }
            ],
            "old_collaterals": [
                {
                    "collateral_id": "11",
                    "name": "Older Collateral",
                    "schedule_text": "Apr 01, 2026 - Apr 09, 2026",
                    "url": "/campaign/demo/?collateral_id=11",
                    "status_label": "Current",
                    "is_selected": "false",
                },
                {
                    "collateral_id": "12",
                    "name": "Current Collateral",
                    "schedule_text": "May 01, 2026 - May 31, 2026",
                    "url": "/campaign/demo/?collateral_id=12",
                    "status_label": "Selected",
                    "is_selected": "true",
                }
            ],
            "selected_collateral_id": "12",
            "collateral_cards": {
                "current": {"title": "Current Collateral", "reached": 30, "opened": 20, "video": 12, "pdf": 8, "reached_pct": 25, "opened_pct": 66.7, "video_pct": 60, "pdf_pct": 40},
                "best": {"title": "Week 1 Best", "reached": 30, "opened": 20, "video": 12, "pdf": 8, "reached_pct": 25, "opened_pct": 66.7, "video_pct": 60, "pdf_pct": 40},
                "benchmark": {"reached": 40, "opened": 30, "video": 20, "pdf": 10, "reached_pct": 30, "opened_pct": 75, "video_pct": 66.7, "pdf_pct": 33.3},
            },
            "show_collateral_comparison_extras": True,
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
            "campaign_score_available": True,
            "weekly_health": 45,
            "weekly_wow": 0,
            "weekly_benchmark_label": "Average",
            "weekly_color": "yellow",
            "weekly_score_available": True,
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
        self.assertContains(response, "Switch Collateral")
        self.assertContains(response, 'class="comparison-grid single"')
        self.assertNotContains(response, "<h4>Best Collateral</h4>")
        self.assertNotContains(response, "<h4>Benchmark Best</h4>")
        self.assertContains(response, "/campaign/demo/?collateral_id=11")
        self.assertContains(response, "Selected")
        self.assertContains(response, "/campaign/demo/states/")
        self.assertContains(response, "Asha Mehta")
        self.assertContains(response, "doctor-count-btn")
        self.assertContains(response, 'data-metric-label="Collateral Sent"')
        self.assertContains(response, 'data-metric-label="Viewed"')
        self.assertContains(response, 'data-metric-label="Video Played"')
        self.assertContains(response, 'data-metric-label="PDF / Collateral Saved"')
        self.assertContains(response, "S. No.")
        self.assertContains(response, "Assigned Doctors")
        self.assertContains(response, "page-loading")
        self.assertNotContains(response, "<strong>Karnataka</strong>", html=True)
        self.assertNotContains(response, "Sent 30")
        self.assertContains(response, "FR-101")
        self.assertNotContains(response, "Data Note")
        self.assertNotContains(response, "Hidden diagnostic note")
        self.assertNotContains(response, "Action Required This Week")
        self.assertNotContains(response, "Weekly KPI Table")
        self.assertNotContains(response, "Back to Menu")

    def test_campaign_overview_hides_comparison_extras_for_single_collateral(self):
        session = self.client.session
        session["auth_demo"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

        context = {
            "selected_campaign": "demo",
            "brand_name": "Demo Brand",
            "brand_logo_text": "Demo",
            "company_logo_url": None,
            "selected_schema": "gold_demo",
            "weekly_rows": [],
            "error_message": None,
            "schedule_text": "Jun 01, 2026 - Jun 07, 2026",
            "collateral_name": "Only Collateral",
            "state_attention": [],
            "state_attention_card": [],
            "action_panel": {},
            "field_rep_insights": [],
            "field_rep_summary": {
                "total_reps": 0,
                "total_doctors_assigned": 0,
                "doctors_sent": 0,
                "doctors_viewed": 0,
                "doctors_video_played": 0,
                "doctors_pdf_downloaded": 0,
            },
            "old_collaterals": [],
            "current_field_rep_collateral_id": "1",
            "collateral_cards": {
                "current": {"title": "Only Collateral", "reached": 10, "opened": 5, "video": 2, "pdf": 1, "reached_pct": 50, "opened_pct": 50, "video_pct": 40, "pdf_pct": 20},
                "best": {"title": "Only Collateral", "reached": 11, "opened": 6, "video": 2, "pdf": 1, "reached_pct": 55, "opened_pct": 54.5, "video_pct": 33.3, "pdf_pct": 16.7},
                "benchmark": {"reached": 12, "opened": 7, "video": 3, "pdf": 2, "reached_pct": 60, "opened_pct": 58.3, "video_pct": 42.9, "pdf_pct": 28.6},
            },
            "show_collateral_comparison_extras": False,
            "trend_labels": [],
            "reached_pct_series": [],
            "opened_pct_series": [],
            "pdf_pct_series": [],
            "video_pct_series": [],
            "week_options": [],
            "selected_week": None,
            "campaign_health": 0,
            "campaign_wow": 0,
            "campaign_benchmark_label": "Insufficient Data",
            "campaign_color": "red",
            "campaign_score_available": False,
            "weekly_health": 0,
            "weekly_wow": 0,
            "weekly_benchmark_label": "Insufficient Data",
            "weekly_color": "red",
            "weekly_score_available": False,
            "kpi_reached": 10,
            "kpi_opened": 5,
            "kpi_video": 2,
            "kpi_pdf": 1,
            "kpi_reached_pct": 50,
            "kpi_opened_pct": 50,
            "kpi_video_pct": 40,
            "kpi_pdf_pct": 20,
            "week_of": "All Weeks",
        }

        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=context):
            response = self.client.get("/campaign/demo/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Current Collateral")
        self.assertContains(response, "Only Collateral")
        self.assertContains(response, 'class="comparison-grid single"')
        self.assertNotContains(response, "<h4>Best Collateral</h4>")
        self.assertNotContains(response, "<h4>Benchmark Best</h4>")

    def test_campaign_overview_passes_selected_collateral_to_context(self):
        session = self.client.session
        session["auth_demo"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

        context = {
            "selected_campaign": "demo",
            "brand_name": "Demo Brand",
            "brand_logo_text": "Demo",
            "company_logo_url": None,
            "selected_schema": "gold_demo",
            "weekly_rows": [],
            "error_message": None,
            "schedule_text": "May 10, 2026 - Jun 15, 2026",
            "collateral_name": "MINI CME POST IMMUNE LAG",
            "state_attention": [],
            "state_attention_card": [],
            "action_panel": {},
            "field_rep_insights": [],
            "field_rep_summary": {"total_reps": 0, "total_doctors_assigned": 0},
            "old_collaterals": [],
            "selected_collateral_id": "11",
            "current_field_rep_collateral_id": "11",
            "field_rep_detail_url": "/campaign/demo/field-rep-insights/details/?collateral_id=11",
            "collateral_cards": {"current": {}, "best": {}, "benchmark": {}},
            "show_collateral_comparison_extras": False,
            "trend_labels": [],
            "reached_pct_series": [],
            "opened_pct_series": [],
            "pdf_pct_series": [],
            "video_pct_series": [],
            "week_options": [],
            "selected_week": 2,
            "campaign_health": 0,
            "campaign_wow": 0,
            "campaign_benchmark_label": "Insufficient Data",
            "campaign_color": "red",
            "campaign_score_available": False,
            "weekly_health": 0,
            "weekly_wow": 0,
            "weekly_benchmark_label": "Insufficient Data",
            "weekly_color": "red",
            "weekly_score_available": False,
            "week_of": "Week -",
            "kpi_reached": 0,
            "kpi_reached_pct": 0,
            "kpi_opened": 0,
            "kpi_opened_pct": 0,
            "kpi_video": 0,
            "kpi_video_pct": 0,
            "kpi_pdf": 0,
            "kpi_pdf_pct": 0,
        }
        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=context) as context_mock:
            response = self.client.get("/campaign/demo/?collateral_id=11&week=2")

        self.assertEqual(response.status_code, 200)
        context_mock.assert_called_once_with(
            "demo",
            2,
            selected_collateral_id="11",
            include_field_rep_doctor_details=False,
        )

    def test_state_list_page_renders_full_state_view(self):
        session = self.client.session
        session["auth_demo"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

        context = {
            "selected_campaign": "demo",
            "brand_name": "Demo Brand",
            "collateral_name": "Current Collateral",
            "selected_week": None,
            "error_message": None,
            "state_attention": [
                {"state": "Andhra Pradesh", "open_pct": 10, "reached_pct": 20, "consumed_pct": 5, "health_score": 12, "label": "Low"},
                {"state": "Maharashtra", "open_pct": 30, "reached_pct": 40, "consumed_pct": 20, "health_score": 35, "label": "Medium"},
            ],
        }
        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_report_context", return_value=context):
            response = self.client.get("/campaign/demo/states/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "States Requiring Attention")
        self.assertContains(response, "Andhra Pradesh")
        self.assertContains(response, "Maharashtra")
        self.assertContains(response, "page-loading")
        self.assertNotContains(response, "state-row-extra")

    def test_collateral_field_rep_page_renders_selected_collateral(self):
        session = self.client.session
        session["auth_demo"] = True
        session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = session.session_key

        context = {
            "selected_campaign": "demo",
            "selected_collateral_id": "11",
            "brand_name": "Demo Brand",
            "brand_logo_text": "Demo",
            "company_logo_url": None,
            "collateral_name": "Older Collateral",
            "schedule_text": "Apr 01, 2026 - Apr 09, 2026",
            "field_rep_summary": {
                "total_reps": 1,
                "total_doctors_assigned": 120,
                "doctors_sent": 12,
                "doctors_viewed": 4,
                "doctors_video_played": 1,
                "doctors_pdf_downloaded": 3,
            },
            "field_rep_insights": [
                {
                    "field_rep_id": "FR-101",
                    "field_rep_name": "Asha Mehta",
                    "total_doctors_assigned": 120,
                    "doctors_sent": 12,
                    "doctors_viewed": 4,
                    "doctors_video_played": 1,
                    "doctors_pdf_downloaded": 3,
                    "assigned_doctors_json": '[{"name":"Dr Meera Rao","phone":"+919999999999"}]',
                    "sent_doctors_json": '[{"name":"Dr Sent","phone":"+911111111111"}]',
                    "viewed_doctors_json": '[{"name":"Dr Viewed","phone":"+912222222222"}]',
                    "video_doctors_json": '[{"name":"Dr Video","phone":"+913333333333"}]',
                    "pdf_doctors_json": '[{"name":"Dr PDF","phone":"+914444444444"}]',
                }
            ],
            "old_collaterals": [],
            "error_message": None,
        }
        with patch(
            "dashboard.views.build_report_access",
            return_value=type("Access", (), {"session_key": "auth_demo"})(),
        ), patch("dashboard.views._build_collateral_field_rep_context", return_value=context):
            response = self.client.get("/campaign/demo/collateral/11/field-rep-insights/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Older Collateral")
        self.assertContains(response, "This page is filtered to collateral ID 11 only.")
        self.assertContains(response, "Asha Mehta")
        self.assertContains(response, "doctor-count-btn")
        self.assertContains(response, 'data-metric-label="Collateral Sent"')
        self.assertContains(response, "S. No.")
        self.assertContains(response, "Assigned Doctors")
        self.assertContains(response, "12")
        self.assertContains(response, "page-loading")
        self.assertNotContains(response, "Sent 12")


class V2ReportingPreservationTests(SimpleTestCase):
    def test_v2_source_safety_fails_before_blank_reporting_rebuild(self):
        source = {key: [{"id": "1"}] for key in v2_reporting.REQUIRED_V2_SOURCE_KEYS}
        source["inclinic_share_event_v2"] = []

        with self.assertRaisesMessage(
            RuntimeError,
            "Existing InClinic reporting tables were not replaced",
        ):
            v2_reporting._validate_required_v2_source_counts(source)

    def test_v2_source_safety_allows_populated_required_sources(self):
        source = {key: [{"id": "1"}] for key in v2_reporting.REQUIRED_V2_SOURCE_KEYS}

        v2_reporting._validate_required_v2_source_counts(source)

    def test_campaign_privacy_filter_keeps_only_allowed_inclinic_source_rows(self):
        source = {
            "campaign_v2": [
                {"legacy_campaign_id": "Keep Campaign", "name": "Allowed"},
                {"legacy_campaign_id": "Other Campaign", "name": "Blocked"},
            ],
            "campaign_field_rep_assignment_v2": [
                {"legacy_campaign_id": "Keep Campaign", "field_rep_id": "rep-1"},
                {"legacy_campaign_id": "Other Campaign", "field_rep_id": "rep-2"},
            ],
            "inclinic_campaign_field_rep_assignment_v2": [],
            "doctor_field_rep_roster_bridge_v2": [],
            "inclinic_assigned_doctor_roster_v2": [],
            "field_rep_v2": [
                {"current_campaign_fieldrep_id": "rep-1", "display_name": "Allowed Rep"},
                {"current_campaign_fieldrep_id": "rep-2", "display_name": "Blocked Rep"},
            ],
            "inclinic_field_rep_identity_v2": [
                {"campaign_fieldrep_id": "rep-1", "email_normalized": "allowed@example.com"},
                {"campaign_fieldrep_id": "rep-2", "email_normalized": "blocked@example.com"},
            ],
        }

        filtered = v2_reporting._apply_campaign_privacy_to_source(source, {normalize_campaign_id("Keep Campaign")})

        self.assertEqual([row["name"] for row in filtered["campaign_v2"]], ["Allowed"])
        self.assertEqual([row["field_rep_id"] for row in filtered["campaign_field_rep_assignment_v2"]], ["rep-1"])
        self.assertEqual([row["display_name"] for row in filtered["field_rep_v2"]], ["Allowed Rep"])
        self.assertEqual([row["email_normalized"] for row in filtered["inclinic_field_rep_identity_v2"]], ["allowed@example.com"])

    def test_person_privacy_filter_keeps_test_user_only_in_allowed_inclinic_campaign(self):
        rules = [
            {
                "email_normalized": "test.rep@example.com",
                "phone_normalized": "9876543210",
                "allowed_campaign_id_normalized": normalize_campaign_id("Allowed Campaign"),
            }
        ]
        source = {
            "inclinic_collateral_transaction_v2": [
                {"legacy_campaign_id": "Allowed Campaign", "doctor_phone_normalized": "9876543210", "campaign_fieldrep_id": "rep-1"},
                {"legacy_campaign_id": "Other Campaign", "doctor_phone_normalized": "9876543210", "campaign_fieldrep_id": "rep-1"},
                {"legacy_campaign_id": "Other Campaign", "doctor_phone_normalized": "9999999999", "campaign_fieldrep_id": "rep-2"},
            ],
            "inclinic_share_event_v2": [],
            "doctor_field_rep_roster_bridge_v2": [],
            "inclinic_assigned_doctor_roster_v2": [],
            "campaign_field_rep_assignment_v2": [
                {"legacy_campaign_id": "Allowed Campaign", "field_rep_id": "rep-1"},
                {"legacy_campaign_id": "Other Campaign", "field_rep_id": "rep-1"},
                {"legacy_campaign_id": "Other Campaign", "field_rep_id": "rep-2"},
            ],
            "inclinic_campaign_field_rep_assignment_v2": [],
            "field_rep_v2": [
                {"current_campaign_fieldrep_id": "rep-1", "primary_email": "test.rep@example.com", "phone_number": "9876543210"},
                {"current_campaign_fieldrep_id": "rep-2", "primary_email": "real.rep@example.com", "phone_number": "9999999999"},
            ],
            "inclinic_field_rep_identity_v2": [
                {"campaign_fieldrep_id": "rep-1", "email_normalized": "test.rep@example.com"},
                {"campaign_fieldrep_id": "rep-2", "email_normalized": "real.rep@example.com"},
            ],
        }

        filtered = v2_reporting._apply_person_privacy_to_source(source, rules)

        self.assertEqual([row["legacy_campaign_id"] for row in filtered["inclinic_collateral_transaction_v2"]], ["Allowed Campaign", "Other Campaign"])
        self.assertEqual([row["field_rep_id"] for row in filtered["campaign_field_rep_assignment_v2"]], ["rep-1", "rep-2"])
        self.assertEqual([row["legacy_campaign_id"] for row in filtered["campaign_field_rep_assignment_v2"]], ["Allowed Campaign", "Other Campaign"])
        self.assertEqual([row["current_campaign_fieldrep_id"] for row in filtered["field_rep_v2"]], ["rep-1", "rep-2"])

    def test_field_rep_state_falls_back_to_inclinic_identity_v2_state(self):
        source = {
            "field_rep_v2": [
                {
                    "id": "2282",
                    "current_campaign_fieldrep_id": "2282",
                    "current_brand_supplied_field_rep_id": "5014",
                    "display_name": "Deen Bandhu",
                    "state": "",
                    "is_active": "1",
                }
            ],
            "inclinic_field_rep_identity_v2": [
                {
                    "campaign_fieldrep_id": "2282",
                    "campaign_fieldrep_state": "uttar pradesh",
                    "is_current": "1",
                    "source_updated_at": "2026-06-05 09:30:00",
                }
            ],
        }

        rows = v2_reporting._field_rep_rows(source, "2026-06-05T00:00:00+00:00")

        self.assertEqual(rows[0]["state"], "uttar pradesh")
        self.assertEqual(rows[0]["state_normalized"], "Uttar Pradesh")

    def test_field_rep_state_uses_preserved_fallback_when_v2_sources_are_blank(self):
        source = {
            "field_rep_v2": [
                {
                    "id": "2282",
                    "current_campaign_fieldrep_id": "2282",
                    "current_brand_supplied_field_rep_id": "5014",
                    "display_name": "Deen Bandhu",
                    "state": "",
                    "is_active": "1",
                }
            ],
            "inclinic_field_rep_identity_v2": [
                {
                    "campaign_fieldrep_id": "2282",
                    "campaign_fieldrep_state": "",
                    "is_current": "1",
                }
            ],
        }

        rows = v2_reporting._field_rep_rows(
            source,
            "2026-06-05T00:00:00+00:00",
            {"2282": "Maharashtra"},
        )

        self.assertEqual(rows[0]["state"], "Maharashtra")
        self.assertEqual(rows[0]["state_normalized"], "Maharashtra")

    def test_field_rep_state_keeps_master_state_before_identity_fallback(self):
        source = {
            "field_rep_v2": [
                {
                    "id": "2282",
                    "current_campaign_fieldrep_id": "2282",
                    "current_brand_supplied_field_rep_id": "5014",
                    "display_name": "Deen Bandhu",
                    "state": "Delhi",
                    "is_active": "1",
                }
            ],
            "inclinic_field_rep_identity_v2": [
                {
                    "campaign_fieldrep_id": "2282",
                    "campaign_fieldrep_state": "uttar pradesh",
                    "is_current": "1",
                }
            ],
        }

        rows = v2_reporting._field_rep_rows(source, "2026-06-05T00:00:00+00:00")

        self.assertEqual(rows[0]["state"], "Delhi")
        self.assertEqual(rows[0]["state_normalized"], "Delhi")

    @patch.dict(os.environ, {"INCLINIC_REPORTING_SOURCE_MODE": "v2"}, clear=False)
    @patch("etl.inclinic_pipeline.log_run")
    @patch("etl.inclinic_pipeline.build_gold")
    @patch("etl.inclinic_pipeline.build_v2_reporting")
    @patch("etl.inclinic_pipeline.refresh_raw_v2_from_source")
    @patch("etl.inclinic_pipeline.ensure_control_tables")
    def test_inclinic_v2_pipeline_refreshes_source_by_default(
        self,
        ensure_control_tables,
        refresh_raw_v2_from_source,
        build_v2_reporting,
        build_gold,
        log_run,
    ):
        os.environ.pop("INCLINIC_REPORTING_REFRESH_RAW_V2_FROM_SOURCE", None)
        refresh_raw_v2_from_source.return_value = {"raw_v2_inclinic.inclinic_field_rep_identity_v2": 1}
        build_v2_reporting.return_value = {"counts": {}, "issues": {}, "preservation_counts": {}}

        result = inclinic_pipeline.run_pipeline(run_id="state-refresh-test", trigger_type="manual")

        self.assertEqual(result["status"], "SUCCESS")
        refresh_raw_v2_from_source.assert_called_once_with("state-refresh-test")
        build_gold.assert_called_once_with("state-refresh-test")
        log_run.assert_called_once()

    def test_share_rep_resolution_trusts_source_id_when_email_conflicts(self):
        source = {
            "inclinic_field_rep_identity_v2": [
                {
                    "email_normalized": "rameshkumar.pharma@apexlab.com",
                    "source_table": "auth_user",
                    "source_column": "email",
                    "campaign_fieldrep_id": "140",
                    "is_current": "1",
                },
                {
                    "email_normalized": "rameshkumar.pharma@apexlab.com",
                    "source_table": "user_management_user",
                    "source_column": "email",
                    "campaign_fieldrep_id": "136",
                    "is_current": "1",
                },
            ],
            "campaign_field_rep_assignment_v2": [
                {"legacy_campaign_id": "camp-1", "field_rep_id": "136", "is_current": "1"},
                {"legacy_campaign_id": "camp-1", "field_rep_id": "138", "is_current": "1"},
                {"legacy_campaign_id": "camp-1", "field_rep_id": "140", "is_current": "1"},
            ],
            "inclinic_campaign_field_rep_assignment_v2": [],
        }
        identities_by_email, assigned_reps_by_campaign = v2_reporting._share_email_resolution_context(source)
        share = {
            "campaign_fieldrep_id": "138",
            "old_field_rep_id": "138",
            "field_rep_email_normalized": "rameshkumar.pharma@apexlab.com",
            "field_rep_email_matches_campaign_fieldrep": "0",
        }

        self.assertEqual(v2_reporting._resolve_share_rep_id(share, "camp-1", identities_by_email, assigned_reps_by_campaign), "138")

    def test_share_rep_resolution_trusts_source_id_when_email_matches(self):
        identities_by_email, assigned_reps_by_campaign = v2_reporting._share_email_resolution_context(
            {"inclinic_field_rep_identity_v2": [], "campaign_field_rep_assignment_v2": [], "inclinic_campaign_field_rep_assignment_v2": []}
        )
        share = {
            "campaign_fieldrep_id": "138",
            "old_field_rep_id": "138",
            "field_rep_email_normalized": "rakesh@example.com",
            "field_rep_email_matches_campaign_fieldrep": "1",
        }

        self.assertEqual(
            v2_reporting._resolve_share_rep_id(share, "camp-1", identities_by_email, assigned_reps_by_campaign),
            "138",
        )

    def test_share_rep_resolution_keeps_source_id_when_email_conflict_is_unresolved(self):
        identities_by_email, assigned_reps_by_campaign = v2_reporting._share_email_resolution_context(
            {
                "inclinic_field_rep_identity_v2": [],
                "campaign_field_rep_assignment_v2": [{"legacy_campaign_id": "camp-1", "field_rep_id": "138", "is_current": "1"}],
                "inclinic_campaign_field_rep_assignment_v2": [],
            }
        )
        share = {
            "campaign_fieldrep_id": "138",
            "old_field_rep_id": "138",
            "field_rep_email_normalized": "unknown@example.com",
            "field_rep_email_matches_campaign_fieldrep": "0",
        }

        self.assertEqual(v2_reporting._resolve_share_rep_id(share, "camp-1", identities_by_email, assigned_reps_by_campaign), "138")

    def test_preservation_ignores_run_metadata_only_changes(self):
        columns = ["id", "transaction_identity_key", "doctor_name", "_silver_updated_at", "_as_of_run_id"]
        existing = [
            {
                "id": "old-1",
                "transaction_identity_key": "tx-key-1",
                "doctor_name": "Dr A",
                "_silver_updated_at": "2026-01-01T00:00:00+00:00",
                "_as_of_run_id": "old-run",
            }
        ]
        current = [
            {
                "id": "old-1",
                "transaction_identity_key": "tx-key-1",
                "doctor_name": "Dr A",
                "_silver_updated_at": "2026-06-03T00:00:00+00:00",
                "_as_of_run_id": "new-run",
            }
        ]

        with (
            patch.object(v2_reporting, "table_exists", return_value=True),
            patch.object(v2_reporting, "fetch_table", return_value=existing),
            patch.object(v2_reporting, "_ensure_preservation_archive_table") as ensure_archive,
            patch.object(v2_reporting, "connection") as connection_mock,
        ):
            result = v2_reporting._archive_replaced_reporting_rows(
                "silver",
                "fact_collateral_transaction",
                columns,
                current,
                run_id="new-run",
                now="2026-06-03T00:00:00+00:00",
            )

        self.assertEqual(result, {"missing": 0, "changed": 0, "archived": 0})
        ensure_archive.assert_not_called()
        connection_mock.cursor.assert_not_called()

    def test_preservation_archives_rows_missing_from_current_v2_source(self):
        columns = ["id", "transaction_identity_key", "doctor_name", "_silver_updated_at"]
        existing = [
            {
                "id": "old-1",
                "transaction_identity_key": "tx-key-1",
                "doctor_name": "Dr A",
                "_silver_updated_at": "2026-01-01T00:00:00+00:00",
            }
        ]
        cursor_context = MagicMock()
        cursor = MagicMock()
        cursor_context.__enter__.return_value = cursor

        with (
            patch.object(v2_reporting, "table_exists", return_value=True),
            patch.object(v2_reporting, "fetch_table", return_value=existing),
            patch.object(v2_reporting, "_ensure_preservation_archive_table") as ensure_archive,
            patch.object(v2_reporting, "connection") as connection_mock,
        ):
            connection_mock.cursor.return_value = cursor_context
            result = v2_reporting._archive_replaced_reporting_rows(
                "silver",
                "fact_collateral_transaction",
                columns,
                [],
                run_id="new-run",
                now="2026-06-03T00:00:00+00:00",
            )

        self.assertEqual(result, {"missing": 1, "changed": 0, "archived": 1})
        ensure_archive.assert_called_once()
        cursor.executemany.assert_called_once()
        archived_values = cursor.executemany.call_args.args[1][0]
        self.assertEqual(archived_values[0], "silver")
        self.assertEqual(archived_values[1], "fact_collateral_transaction")
        self.assertEqual(archived_values[5], "missing_from_current_v2_source")

    def test_bronze_compat_drop_skips_existing_tables(self):
        with (
            patch.object(v2_reporting, "ensure_schema"),
            patch.object(v2_reporting, "_bronze_relation_kind", return_value="r"),
            patch.object(v2_reporting, "execute") as execute_mock,
        ):
            v2_reporting._drop_bronze_views()

        execute_mock.assert_not_called()

    def test_bronze_compat_prepare_archives_existing_table_without_drop(self):
        with (
            patch.object(v2_reporting, "_bronze_relation_kind", return_value="r"),
            patch.object(v2_reporting, "_archive_existing_bronze_relation") as archive_mock,
            patch.object(v2_reporting, "execute") as execute_mock,
        ):
            v2_reporting._prepare_bronze_compat_relation("campaign_fieldrep")

        archive_mock.assert_called_once_with("campaign_fieldrep", "r")
        execute_mock.assert_not_called()

    def test_bronze_compat_archive_moves_table_without_delete(self):
        with (
            patch.object(v2_reporting, "ensure_schema"),
            patch.object(v2_reporting, "execute") as execute_mock,
        ):
            v2_reporting._archive_existing_bronze_relation("campaign_fieldrep", "r")

        executed_sql = "\n".join(call.args[0] for call in execute_mock.call_args_list)
        self.assertIn("ALTER TABLE", executed_sql)
        self.assertIn("RENAME TO", executed_sql)
        self.assertIn("SET SCHEMA", executed_sql)
        self.assertNotIn("DROP TABLE", executed_sql)

    def test_reporting_correction_rules_are_campaign_and_rep_scoped(self):
        keep_rule = v2_reporting.ReportingCorrectionRule(
            correction_id="rule-1",
            rule_type=v2_reporting.RULE_KEEP_DOCTOR_WITH_REP,
            system_name="inclinic",
            campaign_id="camp-1",
            doctor_phone="7086179396",
            doctor_phone_normalized="7086179396",
            doctor_name="SUMEET KR BAKALI",
            field_rep_brand_supplied_id="",
            expected_field_rep_brand_supplied_id="1451",
            affected_field_rep_brand_supplied_ids="2731",
            reason="brand correction",
            created_by="test",
        )
        invalid_phone_rule = v2_reporting.ReportingCorrectionRule(
            correction_id="rule-2",
            rule_type=v2_reporting.RULE_EXCLUDE_INVALID_PHONE,
            system_name="inclinic",
            campaign_id="camp-1",
            doctor_phone="964512884",
            doctor_phone_normalized="964512884",
            doctor_name="Dr.J Prakash",
            field_rep_brand_supplied_id="10340",
            expected_field_rep_brand_supplied_id="",
            affected_field_rep_brand_supplied_ids="",
            reason="invalid phone",
            created_by="test",
        )
        brand_by_rep_id = {"174": "2731", "175": "1451", "100": "10340"}

        wrong_duplicate_row = {
            "legacy_campaign_id": "camp-1",
            "campaign_fieldrep_id": "174",
            "brand_supplied_field_rep_id": "2731",
            "doctor_phone_normalized": "7086179396",
            "doctor_name_raw": "SUMEET KR BAKALI",
        }
        expected_duplicate_row = {**wrong_duplicate_row, "campaign_fieldrep_id": "175", "brand_supplied_field_rep_id": "1451"}

        self.assertTrue(v2_reporting._should_exclude_roster_row(wrong_duplicate_row, [keep_rule], brand_by_rep_id))
        self.assertFalse(v2_reporting._should_exclude_roster_row(expected_duplicate_row, [keep_rule], brand_by_rep_id))
        self.assertFalse(
            v2_reporting._should_exclude_roster_row(
                {**wrong_duplicate_row, "legacy_campaign_id": "other-camp"},
                [keep_rule],
                brand_by_rep_id,
            )
        )
        self.assertTrue(
            v2_reporting._should_exclude_activity_row(
                campaign_id="camp-1",
                rep_id="100",
                brand_id="10340",
                doctor_name="Dr.J Prakash",
                phone_values=["964512884"],
                rules=[invalid_phone_rule],
                brand_by_rep_id=brand_by_rep_id,
            )
        )
        self.assertFalse(
            v2_reporting._should_exclude_activity_row(
                campaign_id="camp-1",
                rep_id="100",
                brand_id="10340",
                doctor_name="Dr.J Prakash",
                phone_values=["9999999999"],
                rules=[invalid_phone_rule],
                brand_by_rep_id=brand_by_rep_id,
            )
        )

    def test_field_rep_insights_query_applies_active_reporting_correction_rules(self):
        captured: dict[str, str] = {}

        def fake_fetch(sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
            captured["sql"] = sql
            return []

        with patch("dashboard.views._table_exists", return_value=True), patch(
            "dashboard.views._optional_table_exists",
            return_value=False,
        ), patch("dashboard.views._fetch_dicts", side_effect=fake_fetch):
            dashboard.views._field_rep_insight_rows(
                "83ce7fc7c965433ab2b9717394abe3c1",
                ["83ce7fc7c965433ab2b9717394abe3c1"],
                [],
            )

        sql = captured["sql"]
        self.assertIn("active_reporting_correction_rules", sql)
        self.assertIn("ops.reporting_data_correction_rule", sql)
        self.assertIn("keep_doctor_with_field_rep", sql)
        self.assertIn("exclude_invalid_doctor_phone", sql)
        self.assertIn("rule.expected_field_rep_brand_supplied_key", sql)
        self.assertIn("rule.affected_field_rep_brand_supplied_ids", sql)
        self.assertIn("rule_corrected_activity AS", sql)
        self.assertIn("FROM activity_key_candidates akc", sql)
        self.assertIn("JOIN assigned_reps ar_rule", sql)
        self.assertIn("FROM rule_corrected_activity", sql)
        self.assertIn("transaction_doctor_lookup AS", sql)
        self.assertIn("rep_evidence_latest AS", sql)
        self.assertNotIn("FROM silver.fact_collateral_transaction tx\n                    WHERE", sql)

    def test_field_rep_insights_state_preserves_display_value_and_prefers_known_over_unknown(self):
        captured: dict[str, str] = {}

        def fake_fetch(sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
            captured["sql"] = sql
            return []

        with patch("dashboard.views._table_exists", return_value=True), patch(
            "dashboard.views._optional_table_exists",
            return_value=False,
        ), patch("dashboard.views._fetch_dicts", side_effect=fake_fetch):
            dashboard.views._field_rep_insight_rows(
                "83ce7fc7c965433ab2b9717394abe3c1",
                ["83ce7fc7c965433ab2b9717394abe3c1"],
                [],
                include_doctor_details=False,
            )

        sql = captured["sql"]
        self.assertIn("NULLIF(btrim(cfr.state), '')", sql)
        self.assertIn("FILTER (WHERE state_normalized <> 'UNKNOWN')", sql)
