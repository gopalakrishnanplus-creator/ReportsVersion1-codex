from __future__ import annotations

from unittest.mock import MagicMock, patch

from django.conf import settings
from django.test import RequestFactory, SimpleTestCase
from django.urls import resolve

import dashboard.views
from dashboard.internal_data_admin import (
    ColumnInfo,
    RAW_AUDIT_COLUMN_NAMES,
    RAW_DEDUPE_BATCH_SIZE,
    TableInfo,
    _batch_cleanup_confirmation_phrase,
    _cleanup_candidate_columns,
    _cleanup_inverse_match_condition,
    _raw_dedupe_confirmation_phrase,
    _raw_dedupe_report_snapshot,
    _raw_dedupe_validation,
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
        self.assertEqual(resolve("/_internal/data-admin/").view_name, "internal-data-admin-home")
        self.assertEqual(resolve("/_internal/data-admin/login/").view_name, "internal-data-admin-login")
        self.assertEqual(resolve("/_internal/data-admin/cleanup/").view_name, "internal-data-admin-cleanup")
        self.assertEqual(resolve("/_internal/data-admin/raw-downloads/").view_name, "internal-data-admin-raw-downloads")
        self.assertEqual(resolve("/_internal/data-admin/raw-dedupe/").view_name, "internal-data-admin-raw-dedupe")
        self.assertEqual(
            resolve("/_internal/data-admin/raw-downloads/raw_server1/campaign_fieldrep/download/").view_name,
            "internal-data-admin-raw-download",
        )
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

    def test_raw_download_helpers_only_include_raw_source_columns(self):
        columns = [
            ColumnInfo("id", "text", False, 1, None, False, False),
            ColumnInfo("campaign_id", "text", True, 2, None, False, False),
            ColumnInfo("_record_hash", "text", True, 3, None, False, False),
            ColumnInfo("_ingested_at", "text", True, 4, None, False, False),
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


class DashboardAccessViewTests(SimpleTestCase):
    def test_engagement_health_uses_actual_campaign_denominator(self):
        score = dashboard.views._engagement_health_score(reached=5, opened=5, consumed=5, total_doctors=1000)
        self.assertAlmostEqual(score, 66.8, places=1)

    def test_weekly_health_uses_four_week_base_and_caps_reach(self):
        score = dashboard.views._weekly_engagement_health_score(reached=80, opened=40, consumed=20, total_doctors=100)
        self.assertAlmostEqual(score, 66.7, places=1)

    def test_field_rep_insights_select_brand_supplied_rep_id_for_display(self):
        with patch("dashboard.views._fetch_dicts", return_value=[]) as fetch_mock:
            dashboard.views._field_rep_insight_rows("brand-1", ["brand-1"], ["9"])

        sql = fetch_mock.call_args.args[0]
        self.assertIn("brand_supplied_field_rep_id", sql)
        self.assertIn("field_rep_display_id", sql)
        self.assertIn("AS field_rep_id", sql)
        self.assertNotIn("d.source", sql)
        self.assertNotIn("CASE WHEN COALESCE(ad.total_doctors_assigned, 0) > 0", sql)
        self.assertIn("activity_for_rep AS", sql)
        self.assertNotIn("GREATEST(", sql)
        self.assertIn("assignment_note", sql)
        self.assertIn("raw_assigned_reps AS", sql)
        self.assertIn("GROUP BY field_rep_id", sql)
        self.assertIn("auth_user_id_key", sql)
        self.assertIn("share_rep_id_email_map AS", sql)
        self.assertIn("activity_key_candidates AS", sql)
        self.assertIn("matched_activity AS", sql)
        self.assertIn("assigned_doctor_rows AS", sql)
        self.assertIn("assigned_doctors_json", sql)
        self.assertIn("linked_share.field_rep_email", sql)
        self.assertIn("'email'::text AS key_type", sql)
        self.assertNotIn("canonical_activity_rep AS", sql)
        self.assertIn("COALESCE(NULLIF(tx.doctor_phone_normalized, ''), tx.doctor_identity_key) AS doctor_key", sql)
        self.assertIn("COALESCE(NULLIF(s.doctor_identifier_normalized, ''), s.doctor_identity_key) AS doctor_key", sql)
        self.assertIn("COUNT(DISTINCT doctor_key)", sql)
        self.assertIn("lower(COALESCE(tx.pdf_completed", sql)
        self.assertNotIn("tx.pdf_completed_flag", sql)

    def test_state_attention_uses_rep_aliases_and_effective_reach(self):
        latest_week = {"week_start_date": "2026-04-10", "week_end_date": "2026-04-16"}
        with patch("dashboard.views._optional_table_exists", return_value=True), patch(
            "dashboard.views._fetch_dicts",
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
        self.assertIn("lower(btrim(d.state_normalized)) IN ('null', 'none', 'unknown')", sql)
        self.assertIn("lower(btrim(base.state_normalized)) IN ('null', 'none', 'unknown')", sql)
        self.assertNotIn("state_normalized <> 'UNKNOWN'", sql)
        self.assertNotIn("total_state,0)/4.0", sql)

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
                    "assignment_note": "Hidden diagnostic note",
                }
            ],
            "old_collaterals": [
                {
                    "collateral_id": "11",
                    "name": "Older Collateral",
                    "schedule_text": "Apr 01, 2026 - Apr 09, 2026",
                    "url": "/campaign/demo/collateral/11/field-rep-insights/",
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
        self.assertContains(response, "Old Collaterals")
        self.assertContains(response, "/campaign/demo/collateral/11/field-rep-insights/")
        self.assertContains(response, "/campaign/demo/states/")
        self.assertContains(response, "Asha Mehta")
        self.assertContains(response, "doctor-count-btn")
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
        self.assertContains(response, "Assigned Doctors")
        self.assertContains(response, "12")
        self.assertContains(response, "page-loading")
        self.assertNotContains(response, "Sent 12")
