from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.contrib import messages
from django.core import signing
from django.db import DatabaseError, connection, transaction
from django.http import Http404, HttpRequest, HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.crypto import constant_time_compare
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods
from psycopg2 import sql

from etl.reporting_corrections import (
    RULE_EXCLUDE_INVALID_PHONE,
    RULE_KEEP_DOCTOR_WITH_REP,
    create_reporting_correction_rule,
    deactivate_reporting_correction_rule,
    ensure_reporting_correction_table,
    list_reporting_correction_rules,
)
from etl.reporting_privacy import (
    create_raw_visibility_rule,
    create_campaign_privacy_allowlist_rule,
    create_person_privacy_rule,
    deactivate_raw_visibility_rule,
    deactivate_campaign_privacy_allowlist_rule,
    deactivate_person_privacy_rule,
    ensure_campaign_privacy_table,
    list_raw_visibility_rules,
    list_raw_visibility_table_options,
    list_campaign_privacy_allowlist_rules,
    list_person_privacy_rules,
)
from etl.sapa_growth.specs import MYSQL_TABLE_SPECS
from etl.weekly_transfer_cleanup import INCLINIC_SPECS, RFA_SPECS, ensure_cleanup_tables, run_weekly_transfer_cleanup


SESSION_KEY = "internal_data_admin_authenticated"
SESSION_USER_KEY = "internal_data_admin_username"
ROW_TOKEN_SALT = "dashboard.internal-data-admin.row"
PAGE_SIZE = 75
RAW_EXPORT_BATCH_SIZE = 1000
AUDIT_SCHEMA = "ops"
AUDIT_TABLE = "internal_dashboard_audit"
RAW_DEDUPE_ARCHIVE_TABLE = "raw_duplicate_archive"
RAW_DEDUPE_CONFIRM_PREFIX = "ARCHIVE RAW DUPLICATES"
RAW_DEDUPE_BATCH_SIZE = 20000
TRANSFER_CLEANUP_CONFIRMATIONS = {
    "inclinic": "RUN INCLINIC TRANSFER CLEANUP",
    "rfa": "RUN RFA TRANSFER CLEANUP",
    "all": "RUN INCLINIC AND RFA TRANSFER CLEANUP",
}
RFA_TRANSFER_CLEANUP_CONFIRMATION = TRANSFER_CLEANUP_CONFIRMATIONS["rfa"]
TRANSFER_CLEANUP_PAGE_SIZE = 100
RFA_TRANSFER_CLEANUP_PAGE_SIZE = TRANSFER_CLEANUP_PAGE_SIZE
TRANSFER_CLEANUP_STATUS_OPTIONS = (
    {"value": "all", "label": "All cleanup statuses"},
    {"value": "never_cleaned", "label": "Not attempted yet"},
    {"value": "PENDING", "label": "Pending"},
    {"value": "DELETED", "label": "Deleted from source"},
    {"value": "ALREADY_ABSENT", "label": "Already absent from source"},
    {"value": "GUARD_BLOCKED", "label": "Guard blocked"},
    {"value": "FAILED", "label": "Failed"},
)
RFA_TRANSFER_CLEANUP_STATUS_OPTIONS = TRANSFER_CLEANUP_STATUS_OPTIONS
TRANSFER_CLEANUP_DOMAIN_OPTIONS = (
    {"value": "all", "label": "InClinic + RFA/SAPA"},
    {"value": "inclinic", "label": "InClinic only"},
    {"value": "rfa", "label": "RFA/SAPA only"},
)
TRANSFER_CLEANUP_DOMAIN_LABELS = {
    "all": "InClinic + RFA/SAPA",
    "inclinic": "InClinic",
    "rfa": "RFA/SAPA",
}
TRANSFER_CLEANUP_DOMAIN_NOTES = {
    "inclinic": "Deletes only transferred collateral transaction/activity rows from InClinic source tables.",
    "rfa": "Deletes only transferred RFA V2 activity rows plus patient submission and submission-red-flag rows from SAPA/RFA source tables.",
}
INCLINIC_TRANSFER_CLEANUP_ALLOWED_SOURCE_TABLES = frozenset(spec.source_table for spec in INCLINIC_SPECS)
INCLINIC_TRANSFER_CLEANUP_ALLOWED_RAW_TABLES = frozenset((spec.raw_schema, spec.raw_table) for spec in INCLINIC_SPECS)
INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA = "raw_v2_inclinic"
INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE = "inclinic_collateral_transaction_v2"
INCLINIC_TRANSFER_CLEANUP_V1_SOURCE_TABLE = "sharing_management_collateraltransaction"
INCLINIC_TRANSFER_CLEANUP_ACTIVITY_TYPES = {
    "sharing_management_collateraltransaction": "inclinic_collateral_transaction",
    "inclinic_collateral_transaction_v2": "inclinic_collateral_transaction_v2",
}
RFA_TRANSFER_CLEANUP_ALLOWED_SOURCE_TABLES = frozenset(spec.source_table for spec in RFA_SPECS)
RFA_TRANSFER_CLEANUP_ALLOWED_RAW_TABLES = frozenset((spec.raw_schema, spec.raw_table) for spec in RFA_SPECS)
RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA = "raw_sapa_mysql"
RFA_TRANSFER_CLEANUP_V2_RAW_TABLE = "rfa_activity_event_raw"
RFA_TRANSFER_CLEANUP_V2_SOURCE_TABLE = "rfa_activity_event_v2"
RFA_TRANSFER_CLEANUP_ACTIVITY_TYPES = {
    "rfa_activity_event_v2": "all_rfa_activity_events",
    "redflags_patientsubmission": "rfa_patient_submission",
    "gnd_gndpatientsubmission": "gnd_patient_submission",
    "redflags_submissionredflag": "rfa_submission_red_flag",
    "gnd_gndsubmissionredflag": "gnd_submission_red_flag",
}
RFA_TRANSFER_CLEANUP_DISPLAY_FIELDS = (
    "patient_id",
    "doctor_id",
    "form_id",
    "overall_flag_code",
    "submission_id",
    "red_flag_id",
)
RAW_AUDIT_COLUMN_NAMES = frozenset(
    {
        "_ingestion_run_id",
        "_ingested_at",
        "_source_server",
        "_source_system",
        "_source_table",
        "_extract_started_at",
        "_extract_ended_at",
        "_record_hash",
        "_source_payload_hash",
        "_is_deleted",
        "_dq_status",
        "_dq_errors",
    }
)
LAYER_ORDER = {
    "raw": 1,
    "bronze": 2,
    "silver": 3,
    "gold": 4,
}
CLEANUP_SYSTEM_KEYS = ("inclinic", "sapa", "pe")
CLEANUP_LAYER_OPTIONS = [
    {"key": "raw", "label": "RAW and everything downstream"},
    {"key": "bronze", "label": "BRONZE, SILVER, and GOLD"},
    {"key": "silver", "label": "SILVER and GOLD"},
    {"key": "gold", "label": "GOLD only"},
]
CLEANUP_BATCH_MODES = {
    "delete_listed": {
        "label": "Delete only listed campaigns",
        "summary": "Listed campaign IDs are deleted across the selected systems; everything else is kept.",
    },
    "keep_listed": {
        "label": "Keep listed campaigns, delete the rest",
        "summary": "Listed campaign IDs are protected; other campaign-scoped records are deleted across the selected systems.",
    },
}
REPORTING_CORRECTION_RULE_TYPES = [
    {
        "key": RULE_KEEP_DOCTOR_WITH_REP,
        "label": "Keep doctor with one ASM",
        "summary": "Use for duplicate assignment. Keeps the listed doctor phone with the expected brand-supplied Field Rep ID and excludes other reps from assigned-doctor denominator.",
    },
    {
        "key": RULE_EXCLUDE_INVALID_PHONE,
        "label": "Exclude invalid doctor phone",
        "summary": "Use for wrong doctor numbers. Excludes matching roster and engagement rows from reporting counts without deleting source/raw data.",
    },
]
LAYER_LABELS = {
    "raw": "RAW",
    "bronze": "BRONZE",
    "silver": "SILVER",
    "gold": "GOLD",
}
REGISTRY_TABLES = {
    "inclinic": [("gold_global", "campaign_registry")],
    "sapa": [("gold_sapa", "campaign_registry"), ("gold_sapa", "dim_campaign")],
    "pe": [("gold_pe_global", "campaign_registry")],
}
REGISTRY_MATCH_COLUMNS = [
    "brand_campaign_id",
    "campaign_id",
    "campaign_id_resolved",
    "pe_campaign_id",
    "rfa_campaign_id",
    "source_campaign_id",
    "master_campaign_id",
    "campaign_identifier",
    "campaign_key",
    "gold_schema_name",
    "schema_name",
    "campaign_schema",
]
REGISTRY_SCHEMA_COLUMNS = {"gold_schema_name", "schema_name", "campaign_schema"}
CLEANUP_MATCH_COLUMNS = [
    "brand_campaign_id",
    "campaign_id",
    "campaign_id_resolved",
    "pe_campaign_id",
    "rfa_campaign_id",
    "source_campaign_id",
    "master_campaign_id",
    "campaign_identifier",
    "campaign_key",
    "campaign_code",
    "campaign_uuid",
]


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    default: str | None
    is_identity: bool
    is_generated: bool


@dataclass(frozen=True)
class TableInfo:
    schema: str
    name: str
    columns: list[ColumnInfo]
    primary_key: list[str]


@dataclass(frozen=True)
class SystemProfile:
    key: str
    label: str
    short_label: str
    description: str
    cleanup_summary: str
    source_guidance: str
    cleanup_steps: list[str]


class _CsvEcho:
    def write(self, value: str) -> str:
        return value


SYSTEM_PROFILES = {
    "inclinic": SystemProfile(
        key="inclinic",
        label="Inclinic Reporting",
        short_label="Inclinic",
        description="In-clinic sharing campaign data from MySQL source copies through RAW, BRONZE, SILVER, and campaign GOLD schemas.",
        cleanup_summary="Usually delete source/raw rows first, then rerun Inclinic ETL. Do not chase every derived row unless you are clearing stale report output before a rebuild.",
        source_guidance="Start with raw_v2_inclinic/raw_v2_master for current campaign, collateral, share, transaction, doctor, and field-rep records; use raw_server1/raw_server2 for legacy source copies.",
        cleanup_steps=[
            "Remove campaign identity rows from raw_server2.campaign_management_campaign and raw_server1.campaign_campaign.",
            "Remove engagement rows from raw_server2.sharing_management_collateraltransaction and raw_server2.sharing_management_sharelog.",
            "Remove campaign collateral bridge rows before collateral rows when the collateral belongs only to that dummy campaign.",
            "Rerun the Inclinic ETL so bronze, silver, gold_global, and gold_campaign_* rebuild from the cleaned source state.",
        ],
    ),
    "sapa": SystemProfile(
        key="sapa",
        label="SAPA / RFA",
        short_label="SAPA/RFA",
        description="Red Flag Alert and SAPA Growth data across raw_sapa_*, bronze_sapa, silver_sapa, and gold_sapa reporting tables.",
        cleanup_summary="Prefer deleting seeded/source rows from raw_sapa_mysql or raw_sapa_api and then rerunning the SAPA pipeline.",
        source_guidance="Use raw_sapa_mysql for campaign, doctor, clinic, screening, follow-up, reminder, and metric-event records; use raw_sapa_api for WordPress webinar/course/video fixture rows.",
        cleanup_steps=[
            "Delete test doctors, clinic mappings, and campaign rows from raw_sapa_mysql before derived SAPA tables.",
            "Delete matching WordPress fixture records from raw_sapa_api when course/webinar/video data was test-only.",
            "Rerun the SAPA/RFA ETL so bronze_sapa, silver_sapa, and gold_sapa are republished consistently.",
            "Delete directly from gold_sapa only when you need to clear stale dashboard output before a full rebuild.",
        ],
    ),
    "pe": SystemProfile(
        key="pe",
        label="Patient Education",
        short_label="PE",
        description="Patient Education campaign data across raw_pe_*, bronze_pe, silver_pe, gold_pe_global, and gold_pe_campaign_* schemas.",
        cleanup_summary="Prefer deleting raw PE campaign/share/playback rows and rerunning PE ETL; derived PE schemas should normally be rebuilt.",
        source_guidance="Use raw_pe_master for campaign, enrollment, doctor, brand, field-rep, trigger, and catalog records; use raw_pe_portal and raw_v2_pe_portal for share, playback, banner-click, and V2 portal activity.",
        cleanup_steps=[
            "Remove campaign and enrollment records from raw_pe_master when retiring a dummy PE campaign.",
            "Remove share, playback, and banner-click activity from raw_pe_portal for that test campaign.",
            "Only delete catalog/video/bundle records when they were created exclusively for the dummy campaign.",
            "Rerun PE ETL so bronze_pe, silver_pe, gold_pe_global, and gold_pe_campaign_* are rebuilt safely.",
        ],
    ),
    "shared": SystemProfile(
        key="shared",
        label="Shared Ops / Control",
        short_label="Shared",
        description="Operational and preservation tables used by multiple pipelines, including ETL run logs, rules, dashboard audit records, and archived pre-V2 reporting rows.",
        cleanup_summary="These tables control, describe, or preserve pipeline behavior. Clean logs and rules deliberately; do not delete audit or preservation history casually.",
        source_guidance="Use control tables for run-log cleanup, ops tables for rules/configuration cleanup, and archive tables only for verified historical preservation review.",
        cleanup_steps=[
            "Delete old control.etl_run_log rows only when historical troubleshooting data is no longer needed.",
            "Delete or disable ops rules only after confirming the relevant pipeline no longer depends on them.",
            "Do not delete archive.reporting_v2_preserved_rows unless the archived rows have been exported or formally accepted as no longer needed.",
            "The internal dashboard audit table is hidden from CRUD to preserve mutation history.",
        ],
    ),
}


def _admin_config() -> dict[str, str]:
    return getattr(settings, "INTERNAL_DATA_ADMIN", {})


def _is_authenticated(request: HttpRequest) -> bool:
    return bool(request.session.get(SESSION_KEY))


def _require_auth(request: HttpRequest):
    if not _is_authenticated(request):
        return redirect(f"{reverse('internal-data-admin-login')}?next={request.get_full_path()}")
    return None


def _fetch_dicts(query, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _execute(query, params: list[Any] | tuple[Any, ...] | None = None) -> None:
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])


def _is_relevant_schema(schema: str) -> bool:
    if schema in {"raw_server1", "raw_server2", "bronze", "silver", "gold_global", "control", "ops", "archive"}:
        return True
    return schema.startswith(("raw_", "bronze_", "silver_", "gold_"))


def _is_managed_table(schema: str, table: str) -> bool:
    return _is_relevant_schema(schema) and not (schema == AUDIT_SCHEMA and table == AUDIT_TABLE)


def _system_key_for_schema(schema: str) -> str:
    if schema in {"raw_server1", "raw_server2", "raw_v2_master", "raw_v2_inclinic", "bronze", "silver", "gold_global"} or schema.startswith("gold_campaign_"):
        return "inclinic"
    if schema in {"raw_sapa_mysql", "raw_sapa_api", "bronze_sapa", "silver_sapa", "gold_sapa", "gold_sapa_stage"}:
        return "sapa"
    if schema in {"raw_pe_master", "raw_pe_portal", "raw_v2_pe_portal", "bronze_pe", "silver_pe", "gold_pe_global"} or schema.startswith("gold_pe_campaign_"):
        return "pe"
    if schema in {"control", "ops", "archive"}:
        return "shared"
    return "shared"


def _system_profile_for_schema(schema: str) -> SystemProfile:
    return SYSTEM_PROFILES[_system_key_for_schema(schema)]


def _layer_for_schema(schema: str) -> str:
    if schema.startswith("raw_") or schema in {"raw_server1", "raw_server2"}:
        return "RAW source copy"
    if schema.startswith("bronze") or schema == "bronze":
        return "BRONZE derived"
    if schema.startswith("silver") or schema == "silver":
        return "SILVER derived"
    if schema.startswith("gold"):
        return "GOLD report output"
    if schema == "control":
        return "CONTROL run metadata"
    if schema == "ops":
        return "OPS rules/config"
    if schema == "archive":
        return "ARCHIVE historical preservation"
    return "Reporting table"


def _layer_key_for_schema(schema: str) -> str:
    if schema.startswith("raw_") or schema in {"raw_server1", "raw_server2"}:
        return "raw"
    if schema.startswith("bronze") or schema == "bronze":
        return "bronze"
    if schema.startswith("silver") or schema == "silver":
        return "silver"
    if schema.startswith("gold"):
        return "gold"
    return "other"


def _table_cleanup_note(info: TableInfo) -> str:
    layer = _layer_for_schema(info.schema)
    if layer == "RAW source copy":
        return "Best cleanup starting point. Delete dummy/test/source-removed records here, then rerun the matching ETL so derived layers regenerate."
    if layer in {"BRONZE derived", "SILVER derived", "GOLD report output"}:
        return "Derived table. You usually do not need to delete here table by table; clean RAW/source rows and rerun ETL. Direct delete is for emergency stale or corrupt derived output only."
    if info.schema == "control":
        return "Run metadata. Safe to prune old logs when no longer needed, but this does not remove campaign data."
    if info.schema == "ops":
        return "Operational rules/configuration. Deleting here can change future pipeline behavior, so use a clear reason."
    return "Review dependencies before changing this table."


def _ensure_audit_table() -> None:
    _execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(AUDIT_SCHEMA)))
    _execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                action TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_locator JSONB,
                before_data JSONB,
                after_data JSONB,
                reason TEXT,
                actor TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(AUDIT_TABLE))
    )


def _table_exists(schema: str, table: str) -> bool:
    rows = _fetch_dicts(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
          AND table_type = 'BASE TABLE'
        LIMIT 1
        """,
        [schema, table],
    )
    return bool(rows)


def _table_columns(schema: str, table: str) -> set[str]:
    rows = _fetch_dicts(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        [schema, table],
    )
    return {row["column_name"] for row in rows}


def _transfer_cleanup_domains(selected_domain: str) -> list[str]:
    cleaned = (selected_domain or "all").strip().lower()
    if cleaned == "all":
        return ["inclinic", "rfa"]
    if cleaned in {"inclinic", "rfa"}:
        return [cleaned]
    raise ValueError("Choose InClinic, RFA/SAPA, or both before reviewing transfer cleanup.")


def _transfer_cleanup_domain_options(selected_domain: str) -> list[dict[str, str | bool]]:
    cleaned = (selected_domain or "all").strip().lower()
    return [{**option, "is_selected": option["value"] == cleaned} for option in TRANSFER_CLEANUP_DOMAIN_OPTIONS]


def _transfer_cleanup_specs(selected_domain: str, selected_source_table: str = "all") -> tuple[Any, ...]:
    domains = _transfer_cleanup_domains(selected_domain)
    specs = [spec for spec in (*INCLINIC_SPECS, *RFA_SPECS) if spec.domain in domains]
    selected = (selected_source_table or "all").strip()
    if selected != "all":
        specs = [spec for spec in specs if spec.source_table == selected]
    if not specs:
        raise ValueError("Select one of the approved transfer cleanup source tables.")
    return tuple(specs)


def _transfer_cleanup_table_options(selected_domain: str, selected_source_table: str = "all") -> list[dict[str, Any]]:
    selected = (selected_source_table or "all").strip()
    options = [
        {
            "value": "all",
            "label": "All approved transfer tables",
            "is_selected": selected == "all",
        }
    ]
    for spec in _transfer_cleanup_specs(selected_domain, "all"):
        label = f"{TRANSFER_CLEANUP_DOMAIN_LABELS.get(spec.domain, spec.domain)} / {spec.source_table}"
        options.append(
            {
                "value": spec.source_table,
                "label": label,
                "raw_table": f"{spec.raw_schema}.{spec.raw_table}",
                "key_column": spec.key_column,
                "is_selected": selected == spec.source_table,
            }
        )
    return options


def _transfer_cleanup_status_options(selected_cleanup_status: str) -> list[dict[str, str | bool]]:
    cleaned = (selected_cleanup_status or "all").strip()
    return [{**option, "is_selected": option["value"] == cleaned} for option in TRANSFER_CLEANUP_STATUS_OPTIONS]


def _transfer_cleanup_assert_scope() -> None:
    inclinic_source_tables = {spec.source_table for spec in INCLINIC_SPECS}
    inclinic_raw_tables = {(spec.raw_schema, spec.raw_table) for spec in INCLINIC_SPECS}
    expected_inclinic_source_tables = {
        "sharing_management_collateraltransaction",
        "inclinic_collateral_transaction_v2",
    }
    expected_inclinic_raw_tables = {
        ("raw_server2", "sharing_management_collateraltransaction"),
        ("raw_v2_inclinic", "inclinic_collateral_transaction_v2"),
    }
    if inclinic_source_tables != expected_inclinic_source_tables or inclinic_raw_tables != expected_inclinic_raw_tables:
        raise RuntimeError("InClinic transfer cleanup scope changed; source deletion is blocked until reviewed.")
    _rfa_transfer_cleanup_assert_scope()


def _transfer_cleanup_confirmation(selected_domain: str) -> str:
    cleaned = (selected_domain or "all").strip().lower()
    return TRANSFER_CLEANUP_CONFIRMATIONS.get(cleaned, TRANSFER_CLEANUP_CONFIRMATIONS["all"])


def _cleanup_counts_for_source(domain: str, source_table: str) -> dict[str, int]:
    status_rows = _fetch_dicts(
        """
        SELECT delete_status, COUNT(*)::text AS row_count
        FROM control.transfer_cleanup_manifest
        WHERE domain = %s
          AND source_table = %s
        GROUP BY delete_status
        """,
        [domain, source_table],
    )
    return {item["delete_status"] or "UNKNOWN": int(item["row_count"] or 0) for item in status_rows}


def _latest_transfer_cleanup(domains: list[str]) -> dict[str, Any] | None:
    rows = _fetch_dicts(
        """
        SELECT cleanup_run_id, status, started_at, ended_at
        FROM control.transfer_cleanup_run_log
        ORDER BY started_at DESC
        LIMIT 25
        """
    )
    domain_set = set(domains)
    for row in rows:
        row_domains = {item.strip() for item in (row.get("domains") or "").split(",") if item.strip()}
        if row_domains & domain_set:
            return row
    return None


def _sql_first_text_field(columns: set[str], *field_names: str) -> sql.Composed:
    expressions = [
        sql.SQL("NULLIF({field}::text, '')").format(field=sql.Identifier(field_name))
        for field_name in field_names
        if field_name in columns
    ]
    if not expressions:
        return sql.SQL("''::text")
    return sql.SQL("COALESCE(") + sql.SQL(", ").join(expressions) + sql.SQL(", ''::text)")


def _inclinic_transfer_cleanup_summary_for_legacy(spec: Any) -> dict[str, Any]:
    row = {
        "domain": "inclinic",
        "domain_label": TRANSFER_CLEANUP_DOMAIN_LABELS["inclinic"],
        "source_table": spec.source_table,
        "activity_type": INCLINIC_TRANSFER_CLEANUP_ACTIVITY_TYPES.get(spec.source_table, ""),
        "raw_table": "raw_server2.sharing_management_collateraltransaction + raw_v2_inclinic.inclinic_collateral_transaction_v2",
        "legacy_raw_table": "raw_server2.sharing_management_collateraltransaction",
        "key_column": "id / source_pk_value",
        "source_pk_column": "id",
        "reporting_source": "V1 RAW + V2 transaction traceability",
        "total_rows": 0,
        "unique_keys": 0,
        "latest_ingested_at": "",
        "latest_ingestion_run_id": "",
        "cleanup_counts": _cleanup_counts_for_source("inclinic", spec.source_table),
        "error": "",
    }
    parts: list[sql.Composed] = []
    params: list[Any] = []
    legacy_columns = _table_columns(spec.raw_schema, spec.raw_table) if _table_exists(spec.raw_schema, spec.raw_table) else set()
    if spec.key_column in legacy_columns:
        parts.append(
            sql.SQL(
                """
                SELECT {key_column}::text AS source_pk,
                       COALESCE("_ingested_at"::text, '') AS latest_at,
                       COALESCE("_ingestion_run_id"::text, '') AS run_id
                FROM {schema}.{table}
                WHERE COALESCE({key_column}::text, '') <> ''
                """
            ).format(
                key_column=sql.Identifier(spec.key_column),
                schema=sql.Identifier(spec.raw_schema),
                table=sql.Identifier(spec.raw_table),
            )
        )
    v2_columns = _table_columns(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA, INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE) if _table_exists(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA, INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE) else set()
    if {"source_table", "source_pk_value"}.issubset(v2_columns):
        latest_expr = _sql_first_text_field(v2_columns, "source_updated_at", "migrated_at", "source_created_at", "old_updated_at", "old_created_at")
        parts.append(
            sql.SQL(
                """
                SELECT "source_pk_value"::text AS source_pk,
                       {latest_expr} AS latest_at,
                       ''::text AS run_id
                FROM {schema}.{table}
                WHERE "source_table" = %s
                  AND COALESCE("source_pk_value"::text, '') <> ''
                """
            ).format(
                latest_expr=latest_expr,
                schema=sql.Identifier(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA),
                table=sql.Identifier(INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE),
            )
        )
        params.append(INCLINIC_TRANSFER_CLEANUP_V1_SOURCE_TABLE)
    if not parts:
        row["error"] = "No V1 RAW or V2 transaction traceability table is available yet."
        return row
    summary_rows = _fetch_dicts(
        sql.SQL(
            """
            SELECT
                COUNT(*)::text AS total_rows,
                COUNT(DISTINCT NULLIF(source_pk, ''))::text AS unique_keys,
                COALESCE(MAX(latest_at), '') AS latest_ingested_at,
                COALESCE(MAX(NULLIF(run_id, '')), '') AS latest_ingestion_run_id
            FROM ({source_rows}) evidence
            """
        ).format(source_rows=sql.SQL(" UNION ALL ").join(parts)),
        params,
    )
    if summary_rows:
        row["total_rows"] = int(summary_rows[0].get("total_rows") or 0)
        row["unique_keys"] = int(summary_rows[0].get("unique_keys") or 0)
        row["latest_ingested_at"] = summary_rows[0].get("latest_ingested_at") or ""
        row["latest_ingestion_run_id"] = summary_rows[0].get("latest_ingestion_run_id") or ""
    return row


def _inclinic_transfer_cleanup_summary_for_v2(spec: Any) -> dict[str, Any]:
    row = {
        "domain": "inclinic",
        "domain_label": TRANSFER_CLEANUP_DOMAIN_LABELS["inclinic"],
        "source_table": spec.source_table,
        "activity_type": INCLINIC_TRANSFER_CLEANUP_ACTIVITY_TYPES.get(spec.source_table, ""),
        "raw_table": f"{spec.raw_schema}.{spec.raw_table}",
        "legacy_raw_table": "",
        "key_column": spec.key_column,
        "source_pk_column": spec.key_column,
        "reporting_source": "V2 transaction table",
        "total_rows": 0,
        "unique_keys": 0,
        "latest_ingested_at": "",
        "latest_ingestion_run_id": "",
        "cleanup_counts": _cleanup_counts_for_source("inclinic", spec.source_table),
        "error": "",
    }
    columns = _table_columns(spec.raw_schema, spec.raw_table) if _table_exists(spec.raw_schema, spec.raw_table) else set()
    if spec.key_column not in columns:
        row["error"] = "V2 transaction reporting table is not available yet."
        return row
    latest_expr = _sql_first_text_field(columns, "source_updated_at", "migrated_at", "source_created_at", "old_updated_at", "old_created_at")
    count_rows = _fetch_dicts(
        sql.SQL(
            """
            SELECT
                COUNT(*)::text AS total_rows,
                COUNT(DISTINCT NULLIF({key_column}::text, ''))::text AS unique_keys,
                COALESCE(MAX({latest_expr}), '') AS latest_ingested_at
            FROM {schema}.{table}
            WHERE COALESCE({key_column}::text, '') <> ''
            """
        ).format(
            key_column=sql.Identifier(spec.key_column),
            latest_expr=latest_expr,
            schema=sql.Identifier(spec.raw_schema),
            table=sql.Identifier(spec.raw_table),
        )
    )
    if count_rows:
        row["total_rows"] = int(count_rows[0].get("total_rows") or 0)
        row["unique_keys"] = int(count_rows[0].get("unique_keys") or 0)
        row["latest_ingested_at"] = count_rows[0].get("latest_ingested_at") or ""
    return row


def _inclinic_transfer_cleanup_raw_summary() -> dict[str, Any]:
    ensure_cleanup_tables()
    rows = []
    total_rows = 0
    total_unique_keys = 0
    for spec in INCLINIC_SPECS:
        if spec.source_table == INCLINIC_TRANSFER_CLEANUP_V1_SOURCE_TABLE:
            row = _inclinic_transfer_cleanup_summary_for_legacy(spec)
        else:
            row = _inclinic_transfer_cleanup_summary_for_v2(spec)
        rows.append(row)
        total_rows += int(row.get("total_rows") or 0)
        total_unique_keys += int(row.get("unique_keys") or 0)
    return {
        "rows": rows,
        "table_count": len(rows),
        "total_rows": total_rows,
        "total_unique_keys": total_unique_keys,
        "latest_cleanup": _latest_transfer_cleanup(["inclinic"]),
    }


def _rfa_transfer_cleanup_assert_scope() -> None:
    source_tables = {spec.source_table for spec in RFA_SPECS}
    raw_tables = {(spec.raw_schema, spec.raw_table) for spec in RFA_SPECS}
    expected_source_tables = {
        "rfa_activity_event_v2",
        "redflags_patientsubmission",
        "gnd_gndpatientsubmission",
        "redflags_submissionredflag",
        "gnd_gndsubmissionredflag",
    }
    expected_raw_tables = {
        ("raw_sapa_mysql", "rfa_activity_event_raw"),
        ("raw_sapa_mysql", "redflags_patientsubmission_raw"),
        ("raw_sapa_mysql", "gnd_gndpatientsubmission_raw"),
        ("raw_sapa_mysql", "redflags_submissionredflag_raw"),
        ("raw_sapa_mysql", "gnd_gndsubmissionredflag_raw"),
    }
    if source_tables != expected_source_tables or raw_tables != expected_raw_tables:
        raise RuntimeError("RFA transfer cleanup scope changed; source deletion is blocked until reviewed.")


def _rfa_transfer_cleanup_table_options(selected_source_table: str = "all") -> list[dict[str, Any]]:
    options = [
        {
            "value": "all",
            "label": "All approved RFA transfer tables",
            "is_selected": selected_source_table == "all",
        }
    ]
    for spec in RFA_SPECS:
        options.append(
            {
                "value": spec.source_table,
                "label": spec.source_table,
                "raw_table": f"{spec.raw_schema}.{spec.raw_table}",
                "key_column": spec.key_column,
                "is_selected": selected_source_table == spec.source_table,
            }
        )
    return options


def _rfa_transfer_cleanup_source_columns(spec: Any) -> set[str]:
    source_spec = MYSQL_TABLE_SPECS.get(spec.source_table)
    return set(source_spec.columns if source_spec else ())


def _rfa_transfer_cleanup_v2_available() -> bool:
    return _table_exists(RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA, RFA_TRANSFER_CLEANUP_V2_RAW_TABLE)


def _rfa_transfer_cleanup_v2_columns() -> set[str]:
    if not _rfa_transfer_cleanup_v2_available():
        return set()
    return _table_columns(RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA, RFA_TRANSFER_CLEANUP_V2_RAW_TABLE)


def _rfa_transfer_cleanup_v2_usable() -> bool:
    columns = _rfa_transfer_cleanup_v2_columns()
    return {"source_table", "source_pk_value"}.issubset(columns)


def _rfa_transfer_cleanup_selected_specs(source_table: str) -> tuple[Any, ...]:
    selected = (source_table or "all").strip()
    if selected == "all":
        return RFA_SPECS
    matches = tuple(spec for spec in RFA_SPECS if spec.source_table == selected)
    if not matches:
        raise ValueError("Select one of the approved RFA transfer tables.")
    return matches


def _rfa_transfer_cleanup_raw_summary() -> dict[str, Any]:
    ensure_cleanup_tables()
    rows: list[dict[str, Any]] = []
    total_rows = 0
    total_unique_keys = 0
    use_v2 = _rfa_transfer_cleanup_v2_usable()
    v2_columns = _rfa_transfer_cleanup_v2_columns() if use_v2 else set()
    for spec in RFA_SPECS:
        is_activity_event_v2 = spec.source_table == RFA_TRANSFER_CLEANUP_V2_SOURCE_TABLE
        row = {
            "domain": "rfa",
            "domain_label": TRANSFER_CLEANUP_DOMAIN_LABELS["rfa"],
            "source_table": spec.source_table,
            "activity_type": RFA_TRANSFER_CLEANUP_ACTIVITY_TYPES.get(spec.source_table, ""),
            "raw_table": (
                f"{RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA}.{RFA_TRANSFER_CLEANUP_V2_RAW_TABLE}"
                if use_v2
                else f"{spec.raw_schema}.{spec.raw_table}"
            ),
            "legacy_raw_table": f"{spec.raw_schema}.{spec.raw_table}",
            "key_column": spec.key_column if is_activity_event_v2 or not use_v2 else "source_pk_value",
            "source_pk_column": spec.key_column,
            "reporting_source": "RFA V2 activity table" if is_activity_event_v2 and use_v2 else "V2 activity events" if use_v2 else "Legacy RAW table",
            "total_rows": 0,
            "unique_keys": 0,
            "latest_ingested_at": "",
            "latest_ingestion_run_id": "",
            "cleanup_counts": {},
            "error": "",
        }
        if use_v2:
            latest_expr = '"event_at"' if "event_at" in v2_columns else '"_ingested_at"'
            key_column = spec.key_column if is_activity_event_v2 else "source_pk_value"
            source_filter = sql.SQL("") if is_activity_event_v2 else sql.SQL('AND "source_table" = %s')
            source_params = [] if is_activity_event_v2 else [spec.source_table]
            count_rows = _fetch_dicts(
                sql.SQL(
                    """
                    SELECT
                        COUNT(*)::text AS total_rows,
                        COUNT(DISTINCT NULLIF({key_column}, ''))::text AS unique_keys,
                        COALESCE(MAX({latest_expr}), '') AS latest_ingested_at
                    FROM {schema}.{table}
                    WHERE COALESCE({key_column}, '') <> ''
                      {source_filter}
                    """
                ).format(
                    key_column=sql.Identifier(key_column),
                    latest_expr=sql.SQL(latest_expr),
                    schema=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA),
                    table=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_TABLE),
                    source_filter=source_filter,
                ),
                source_params,
            )
            if count_rows:
                row["total_rows"] = int(count_rows[0].get("total_rows") or 0)
                row["unique_keys"] = int(count_rows[0].get("unique_keys") or 0)
                row["latest_ingested_at"] = count_rows[0].get("latest_ingested_at") or ""
                total_rows += row["total_rows"]
                total_unique_keys += row["unique_keys"]

            latest_run_rows = _fetch_dicts(
                sql.SQL(
                    """
                    SELECT COALESCE("_ingestion_run_id", '') AS latest_ingestion_run_id
                    FROM {schema}.{table}
                    WHERE COALESCE({key_column}, '') <> ''
                      {source_filter}
                    ORDER BY "_ingested_at" DESC NULLS LAST
                    LIMIT 1
                    """
                ).format(
                    key_column=sql.Identifier(key_column),
                    schema=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA),
                    table=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_TABLE),
                    source_filter=source_filter,
                ),
                source_params,
            )
            if latest_run_rows:
                row["latest_ingestion_run_id"] = latest_run_rows[0].get("latest_ingestion_run_id") or ""
        elif not _table_exists(spec.raw_schema, spec.raw_table):
            row["error"] = "Reporting RAW table is not available yet."
            rows.append(row)
            continue
        else:
            count_rows = _fetch_dicts(
                sql.SQL(
                    """
                    SELECT
                        COUNT(*)::text AS total_rows,
                        COUNT(DISTINCT NULLIF({key_column}, ''))::text AS unique_keys,
                        COALESCE(MAX("_ingested_at"), '') AS latest_ingested_at
                    FROM {schema}.{table}
                    """
                ).format(
                    key_column=sql.Identifier(spec.key_column),
                    schema=sql.Identifier(spec.raw_schema),
                    table=sql.Identifier(spec.raw_table),
                )
            )
            if count_rows:
                row["total_rows"] = int(count_rows[0].get("total_rows") or 0)
                row["unique_keys"] = int(count_rows[0].get("unique_keys") or 0)
                row["latest_ingested_at"] = count_rows[0].get("latest_ingested_at") or ""
                total_rows += row["total_rows"]
                total_unique_keys += row["unique_keys"]

            latest_run_rows = _fetch_dicts(
                sql.SQL(
                    """
                    SELECT COALESCE("_ingestion_run_id", '') AS latest_ingestion_run_id
                    FROM {schema}.{table}
                    ORDER BY "_ingested_at" DESC NULLS LAST
                    LIMIT 1
                    """
                ).format(schema=sql.Identifier(spec.raw_schema), table=sql.Identifier(spec.raw_table))
            )
            if latest_run_rows:
                row["latest_ingestion_run_id"] = latest_run_rows[0].get("latest_ingestion_run_id") or ""

        status_rows = _fetch_dicts(
            """
            SELECT delete_status, COUNT(*)::text AS row_count
            FROM control.transfer_cleanup_manifest
            WHERE domain = 'rfa'
              AND source_table = %s
            GROUP BY delete_status
            """,
            [spec.source_table],
        )
        row["cleanup_counts"] = {item["delete_status"] or "UNKNOWN": int(item["row_count"] or 0) for item in status_rows}
        rows.append(row)

    latest_cleanup = _fetch_dicts(
        """
        SELECT cleanup_run_id, status, started_at, ended_at
        FROM control.transfer_cleanup_run_log
        WHERE domains LIKE %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        ["%rfa%"],
    )
    return {
        "rows": rows,
        "table_count": len(rows),
        "total_rows": total_rows,
        "total_unique_keys": total_unique_keys,
        "latest_cleanup": latest_cleanup[0] if latest_cleanup else None,
    }


def _transfer_cleanup_raw_summary(selected_domain: str) -> dict[str, Any]:
    ensure_cleanup_tables()
    domains = _transfer_cleanup_domains(selected_domain)
    summaries = []
    if "inclinic" in domains:
        summaries.append(_inclinic_transfer_cleanup_raw_summary())
    if "rfa" in domains:
        summaries.append(_rfa_transfer_cleanup_raw_summary())
    rows = [row for summary in summaries for row in summary.get("rows", [])]
    return {
        "rows": rows,
        "table_count": len(rows),
        "total_rows": sum(int(summary.get("total_rows") or 0) for summary in summaries),
        "total_unique_keys": sum(int(summary.get("total_unique_keys") or 0) for summary in summaries),
        "latest_cleanup": _latest_transfer_cleanup(domains),
    }


def _rfa_transfer_cleanup_sql_field(spec: Any, field_name: str) -> sql.Composed:
    if field_name in _rfa_transfer_cleanup_source_columns(spec):
        return sql.SQL("COALESCE(r.{field}::text, '') AS {alias}").format(
            field=sql.Identifier(field_name),
            alias=sql.Identifier(field_name),
        )
    return sql.SQL("''::text AS {alias}").format(alias=sql.Identifier(field_name))


def _rfa_transfer_cleanup_v2_sql_field(columns: set[str], field_name: str, alias: str | None = None) -> sql.Composed:
    output_alias = alias or field_name
    if field_name in columns:
        return sql.SQL("COALESCE(r.{field}::text, '') AS {alias}").format(
            field=sql.Identifier(field_name),
            alias=sql.Identifier(output_alias),
        )
    return sql.SQL("''::text AS {alias}").format(alias=sql.Identifier(output_alias))


def _rfa_transfer_cleanup_raw_records_v2(
    *,
    specs: tuple[Any, ...],
    search: str,
    cleanup_status: str,
    run_id: str,
    limit: int,
) -> dict[str, Any]:
    columns = _rfa_transfer_cleanup_v2_columns()
    cleaned_search = (search or "").strip().lower()
    cleaned_status = (cleanup_status or "all").strip()
    cleaned_run_id = (run_id or "").strip()
    rows: list[dict[str, Any]] = []

    for spec in specs:
        is_activity_event_v2 = spec.source_table == RFA_TRANSFER_CLEANUP_V2_SOURCE_TABLE
        key_column = spec.key_column if is_activity_event_v2 else "source_pk_value"
        where_parts = [
            sql.SQL("COALESCE(r.{key_column}, '') <> ''").format(key_column=sql.Identifier(key_column)),
        ]
        params: list[Any] = [
            spec.source_table,
            f"{RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA}.{RFA_TRANSFER_CLEANUP_V2_RAW_TABLE}",
            spec.source_table,
        ]
        if not is_activity_event_v2:
            where_parts.append(sql.SQL("r.\"source_table\" = %s"))
            params.append(spec.source_table)

        if cleaned_search:
            searchable_fields = [
                spec.key_column,
                "source_table",
                "source_pk_column",
                "source_pk_value",
                "activity_event_uuid",
                "activity_type",
                "patient_id_raw",
                "doctor_uuid",
                "form_id_raw",
                "red_flag_id_raw",
                "overall_flag_code",
                "_ingestion_run_id",
            ]
            table_fields = [field for field in searchable_fields if field in columns]
            where_parts.append(
                sql.SQL("(")
                + sql.SQL(" OR ").join(
                    sql.SQL("LOWER(COALESCE(r.{field}::text, '')) LIKE %s").format(field=sql.Identifier(field))
                    for field in table_fields
                )
                + sql.SQL(")")
            )
            params.extend([f"%{cleaned_search}%"] * len(table_fields))

        if cleaned_status and cleaned_status != "all":
            if cleaned_status == "never_cleaned":
                where_parts.append(sql.SQL("m.delete_status IS NULL"))
            else:
                where_parts.append(sql.SQL("m.delete_status = %s"))
                params.append(cleaned_status)

        if cleaned_run_id:
            where_parts.append(
                sql.SQL("(r.\"_ingestion_run_id\" = %s OR m.cleanup_run_id = %s OR m.pipeline_run_id = %s)")
            )
            params.extend([cleaned_run_id, cleaned_run_id, cleaned_run_id])

        params.append(limit)
        query = sql.SQL(
            """
            SELECT
                %s::text AS source_table,
                %s::text AS raw_table,
                COALESCE(r.{key_column}, '') AS source_pk,
                {source_pk_column},
                {activity_type},
                {event_at},
                {patient_id},
                {doctor_id},
                {form_id},
                {overall_flag_code},
                ''::text AS submission_id,
                {red_flag_id},
                COALESCE(r."_ingestion_run_id", '') AS ingestion_run_id,
                COALESCE(r."_ingested_at", '') AS ingested_at,
                COALESCE(m.cleanup_run_id, '') AS cleanup_run_id,
                COALESCE(m.pipeline_run_id, '') AS cleanup_pipeline_run_id,
                COALESCE(m.delete_status, 'NOT_ATTEMPTED') AS cleanup_status,
                COALESCE(m.deleted_at, '') AS cleanup_status_at,
                COALESCE(m.delete_error, '') AS cleanup_error
            FROM {schema}.{table} r
            LEFT JOIN LATERAL (
                SELECT cleanup_run_id, pipeline_run_id, delete_status, deleted_at, delete_error
                FROM control.transfer_cleanup_manifest m
                WHERE m.domain = 'rfa'
                  AND m.source_table = %s
                  AND m.source_pk = r.{key_column}
                ORDER BY manifest_id DESC
                LIMIT 1
            ) m ON TRUE
            WHERE {where_clause}
            ORDER BY r."_ingested_at" DESC NULLS LAST, source_pk
            LIMIT %s
            """
        ).format(
            key_column=sql.Identifier(key_column),
            source_pk_column=(
                sql.SQL("'activity_event_uuid'::text AS source_pk_column")
                if is_activity_event_v2
                else _rfa_transfer_cleanup_v2_sql_field(columns, "source_pk_column")
            ),
            activity_type=_rfa_transfer_cleanup_v2_sql_field(columns, "activity_type"),
            event_at=_rfa_transfer_cleanup_v2_sql_field(columns, "event_at"),
            patient_id=_rfa_transfer_cleanup_v2_sql_field(columns, "patient_id_raw", "patient_id"),
            doctor_id=_rfa_transfer_cleanup_v2_sql_field(columns, "doctor_uuid", "doctor_id"),
            form_id=_rfa_transfer_cleanup_v2_sql_field(columns, "form_id_raw", "form_id"),
            overall_flag_code=_rfa_transfer_cleanup_v2_sql_field(columns, "overall_flag_code"),
            red_flag_id=_rfa_transfer_cleanup_v2_sql_field(columns, "red_flag_id_raw", "red_flag_id"),
            schema=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA),
            table=sql.Identifier(RFA_TRANSFER_CLEANUP_V2_RAW_TABLE),
            where_clause=sql.SQL(" AND ").join(where_parts),
        )
        rows.extend(_fetch_dicts(query, params))

    rows.sort(key=lambda item: (item.get("event_at") or item.get("ingested_at") or "", item.get("source_table") or "", item.get("source_pk") or ""), reverse=True)
    return {
        "rows": rows[:limit],
        "row_count": len(rows[:limit]),
        "limit": limit,
        "is_limited": len(rows) > limit,
        "errors": [],
        "uses_v2": True,
    }


def _rfa_transfer_cleanup_raw_records(
    *,
    source_table: str = "all",
    search: str = "",
    cleanup_status: str = "all",
    run_id: str = "",
    limit: int = RFA_TRANSFER_CLEANUP_PAGE_SIZE,
) -> dict[str, Any]:
    ensure_cleanup_tables()
    specs = _rfa_transfer_cleanup_selected_specs(source_table)
    cleaned_search = (search or "").strip().lower()
    cleaned_status = (cleanup_status or "all").strip()
    cleaned_run_id = (run_id or "").strip()
    limit = max(1, min(int(limit or RFA_TRANSFER_CLEANUP_PAGE_SIZE), 500))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    if _rfa_transfer_cleanup_v2_usable():
        return _rfa_transfer_cleanup_raw_records_v2(
            specs=specs,
            search=cleaned_search,
            cleanup_status=cleaned_status,
            run_id=cleaned_run_id,
            limit=limit,
        )

    for spec in specs:
        if not _table_exists(spec.raw_schema, spec.raw_table):
            errors.append(f"{spec.raw_schema}.{spec.raw_table} is not available yet.")
            continue

        where_parts = [sql.SQL("COALESCE(r.{key_column}::text, '') <> ''").format(key_column=sql.Identifier(spec.key_column))]
        params: list[Any] = [spec.source_table, f"{spec.raw_schema}.{spec.raw_table}", spec.source_table]

        if cleaned_search:
            searchable_fields = [spec.key_column, "_ingestion_run_id", *RFA_TRANSFER_CLEANUP_DISPLAY_FIELDS]
            source_columns = _rfa_transfer_cleanup_source_columns(spec)
            table_fields = [
                field
                for field in searchable_fields
                if field == spec.key_column or field in source_columns or field.startswith("_")
            ]
            where_parts.append(
                sql.SQL("(")
                + sql.SQL(" OR ").join(
                    sql.SQL("LOWER(COALESCE(r.{field}::text, '')) LIKE %s").format(field=sql.Identifier(field))
                    for field in dict.fromkeys(table_fields)
                )
                + sql.SQL(")")
            )
            params.extend([f"%{cleaned_search}%"] * len(dict.fromkeys(table_fields)))

        if cleaned_status and cleaned_status != "all":
            if cleaned_status == "never_cleaned":
                where_parts.append(sql.SQL("m.delete_status IS NULL"))
            else:
                where_parts.append(sql.SQL("m.delete_status = %s"))
                params.append(cleaned_status)

        if cleaned_run_id:
            where_parts.append(
                sql.SQL("(r.\"_ingestion_run_id\" = %s OR m.cleanup_run_id = %s OR m.pipeline_run_id = %s)")
            )
            params.extend([cleaned_run_id, cleaned_run_id, cleaned_run_id])

        params.append(limit)
        query = sql.SQL(
            """
            SELECT
                %s::text AS source_table,
                %s::text AS raw_table,
                COALESCE(r.{key_column}::text, '') AS source_pk,
                {display_fields},
                COALESCE(r."_ingestion_run_id", '') AS ingestion_run_id,
                COALESCE(r."_ingested_at", '') AS ingested_at,
                COALESCE(m.cleanup_run_id, '') AS cleanup_run_id,
                COALESCE(m.pipeline_run_id, '') AS cleanup_pipeline_run_id,
                COALESCE(m.delete_status, 'NOT_ATTEMPTED') AS cleanup_status,
                COALESCE(m.deleted_at, '') AS cleanup_status_at,
                COALESCE(m.delete_error, '') AS cleanup_error
            FROM {schema}.{table} r
            LEFT JOIN LATERAL (
                SELECT cleanup_run_id, pipeline_run_id, delete_status, deleted_at, delete_error
                FROM control.transfer_cleanup_manifest m
                WHERE m.domain = 'rfa'
                  AND m.source_table = %s
                  AND m.source_pk = r.{key_column}::text
                ORDER BY manifest_id DESC
                LIMIT 1
            ) m ON TRUE
            WHERE {where_clause}
            ORDER BY r."_ingested_at" DESC NULLS LAST, source_pk
            LIMIT %s
            """
        ).format(
            key_column=sql.Identifier(spec.key_column),
            display_fields=sql.SQL(", ").join(
                _rfa_transfer_cleanup_sql_field(spec, field_name) for field_name in RFA_TRANSFER_CLEANUP_DISPLAY_FIELDS
            ),
            schema=sql.Identifier(spec.raw_schema),
            table=sql.Identifier(spec.raw_table),
            where_clause=sql.SQL(" AND ").join(where_parts),
        )
        rows.extend(_fetch_dicts(query, params))

    rows.sort(key=lambda item: (item.get("ingested_at") or "", item.get("source_table") or "", item.get("source_pk") or ""), reverse=True)
    return {
        "rows": rows[:limit],
        "row_count": len(rows[:limit]),
        "limit": limit,
        "is_limited": len(rows) > limit,
        "errors": errors,
        "uses_v2": False,
    }


def _inclinic_sql_first_field(columns: set[str], alias: str, *field_names: str) -> sql.Composed:
    expressions = [
        sql.SQL("NULLIF(r.{field}::text, '')").format(field=sql.Identifier(field_name))
        for field_name in field_names
        if field_name in columns
    ]
    if not expressions:
        return sql.SQL("''::text AS {alias}").format(alias=sql.Identifier(alias))
    return (
        sql.SQL("COALESCE(")
        + sql.SQL(", ").join(expressions)
        + sql.SQL(", ''::text) AS {alias}").format(alias=sql.Identifier(alias))
    )


def _inclinic_transfer_cleanup_records_from_legacy_raw(
    spec: Any,
    *,
    search: str,
    cleanup_status: str,
    run_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    if not _table_exists(spec.raw_schema, spec.raw_table):
        return []
    columns = _table_columns(spec.raw_schema, spec.raw_table)
    if spec.key_column not in columns:
        return []
    cleaned_search = (search or "").strip().lower()
    cleaned_status = (cleanup_status or "all").strip()
    cleaned_run_id = (run_id or "").strip()
    where_parts = [sql.SQL("COALESCE(r.{key_column}::text, '') <> ''").format(key_column=sql.Identifier(spec.key_column))]
    params: list[Any] = ["inclinic", spec.source_table, f"{spec.raw_schema}.{spec.raw_table}", spec.key_column, spec.source_table]

    if cleaned_search:
        searchable_fields = [
            spec.key_column,
            "_ingestion_run_id",
            "brand_campaign_id",
            "doctor_unique_id",
            "doctor_number",
            "collateral_id",
            "field_rep_id",
            "field_rep_unique_id",
            "transaction_id",
        ]
        table_fields = [field for field in searchable_fields if field in columns]
        if table_fields:
            where_parts.append(
                sql.SQL("(")
                + sql.SQL(" OR ").join(
                    sql.SQL("LOWER(COALESCE(r.{field}::text, '')) LIKE %s").format(field=sql.Identifier(field))
                    for field in table_fields
                )
                + sql.SQL(")")
            )
            params.extend([f"%{cleaned_search}%"] * len(table_fields))

    if cleaned_status and cleaned_status != "all":
        if cleaned_status == "never_cleaned":
            where_parts.append(sql.SQL("m.delete_status IS NULL"))
        else:
            where_parts.append(sql.SQL("m.delete_status = %s"))
            params.append(cleaned_status)

    if cleaned_run_id:
        where_parts.append(sql.SQL("(r.\"_ingestion_run_id\" = %s OR m.cleanup_run_id = %s OR m.pipeline_run_id = %s)"))
        params.extend([cleaned_run_id, cleaned_run_id, cleaned_run_id])

    params.append(limit)
    query = sql.SQL(
        """
        SELECT
            %s::text AS domain,
            %s::text AS source_table,
            %s::text AS raw_table,
            COALESCE(r.{key_column}::text, '') AS source_pk,
            %s::text AS source_pk_column,
            'inclinic_collateral_transaction'::text AS activity_type,
            {event_at},
            {campaign_id},
            {doctor_id},
            {doctor_phone},
            {collateral_id},
            {field_rep_id},
            ''::text AS patient_id,
            ''::text AS form_id,
            ''::text AS overall_flag_code,
            ''::text AS submission_id,
            ''::text AS red_flag_id,
            COALESCE(r."_ingestion_run_id", '') AS ingestion_run_id,
            COALESCE(r."_ingested_at", '') AS ingested_at,
            COALESCE(m.cleanup_run_id, '') AS cleanup_run_id,
            COALESCE(m.pipeline_run_id, '') AS cleanup_pipeline_run_id,
            COALESCE(m.delete_status, 'NOT_ATTEMPTED') AS cleanup_status,
            COALESCE(m.deleted_at, '') AS cleanup_status_at,
            COALESCE(m.delete_error, '') AS cleanup_error
        FROM {schema}.{table} r
        LEFT JOIN LATERAL (
            SELECT cleanup_run_id, pipeline_run_id, delete_status, deleted_at, delete_error
            FROM control.transfer_cleanup_manifest m
            WHERE m.domain = 'inclinic'
              AND m.source_table = %s
              AND m.source_pk = r.{key_column}::text
            ORDER BY manifest_id DESC
            LIMIT 1
        ) m ON TRUE
        WHERE {where_clause}
        ORDER BY r."_ingested_at" DESC NULLS LAST, source_pk
        LIMIT %s
        """
    ).format(
        key_column=sql.Identifier(spec.key_column),
        event_at=_inclinic_sql_first_field(columns, "event_at", "transaction_date", "sent_at", "created_at"),
        campaign_id=_inclinic_sql_first_field(columns, "campaign_id", "brand_campaign_id"),
        doctor_id=_inclinic_sql_first_field(columns, "doctor_id", "doctor_unique_id"),
        doctor_phone=_inclinic_sql_first_field(columns, "doctor_phone", "doctor_number"),
        collateral_id=_inclinic_sql_first_field(columns, "collateral_id", "collateral_id"),
        field_rep_id=_inclinic_sql_first_field(columns, "field_rep_id", "field_rep_id", "field_rep_unique_id"),
        schema=sql.Identifier(spec.raw_schema),
        table=sql.Identifier(spec.raw_table),
        where_clause=sql.SQL(" AND ").join(where_parts),
    )
    return _fetch_dicts(query, params)


def _inclinic_transfer_cleanup_records_from_v2(
    spec: Any,
    *,
    legacy_traceability: bool,
    search: str,
    cleanup_status: str,
    run_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    if not _table_exists(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA, INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE):
        return []
    columns = _table_columns(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA, INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE)
    key_column = "source_pk_value" if legacy_traceability else spec.key_column
    if key_column not in columns:
        return []
    cleaned_search = (search or "").strip().lower()
    cleaned_status = (cleanup_status or "all").strip()
    cleaned_run_id = (run_id or "").strip()
    where_parts = [sql.SQL("COALESCE(r.{key_column}::text, '') <> ''").format(key_column=sql.Identifier(key_column))]
    params: list[Any] = [
        "inclinic",
        spec.source_table,
        f"{INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA}.{INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE}",
        spec.key_column if not legacy_traceability else "source_pk_value",
        spec.source_table,
    ]

    if legacy_traceability:
        where_parts.append(sql.SQL("r.\"source_table\" = %s"))
        params.append(INCLINIC_TRANSFER_CLEANUP_V1_SOURCE_TABLE)

    if cleaned_search:
        searchable_fields = [
            key_column,
            "source_table",
            "source_pk_column",
            "source_pk_value",
            "transaction_uuid",
            "old_id",
            "old_transaction_id",
            "legacy_campaign_id",
            "old_brand_campaign_id",
            "doctor_uuid",
            "inclinic_doctor_uuid",
            "old_doctor_unique_id",
            "old_doctor_number",
            "collateral_uuid",
            "old_collateral_id",
            "old_field_rep_id",
            "brand_supplied_field_rep_id",
        ]
        table_fields = list(dict.fromkeys(field for field in searchable_fields if field in columns))
        if table_fields:
            where_parts.append(
                sql.SQL("(")
                + sql.SQL(" OR ").join(
                    sql.SQL("LOWER(COALESCE(r.{field}::text, '')) LIKE %s").format(field=sql.Identifier(field))
                    for field in table_fields
                )
                + sql.SQL(")")
            )
            params.extend([f"%{cleaned_search}%"] * len(table_fields))

    if cleaned_status and cleaned_status != "all":
        if cleaned_status == "never_cleaned":
            where_parts.append(sql.SQL("m.delete_status IS NULL"))
        else:
            where_parts.append(sql.SQL("m.delete_status = %s"))
            params.append(cleaned_status)

    if cleaned_run_id:
        where_parts.append(sql.SQL("(m.cleanup_run_id = %s OR m.pipeline_run_id = %s)"))
        params.extend([cleaned_run_id, cleaned_run_id])

    params.append(limit)
    query = sql.SQL(
        """
        SELECT
            %s::text AS domain,
            %s::text AS source_table,
            %s::text AS raw_table,
            COALESCE(r.{key_column}::text, '') AS source_pk,
            %s::text AS source_pk_column,
            %s::text AS activity_type,
            {event_at},
            {campaign_id},
            {doctor_id},
            {doctor_phone},
            {collateral_id},
            {field_rep_id},
            ''::text AS patient_id,
            ''::text AS form_id,
            ''::text AS overall_flag_code,
            ''::text AS submission_id,
            ''::text AS red_flag_id,
            ''::text AS ingestion_run_id,
            {ingested_at},
            COALESCE(m.cleanup_run_id, '') AS cleanup_run_id,
            COALESCE(m.pipeline_run_id, '') AS cleanup_pipeline_run_id,
            COALESCE(m.delete_status, 'NOT_ATTEMPTED') AS cleanup_status,
            COALESCE(m.deleted_at, '') AS cleanup_status_at,
            COALESCE(m.delete_error, '') AS cleanup_error
        FROM {schema}.{table} r
        LEFT JOIN LATERAL (
            SELECT cleanup_run_id, pipeline_run_id, delete_status, deleted_at, delete_error
            FROM control.transfer_cleanup_manifest m
            WHERE m.domain = 'inclinic'
              AND m.source_table = %s
              AND m.source_pk = r.{key_column}::text
            ORDER BY manifest_id DESC
            LIMIT 1
        ) m ON TRUE
        WHERE {where_clause}
        ORDER BY event_at DESC NULLS LAST, source_pk
        LIMIT %s
        """
    ).format(
        key_column=sql.Identifier(key_column),
        event_at=_inclinic_sql_first_field(columns, "event_at", "old_transaction_date", "old_sent_at", "source_updated_at", "migrated_at", "source_created_at"),
        campaign_id=_inclinic_sql_first_field(columns, "campaign_id", "legacy_campaign_id", "old_brand_campaign_id", "campaign_uuid"),
        doctor_id=_inclinic_sql_first_field(columns, "doctor_id", "doctor_uuid", "inclinic_doctor_uuid", "old_doctor_unique_id"),
        doctor_phone=_inclinic_sql_first_field(columns, "doctor_phone", "doctor_phone_normalized", "old_doctor_number"),
        collateral_id=_inclinic_sql_first_field(columns, "collateral_id", "collateral_uuid", "old_collateral_id"),
        field_rep_id=_inclinic_sql_first_field(columns, "field_rep_id", "resolved_field_rep_uuid", "old_field_rep_id", "brand_supplied_field_rep_id"),
        ingested_at=_inclinic_sql_first_field(columns, "ingested_at", "migrated_at", "source_updated_at", "source_created_at"),
        schema=sql.Identifier(INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA),
        table=sql.Identifier(INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE),
        where_clause=sql.SQL(" AND ").join(where_parts),
    )
    params.insert(4, INCLINIC_TRANSFER_CLEANUP_ACTIVITY_TYPES.get(spec.source_table, "inclinic_collateral_transaction"))
    return _fetch_dicts(query, params)


def _inclinic_transfer_cleanup_raw_records(
    *,
    specs: tuple[Any, ...],
    search: str,
    cleanup_status: str,
    run_id: str,
    limit: int,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for spec in specs:
        if spec.source_table == INCLINIC_TRANSFER_CLEANUP_V1_SOURCE_TABLE:
            rows.extend(
                _inclinic_transfer_cleanup_records_from_legacy_raw(
                    spec,
                    search=search,
                    cleanup_status=cleanup_status,
                    run_id=run_id,
                    limit=limit,
                )
            )
            rows.extend(
                _inclinic_transfer_cleanup_records_from_v2(
                    spec,
                    legacy_traceability=True,
                    search=search,
                    cleanup_status=cleanup_status,
                    run_id=run_id,
                    limit=limit,
                )
            )
        else:
            rows.extend(
                _inclinic_transfer_cleanup_records_from_v2(
                    spec,
                    legacy_traceability=False,
                    search=search,
                    cleanup_status=cleanup_status,
                    run_id=run_id,
                    limit=limit,
                )
            )
    rows.sort(key=lambda item: (item.get("event_at") or item.get("ingested_at") or "", item.get("source_table") or "", item.get("source_pk") or ""), reverse=True)
    return {
        "rows": rows[:limit],
        "row_count": len(rows[:limit]),
        "limit": limit,
        "is_limited": len(rows) > limit,
        "errors": [],
        "uses_v2": True,
    }


def _transfer_cleanup_raw_records(
    *,
    selected_domain: str = "all",
    source_table: str = "all",
    search: str = "",
    cleanup_status: str = "all",
    run_id: str = "",
    limit: int = TRANSFER_CLEANUP_PAGE_SIZE,
) -> dict[str, Any]:
    ensure_cleanup_tables()
    specs = _transfer_cleanup_specs(selected_domain, source_table)
    cleaned_search = (search or "").strip().lower()
    cleaned_status = (cleanup_status or "all").strip()
    cleaned_run_id = (run_id or "").strip()
    limit = max(1, min(int(limit or TRANSFER_CLEANUP_PAGE_SIZE), 500))
    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    inclinic_specs = tuple(spec for spec in specs if spec.domain == "inclinic")
    if inclinic_specs:
        result = _inclinic_transfer_cleanup_raw_records(
            specs=inclinic_specs,
            search=cleaned_search,
            cleanup_status=cleaned_status,
            run_id=cleaned_run_id,
            limit=limit,
        )
        rows.extend(result["rows"])
        errors.extend(result.get("errors", []))

    rfa_specs = tuple(spec for spec in specs if spec.domain == "rfa")
    if rfa_specs:
        rfa_source_table = "all" if len(rfa_specs) == len(RFA_SPECS) else rfa_specs[0].source_table
        result = _rfa_transfer_cleanup_raw_records(
            source_table=rfa_source_table,
            search=cleaned_search,
            cleanup_status=cleaned_status,
            run_id=cleaned_run_id,
            limit=limit,
        )
        for row in result["rows"]:
            row.setdefault("domain", "rfa")
            row.setdefault("campaign_id", "")
            row.setdefault("collateral_id", "")
            row.setdefault("field_rep_id", "")
            row.setdefault("doctor_phone", "")
        rows.extend(result["rows"])
        errors.extend(result.get("errors", []))

    rows.sort(key=lambda item: (item.get("event_at") or item.get("ingested_at") or "", item.get("domain") or "", item.get("source_table") or "", item.get("source_pk") or ""), reverse=True)
    return {
        "rows": rows[:limit],
        "row_count": len(rows[:limit]),
        "limit": limit,
        "is_limited": len(rows) > limit,
        "errors": errors,
    }


def _transfer_cleanup_recent_runs(selected_domain: str = "all", limit: int = 8) -> list[dict[str, Any]]:
    ensure_cleanup_tables()
    domains = set(_transfer_cleanup_domains(selected_domain))
    rows = _fetch_dicts(
        """
        SELECT cleanup_run_id, started_at, ended_at, status, domains, notes
        FROM control.transfer_cleanup_run_log
        ORDER BY started_at DESC
        LIMIT 50
        """
    )
    matched = []
    for row in rows:
        run_domains = {item.strip() for item in (row.get("domains") or "").split(",") if item.strip()}
        if run_domains & domains:
            matched.append(row)
        if len(matched) >= limit:
            break
    return matched


def _rfa_transfer_cleanup_recent_runs(limit: int = 8) -> list[dict[str, Any]]:
    ensure_cleanup_tables()
    return _fetch_dicts(
        """
        SELECT cleanup_run_id, started_at, ended_at, status, domains, notes
        FROM control.transfer_cleanup_run_log
        WHERE domains LIKE %s
        ORDER BY started_at DESC
        LIMIT %s
        """,
        ["%rfa%", limit],
    )


def _managed_table_refs() -> list[dict[str, str]]:
    raw_tables = _fetch_dicts(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    )
    refs: list[dict[str, str]] = []
    for row in raw_tables:
        schema = row["table_schema"]
        table = row["table_name"]
        if _is_managed_table(schema, table):
            refs.append({"schema": schema, "name": table})
    return refs


def _is_raw_table_ref(schema: str, table: str) -> bool:
    return _is_managed_table(schema, table) and _layer_key_for_schema(schema) == "raw"


def _raw_table_refs() -> list[dict[str, str]]:
    return [row for row in _managed_table_refs() if _is_raw_table_ref(row["schema"], row["name"])]


def _list_tables() -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for row in _managed_table_refs():
        schema = row["schema"]
        table = row["name"]
        system = _system_profile_for_schema(schema)
        tables.append(
            {
                "schema": schema,
                "name": table,
                "row_count": _table_count(schema, table),
                "href": reverse("internal-data-admin-table", args=[schema, table]),
                "system_key": system.key,
                "system_label": system.short_label,
                "layer": _layer_for_schema(schema),
            }
        )
    return tables


def _table_count(schema: str, table: str) -> int | None:
    try:
        rows = _fetch_dicts(
            sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        return int(rows[0]["row_count"])
    except DatabaseError:
        return None


def _source_fingerprint_columns(info: TableInfo) -> list[str]:
    return [column.name for column in info.columns if column.name not in RAW_AUDIT_COLUMN_NAMES]


def _qualified_column_sql(column: str, relation: str | None = None) -> sql.Composable:
    if relation:
        return sql.SQL("{}.{}").format(sql.Identifier(relation), sql.Identifier(column))
    return sql.Identifier(column)


def _raw_payload_sql(info: TableInfo, relation: str | None = None) -> sql.Composable:
    source_columns = _source_fingerprint_columns(info)
    if source_columns:
        return sql.SQL("jsonb_build_array({})").format(
            sql.SQL(", ").join(_qualified_column_sql(column, relation) for column in source_columns)
        )
    if relation:
        return sql.SQL("jsonb_build_array({}.ctid::text)").format(sql.Identifier(relation))
    return sql.SQL("jsonb_build_array(ctid::text)")


def _raw_fingerprint_sql(info: TableInfo, relation: str | None = None) -> sql.Composable:
    payload_hash = sql.SQL("md5(({})::text)").format(_raw_payload_sql(info, relation=relation))
    column_names = {column.name for column in info.columns}
    if "_source_payload_hash" in column_names:
        return sql.SQL("COALESCE(NULLIF({}, ''), {})").format(
            _qualified_column_sql("_source_payload_hash", relation),
            payload_hash,
        )
    return payload_hash


def _raw_table_stats(info: TableInfo) -> dict[str, Any]:
    rows = _fetch_dicts(
        sql.SQL(
            """
            WITH grouped AS (
                SELECT {} AS row_fingerprint, COUNT(*)::bigint AS group_count
                FROM {}.{}
                GROUP BY 1
            )
            SELECT
                COALESCE(SUM(group_count), 0)::bigint AS total_rows,
                COUNT(*)::bigint AS unique_rows,
                COALESCE(SUM(group_count - 1), 0)::bigint AS duplicate_rows,
                COUNT(*) FILTER (WHERE group_count > 1)::bigint AS duplicate_groups,
                COALESCE(MAX(group_count), 0)::bigint AS largest_duplicate_group
            FROM grouped
            """
        ).format(
            _raw_fingerprint_sql(info),
            sql.Identifier(info.schema),
            sql.Identifier(info.name),
        )
    )
    stats = rows[0] if rows else {}
    latest_ingested_at = None
    if "_ingested_at" in {column.name for column in info.columns}:
        latest_rows = _fetch_dicts(
            sql.SQL("SELECT MAX(NULLIF({}, '')) AS latest_ingested_at FROM {}.{}").format(
                sql.Identifier("_ingested_at"),
                sql.Identifier(info.schema),
                sql.Identifier(info.name),
            )
        )
        latest_ingested_at = latest_rows[0]["latest_ingested_at"] if latest_rows else None

    return {
        "total_rows": int(stats.get("total_rows") or 0),
        "unique_rows": int(stats.get("unique_rows") or 0),
        "duplicate_rows": int(stats.get("duplicate_rows") or 0),
        "duplicate_groups": int(stats.get("duplicate_groups") or 0),
        "largest_duplicate_group": int(stats.get("largest_duplicate_group") or 0),
        "latest_ingested_at": latest_ingested_at,
    }


def _raw_table_info(schema: str, table: str) -> TableInfo:
    info = _table_info(schema, table)
    if not _is_raw_table_ref(info.schema, info.name):
        raise Http404("Only RAW tables can be downloaded from this page.")
    return info


def _raw_summary_cards() -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for row in _raw_table_refs():
        info = _table_info(row["schema"], row["name"])
        profile = _system_profile_for_schema(info.schema)
        card = {
            "schema": info.schema,
            "name": info.name,
            "system_key": profile.key,
            "system_label": profile.short_label,
            "layer": _layer_for_schema(info.schema),
            "view_href": reverse("internal-data-admin-table", args=[info.schema, info.name]),
            "download_href": reverse("internal-data-admin-raw-download", args=[info.schema, info.name]),
            "error": None,
        }
        try:
            stats = _raw_table_stats(info)
            card.update(stats)
            card["has_duplicates"] = stats["duplicate_rows"] > 0
        except DatabaseError as exc:
            card.update(
                {
                    "total_rows": None,
                    "unique_rows": None,
                    "duplicate_rows": None,
                    "duplicate_groups": None,
                    "largest_duplicate_group": None,
                    "latest_ingested_at": None,
                    "has_duplicates": False,
                    "error": str(exc),
                }
            )
        cards.append(card)
    return cards


def _raw_summary_totals(cards: list[dict[str, Any]]) -> dict[str, int]:
    ready_cards = [card for card in cards if card.get("error") is None]
    return {
        "table_count": len(cards),
        "total_rows": sum(int(card.get("total_rows") or 0) for card in ready_cards),
        "unique_rows": sum(int(card.get("unique_rows") or 0) for card in ready_cards),
        "duplicate_rows": sum(int(card.get("duplicate_rows") or 0) for card in ready_cards),
        "duplicate_tables": sum(1 for card in ready_cards if int(card.get("duplicate_rows") or 0) > 0),
    }


def _raw_system_cards(cards: list[dict[str, Any]], selected_system: str) -> list[dict[str, Any]]:
    system_cards: list[dict[str, Any]] = [
        {
            "key": "all",
            "label": "All RAW Tables",
            "summary": "Every RAW source copy table.",
            "table_count": len(cards),
            "row_count": sum(int(card.get("total_rows") or 0) for card in cards if card.get("error") is None),
            "duplicate_count": sum(int(card.get("duplicate_rows") or 0) for card in cards if card.get("error") is None),
            "is_selected": selected_system == "all",
            "href": reverse("internal-data-admin-raw-downloads"),
        }
    ]
    for system_key in CLEANUP_SYSTEM_KEYS:
        profile = SYSTEM_PROFILES[system_key]
        profile_cards = [card for card in cards if card["system_key"] == system_key]
        system_cards.append(
            {
                "key": system_key,
                "label": profile.label,
                "summary": profile.source_guidance,
                "table_count": len(profile_cards),
                "row_count": sum(int(card.get("total_rows") or 0) for card in profile_cards if card.get("error") is None),
                "duplicate_count": sum(int(card.get("duplicate_rows") or 0) for card in profile_cards if card.get("error") is None),
                "is_selected": selected_system == system_key,
                "href": f"{reverse('internal-data-admin-raw-downloads')}?system={system_key}",
            }
        )
    return system_cards


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _stream_table_csv(info: TableInfo):
    pseudo_buffer = _CsvEcho()
    writer = csv.writer(pseudo_buffer)
    column_names = [column.name for column in info.columns]
    yield writer.writerow(column_names)

    query = sql.SQL("SELECT {} FROM {}.{} ORDER BY ctid").format(
        sql.SQL(", ").join(sql.Identifier(column) for column in column_names),
        sql.Identifier(info.schema),
        sql.Identifier(info.name),
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(RAW_EXPORT_BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                yield writer.writerow([_csv_value(value) for value in row])


def _raw_export_filename(schema: str, table: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", f"{schema}.{table}.csv")


def _raw_dedupe_system_options(selected_system: str) -> list[dict[str, Any]]:
    options = [
        {
            "key": "all",
            "label": "All RAW Tables",
            "summary": "Inspect exact duplicates across every RAW source copy.",
            "is_selected": selected_system == "all",
            "href": reverse("internal-data-admin-raw-dedupe"),
        }
    ]
    for key in CLEANUP_SYSTEM_KEYS:
        profile = SYSTEM_PROFILES[key]
        options.append(
            {
                "key": key,
                "label": profile.label,
                "summary": profile.source_guidance,
                "is_selected": selected_system == key,
                "href": f"{reverse('internal-data-admin-raw-dedupe')}?system={key}",
            }
        )
    return options


def _raw_dedupe_target_allowed(selected_system: str, schema: str, table: str) -> bool:
    return any(ref["schema"] == schema and ref["name"] == table for ref in _raw_dedupe_target_refs(selected_system))


def _raw_dedupe_target_refs(selected_system: str) -> list[dict[str, str]]:
    refs = _raw_table_refs()
    if selected_system == "all":
        return refs
    return [ref for ref in refs if _system_key_for_schema(ref["schema"]) == selected_system]


def _raw_dedupe_plan_for_refs(refs: list[dict[str, str]]) -> dict[str, Any]:
    table_rows: list[dict[str, Any]] = []
    for ref in refs:
        info = _table_info(ref["schema"], ref["name"])
        profile = _system_profile_for_schema(info.schema)
        row = {
            "schema": info.schema,
            "table": info.name,
            "system_key": profile.key,
            "system_label": profile.short_label,
            "source_column_count": len(_source_fingerprint_columns(info)),
            "view_href": reverse("internal-data-admin-table", args=[info.schema, info.name]),
            "error": None,
        }
        try:
            stats = _raw_table_stats(info)
            row.update(stats)
            row["has_duplicates"] = stats["duplicate_rows"] > 0
        except DatabaseError as exc:
            row.update(
                {
                    "total_rows": None,
                    "unique_rows": None,
                    "duplicate_rows": None,
                    "duplicate_groups": None,
                    "largest_duplicate_group": None,
                    "latest_ingested_at": None,
                    "has_duplicates": False,
                    "error": str(exc),
                }
            )
        table_rows.append(row)

    ready_rows = [row for row in table_rows if row.get("error") is None]
    duplicate_rows = sorted(
        [row for row in ready_rows if int(row.get("duplicate_rows") or 0) > 0],
        key=lambda row: int(row.get("duplicate_rows") or 0),
        reverse=True,
    )
    return {
        "rows": table_rows,
        "duplicate_rows": duplicate_rows,
        "table_count": len(table_rows),
        "duplicate_table_count": len(duplicate_rows),
        "total_rows": sum(int(row.get("total_rows") or 0) for row in ready_rows),
        "unique_rows": sum(int(row.get("unique_rows") or 0) for row in ready_rows),
        "duplicate_row_count": sum(int(row.get("duplicate_rows") or 0) for row in ready_rows),
        "duplicate_group_count": sum(int(row.get("duplicate_groups") or 0) for row in ready_rows),
        "has_errors": any(row.get("error") for row in table_rows),
    }


def _raw_dedupe_next_row(plan: dict[str, Any]) -> dict[str, Any] | None:
    duplicate_rows = plan.get("duplicate_rows") or []
    if not duplicate_rows:
        return None
    return max(duplicate_rows, key=lambda row: int(row.get("duplicate_rows") or 0))


def _raw_dedupe_report_refs(selected_system: str) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for ref in _managed_table_refs():
        schema = ref["schema"]
        layer_key = _layer_key_for_schema(schema)
        if layer_key not in {"bronze", "silver", "gold"}:
            continue
        if selected_system != "all" and _system_key_for_schema(schema) != selected_system:
            continue
        refs.append(ref)
    return refs


def _raw_dedupe_plan(selected_system: str) -> dict[str, Any]:
    plan = _raw_dedupe_plan_for_refs(_raw_dedupe_target_refs(selected_system))
    return {
        **plan,
        "selected_system": selected_system,
    }


def _raw_dedupe_report_snapshot(selected_system: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for ref in _raw_dedupe_report_refs(selected_system):
        schema = ref["schema"]
        table = ref["name"]
        profile = _system_profile_for_schema(schema)
        layer_key = _layer_key_for_schema(schema)
        snapshot = {
            "schema": schema,
            "table": table,
            "system_key": profile.key,
            "system_label": profile.short_label,
            "layer_key": layer_key,
            "layer_label": LAYER_LABELS.get(layer_key, layer_key.upper()),
            "row_count": None,
            "error": None,
        }
        try:
            count_rows = _fetch_dicts(
                sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            )
            snapshot["row_count"] = int(count_rows[0]["row_count"])
        except DatabaseError as exc:
            snapshot["error"] = str(exc)
        rows.append(snapshot)

    ready_rows = [row for row in rows if row.get("error") is None]
    return {
        "rows": rows,
        "table_count": len(rows),
        "row_count": sum(int(row.get("row_count") or 0) for row in ready_rows),
        "error_count": len(rows) - len(ready_rows),
    }


def _raw_dedupe_confirmation_phrase(selected_system: str) -> str:
    label = "ALL" if selected_system == "all" else selected_system.upper()
    return f"{RAW_DEDUPE_CONFIRM_PREFIX} {label}"


def _raw_dedupe_run_id() -> str:
    return datetime.now(timezone.utc).strftime("raw-dedupe-%Y%m%dT%H%M%S%fZ")


def _ensure_raw_dedupe_archive_table() -> None:
    _execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(AUDIT_SCHEMA)))
    _execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                dedupe_run_id TEXT NOT NULL,
                archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                source_payload JSONB NOT NULL,
                source_columns JSONB NOT NULL,
                source_ctid TEXT,
                source_record_hash TEXT,
                source_ingested_at TEXT,
                source_ingestion_run_id TEXT,
                dedupe_rank INTEGER,
                duplicate_group_size BIGINT,
                raw_row JSONB NOT NULL,
                reason TEXT,
                actor TEXT
            )
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(RAW_DEDUPE_ARCHIVE_TABLE))
    )
    _execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS ops_raw_duplicate_archive_run_idx
            ON {}.{} (dedupe_run_id, schema_name, table_name)
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(RAW_DEDUPE_ARCHIVE_TABLE))
    )


def _raw_dedupe_order_sql(info: TableInfo, relation: str | None = None) -> sql.SQL:
    column_names = {column.name for column in info.columns}
    parts: list[sql.Composable] = []
    for column in ("_ingested_at", "_extract_ended_at", "_extract_started_at", "_ingestion_run_id"):
        if column in column_names:
            parts.append(sql.SQL("NULLIF({}, '') DESC NULLS LAST").format(_qualified_column_sql(column, relation)))
    if relation:
        parts.append(sql.SQL("{}.ctid DESC").format(sql.Identifier(relation)))
    else:
        parts.append(sql.SQL("ctid DESC"))
    return sql.SQL(", ").join(parts)


def _nullable_source_column_sql(info: TableInfo, column: str) -> sql.Composable:
    if column in {info_column.name for info_column in info.columns}:
        return sql.SQL("NULLIF(src.{}, '')").format(sql.Identifier(column))
    return sql.SQL("NULL::text")


def _archive_and_delete_raw_duplicates_for_table(
    info: TableInfo,
    run_id: str,
    reason: str,
    actor: str,
    max_rows: int | None = None,
) -> dict[str, Any]:
    source_columns = _source_fingerprint_columns(info)
    if not source_columns:
        raise DatabaseError(f"{info.schema}.{info.name} has no source columns to compare safely.")

    payload_sql = _raw_payload_sql(info, relation="src")
    source_fingerprint_sql = _raw_fingerprint_sql(info, relation="source_rows")
    ranked_fingerprint_sql = _raw_fingerprint_sql(info, relation="src")
    order_sql = _raw_dedupe_order_sql(info, relation="src")
    source_columns_json = json.dumps(source_columns)
    record_hash_sql = _nullable_source_column_sql(info, "_record_hash")
    ingested_at_sql = _nullable_source_column_sql(info, "_ingested_at")
    ingestion_run_id_sql = _nullable_source_column_sql(info, "_ingestion_run_id")

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("LOCK TABLE {}.{} IN SHARE ROW EXCLUSIVE MODE").format(
                sql.Identifier(info.schema),
                sql.Identifier(info.name),
            )
        )
        cursor.execute("DROP TABLE IF EXISTS pg_temp.raw_dedupe_to_delete")
        cursor.execute(
            sql.SQL(
                """
                CREATE TEMP TABLE raw_dedupe_to_delete ON COMMIT DROP AS
                WITH duplicate_fingerprints AS (
                    SELECT row_fingerprint, COUNT(*)::bigint AS group_count
                    FROM (
                        SELECT {} AS row_fingerprint
                        FROM {}.{} AS source_rows
                    ) source_rows
                    GROUP BY row_fingerprint
                    HAVING COUNT(*) > 1
                ),
                ranked AS (
                    SELECT
                        src.ctid AS raw_ctid,
                        src.ctid::text AS source_ctid,
                        {} AS source_payload,
                        ROW_NUMBER() OVER (PARTITION BY duplicates.row_fingerprint ORDER BY {}) AS dedupe_rank,
                        duplicates.group_count AS duplicate_group_size
                    FROM {}.{} AS src
                    JOIN duplicate_fingerprints duplicates
                      ON {} = duplicates.row_fingerprint
                )
                SELECT *
                FROM ranked
                WHERE dedupe_rank > 1
                ORDER BY duplicate_group_size DESC, source_ctid
                LIMIT %s
                """
            ).format(
                source_fingerprint_sql,
                sql.Identifier(info.schema),
                sql.Identifier(info.name),
                payload_sql,
                order_sql,
                sql.Identifier(info.schema),
                sql.Identifier(info.name),
                ranked_fingerprint_sql,
            ),
            [max_rows or RAW_DEDUPE_BATCH_SIZE],
        )
        cursor.execute("SELECT COUNT(*) FROM pg_temp.raw_dedupe_to_delete")
        planned_delete_count = int(cursor.fetchone()[0])
        if planned_delete_count <= 0:
            return {"schema": info.schema, "table": info.name, "archived_count": 0, "deleted_count": 0}

        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {}.{}
                  (
                    dedupe_run_id,
                    archived_at,
                    schema_name,
                    table_name,
                    source_payload,
                    source_columns,
                    source_ctid,
                    source_record_hash,
                    source_ingested_at,
                    source_ingestion_run_id,
                    dedupe_rank,
                    duplicate_group_size,
                    raw_row,
                    reason,
                    actor
                  )
                SELECT
                    %s,
                    NOW(),
                    %s,
                    %s,
                    d.source_payload,
                    %s::jsonb,
                    d.source_ctid,
                    {},
                    {},
                    {},
                    d.dedupe_rank,
                    d.duplicate_group_size,
                    to_jsonb(src),
                    %s,
                    %s
                FROM {}.{} AS src
                JOIN pg_temp.raw_dedupe_to_delete AS d
                  ON src.ctid = d.raw_ctid
                """
            ).format(
                sql.Identifier(AUDIT_SCHEMA),
                sql.Identifier(RAW_DEDUPE_ARCHIVE_TABLE),
                record_hash_sql,
                ingested_at_sql,
                ingestion_run_id_sql,
                sql.Identifier(info.schema),
                sql.Identifier(info.name),
            ),
            [run_id, info.schema, info.name, source_columns_json, reason, actor],
        )
        archived_count = cursor.rowcount if cursor.rowcount >= 0 else 0

        cursor.execute(
            sql.SQL(
                """
                DELETE FROM {}.{} AS src
                USING pg_temp.raw_dedupe_to_delete AS d
                WHERE src.ctid = d.raw_ctid
                """
            ).format(sql.Identifier(info.schema), sql.Identifier(info.name))
        )
        deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0

    if archived_count != planned_delete_count or deleted_count != planned_delete_count:
        raise DatabaseError(
            f"RAW dedupe count mismatch for {info.schema}.{info.name}: "
            f"planned {planned_delete_count}, archived {archived_count}, deleted {deleted_count}."
        )

    _audit(
        "raw_dedupe_archive_delete",
        TableInfo(schema=info.schema, name=info.name, columns=[], primary_key=[]),
        {
            "mode": "raw_exact_duplicate_dedupe",
            "dedupe_run_id": run_id,
            "archive_table": f"{AUDIT_SCHEMA}.{RAW_DEDUPE_ARCHIVE_TABLE}",
        },
        {
            "source_columns": source_columns,
            "planned_duplicate_rows": planned_delete_count,
        },
        {
            "archived_count": archived_count,
            "deleted_count": deleted_count,
        },
        reason,
        actor,
    )
    return {
        "schema": info.schema,
        "table": info.name,
        "archived_count": archived_count,
        "deleted_count": deleted_count,
    }


def _raw_dedupe_validation(
    before_plan: dict[str, Any],
    after_plan: dict[str, Any],
    before_report: dict[str, Any],
    after_report: dict[str, Any],
    action_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    before_raw = {(row["schema"], row["table"]): row for row in before_plan["rows"]}
    after_raw = {(row["schema"], row["table"]): row for row in after_plan["rows"]}
    raw_rows: list[dict[str, Any]] = []
    for key, before in before_raw.items():
        after = after_raw.get(key, {})
        deleted_count = sum(
            row["deleted_count"]
            for row in action_rows
            if row["schema"] == key[0] and row["table"] == key[1]
        )
        raw_rows.append(
            {
                "schema": key[0],
                "table": key[1],
                "before_total_rows": int(before.get("total_rows") or 0),
                "after_total_rows": int(after.get("total_rows") or 0),
                "before_unique_rows": int(before.get("unique_rows") or 0),
                "after_unique_rows": int(after.get("unique_rows") or 0),
                "before_duplicate_rows": int(before.get("duplicate_rows") or 0),
                "after_duplicate_rows": int(after.get("duplicate_rows") or 0),
                "deleted_count": deleted_count,
                "unique_preserved": int(before.get("unique_rows") or 0) == int(after.get("unique_rows") or 0),
                "total_delta_matches": int(before.get("total_rows") or 0) - int(after.get("total_rows") or 0) == deleted_count,
            }
        )

    before_report_map = {(row["schema"], row["table"]): row for row in before_report["rows"]}
    after_report_map = {(row["schema"], row["table"]): row for row in after_report["rows"]}
    changed_report_rows: list[dict[str, Any]] = []
    for key, before in before_report_map.items():
        after = after_report_map.get(key)
        if not after or before.get("row_count") != after.get("row_count") or before.get("error") != after.get("error"):
            changed_report_rows.append(
                {
                    "schema": key[0],
                    "table": key[1],
                    "layer_label": before.get("layer_label"),
                    "before_row_count": before.get("row_count"),
                    "after_row_count": after.get("row_count") if after else None,
                    "before_error": before.get("error"),
                    "after_error": after.get("error") if after else "table missing after cleanup",
                }
            )

    raw_unique_changed_count = sum(1 for row in raw_rows if not row["unique_preserved"])
    raw_total_mismatch_count = sum(1 for row in raw_rows if not row["total_delta_matches"])
    report_error_count = int(before_report.get("error_count") or 0) + int(after_report.get("error_count") or 0)
    return {
        "raw_rows": raw_rows,
        "raw_unique_changed_count": raw_unique_changed_count,
        "raw_total_mismatch_count": raw_total_mismatch_count,
        "report_table_count": before_report["table_count"],
        "report_row_count_before": before_report["row_count"],
        "report_row_count_after": after_report["row_count"],
        "report_error_count": report_error_count,
        "report_changed_count": len(changed_report_rows),
        "report_changed_rows": changed_report_rows,
        "passed": raw_unique_changed_count == 0 and raw_total_mismatch_count == 0 and report_error_count == 0 and not changed_report_rows,
    }


def _execute_raw_dedupe(
    selected_system: str,
    reason: str,
    actor: str,
    target: tuple[str, str] | None = None,
    max_rows: int | None = None,
) -> dict[str, Any]:
    run_id = _raw_dedupe_run_id()
    action_rows: list[dict[str, Any]] = []
    target_refs = [{"schema": target[0], "name": target[1]}] if target else _raw_dedupe_target_refs(selected_system)
    with transaction.atomic():
        _ensure_audit_table()
        _ensure_raw_dedupe_archive_table()
        before_plan = _raw_dedupe_plan_for_refs(target_refs)
        before_report = _raw_dedupe_report_snapshot(selected_system)

        for row in before_plan["duplicate_rows"]:
            info = _table_info(row["schema"], row["table"])
            action_rows.append(_archive_and_delete_raw_duplicates_for_table(info, run_id, reason, actor, max_rows=max_rows))

        after_plan = _raw_dedupe_plan_for_refs(target_refs)
        after_report = _raw_dedupe_report_snapshot(selected_system)
        validation = _raw_dedupe_validation(before_plan, after_plan, before_report, after_report, action_rows)
        if not validation["passed"]:
            raise DatabaseError("RAW dedupe validation failed; the archive/delete transaction was rolled back.")

    return {
        "run_id": run_id,
        "archive_table": f"{AUDIT_SCHEMA}.{RAW_DEDUPE_ARCHIVE_TABLE}",
        "archived_count": sum(row["archived_count"] for row in action_rows),
        "deleted_count": sum(row["deleted_count"] for row in action_rows),
        "table_count": sum(1 for row in action_rows if row["deleted_count"] > 0),
        "rows": action_rows,
        "validation": validation,
    }


def _raw_dedupe_json_error(message: str, status: int = 400) -> JsonResponse:
    return JsonResponse({"ok": False, "message": message}, status=status)


def _table_info(schema: str, table: str) -> TableInfo:
    if not _is_managed_table(schema, table) or not _table_exists(schema, table):
        raise Http404("Table is not available in the internal data dashboard.")

    column_rows = _fetch_dicts(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            ordinal_position,
            column_default,
            is_identity,
            is_generated
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    columns = [
        ColumnInfo(
            name=row["column_name"],
            data_type=row["data_type"],
            is_nullable=row["is_nullable"] == "YES",
            ordinal_position=int(row["ordinal_position"]),
            default=row["column_default"],
            is_identity=row["is_identity"] == "YES",
            is_generated=row["is_generated"] != "NEVER",
        )
        for row in column_rows
    ]
    pk_rows = _fetch_dicts(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_catalog = tc.constraint_catalog
         AND kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """,
        [schema, table],
    )
    return TableInfo(schema=schema, name=table, columns=columns, primary_key=[row["column_name"] for row in pk_rows])


def _display_columns(info: TableInfo) -> list[str]:
    priority = [
        "id",
        "brand_campaign_id",
        "campaign_id",
        "name",
        "title",
        "doctor_name",
        "state",
        "status",
        "_dq_status",
        "created_at",
        "updated_at",
    ]
    column_names = [column.name for column in info.columns]
    selected = [name for name in priority if name in column_names]
    for name in column_names:
        if name not in selected:
            selected.append(name)
        if len(selected) >= 12:
            break
    return selected


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _format_value(value: Any, limit: int = 120) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if len(text) > limit:
        return f"{text[:limit - 1]}..."
    return text


def _row_identity(info: TableInfo, row: dict[str, Any]) -> str:
    if info.primary_key:
        return ", ".join(f"{key}={_format_value(row.get(key), 48)}" for key in info.primary_key)
    return f"ctid={row.get('_row_ctid')}"


def _row_locator(info: TableInfo, row: dict[str, Any]) -> dict[str, Any]:
    if info.primary_key:
        return {
            "mode": "pk",
            "values": {key: "" if row.get(key) is None else str(row.get(key)) for key in info.primary_key},
        }
    return {"mode": "ctid", "ctid": str(row["_row_ctid"])}


def _sign_locator(info: TableInfo, row: dict[str, Any]) -> str:
    return signing.dumps(
        {"schema": info.schema, "table": info.name, "locator": _row_locator(info, row)},
        salt=ROW_TOKEN_SALT,
    )


def _load_locator(schema: str, table: str, token: str) -> dict[str, Any]:
    try:
        data = signing.loads(token, salt=ROW_TOKEN_SALT)
    except signing.BadSignature as exc:
        raise Http404("Invalid row locator.") from exc
    if data.get("schema") != schema or data.get("table") != table:
        raise Http404("Row locator does not match this table.")
    locator = data.get("locator")
    if not isinstance(locator, dict) or locator.get("mode") not in {"pk", "ctid"}:
        raise Http404("Row locator is malformed.")
    return locator


def _where_clause(locator: dict[str, Any]) -> tuple[sql.SQL, list[Any]]:
    if locator["mode"] == "ctid":
        return sql.SQL("ctid = %s::tid"), [locator["ctid"]]

    values = locator.get("values") or {}
    if not values:
        raise Http404("Primary key locator has no values.")
    clauses = [sql.SQL("{}::text = %s").format(sql.Identifier(column)) for column in values.keys()]
    return sql.SQL(" AND ").join(clauses), [str(value) for value in values.values()]


def _select_row(info: TableInfo, locator: dict[str, Any], lock: bool = False) -> dict[str, Any] | None:
    where_sql, params = _where_clause(locator)
    columns_sql = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
    query = sql.SQL("SELECT ctid::text AS _row_ctid, {} FROM {}.{} WHERE {} LIMIT 1").format(
        columns_sql,
        sql.Identifier(info.schema),
        sql.Identifier(info.name),
        where_sql,
    )
    if lock:
        query += sql.SQL(" FOR UPDATE")
    rows = _fetch_dicts(query, params)
    return rows[0] if rows else None


def _editable_columns(info: TableInfo) -> list[ColumnInfo]:
    return [
        column
        for column in info.columns
        if not column.is_identity and not column.is_generated and column.name not in info.primary_key
    ]


def _creatable_columns(info: TableInfo) -> list[ColumnInfo]:
    return [column for column in info.columns if not column.is_identity and not column.is_generated]


def _form_values(request: HttpRequest, columns: list[ColumnInfo]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for column in columns:
        if request.POST.get(f"__null__{column.name}") == "1":
            values[column.name] = None
        else:
            values[column.name] = request.POST.get(column.name, "")
    return values


def _audit(action: str, info: TableInfo, locator: dict[str, Any] | None, before: dict[str, Any] | None, after: dict[str, Any] | None, reason: str, actor: str) -> None:
    _ensure_audit_table()
    _execute(
        sql.SQL(
            """
            INSERT INTO {}.{}
              (action, schema_name, table_name, row_locator, before_data, after_data, reason, actor)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s)
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(AUDIT_TABLE)),
        [
            action,
            info.schema,
            info.name,
            json.dumps(_jsonable(locator or {})),
            json.dumps(_jsonable(before or {})),
            json.dumps(_jsonable(after or {})),
            reason,
            actor,
        ],
    )


def _foreign_key_dependencies(info: TableInfo, locator: dict[str, Any]) -> list[dict[str, Any]]:
    if locator.get("mode") != "pk":
        return []

    values = locator.get("values") or {}
    rows = _fetch_dicts(
        """
        SELECT
          tc.table_schema AS ref_schema,
          tc.table_name AS ref_table,
          kcu.column_name AS ref_column,
          ccu.column_name AS source_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_catalog = tc.constraint_catalog
         AND kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_catalog = tc.constraint_catalog
         AND ccu.constraint_schema = tc.constraint_schema
         AND ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_schema = %s
          AND ccu.table_name = %s
        """,
        [info.schema, info.name],
    )
    dependencies: list[dict[str, Any]] = []
    for row in rows:
        if not _is_managed_table(row["ref_schema"], row["ref_table"]):
            continue
        source_value = values.get(row["source_column"])
        if source_value is None:
            continue
        count = _count_column_value(row["ref_schema"], row["ref_table"], row["ref_column"], source_value)
        if count:
            dependencies.append({**row, "value": source_value, "count": count, "kind": "foreign key"})
    return dependencies


def _semantic_dependency_keys(info: TableInfo, row: dict[str, Any]) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    if info.name in {"campaign_campaign", "campaign_management_campaign"}:
        if row.get("id") is not None:
            keys.append(("campaign_id", str(row["id"])))
        if row.get("brand_campaign_id"):
            keys.append(("brand_campaign_id", str(row["brand_campaign_id"])))
    elif info.name == "collateral_management_collateral" and row.get("id") is not None:
        keys.append(("collateral_id", str(row["id"])))
    elif info.name == "campaign_fieldrep":
        if row.get("id") is not None:
            keys.append(("field_rep_id", str(row["id"])))
        if row.get("brand_supplied_field_rep_id"):
            keys.append(("field_rep_id", str(row["brand_supplied_field_rep_id"])))
    elif info.name == "doctor_viewer_doctor":
        if row.get("id") is not None:
            keys.append(("doctor_master_id_resolved", str(row["id"])))
    return list(dict.fromkeys(keys))


def _column_exists(schema: str, table: str, column: str) -> bool:
    rows = _fetch_dicts(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        [schema, table, column],
    )
    return bool(rows)


def _count_column_value(schema: str, table: str, column: str, value: Any) -> int:
    rows = _fetch_dicts(
        sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{} WHERE {}::text = %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
        ),
        [str(value)],
    )
    return int(rows[0]["row_count"])


def _semantic_dependencies(info: TableInfo, row: dict[str, Any]) -> list[dict[str, Any]]:
    keys = _semantic_dependency_keys(info, row)
    if not keys:
        return []

    dependencies: list[dict[str, Any]] = []
    for table_ref in _list_tables():
        schema = table_ref["schema"]
        table = table_ref["name"]
        if schema == info.schema and table == info.name:
            continue
        for column, value in keys:
            if not _column_exists(schema, table, column):
                continue
            count = _count_column_value(schema, table, column, value)
            if count:
                dependencies.append(
                    {
                        "ref_schema": schema,
                        "ref_table": table,
                        "ref_column": column,
                        "source_column": column,
                        "value": value,
                        "count": count,
                        "kind": "semantic reference",
                    }
                )
    return dependencies


def _delete_dependencies(info: TableInfo, row: dict[str, Any], locator: dict[str, Any]) -> list[dict[str, Any]]:
    return _foreign_key_dependencies(info, locator) + _semantic_dependencies(info, row)


def _group_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for table in tables:
        groups.setdefault(table["schema"], []).append(table)
    return [{"schema": schema, "tables": rows} for schema, rows in groups.items()]


def _system_cards(tables: list[dict[str, Any]], selected_system: str) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for profile in SYSTEM_PROFILES.values():
        profile_tables = [table for table in tables if table["system_key"] == profile.key]
        cards.append(
            {
                "profile": profile,
                "table_count": len(profile_tables),
                "row_count": sum(table["row_count"] or 0 for table in profile_tables),
                "is_selected": selected_system == profile.key,
                "href": f"{reverse('internal-data-admin-home')}?system={profile.key}",
            }
        )
    return cards


def _selected_system(request: HttpRequest) -> str:
    requested = (request.GET.get("system") or "all").strip().lower()
    if requested in SYSTEM_PROFILES or requested == "all":
        return requested
    return "all"


def _current_system_context(schema: str) -> dict[str, Any]:
    profile = _system_profile_for_schema(schema)
    return {
        "profile": profile,
        "layer": _layer_for_schema(schema),
        "home_href": f"{reverse('internal-data-admin-home')}?system={profile.key}",
    }


def _load_selected_rows(info: TableInfo, tokens: list[str]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for token in tokens:
        if not token or token in seen:
            continue
        seen.add(token)
        locator = _load_locator(info.schema, info.name, token)
        row = _select_row(info, locator)
        if not row:
            continue
        dependencies = _delete_dependencies(info, row, locator)
        selected.append(
            {
                "token": token,
                "locator": locator,
                "row": row,
                "identity": _row_identity(info, row),
                "dependencies": dependencies,
                "dependency_count": len(dependencies),
            }
        )
    return selected


def _bulk_delete_selected(info: TableInfo, selected_rows: list[dict[str, Any]], reason: str, actor: str) -> int:
    deleted_count = 0
    with transaction.atomic():
        for selected in selected_rows:
            before = _select_row(info, selected["locator"], lock=True)
            if not before:
                continue
            latest_dependencies = _delete_dependencies(info, before, selected["locator"])
            if latest_dependencies:
                raise DatabaseError(f"Delete blocked for {selected['identity']} because dependencies appeared while processing.")
            where_sql, where_params = _where_clause(selected["locator"])
            _execute(
                sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                    sql.Identifier(info.schema),
                    sql.Identifier(info.name),
                    where_sql,
                ),
                where_params,
            )
            _audit("bulk_delete", info, selected["locator"], before, None, reason, actor)
            deleted_count += 1
    return deleted_count


def _cleanup_system_options() -> list[dict[str, str]]:
    return [
        {
            "key": key,
            "label": SYSTEM_PROFILES[key].label,
            "summary": SYSTEM_PROFILES[key].cleanup_summary,
        }
        for key in CLEANUP_SYSTEM_KEYS
    ]


def _cleanup_layer_option_label(layer_key: str) -> str:
    for option in CLEANUP_LAYER_OPTIONS:
        if option["key"] == layer_key:
            return option["label"]
    return LAYER_LABELS.get(layer_key, layer_key.upper())


def _dedupe_text_values(values: list[Any] | tuple[Any, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _normalize_cleanup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _cleanup_value_variants(values: list[str]) -> list[str]:
    variants: list[str] = []
    for value in values:
        text = value.strip()
        if not text:
            continue
        compact = re.sub(r"\s+", "", text)
        underscored = re.sub(r"[\s-]+", "_", text)
        dashed = re.sub(r"[\s_]+", "-", text)
        variants.extend(
            [
                text,
                compact,
                underscored,
                dashed,
                text.replace("-", "_"),
                text.replace("_", "-"),
                _normalize_cleanup_key(text),
            ]
        )
    return [value.lower() for value in _dedupe_text_values(variants)]


def _text_match_condition(columns: list[str], match_values: list[str]) -> tuple[sql.SQL, list[Any]]:
    clauses = [
        sql.SQL("LOWER(BTRIM({}::text)) = ANY(%s)").format(sql.Identifier(column))
        for column in columns
    ]
    return sql.SQL("({})").format(sql.SQL(" OR ").join(clauses)), [match_values for _ in columns]


def _campaign_presence_condition(columns: list[str]) -> sql.SQL:
    clauses = [
        sql.SQL("NULLIF(BTRIM({}::text), '') IS NOT NULL").format(sql.Identifier(column))
        for column in columns
    ]
    return sql.SQL("({})").format(sql.SQL(" OR ").join(clauses))


def _registry_rows_for_entity(schema: str, table: str, match_values: list[str]) -> list[dict[str, Any]]:
    if not _table_exists(schema, table):
        return []
    info = _table_info(schema, table)
    column_names = [column.name for column in info.columns]
    match_columns = [column for column in REGISTRY_MATCH_COLUMNS if column in column_names]
    if not match_columns:
        return []

    select_columns = _dedupe_text_values(match_columns + [column for column in REGISTRY_SCHEMA_COLUMNS if column in column_names])
    where_sql, params = _text_match_condition(match_columns, match_values)
    return _fetch_dicts(
        sql.SQL("SELECT {} FROM {}.{} WHERE {} LIMIT 50").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in select_columns),
            sql.Identifier(schema),
            sql.Identifier(table),
            where_sql,
        ),
        params,
    )


def _campaign_gold_schema_prefixes(system_key: str) -> tuple[str, ...]:
    if system_key == "inclinic":
        return ("gold_campaign_",)
    if system_key == "pe":
        return ("gold_pe_campaign_",)
    return ()


def _matching_campaign_gold_schemas(system_key: str, values: list[str]) -> list[str]:
    prefixes = _campaign_gold_schema_prefixes(system_key)
    if not prefixes:
        return []

    normalized_values = {_normalize_cleanup_key(value) for value in values if value}
    rows = _fetch_dicts(
        """
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema
        """
    )
    schemas: list[str] = []
    for row in rows:
        schema = row["table_schema"]
        for prefix in prefixes:
            if not schema.startswith(prefix):
                continue
            suffix = schema[len(prefix):]
            if _normalize_cleanup_key(schema) in normalized_values or _normalize_cleanup_key(suffix) in normalized_values:
                schemas.append(schema)
    return schemas


def _cleanup_entity_scope(system_key: str, entity_key: str) -> dict[str, Any]:
    display_values = _dedupe_text_values([entity_key])
    scoped_gold_schemas: list[str] = []
    registry_sources: list[str] = []
    match_values = _cleanup_value_variants(display_values)

    for schema, table in REGISTRY_TABLES.get(system_key, []):
        rows = _registry_rows_for_entity(schema, table, match_values)
        if not rows:
            continue
        registry_sources.append(f"{schema}.{table}")
        for row in rows:
            for column, value in row.items():
                if value is None:
                    continue
                display_values = _dedupe_text_values(display_values + [value])
                if column in REGISTRY_SCHEMA_COLUMNS:
                    scoped_gold_schemas = _dedupe_text_values(scoped_gold_schemas + [value])
        match_values = _cleanup_value_variants(display_values)

    scoped_gold_schemas = _dedupe_text_values(
        scoped_gold_schemas + _matching_campaign_gold_schemas(system_key, display_values + scoped_gold_schemas)
    )
    display_values = _dedupe_text_values(display_values + scoped_gold_schemas)

    return {
        "display_values": display_values,
        "match_values": _cleanup_value_variants(display_values),
        "scoped_gold_schemas": scoped_gold_schemas,
        "registry_sources": registry_sources,
    }


def _cleanup_scope_for_keys(system_key: str, entity_keys: list[str]) -> dict[str, Any]:
    display_values: list[str] = []
    scoped_gold_schemas: list[str] = []
    registry_sources: list[str] = []
    for entity_key in entity_keys:
        scope = _cleanup_entity_scope(system_key, entity_key)
        display_values = _dedupe_text_values(display_values + scope["display_values"])
        scoped_gold_schemas = _dedupe_text_values(scoped_gold_schemas + scope["scoped_gold_schemas"])
        registry_sources = _dedupe_text_values(registry_sources + scope["registry_sources"])
    return {
        "display_values": display_values,
        "match_values": _cleanup_value_variants(display_values),
        "scoped_gold_schemas": scoped_gold_schemas,
        "registry_sources": registry_sources,
    }


def _cleanup_candidate_columns(info: TableInfo) -> list[str]:
    column_names = [column.name for column in info.columns]
    selected = [column for column in CLEANUP_MATCH_COLUMNS if column in column_names]

    for column in column_names:
        lowered = column.lower()
        if column in selected:
            continue
        if "campaign" in lowered and any(marker in lowered for marker in ("id", "key", "code", "uuid")):
            selected.append(column)

    campaign_table = "campaign" in info.name.lower() or info.name.lower() in {"campaign_registry", "dim_campaign"}
    if campaign_table:
        for column in ("underlying_key", "id"):
            if column in column_names and column not in selected:
                selected.append(column)

    return selected


def _cleanup_match_condition(info: TableInfo, scope: dict[str, Any]) -> dict[str, Any] | None:
    scoped_gold_schemas = set(scope["scoped_gold_schemas"])
    if info.schema in scoped_gold_schemas and info.schema.startswith(_campaign_gold_schema_prefixes(_system_key_for_schema(info.schema))):
        return {
            "where_sql": sql.SQL("TRUE"),
            "params": [],
            "match_columns": [],
            "scope_note": "entire matched campaign GOLD schema",
        }

    match_columns = _cleanup_candidate_columns(info)
    match_values = scope["match_values"]
    if not match_columns or not match_values:
        return None

    where_sql, params = _text_match_condition(match_columns, match_values)
    return {
        "where_sql": where_sql,
        "params": params,
        "match_columns": match_columns,
        "scope_note": "campaign/entity key columns",
    }


def _cleanup_inverse_match_condition(info: TableInfo, keep_scope: dict[str, Any]) -> dict[str, Any] | None:
    system_key = _system_key_for_schema(info.schema)
    prefixes = _campaign_gold_schema_prefixes(system_key)
    if prefixes and info.schema.startswith(prefixes):
        if info.schema in set(keep_scope["scoped_gold_schemas"]):
            return None
        return {
            "where_sql": sql.SQL("TRUE"),
            "params": [],
            "match_columns": [],
            "scope_note": "entire campaign GOLD schema not in keep list",
        }

    match_columns = _cleanup_candidate_columns(info)
    keep_values = keep_scope["match_values"]
    if not match_columns or not keep_values:
        return None

    protected_sql, protected_params = _text_match_condition(match_columns, keep_values)
    return {
        "where_sql": sql.SQL("{} AND NOT {}").format(_campaign_presence_condition(match_columns), protected_sql),
        "params": protected_params,
        "match_columns": match_columns,
        "scope_note": "campaign-scoped rows excluding keep list",
    }


def _cleanup_count(info: TableInfo, where_sql: sql.SQL, params: list[Any]) -> int:
    rows = _fetch_dicts(
        sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{} WHERE {}").format(
            sql.Identifier(info.schema),
            sql.Identifier(info.name),
            where_sql,
        ),
        params,
    )
    return int(rows[0]["row_count"])


def _cleanup_confirmation_phrase(system_key: str, entity_key: str) -> str:
    return f"CLEANUP {system_key.upper()} {entity_key.strip()}"


def _batch_cleanup_confirmation_phrase(mode: str, campaign_count: int) -> str:
    if mode == "keep_listed":
        return f"KEEP {campaign_count} CAMPAIGNS DELETE REST"
    return f"DELETE {campaign_count} LISTED CAMPAIGNS"


def _parse_campaign_ids(raw_value: str) -> list[str]:
    return _dedupe_text_values(re.split(r"[\n,;\t]+", raw_value or ""))


def _parse_raw_visibility_record_ids(raw_value: str) -> list[str]:
    return _dedupe_text_values(re.split(r"[\n,;\t]+", raw_value or ""))


def _parse_raw_visibility_table_ref(raw_value: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in (raw_value or "").split("||")]
    if len(parts) != 3 or not all(parts):
        raise ValueError("Select a system and RAW table.")
    return parts[0].lower(), parts[1], parts[2]


def _values_getlist(values: Any, key: str) -> list[str]:
    if hasattr(values, "getlist"):
        return values.getlist(key)
    raw = values.get(key) if hasattr(values, "get") else None
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    return [str(raw)]


def _selected_cleanup_systems(values: Any, fallback_system: str | None = None) -> list[str]:
    requested = [value.strip().lower() for value in _values_getlist(values, "systems") if value.strip()]
    if not requested:
        single_system = ((values.get("system") if hasattr(values, "get") else None) or fallback_system or "").strip().lower()
        if single_system in CLEANUP_SYSTEM_KEYS:
            requested = [single_system]
    if not requested or "all" in requested:
        return list(CLEANUP_SYSTEM_KEYS)
    return [system for system in CLEANUP_SYSTEM_KEYS if system in requested]


def _cleanup_plan(system_key: str, start_layer: str, entity_key: str) -> dict[str, Any]:
    entity_key = entity_key.strip()
    if system_key not in CLEANUP_SYSTEM_KEYS:
        raise ValueError("Choose Inclinic, SAPA/RFA, or Patient Education before planning cleanup.")
    if start_layer not in LAYER_ORDER:
        raise ValueError("Choose a valid starting layer for cleanup.")
    if not entity_key:
        raise ValueError("Enter a campaign, schema, or source entity key before planning cleanup.")

    start_rank = LAYER_ORDER[start_layer]
    scope = _cleanup_entity_scope(system_key, entity_key)
    plan_rows: list[dict[str, Any]] = []

    for ref in _managed_table_refs():
        schema = ref["schema"]
        if _system_key_for_schema(schema) != system_key:
            continue
        layer_key = _layer_key_for_schema(schema)
        layer_rank = LAYER_ORDER.get(layer_key)
        if layer_rank is None or layer_rank < start_rank:
            continue

        info = _table_info(schema, ref["name"])
        match = _cleanup_match_condition(info, scope)
        if not match:
            continue

        count = _cleanup_count(info, match["where_sql"], match["params"])
        if count <= 0:
            continue

        plan_rows.append(
            {
                "schema": info.schema,
                "table": info.name,
                "layer_key": layer_key,
                "layer_label": LAYER_LABELS[layer_key],
                "delete_rank": layer_rank,
                "count": count,
                "match_columns": ", ".join(match["match_columns"]) if match["match_columns"] else "entire table",
                "scope_note": match["scope_note"],
                "where_sql": match["where_sql"],
                "params": match["params"],
            }
        )

    plan_rows.sort(key=lambda row: (-row["delete_rank"], row["schema"], row["table"]))
    layer_totals = []
    for layer_key in ("gold", "silver", "bronze", "raw"):
        total = sum(row["count"] for row in plan_rows if row["layer_key"] == layer_key)
        if total:
            layer_totals.append({"label": LAYER_LABELS[layer_key], "count": total})

    return {
        "system_key": system_key,
        "system_label": SYSTEM_PROFILES[system_key].label,
        "start_layer": start_layer,
        "start_layer_label": _cleanup_layer_option_label(start_layer),
        "entity_key": entity_key,
        "key_values": scope["display_values"],
        "scoped_gold_schemas": scope["scoped_gold_schemas"],
        "registry_sources": scope["registry_sources"],
        "rows": plan_rows,
        "table_count": len(plan_rows),
        "total_count": sum(row["count"] for row in plan_rows),
        "layer_totals": layer_totals,
    }


def _cleanup_rows_for_scope(system_keys: list[str], start_layer: str, mode: str, scope_by_system: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    start_rank = LAYER_ORDER[start_layer]
    plan_rows: list[dict[str, Any]] = []

    for ref in _managed_table_refs():
        schema = ref["schema"]
        system_key = _system_key_for_schema(schema)
        if system_key not in system_keys:
            continue
        layer_key = _layer_key_for_schema(schema)
        layer_rank = LAYER_ORDER.get(layer_key)
        if layer_rank is None or layer_rank < start_rank:
            continue

        info = _table_info(schema, ref["name"])
        if mode == "keep_listed":
            match = _cleanup_inverse_match_condition(info, scope_by_system[system_key])
        else:
            match = _cleanup_match_condition(info, scope_by_system[system_key])
        if not match:
            continue

        count = _cleanup_count(info, match["where_sql"], match["params"])
        if count <= 0:
            continue

        plan_rows.append(
            {
                "system_key": system_key,
                "system_label": SYSTEM_PROFILES[system_key].short_label,
                "schema": info.schema,
                "table": info.name,
                "layer_key": layer_key,
                "layer_label": LAYER_LABELS[layer_key],
                "delete_rank": layer_rank,
                "count": count,
                "match_columns": ", ".join(match["match_columns"]) if match["match_columns"] else "entire table",
                "scope_note": match["scope_note"],
                "where_sql": match["where_sql"],
                "params": match["params"],
            }
        )

    plan_rows.sort(key=lambda row: (-row["delete_rank"], row["system_label"], row["schema"], row["table"]))
    return plan_rows


def _batch_cleanup_plan(system_keys: list[str], start_layer: str, campaign_ids: list[str], mode: str) -> dict[str, Any]:
    if not system_keys:
        raise ValueError("Choose at least one reporting system for batch cleanup.")
    if start_layer not in LAYER_ORDER:
        raise ValueError("Choose a valid starting layer for batch cleanup.")
    if mode not in CLEANUP_BATCH_MODES:
        raise ValueError("Choose whether the listed campaigns should be deleted or kept.")
    if not campaign_ids:
        raise ValueError("Enter at least one campaign ID before planning batch cleanup.")

    scope_by_system = {system_key: _cleanup_scope_for_keys(system_key, campaign_ids) for system_key in system_keys}
    plan_rows = _cleanup_rows_for_scope(system_keys, start_layer, mode, scope_by_system)
    layer_totals = []
    for layer_key in ("gold", "silver", "bronze", "raw"):
        total = sum(row["count"] for row in plan_rows if row["layer_key"] == layer_key)
        if total:
            layer_totals.append({"label": LAYER_LABELS[layer_key], "count": total})

    system_totals = []
    for system_key in system_keys:
        total = sum(row["count"] for row in plan_rows if row["system_key"] == system_key)
        if total:
            system_totals.append({"label": SYSTEM_PROFILES[system_key].short_label, "count": total})

    scoped_gold_schemas: list[str] = []
    registry_sources: list[str] = []
    key_values: list[str] = []
    for scope in scope_by_system.values():
        scoped_gold_schemas = _dedupe_text_values(scoped_gold_schemas + scope["scoped_gold_schemas"])
        registry_sources = _dedupe_text_values(registry_sources + scope["registry_sources"])
        key_values = _dedupe_text_values(key_values + scope["display_values"])

    return {
        "mode": mode,
        "mode_label": CLEANUP_BATCH_MODES[mode]["label"],
        "mode_summary": CLEANUP_BATCH_MODES[mode]["summary"],
        "system_keys": system_keys,
        "system_labels": [SYSTEM_PROFILES[system_key].short_label for system_key in system_keys],
        "start_layer": start_layer,
        "start_layer_label": _cleanup_layer_option_label(start_layer),
        "campaign_ids": campaign_ids,
        "campaign_ids_text": "\n".join(campaign_ids),
        "key_values": key_values,
        "scoped_gold_schemas": scoped_gold_schemas,
        "registry_sources": registry_sources,
        "rows": plan_rows,
        "table_count": len(plan_rows),
        "total_count": sum(row["count"] for row in plan_rows),
        "layer_totals": layer_totals,
        "system_totals": system_totals,
    }


def _execute_hierarchy_cleanup(plan: dict[str, Any], reason: str, actor: str) -> dict[str, Any]:
    deleted_rows: list[dict[str, Any]] = []
    with transaction.atomic():
        for row in plan["rows"]:
            info = TableInfo(schema=row["schema"], name=row["table"], columns=[], primary_key=[])
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                        sql.Identifier(row["schema"]),
                        sql.Identifier(row["table"]),
                        row["where_sql"],
                    ),
                    row["params"],
                )
                deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0

            locator = {
                "mode": "hierarchy_cleanup",
                "system": plan.get("system_key") or ",".join(plan.get("system_keys", [])),
                "start_layer": plan["start_layer"],
                "entity_key": plan.get("entity_key"),
                "campaign_ids": plan.get("campaign_ids", []),
                "cleanup_mode": plan.get("mode", "single_entity"),
            }
            before = {
                "planned_count": row["count"],
                "deleted_count": deleted_count,
                "layer": row["layer_label"],
                "match_columns": row["match_columns"],
                "scope_note": row["scope_note"],
                "key_values": plan["key_values"],
                "scoped_gold_schemas": plan["scoped_gold_schemas"],
                "system": row.get("system_label"),
                "cleanup_mode": plan.get("mode", "single_entity"),
            }
            _audit("hierarchy_cleanup", info, locator, before, None, reason, actor)
            deleted_rows.append({**row, "deleted_count": deleted_count})

    return {
        "deleted_count": sum(row["deleted_count"] for row in deleted_rows),
        "table_count": len(deleted_rows),
        "rows": deleted_rows,
    }


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_login(request: HttpRequest) -> HttpResponse:
    if _is_authenticated(request):
        return redirect("internal-data-admin-home")

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        config = _admin_config()
        if constant_time_compare(username, config.get("USERNAME", "")) and constant_time_compare(password, config.get("PASSWORD", "")):
            request.session[SESSION_KEY] = True
            request.session[SESSION_USER_KEY] = username
            return redirect(request.GET.get("next") or "internal-data-admin-home")
        error_message = "Invalid internal dashboard credentials."

    return render(request, "dashboard/internal_data_admin/login.html", {"error_message": error_message})


@never_cache
def internal_data_admin_logout(request: HttpRequest) -> HttpResponse:
    request.session.pop(SESSION_KEY, None)
    request.session.pop(SESSION_USER_KEY, None)
    return redirect("internal-data-admin-login")


@never_cache
def internal_data_admin_home(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    _ensure_audit_table()
    all_tables = _list_tables()
    selected_system = _selected_system(request)
    visible_tables = (
        all_tables
        if selected_system == "all"
        else [table for table in all_tables if table["system_key"] == selected_system]
    )
    selected_profile = SYSTEM_PROFILES.get(selected_system)
    return render(
        request,
        "dashboard/internal_data_admin/home.html",
        {
            "system_cards": _system_cards(all_tables, selected_system),
            "selected_system": selected_system,
            "selected_profile": selected_profile,
            "table_groups": _group_tables(visible_tables),
            "actor": request.session.get(SESSION_USER_KEY, "internal_admin"),
        },
    )


@never_cache
def internal_data_admin_raw_downloads(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    selected_system = _selected_system(request)
    all_cards = _raw_summary_cards()
    visible_cards = (
        all_cards
        if selected_system == "all"
        else [card for card in all_cards if card["system_key"] == selected_system]
    )
    return render(
        request,
        "dashboard/internal_data_admin/raw_downloads.html",
        {
            "summary_cards": visible_cards,
            "totals": _raw_summary_totals(visible_cards),
            "system_cards": _raw_system_cards(all_cards, selected_system),
            "selected_system": selected_system,
        },
    )


@never_cache
def internal_data_admin_raw_download(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _raw_table_info(schema, table)
    response = StreamingHttpResponse(
        _stream_table_csv(info),
        content_type="text/csv; charset=utf-8",
    )
    response["Content-Disposition"] = f'attachment; filename="{_raw_export_filename(schema, table)}"'
    response["X-Content-Type-Options"] = "nosniff"
    return response


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_raw_dedupe(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    values = request.POST if request.method == "POST" else request.GET
    selected_system = (values.get("system") or "inclinic").strip().lower()
    if selected_system not in {*CLEANUP_SYSTEM_KEYS, "all"}:
        selected_system = "inclinic"

    result = None
    phrase = _raw_dedupe_confirmation_phrase(selected_system)
    plan = _raw_dedupe_plan(selected_system)
    report_snapshot = _raw_dedupe_report_snapshot(selected_system)

    if request.method == "POST":
        dedupe_action = request.POST.get("dedupe_action") or "preview"
        if dedupe_action == "execute_next_batch":
            confirmation = (request.POST.get("confirmation") or "").strip()
            reason = (request.POST.get("reason") or "").strip()
            if plan["has_errors"]:
                return _raw_dedupe_json_error("RAW dedupe is blocked until every RAW table in the selected scope can be inspected.")
            if confirmation != phrase:
                return _raw_dedupe_json_error(f'Type "{phrase}" to confirm RAW archive and dedupe.')
            if len(reason) < 8:
                return _raw_dedupe_json_error("Please provide a clear reason before archiving and removing RAW duplicates.")
            target_row = _raw_dedupe_next_row(plan)
            if not target_row:
                return JsonResponse(
                    {
                        "ok": True,
                        "complete": True,
                        "message": "No duplicate source rows remain in this scope.",
                        "remaining_duplicate_rows": 0,
                        "remaining_duplicate_tables": 0,
                    }
                )
            target_schema = target_row["schema"]
            target_table = target_row["table"]
            try:
                result = _execute_raw_dedupe(
                    selected_system,
                    reason,
                    request.session.get(SESSION_USER_KEY, "internal_admin"),
                    target=(target_schema, target_table),
                    max_rows=RAW_DEDUPE_BATCH_SIZE,
                )
                plan = _raw_dedupe_plan(selected_system)
                return JsonResponse(
                    {
                        "ok": True,
                        "complete": plan["duplicate_row_count"] <= 0,
                        "run_id": result["run_id"],
                        "table": f"{target_schema}.{target_table}",
                        "deleted_count": result["deleted_count"],
                        "validation_passed": result["validation"]["passed"],
                        "remaining_duplicate_rows": plan["duplicate_row_count"],
                        "remaining_duplicate_tables": plan["duplicate_table_count"],
                        "message": (
                            f"Archived and removed {result['deleted_count']} rows from "
                            f"{target_schema}.{target_table}."
                        ),
                    }
                )
            except Exception as exc:
                return _raw_dedupe_json_error(f"RAW dedupe failed and was rolled back: {exc}", status=500)

        if dedupe_action == "execute":
            messages.error(request, "Full-scope browser execution is disabled. Run one RAW table batch at a time to avoid web timeouts.")
        if dedupe_action == "execute_table":
            confirmation = (request.POST.get("confirmation") or "").strip()
            reason = (request.POST.get("reason") or "").strip()
            target_schema = (request.POST.get("target_schema") or "").strip()
            target_table = (request.POST.get("target_table") or "").strip()
            target_ref = (request.POST.get("target_table_ref") or "").strip()
            if (not target_schema or not target_table) and "." in target_ref:
                target_schema, target_table = target_ref.split(".", 1)
            if plan["duplicate_row_count"] <= 0:
                messages.error(request, "RAW dedupe is blocked because the dry-run found no duplicate source rows.")
            elif plan["has_errors"]:
                messages.error(request, "RAW dedupe is blocked until every RAW table in the selected scope can be inspected.")
            elif not _raw_dedupe_target_allowed(selected_system, target_schema, target_table):
                messages.error(request, "Choose a RAW table from the current dry-run scope before archiving duplicates.")
            elif confirmation != phrase:
                messages.error(request, f'Type "{phrase}" to confirm RAW archive and dedupe.')
            elif len(reason) < 8:
                messages.error(request, "Please provide a clear reason before archiving and removing RAW duplicates.")
            else:
                try:
                    result = _execute_raw_dedupe(
                        selected_system,
                        reason,
                        request.session.get(SESSION_USER_KEY, "internal_admin"),
                        target=(target_schema, target_table),
                        max_rows=RAW_DEDUPE_BATCH_SIZE,
                    )
                    messages.success(
                        request,
                        f"RAW dedupe archived and removed {result['deleted_count']} duplicate rows from {target_schema}.{target_table}.",
                    )
                    plan = _raw_dedupe_plan(selected_system)
                    report_snapshot = _raw_dedupe_report_snapshot(selected_system)
                except Exception as exc:
                    messages.error(request, f"RAW dedupe failed and was rolled back: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/raw_dedupe.html",
        {
            "system_options": _raw_dedupe_system_options(selected_system),
            "selected_system": selected_system,
            "plan": plan,
            "report_snapshot": report_snapshot,
            "phrase": phrase,
            "result": result,
            "batch_size": RAW_DEDUPE_BATCH_SIZE,
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_transfer_cleanup(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    _transfer_cleanup_assert_scope()

    selected_domain = (request.GET.get("domain") or request.POST.get("domain") or "all").strip().lower()
    selected_source_table = (request.GET.get("source_table") or request.POST.get("source_table") or "all").strip()
    selected_cleanup_status = (request.GET.get("cleanup_status") or request.POST.get("cleanup_status") or "all").strip()
    search_query = (request.GET.get("q") or request.POST.get("q") or "").strip()
    run_id_filter = (request.GET.get("run_id") or request.POST.get("run_id") or "").strip()
    result = None

    try:
        run_domains = _transfer_cleanup_domains(selected_domain)
    except ValueError as exc:
        messages.error(request, str(exc))
        selected_domain = "all"
        run_domains = ["inclinic", "rfa"]

    confirmation_phrase = _transfer_cleanup_confirmation(selected_domain)

    if request.method == "POST" and (request.POST.get("transfer_action") or "") == "run_transfer_cleanup":
        confirmation = (request.POST.get("confirmation") or "").strip()
        if confirmation != confirmation_phrase:
            messages.error(request, f"Type the exact confirmation phrase: {confirmation_phrase}")
        else:
            try:
                result = run_weekly_transfer_cleanup(run_domains)
                status = result.get("status", "UNKNOWN")
                cleanup_run_id = result.get("cleanup_run_id")
                if status == "SUCCESS":
                    messages.success(request, f"ETL + source cleanup completed for cleanup_run_id={cleanup_run_id}.")
                elif status == "PARTIAL_SUCCESS":
                    messages.warning(
                        request,
                        f"ETL + source cleanup completed with partial success for cleanup_run_id={cleanup_run_id}.",
                    )
                else:
                    messages.error(request, f"ETL + source cleanup failed for cleanup_run_id={cleanup_run_id}.")
            except Exception as exc:
                messages.error(request, f"ETL + source cleanup could not run: {exc}")

    try:
        records = _transfer_cleanup_raw_records(
            selected_domain=selected_domain,
            source_table=selected_source_table,
            search=search_query,
            cleanup_status=selected_cleanup_status,
            run_id=run_id_filter,
        )
    except ValueError as exc:
        messages.error(request, str(exc))
        selected_source_table = "all"
        records = _transfer_cleanup_raw_records(
            selected_domain=selected_domain,
            source_table=selected_source_table,
            search=search_query,
            cleanup_status=selected_cleanup_status,
            run_id=run_id_filter,
        )

    protected_specs = _transfer_cleanup_specs(selected_domain, "all")
    protected_source_tables = sorted({spec.source_table for spec in protected_specs})
    protected_raw_table_values = {f"{spec.raw_schema}.{spec.raw_table}" for spec in protected_specs}
    if "inclinic" in run_domains:
        protected_raw_table_values.add(f"{INCLINIC_TRANSFER_CLEANUP_V2_RAW_SCHEMA}.{INCLINIC_TRANSFER_CLEANUP_V2_RAW_TABLE}")
    if "rfa" in run_domains:
        protected_raw_table_values.add(f"{RFA_TRANSFER_CLEANUP_V2_RAW_SCHEMA}.{RFA_TRANSFER_CLEANUP_V2_RAW_TABLE}")
    protected_raw_tables = sorted(protected_raw_table_values)

    return render(
        request,
        "dashboard/internal_data_admin/rfa_transfer_cleanup.html",
        {
            "confirmation_phrase": confirmation_phrase,
            "domain_options": _transfer_cleanup_domain_options(selected_domain),
            "table_options": _transfer_cleanup_table_options(selected_domain, selected_source_table),
            "status_options": _transfer_cleanup_status_options(selected_cleanup_status),
            "selected_domain": selected_domain,
            "selected_domain_label": TRANSFER_CLEANUP_DOMAIN_LABELS.get(selected_domain, TRANSFER_CLEANUP_DOMAIN_LABELS["all"]),
            "selected_source_table": selected_source_table,
            "selected_cleanup_status": selected_cleanup_status,
            "search_query": search_query,
            "run_id_filter": run_id_filter,
            "summary": _transfer_cleanup_raw_summary(selected_domain),
            "records": records,
            "recent_runs": _transfer_cleanup_recent_runs(selected_domain),
            "result": result,
            "protected_source_tables": protected_source_tables,
            "protected_raw_tables": protected_raw_tables,
            "domain_notes": [TRANSFER_CLEANUP_DOMAIN_NOTES[domain] for domain in run_domains],
        },
    )


def internal_data_admin_rfa_transfer_cleanup(request: HttpRequest) -> HttpResponse:
    return internal_data_admin_transfer_cleanup(request)


def _bulk_reporting_correction_rows(text: str) -> list[dict[str, str]]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    reader = csv.DictReader(io.StringIO(cleaned))
    if not reader.fieldnames:
        raise ValueError("CSV header row is required.")
    rows: list[dict[str, str]] = []
    for index, row in enumerate(reader, start=2):
        normalized = {str(key or "").strip(): str(value or "").strip() for key, value in row.items()}
        if not any(normalized.values()):
            continue
        normalized["_line_number"] = str(index)
        rows.append(normalized)
    return rows


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_corrections(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    ensure_reporting_correction_table()
    actor = request.session.get(SESSION_USER_KEY, "internal_admin")

    if request.method == "POST":
        action = request.POST.get("correction_action") or "add"
        if action == "deactivate":
            correction_id = (request.POST.get("correction_id") or "").strip()
            if deactivate_reporting_correction_rule(correction_id):
                messages.success(request, "Reporting correction rule deactivated. Rerun ETL to refresh derived counts.")
            else:
                messages.error(request, "Correction rule was not found.")
            return redirect("internal-data-admin-corrections")

        if action == "bulk_add":
            try:
                rows = _bulk_reporting_correction_rows(request.POST.get("bulk_csv") or "")
                created = 0
                for row in rows:
                    create_reporting_correction_rule(
                        rule_type=row.get("rule_type", ""),
                        campaign_id=row.get("campaign_id", ""),
                        doctor_phone=row.get("doctor_phone", ""),
                        doctor_name=row.get("doctor_name", ""),
                        field_rep_brand_supplied_id=row.get("field_rep_brand_supplied_id", ""),
                        expected_field_rep_brand_supplied_id=row.get("expected_field_rep_brand_supplied_id", ""),
                        affected_field_rep_brand_supplied_ids=row.get("affected_field_rep_brand_supplied_ids", ""),
                        reason=row.get("reason", "") or f"Reporting correction bulk import line {row.get('_line_number')}",
                        created_by=actor,
                    )
                    created += 1
                messages.success(request, f"Created {created} reporting correction rule{'' if created == 1 else 's'}. Rerun ETL to refresh derived counts.")
                return redirect("internal-data-admin-corrections")
            except Exception as exc:
                messages.error(request, f"Bulk correction import failed: {exc}")

        if action == "add":
            try:
                create_reporting_correction_rule(
                    rule_type=request.POST.get("rule_type", ""),
                    campaign_id=request.POST.get("campaign_id", ""),
                    doctor_phone=request.POST.get("doctor_phone", ""),
                    doctor_name=request.POST.get("doctor_name", ""),
                    field_rep_brand_supplied_id=request.POST.get("field_rep_brand_supplied_id", ""),
                    expected_field_rep_brand_supplied_id=request.POST.get("expected_field_rep_brand_supplied_id", ""),
                    affected_field_rep_brand_supplied_ids=request.POST.get("affected_field_rep_brand_supplied_ids", ""),
                    reason=request.POST.get("reason", ""),
                    created_by=actor,
                )
                messages.success(request, "Reporting correction rule created. Rerun ETL to refresh derived counts.")
                return redirect("internal-data-admin-corrections")
            except Exception as exc:
                messages.error(request, f"Correction rule could not be created: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/corrections.html",
        {
            "rule_types": REPORTING_CORRECTION_RULE_TYPES,
            "rules": list_reporting_correction_rules(include_inactive=True),
            "sample_csv": (
                "rule_type,campaign_id,doctor_phone,doctor_name,field_rep_brand_supplied_id,"
                "expected_field_rep_brand_supplied_id,affected_field_rep_brand_supplied_ids,reason\n"
                f"{RULE_KEEP_DOCTOR_WITH_REP},83ce7fc7c965433ab2b9717394abe3c1,7086179396,SUMEET KR BAKALI,,1451,\"2731\",\"Brand confirmed doctor should stay with ASM 1451\"\n"
                f"{RULE_EXCLUDE_INVALID_PHONE},83ce7fc7c965433ab2b9717394abe3c1,964512884,Dr.J Prakash,10340,,,\"Brand confirmed invalid doctor phone\"\n"
            ),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_privacy(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    ensure_campaign_privacy_table()
    actor = request.session.get(SESSION_USER_KEY, "internal_admin")

    if request.method == "POST":
        action = request.POST.get("privacy_action") or "add"
        if action == "deactivate_raw_visibility":
            rule_id = (request.POST.get("rule_id") or "").strip()
            if deactivate_raw_visibility_rule(rule_id):
                messages.success(request, "RAW visibility rule deactivated. Rerun ETL to refresh derived dashboards.")
            else:
                messages.error(request, "RAW visibility rule was not found.")
            return redirect("internal-data-admin-privacy")

        if action == "deactivate_person":
            rule_id = (request.POST.get("rule_id") or "").strip()
            if deactivate_person_privacy_rule(rule_id):
                messages.success(request, "Person visibility rule deactivated. Rerun ETL to refresh derived dashboards.")
            else:
                messages.error(request, "Person visibility rule was not found.")
            return redirect("internal-data-admin-privacy")

        if action == "deactivate":
            rule_id = (request.POST.get("rule_id") or "").strip()
            if deactivate_campaign_privacy_allowlist_rule(rule_id):
                messages.success(request, "Campaign privacy allowlist entry deactivated. Rerun ETL to refresh derived dashboards.")
            else:
                messages.error(request, "Campaign privacy allowlist entry was not found.")
            return redirect("internal-data-admin-privacy")

        if action == "add_person":
            try:
                create_person_privacy_rule(
                    person_label=request.POST.get("person_label", ""),
                    email=request.POST.get("email", ""),
                    phone=request.POST.get("phone", ""),
                    allowed_campaign_id=request.POST.get("allowed_campaign_id", ""),
                    reason=request.POST.get("reason", ""),
                    created_by=actor,
                )
                messages.success(request, "Person visibility rule created. Rerun ETL to refresh derived dashboards.")
                return redirect("internal-data-admin-privacy")
            except Exception as exc:
                messages.error(request, f"Person visibility rule could not be created: {exc}")

        if action == "add_raw_visibility":
            try:
                system_key, schema_name, table_name = _parse_raw_visibility_table_ref(request.POST.get("table_ref", ""))
                record_ids = _parse_raw_visibility_record_ids(request.POST.get("record_identifiers", ""))
                rule_mode = (request.POST.get("rule_mode") or "hide").strip()
                if not record_ids:
                    raise ValueError("Enter at least one record identifier.")
                with transaction.atomic():
                    for record_identifier in record_ids:
                        create_raw_visibility_rule(
                            system_key=system_key,
                            schema_name=schema_name,
                            table_name=table_name,
                            identifier_column=request.POST.get("identifier_column", ""),
                            record_identifier=record_identifier,
                            rule_mode=rule_mode,
                            reason=request.POST.get("reason", ""),
                            created_by=actor,
                        )
                action_label = "show-only" if rule_mode == "keep_only" else "hide"
                messages.success(
                    request,
                    f"Created {len(record_ids)} RAW {action_label} visibility rule{'' if len(record_ids) == 1 else 's'}. Rerun ETL to refresh derived dashboards.",
                )
                return redirect("internal-data-admin-privacy")
            except Exception as exc:
                messages.error(request, f"RAW visibility rule could not be created: {exc}")

        if action == "add":
            try:
                create_campaign_privacy_allowlist_rule(
                    campaign_id=request.POST.get("campaign_id", ""),
                    reason=request.POST.get("reason", ""),
                    created_by=actor,
                )
                messages.success(request, "Campaign privacy allowlist entry created. Rerun ETL to refresh derived dashboards.")
                return redirect("internal-data-admin-privacy")
            except Exception as exc:
                messages.error(request, f"Campaign privacy entry could not be created: {exc}")

    rules = list_campaign_privacy_allowlist_rules(include_inactive=True)
    active_rules = [rule for rule in rules if rule.get("is_active")]
    person_rules = list_person_privacy_rules(include_inactive=True)
    active_person_rules = [rule for rule in person_rules if rule.get("is_active")]
    raw_visibility_rules = list_raw_visibility_rules(include_inactive=True)
    active_raw_visibility_rules = [rule for rule in raw_visibility_rules if rule.get("is_active")]
    return render(
        request,
        "dashboard/internal_data_admin/privacy.html",
        {
            "rules": rules,
            "active_rules": active_rules,
            "person_rules": person_rules,
            "active_person_rules": active_person_rules,
            "raw_visibility_rules": raw_visibility_rules,
            "active_raw_visibility_rules": active_raw_visibility_rules,
            "raw_visibility_table_options": list_raw_visibility_table_options(),
            "is_allowlist_active": bool(active_rules),
            "is_person_privacy_active": bool(active_person_rules),
            "is_raw_visibility_active": bool(active_raw_visibility_rules),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_cleanup(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    _ensure_audit_table()
    values = request.POST if request.method == "POST" else request.GET
    requested_system = (values.get("system") or "").strip().lower()
    selected_system = requested_system or "inclinic"
    if selected_system not in CLEANUP_SYSTEM_KEYS:
        selected_system = "inclinic"
    selected_start_layer = (values.get("start_layer") or "raw").strip().lower()
    if selected_start_layer not in LAYER_ORDER:
        selected_start_layer = "raw"
    entity_key = (values.get("entity_key") or "").strip()
    batch_fallback_system = requested_system if requested_system in CLEANUP_SYSTEM_KEYS else None
    batch_selected_systems = _selected_cleanup_systems(values, batch_fallback_system)
    batch_start_layer = (values.get("batch_start_layer") or values.get("start_layer") or "raw").strip().lower()
    if batch_start_layer not in LAYER_ORDER:
        batch_start_layer = "raw"
    batch_mode = (values.get("batch_mode") or "delete_listed").strip().lower()
    if batch_mode not in CLEANUP_BATCH_MODES:
        batch_mode = "delete_listed"
    batch_campaign_ids_text = values.get("campaign_ids") or ""
    batch_campaign_ids = _parse_campaign_ids(batch_campaign_ids_text)

    plan = None
    phrase = _cleanup_confirmation_phrase(selected_system, entity_key) if entity_key else ""
    batch_plan = None
    batch_phrase = _batch_cleanup_confirmation_phrase(batch_mode, len(batch_campaign_ids)) if batch_campaign_ids else ""
    if request.method == "POST":
        cleanup_action = request.POST.get("cleanup_action") or "preview"
        if cleanup_action in {"preview", "execute"}:
            try:
                plan = _cleanup_plan(selected_system, selected_start_layer, entity_key)
                phrase = _cleanup_confirmation_phrase(selected_system, entity_key)
            except (DatabaseError, ValueError) as exc:
                messages.error(request, f"Cleanup plan failed: {exc}")
                plan = None

            if cleanup_action == "execute" and plan:
                confirmation = (request.POST.get("confirmation") or "").strip()
                reason = (request.POST.get("reason") or "").strip()
                if plan["total_count"] <= 0:
                    messages.error(request, "Cleanup execution blocked because the current plan has no matching records.")
                elif confirmation != phrase:
                    messages.error(request, f'Type "{phrase}" to confirm hierarchy cleanup.')
                elif len(reason) < 8:
                    messages.error(request, "Please provide a clear reason before running hierarchy cleanup.")
                else:
                    try:
                        result = _execute_hierarchy_cleanup(
                            plan,
                            reason,
                            request.session.get(SESSION_USER_KEY, "internal_admin"),
                        )
                        messages.success(
                            request,
                            f"Hierarchy cleanup deleted {result['deleted_count']} records across {result['table_count']} tables.",
                        )
                        return redirect(
                            f"{reverse('internal-data-admin-cleanup')}?system={selected_system}&start_layer={selected_start_layer}"
                        )
                    except DatabaseError as exc:
                        messages.error(request, f"Hierarchy cleanup failed and was rolled back: {exc}")

        if cleanup_action in {"batch_preview", "batch_execute"}:
            try:
                batch_plan = _batch_cleanup_plan(
                    batch_selected_systems,
                    batch_start_layer,
                    batch_campaign_ids,
                    batch_mode,
                )
                batch_phrase = _batch_cleanup_confirmation_phrase(batch_mode, len(batch_campaign_ids))
            except (DatabaseError, ValueError) as exc:
                messages.error(request, f"Batch cleanup plan failed: {exc}")
                batch_plan = None

            if cleanup_action == "batch_execute" and batch_plan:
                confirmation = (request.POST.get("confirmation") or "").strip()
                reason = (request.POST.get("reason") or "").strip()
                if batch_plan["total_count"] <= 0:
                    messages.error(request, "Batch cleanup execution blocked because the current plan has no matching records.")
                elif confirmation != batch_phrase:
                    messages.error(request, f'Type "{batch_phrase}" to confirm batch cleanup.')
                elif len(reason) < 8:
                    messages.error(request, "Please provide a clear reason before running batch cleanup.")
                else:
                    try:
                        result = _execute_hierarchy_cleanup(
                            batch_plan,
                            reason,
                            request.session.get(SESSION_USER_KEY, "internal_admin"),
                        )
                        messages.success(
                            request,
                            f"Batch cleanup deleted {result['deleted_count']} records across {result['table_count']} tables.",
                        )
                        return redirect(
                            f"{reverse('internal-data-admin-cleanup')}?start_layer={batch_start_layer}"
                        )
                    except DatabaseError as exc:
                        messages.error(
                            request,
                            f"Batch cleanup failed and was rolled back: {exc}",
                        )

    return render(
        request,
        "dashboard/internal_data_admin/cleanup.html",
        {
            "system_options": _cleanup_system_options(),
            "layer_options": CLEANUP_LAYER_OPTIONS,
            "batch_mode_options": [
                {"key": key, **value}
                for key, value in CLEANUP_BATCH_MODES.items()
            ],
            "selected_system": selected_system,
            "selected_start_layer": selected_start_layer,
            "entity_key": entity_key,
            "plan": plan,
            "phrase": phrase,
            "batch_selected_systems": batch_selected_systems,
            "batch_start_layer": batch_start_layer,
            "batch_mode": batch_mode,
            "batch_campaign_ids_text": batch_campaign_ids_text,
            "batch_plan": batch_plan,
            "batch_phrase": batch_phrase,
        },
    )


@never_cache
def internal_data_admin_table(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    page = max(int(request.GET.get("page") or "1"), 1)
    offset = (page - 1) * PAGE_SIZE
    display_columns = _display_columns(info)
    columns_sql = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
    if info.primary_key:
        order_sql = sql.SQL(" ORDER BY {}").format(sql.SQL(", ").join(sql.Identifier(column) for column in info.primary_key))
    else:
        order_sql = sql.SQL(" ORDER BY ctid DESC")
    rows = _fetch_dicts(
        sql.SQL("SELECT ctid::text AS _row_ctid, {} FROM {}.{}{} LIMIT %s OFFSET %s").format(
            columns_sql,
            sql.Identifier(info.schema),
            sql.Identifier(info.name),
            order_sql,
        ),
        [PAGE_SIZE + 1, offset],
    )
    has_next = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]
    prepared_rows = [
        {
            "token": _sign_locator(info, row),
            "identity": _row_identity(info, row),
            "cells": [{"name": column, "value": _format_value(row.get(column), 80)} for column in display_columns],
        }
        for row in rows
    ]
    return render(
        request,
        "dashboard/internal_data_admin/table.html",
        {
            "table": info,
            "rows": prepared_rows,
            "display_columns": display_columns,
            "row_count": _table_count(schema, table),
            "page": page,
            "previous_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if has_next else None,
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
            "bulk_phrase": f"DELETE SELECTED {schema}.{table}",
        },
    )


@never_cache
def internal_data_admin_row(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    dependencies = _delete_dependencies(info, row, locator)
    return render(
        request,
        "dashboard/internal_data_admin/row.html",
        {
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "fields": [{"name": column.name, "value": _format_value(row.get(column.name), 600)} for column in info.columns],
            "dependencies": dependencies[:20],
            "dependency_count": len(dependencies),
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_new(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    columns = _creatable_columns(info)
    if request.method == "POST":
        values = _form_values(request, columns)
        reason = (request.POST.get("reason") or "").strip()
        if len(reason) < 8:
            messages.error(request, "Please provide a clear reason before creating a record.")
        else:
            try:
                with transaction.atomic():
                    column_names = list(values.keys())
                    if column_names:
                        returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING ctid::text AS _row_ctid, {}").format(
                            sql.Identifier(info.schema),
                            sql.Identifier(info.name),
                            sql.SQL(", ").join(sql.Identifier(column) for column in column_names),
                            sql.SQL(", ").join(sql.Placeholder() for _ in column_names),
                            returning_cols,
                        )
                        inserted = _fetch_dicts(query, list(values.values()))[0]
                    else:
                        returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                        inserted = _fetch_dicts(
                            sql.SQL("INSERT INTO {}.{} DEFAULT VALUES RETURNING ctid::text AS _row_ctid, {}").format(
                                sql.Identifier(info.schema),
                                sql.Identifier(info.name),
                                returning_cols,
                            )
                        )[0]
                    locator = _row_locator(info, inserted)
                    _audit("create", info, locator, None, inserted, reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                messages.success(request, "Record created and audited.")
                return redirect(f"{reverse('internal-data-admin-row', args=[schema, table])}?token={_sign_locator(info, inserted)}")
            except DatabaseError as exc:
                messages.error(request, f"Create failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/form.html",
        {
            "mode": "Create",
            "table": info,
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
            "fields": [
                {
                    "name": column.name,
                    "value": request.POST.get(column.name, "") if request.method == "POST" else "",
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "is_null": request.POST.get(f"__null__{column.name}") == "1",
                    "is_pk": column.name in info.primary_key,
                }
                for column in columns
            ],
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_edit(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    columns = _editable_columns(info)
    if request.method == "POST":
        values = _form_values(request, columns)
        reason = (request.POST.get("reason") or "").strip()
        if len(reason) < 8:
            messages.error(request, "Please provide a clear reason before editing a record.")
        elif not values:
            messages.error(request, "This table has no editable columns for this record.")
        else:
            try:
                with transaction.atomic():
                    before = _select_row(info, locator, lock=True)
                    if not before:
                        raise Http404("Record no longer exists.")
                    where_sql, where_params = _where_clause(locator)
                    assignments = sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(column)) for column in values.keys()
                    )
                    returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                    updated = _fetch_dicts(
                        sql.SQL("UPDATE {}.{} SET {} WHERE {} RETURNING ctid::text AS _row_ctid, {}").format(
                            sql.Identifier(info.schema),
                            sql.Identifier(info.name),
                            assignments,
                            where_sql,
                            returning_cols,
                        ),
                        list(values.values()) + where_params,
                    )
                    if not updated:
                        raise Http404("Record no longer exists.")
                    _audit("update", info, locator, before, updated[0], reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                messages.success(request, "Record updated and audited.")
                return redirect(f"{reverse('internal-data-admin-row', args=[schema, table])}?token={_sign_locator(info, updated[0])}")
            except DatabaseError as exc:
                messages.error(request, f"Update failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/form.html",
        {
            "mode": "Edit",
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
            "fields": [
                {
                    "name": column.name,
                    "value": request.POST.get(column.name, "") if request.method == "POST" else "" if row.get(column.name) is None else str(row.get(column.name)),
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "is_null": request.POST.get(f"__null__{column.name}") == "1" if request.method == "POST" else row.get(column.name) is None,
                    "is_pk": column.name in info.primary_key,
                }
                for column in columns
            ],
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_delete(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    dependencies = _delete_dependencies(info, row, locator)
    phrase = f"DELETE {schema}.{table}"
    if request.method == "POST":
        confirmation = (request.POST.get("confirmation") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        if dependencies:
            messages.error(request, "Delete blocked because dependent records still reference this row.")
        elif confirmation != phrase:
            messages.error(request, f'Type "{phrase}" to confirm this delete.')
        elif len(reason) < 8:
            messages.error(request, "Please provide a clear reason before deleting a record.")
        else:
            try:
                with transaction.atomic():
                    before = _select_row(info, locator, lock=True)
                    if not before:
                        raise Http404("Record no longer exists.")
                    latest_dependencies = _delete_dependencies(info, before, locator)
                    if latest_dependencies:
                        messages.error(request, "Delete blocked because dependent records appeared while processing.")
                    else:
                        where_sql, where_params = _where_clause(locator)
                        _execute(
                            sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                                sql.Identifier(info.schema),
                                sql.Identifier(info.name),
                                where_sql,
                            ),
                            where_params,
                        )
                        _audit("delete", info, locator, before, None, reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                        messages.success(request, "Record deleted and audited.")
                        return redirect("internal-data-admin-table", schema=schema, table=table)
            except DatabaseError as exc:
                messages.error(request, f"Delete failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/delete.html",
        {
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "fields": [{"name": column.name, "value": _format_value(row.get(column.name), 240)} for column in info.columns[:16]],
            "dependencies": dependencies,
            "phrase": phrase,
            "can_delete": not dependencies,
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )


@never_cache
@require_http_methods(["POST"])
def internal_data_admin_bulk_delete(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    tokens = request.POST.getlist("row_token")
    selected_rows = _load_selected_rows(info, tokens)
    phrase = f"DELETE SELECTED {schema}.{table}"

    if not selected_rows:
        messages.error(request, "Select at least one record before using bulk delete.")
        return redirect("internal-data-admin-table", schema=schema, table=table)

    blocked_count = sum(1 for row in selected_rows if row["dependencies"])
    if request.POST.get("bulk_action") == "delete":
        confirmation = (request.POST.get("confirmation") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        if blocked_count:
            messages.error(request, "Bulk delete blocked because one or more selected records have dependencies.")
        elif confirmation != phrase:
            messages.error(request, f'Type "{phrase}" to confirm bulk delete.')
        elif len(reason) < 8:
            messages.error(request, "Please provide a clear reason before bulk deleting records.")
        else:
            try:
                deleted_count = _bulk_delete_selected(
                    info,
                    selected_rows,
                    reason,
                    request.session.get(SESSION_USER_KEY, "internal_admin"),
                )
                messages.success(request, f"Bulk deleted {deleted_count} records and audited each deletion.")
                return redirect("internal-data-admin-table", schema=schema, table=table)
            except DatabaseError as exc:
                messages.error(request, f"Bulk delete failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/bulk_delete.html",
        {
            "table": info,
            "selected_rows": selected_rows,
            "selected_count": len(selected_rows),
            "blocked_count": blocked_count,
            "phrase": phrase,
            "can_delete": blocked_count == 0,
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )
