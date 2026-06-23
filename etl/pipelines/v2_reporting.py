from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import pymysql
from django.conf import settings
from django.db import connection, transaction

from etl.connectors.postgres import execute
from etl.pe_reports.storage import ensure_schema, fetch_table, replace_table, table_exists
from etl.reporting_corrections import (
    RULE_EXCLUDE_INVALID_PHONE,
    RULE_KEEP_DOCTOR_WITH_REP,
    ReportingCorrectionRule,
    active_reporting_correction_rules,
    normalize_key,
    normalize_name,
    normalize_phone,
)
from etl.reporting_privacy import (
    active_campaign_privacy_allowlist,
    active_person_privacy_rules,
    active_raw_visibility_rules,
    filter_rows_by_campaign_fields,
    person_privacy_allowed_campaigns_for_row,
    raw_visibility_entity_ids,
    raw_visibility_keep_only_ids,
    row_matches_raw_visibility_ids,
    row_visible_by_person_privacy,
)


RFA_DEFAULT_DB = "rfa_master_dev"
INCLINIC_DEFAULT_DB = "inclinic_live"
RAW_V2_MASTER_SCHEMA = "raw_v2_master"
RAW_V2_INCLINIC_SCHEMA = "raw_v2_inclinic"
BRONZE_COMPAT_SCHEMA = "bronze"
BRONZE_LEGACY_ARCHIVE_SCHEMA = "bronze_legacy_archive"
BRONZE_COMPAT_TABLES = (
    "campaign_fieldrep",
    "campaign_campaignfieldrep",
    "campaign_campaign",
    "campaign_management_campaign",
    "collateral_management_collateral",
    "collateral_management_campaigncollateral",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime,)):
        return value.isoformat(sep=" ")
    return str(value)


def _clean(value: Any) -> str:
    text = _text(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "t", "yes", "y"}


def _legacy_v2_fallback_enabled() -> bool:
    return os.environ.get("INCLINIC_REPORTING_ENABLE_LEGACY_V2_FALLBACKS", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _num(value: Any) -> str:
    text = _clean(value)
    return text if text else ""


def _phone(value: Any) -> str:
    digits = "".join(ch for ch in _clean(value) if ch.isdigit())
    return digits[-10:] if len(digits) >= 10 else digits


def _norm(value: Any) -> str:
    return "".join(ch.lower() for ch in _clean(value) if ch.isalnum())


def _source_digits(value: Any) -> str:
    return "".join(ch for ch in _clean(value) if ch.isdigit())


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _md5(*parts: Any) -> str:
    text = ":".join(_clean(part) for part in parts if _clean(part))
    return hashlib.md5(text.encode("utf-8")).hexdigest() if text else ""


def _json_payload(row: dict[str, Any]) -> str:
    return json.dumps(row, default=str, sort_keys=True)


def _mysql_conn(server_settings: dict[str, Any], default_db: str):
    database = _clean(server_settings.get("DATABASE")) or default_db
    return pymysql.connect(
        host=server_settings["HOST"],
        port=int(server_settings["PORT"]),
        user=server_settings["USER"],
        password=server_settings["PASSWORD"],
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        charset="utf8mb4",
        connect_timeout=int(server_settings.get("CONNECT_TIMEOUT", 10)),
        read_timeout=int(server_settings.get("READ_TIMEOUT", 60)),
        write_timeout=int(server_settings.get("WRITE_TIMEOUT", 60)),
    )


def _fetch_table(server_settings: dict[str, Any], default_db: str, table: str) -> list[dict[str, Any]]:
    with _mysql_conn(server_settings, default_db) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{table}`")
            return list(cur.fetchall())


def _fetch_optional_table(server_settings: dict[str, Any], default_db: str, table: str) -> list[dict[str, Any]]:
    try:
        return _fetch_table(server_settings, default_db, table)
    except Exception:
        return []


def _source_freshness_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        _field(row, "old_updated_at", "source_updated_at", "updated_at"),
        _field(row, "old_created_at", "source_created_at", "created_at"),
        _field(row, "_ingested_at"),
    )


def _merge_non_empty(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if _clean(value):
            merged[key] = value
    return merged


def _campaign_collateral_merge_key(row: dict[str, Any]) -> tuple[str, ...]:
    old_id = _field(row, "old_id", "id", "source_pk_value")
    if old_id:
        return ("old_id", old_id)
    campaign = _field(row, "legacy_campaign_id", "brand_campaign_id", "old_campaign_id", "campaign_id")
    collateral = _field(row, "old_collateral_id", "collateral_id")
    if campaign and collateral:
        return ("campaign_collateral", campaign, collateral)
    return _raw_v2_row_key("inclinic_campaign_collateral_v2", row)


def _legacy_campaign_collateral_to_v2(row: dict[str, Any], campaign_by_local_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    local_campaign_id = _field(row, "campaign_id")
    local_campaign = campaign_by_local_id.get(local_campaign_id, {})
    brand_campaign_id = _field(local_campaign, "brand_campaign_id") or local_campaign_id
    collateral_id = _field(row, "collateral_id")
    return {
        "source_system": "inclinic",
        "source_database": INCLINIC_DEFAULT_DB,
        "source_table": "collateral_management_campaigncollateral",
        "source_pk_column": "id",
        "source_pk_value": _field(row, "id"),
        "campaign_collateral_uuid": "",
        "campaign_uuid": "",
        "legacy_campaign_id": brand_campaign_id,
        "brand_campaign_id": brand_campaign_id,
        "campaign_id": brand_campaign_id,
        "old_id": _field(row, "id"),
        "old_start_date": _field(row, "start_date"),
        "old_end_date": _field(row, "end_date"),
        "old_created_at": _field(row, "created_at"),
        "old_updated_at": _field(row, "updated_at"),
        "old_campaign_id": local_campaign_id,
        "old_collateral_id": collateral_id,
        "collateral_uuid": "",
        "source_created_at": _field(row, "created_at"),
        "source_updated_at": _field(row, "updated_at"),
        "is_current": "true",
    }


def _merge_campaign_collateral_sources(
    v2_rows: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
    campaign_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    campaign_by_local_id = {_field(row, "id"): row for row in campaign_rows if _field(row, "id")}
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in v2_rows:
        merged[_campaign_collateral_merge_key(row)] = row

    for legacy_row in legacy_rows:
        row = _legacy_campaign_collateral_to_v2(legacy_row, campaign_by_local_id)
        key = _campaign_collateral_merge_key(row)
        existing = merged.get(key)
        if existing is None:
            merged[key] = row
            continue
        if _source_freshness_key(row) >= _source_freshness_key(existing):
            merged[key] = _merge_non_empty(existing, row)

    return list(merged.values())


def _field_rep_merge_key(row: dict[str, Any]) -> tuple[str, ...]:
    rep_id = _field(row, "current_campaign_fieldrep_id", "campaign_fieldrep_id", "id", "source_pk_value")
    if rep_id:
        return ("campaign_fieldrep_id", rep_id)
    brand_id = _field(row, "current_brand_supplied_field_rep_id", "brand_supplied_field_rep_id")
    if brand_id:
        return ("brand_supplied_field_rep_id", brand_id)
    return _raw_v2_row_key("field_rep_v2", row)


def _legacy_field_rep_to_v2(row: dict[str, Any], auth_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    rep_id = _field(row, "id")
    auth = auth_by_id.get(_field(row, "user_id"), {})
    state = _field(row, "state")
    return {
        "source_system": "rfa_master",
        "source_database": RFA_DEFAULT_DB,
        "source_table": "campaign_fieldrep",
        "source_pk_column": "id",
        "source_pk_value": rep_id,
        "field_rep_uuid": "",
        "id": rep_id,
        "current_campaign_fieldrep_id": rep_id,
        "full_name": _field(row, "full_name"),
        "display_name": _field(row, "full_name") or rep_id,
        "phone_number": _field(row, "phone_number"),
        "primary_phone_raw": _field(row, "phone_number"),
        "primary_phone_normalized": _phone(row.get("phone_number")),
        "primary_email": _field(auth, "email"),
        "brand_supplied_field_rep_id": _field(row, "brand_supplied_field_rep_id"),
        "current_brand_supplied_field_rep_id": _field(row, "brand_supplied_field_rep_id"),
        "is_active": "1" if _truthy(row.get("is_active")) else "0",
        "password_hash": _field(row, "password_hash"),
        "created_at": _field(row, "created_at"),
        "updated_at": _field(row, "updated_at"),
        "source_created_at": _field(row, "created_at"),
        "source_updated_at": _field(row, "updated_at"),
        "brand_id": _field(row, "brand_id"),
        "user_id": _field(row, "user_id"),
        "state": state,
        "state_normalized": state,
        "campaign_fieldrep_state": state,
        "status": "active" if _truthy(row.get("is_active")) else "inactive",
    }


def _merge_field_rep_sources(
    v2_rows: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
    auth_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    auth_by_id = {_field(row, "id"): row for row in auth_rows if _field(row, "id")}
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in v2_rows:
        key = _field_rep_merge_key(row)
        existing = merged.get(key)
        if existing is None or _source_freshness_key(row) >= _source_freshness_key(existing):
            merged[key] = row

    for legacy_row in legacy_rows:
        row = _legacy_field_rep_to_v2(legacy_row, auth_by_id)
        key = _field_rep_merge_key(row)
        existing = merged.get(key)
        if existing is None:
            merged[key] = row
            continue
        if _source_freshness_key(row) >= _source_freshness_key(existing):
            merged[key] = _merge_non_empty(existing, row)

    return list(merged.values())


def _field(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = _clean(row.get(name))
        if value:
            return value
    return ""


def _event_date(row: dict[str, Any], *names: str) -> str:
    return _field(row, *names)


def _row_is_current(row: dict[str, Any]) -> bool:
    value = row.get("is_current")
    return value is None or _truthy(value)


def _build_indexes(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        value = _clean(row.get(key))
        if value and value not in out:
            out[value] = row
    return out


DIM_CAMPAIGN_COLUMNS = [
    "id",
    "brand_campaign_id",
    "campaign_uuid",
    "name",
    "brand_id",
    "brand_name",
    "company_name",
    "company_logo",
    "start_date",
    "end_date",
    "status",
    "num_doctors",
    "num_doctors_supported",
    "created_at",
    "updated_at",
    "source_system",
]

DIM_FIELD_REP_COLUMNS = [
    "id",
    "full_name",
    "phone_number",
    "brand_supplied_field_rep_id",
    "is_active",
    "password_hash",
    "created_at",
    "updated_at",
    "brand_id",
    "user_id",
    "state",
    "field_rep_phone_normalized",
    "field_rep_email_best",
    "state_normalized",
    "is_active_flag",
    "created_at_ts",
    "updated_at_ts",
    "campaign_id",
    "source_table",
    "source_field_rep_id",
    "_silver_updated_at",
    "_dq_status",
    "_dq_errors",
]

CAMPAIGN_FIELD_REP_COLUMNS = [
    "id",
    "campaign_id",
    "field_rep_id",
    "created_at",
    "state",
    "assignment_status",
]

DIM_DOCTOR_COLUMNS = [
    "id",
    "name",
    "phone",
    "rep_id",
    "source",
    "doctor_phone_normalized",
    "doctor_identity_source",
    "doctor_identity_key",
    "rep_id_normalized",
    "field_rep_id_resolved",
    "state_normalized",
    "_silver_updated_at",
    "_dq_status",
    "_dq_errors",
]

DIM_COLLATERAL_COLUMNS = [
    "id",
    "type",
    "title",
    "file",
    "vimeo_url",
    "content_id",
    "upload_date",
    "is_active",
    "created_at",
    "updated_at",
    "banner_1",
    "banner_2",
    "campaign_id",
    "created_by_id",
    "description",
    "purpose",
    "doctor_name",
    "webinar_date",
    "webinar_description",
    "webinar_title",
    "webinar_url",
    "is_active_flag",
    "upload_date_ts",
    "created_at_ts",
    "updated_at_ts",
    "webinar_date_dt",
    "collateral_display_name",
    "content_missing_flag",
    "_silver_updated_at",
]

SCHEDULE_COLUMNS = [
    "id",
    "start_date",
    "end_date",
    "created_at",
    "updated_at",
    "campaign_id",
    "collateral_id",
    "schedule_start_ts",
    "schedule_end_ts",
    "schedule_start_date",
    "schedule_end_date",
    "schedule_missing_flag",
    "campaign_id_resolved",
    "collateral_type",
    "collateral_title",
    "_silver_updated_at",
]

FACT_SHARE_COLUMNS = [
    "id",
    "share_channel",
    "share_timestamp",
    "message_text",
    "created_at",
    "updated_at",
    "short_link_id",
    "collateral_id",
    "doctor_identifier",
    "brand_campaign_id",
    "field_rep_email",
    "field_rep_id",
    "_ingestion_run_id",
    "_ingested_at",
    "_source_server",
    "_source_table",
    "_extract_started_at",
    "_extract_ended_at",
    "_record_hash",
    "_is_deleted",
    "_dq_status",
    "_dq_errors",
    "doctor_identifier_normalized",
    "doctor_identity_key",
    "reached_event_ts",
    "share_timestamp_ts",
    "created_at_ts",
    "updated_at_ts",
    "_silver_updated_at",
    "_as_of_run_id",
]

FACT_TRANSACTION_COLUMNS = [
    "id",
    "transaction_id",
    "source_transaction_id",
    "brand_campaign_id",
    "field_rep_id",
    "field_rep_unique_id",
    "doctor_name",
    "doctor_number",
    "doctor_unique_id",
    "collateral_id",
    "transaction_date",
    "has_viewed",
    "downloaded_pdf",
    "pdf_completed",
    "video_view_lt_50",
    "video_view_gt_50",
    "video_completed",
    "pdf_total_pages",
    "last_video_percentage",
    "pdf_last_page",
    "doctor_viewer_engagement_id",
    "share_management_engagement_id",
    "video_tracking_last_event_id",
    "created_at",
    "updated_at",
    "sent_at",
    "viewed_at",
    "first_viewed_at",
    "viewed_last_page_at",
    "video_lt_50_at",
    "video_gt_50_at",
    "video_100_at",
    "last_viewed_at",
    "dv_engagement_id",
    "field_rep_email",
    "share_channel",
    "sm_engagement_id",
    "video_watch_percentage",
    "_ingestion_run_id",
    "_ingested_at",
    "_source_server",
    "_source_table",
    "_extract_started_at",
    "_extract_ended_at",
    "_record_hash",
    "_is_deleted",
    "_dq_status",
    "_dq_errors",
    "doctor_phone_normalized",
    "doctor_identity_key",
    "transaction_identity_key",
    "doctor_master_id_resolved",
    "field_rep_master_id_resolved",
    "brand_supplied_field_rep_id_resolved",
    "has_viewed_flag",
    "downloaded_pdf_flag",
    "pdf_completed_flag",
    "video_view_gt_50_flag",
    "last_video_percentage_num",
    "video_watch_percentage_num",
    "pdf_last_page_num",
    "pdf_total_pages_num",
    "created_at_ts",
    "updated_at_ts",
    "transaction_date_ts",
    "sent_at_ts",
    "viewed_at_ts",
    "first_viewed_at_ts",
    "viewed_last_page_at_ts",
    "video_lt_50_at_ts",
    "video_gt_50_at_ts",
    "video_100_at_ts",
    "last_viewed_at_ts",
    "reached_event_ts",
    "opened_event_ts",
    "video_gt_50_event_ts",
    "pdf_download_event_ts",
    "_silver_updated_at",
    "_as_of_run_id",
]

MAP_CAMPAIGN_COLUMNS = [
    "brand_campaign_id",
    "campaign_id_resolved",
    "distinct_campaign_id_count",
    "_dq_status",
    "_dq_errors",
    "_silver_updated_at",
]

BRIDGE_BASE_COLUMNS = [
    "brand_campaign_id",
    "doctor_identity_key",
    "doctor_master_id_resolved",
    "field_rep_id_resolved",
    "state_normalized",
    "inclusion_reason",
    "_silver_updated_at",
    "_dq_status",
    "_dq_errors",
]

ACTION_FIRST_SEEN_COLUMNS = [
    "brand_campaign_id",
    "collateral_id",
    "doctor_identity_key",
    "reached_first_ts",
    "opened_first_ts",
    "video_gt_50_first_ts",
    "pdf_download_first_ts",
    "last_activity_ts",
    "_silver_updated_at",
]

PRESERVATION_ARCHIVE_SCHEMA = "archive"
PRESERVATION_ARCHIVE_TABLE = "reporting_v2_preserved_rows"

PRESERVATION_FINGERPRINT_EXCLUDED_COLUMNS = {
    "_as_of_run_id",
    "_extract_ended_at",
    "_extract_started_at",
    "_ingested_at",
    "_ingestion_run_id",
    "_silver_updated_at",
}

PRESERVATION_KEY_CANDIDATES: dict[str, tuple[tuple[str, ...], ...]] = {
    "dim_campaign": (("id",), ("brand_campaign_id",), ("campaign_uuid",)),
    "dim_field_rep": (("id",), ("brand_supplied_field_rep_id", "brand_id")),
    "dim_campaign_field_rep_assignment": (("id",), ("campaign_id", "field_rep_id")),
    "dim_doctor": (("doctor_identity_key", "field_rep_id_resolved"), ("id",), ("doctor_phone_normalized", "rep_id")),
    "dim_collateral": (("id",),),
    "bridge_campaign_collateral_schedule": (("id",), ("campaign_id_resolved", "collateral_id")),
    "fact_share_log": (("id",), ("_record_hash",), ("brand_campaign_id", "collateral_id", "doctor_identity_key", "field_rep_id")),
    "fact_collateral_transaction": (("transaction_identity_key",), ("id",), ("_record_hash",)),
    "map_brand_campaign_to_campaign": (("brand_campaign_id",),),
    "bridge_brand_campaign_doctor_base": (("brand_campaign_id", "doctor_identity_key", "field_rep_id_resolved"),),
    "doctor_action_first_seen": (("brand_campaign_id", "collateral_id", "doctor_identity_key"),),
}


def _row_payload_for_columns(row: dict[str, Any], columns: list[str]) -> dict[str, str]:
    return {column: _text(row.get(column)) for column in columns}


def _stable_payload_fingerprint(row: dict[str, Any], columns: list[str]) -> str:
    fingerprint_columns = [column for column in columns if column not in PRESERVATION_FINGERPRINT_EXCLUDED_COLUMNS]
    payload = _row_payload_for_columns(row, fingerprint_columns)
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _preservation_row_key(table: str, row: dict[str, Any], columns: list[str]) -> str:
    for candidate in PRESERVATION_KEY_CANDIDATES.get(table, ()):
        parts = [(column, _clean(row.get(column))) for column in candidate]
        if all(value for _, value in parts):
            return "|".join(f"{column}={value}" for column, value in parts)

    for column in ("id", "transaction_identity_key", "_record_hash", "doctor_identity_key", "brand_campaign_id"):
        if column in columns:
            value = _clean(row.get(column))
            if value:
                return f"{column}={value}"

    return f"payload_md5={_stable_payload_fingerprint(row, columns)}"


def _ensure_preservation_archive_table() -> None:
    ensure_schema(PRESERVATION_ARCHIVE_SCHEMA)
    execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PRESERVATION_ARCHIVE_SCHEMA}.{PRESERVATION_ARCHIVE_TABLE} (
            source_schema TEXT NOT NULL,
            source_table TEXT NOT NULL,
            row_key TEXT NOT NULL,
            row_fingerprint TEXT NOT NULL,
            row_payload JSONB NOT NULL,
            preserve_reason TEXT NOT NULL,
            first_missing_run_id TEXT NOT NULL,
            last_missing_run_id TEXT NOT NULL,
            first_preserved_at TIMESTAMPTZ NOT NULL,
            last_preserved_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source_schema, source_table, row_key, row_fingerprint)
        );
        """
    )
    execute(
        f"""
        CREATE INDEX IF NOT EXISTS reporting_v2_preserved_rows_table_idx
        ON {PRESERVATION_ARCHIVE_SCHEMA}.{PRESERVATION_ARCHIVE_TABLE}
        (source_schema, source_table, preserve_reason);
        """
    )


def _archive_replaced_reporting_rows(
    schema: str,
    table: str,
    columns: list[str],
    current_rows: list[dict[str, Any]],
    *,
    run_id: str,
    now: str,
) -> dict[str, int]:
    if not table_exists(schema, table):
        return {"missing": 0, "changed": 0, "archived": 0}

    existing_rows = fetch_table(schema, table)
    current_fingerprints_by_key: dict[str, set[str]] = defaultdict(set)
    for row in current_rows:
        key = _preservation_row_key(table, row, columns)
        current_fingerprints_by_key[key].add(_stable_payload_fingerprint(row, columns))

    rows_to_archive: list[tuple[str, str, str, str, str, str, str, str, str, str]] = []
    reason_counts = {"missing": 0, "changed": 0}
    for existing in existing_rows:
        row_key = _preservation_row_key(table, existing, columns)
        fingerprint = _stable_payload_fingerprint(existing, columns)
        if row_key not in current_fingerprints_by_key:
            reason = "missing_from_current_v2_source"
            reason_counts["missing"] += 1
        elif fingerprint not in current_fingerprints_by_key[row_key]:
            reason = "replaced_by_current_v2_source"
            reason_counts["changed"] += 1
        else:
            continue

        rows_to_archive.append(
            (
                schema,
                table,
                row_key,
                fingerprint,
                json.dumps(_row_payload_for_columns(existing, columns), sort_keys=True),
                reason,
                run_id,
                run_id,
                now,
                now,
            )
        )

    if not rows_to_archive:
        return {"missing": 0, "changed": 0, "archived": 0}

    _ensure_preservation_archive_table()
    with connection.cursor() as cursor:
        cursor.executemany(
            f"""
            INSERT INTO {PRESERVATION_ARCHIVE_SCHEMA}.{PRESERVATION_ARCHIVE_TABLE}
            (
                source_schema,
                source_table,
                row_key,
                row_fingerprint,
                row_payload,
                preserve_reason,
                first_missing_run_id,
                last_missing_run_id,
                first_preserved_at,
                last_preserved_at
            )
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::timestamptz, %s::timestamptz)
            ON CONFLICT (source_schema, source_table, row_key, row_fingerprint)
            DO UPDATE SET
                preserve_reason = EXCLUDED.preserve_reason,
                last_missing_run_id = EXCLUDED.last_missing_run_id,
                last_preserved_at = EXCLUDED.last_preserved_at
            """,
            rows_to_archive,
        )

    return {**reason_counts, "archived": len(rows_to_archive)}


UNKNOWN_STATE_VALUES = {"", "null", "none", "unknown", "na", "n/a", "-"}


def _clean_state(value: Any) -> str:
    state = _clean(value)
    return "" if state.lower() in UNKNOWN_STATE_VALUES else state


def _state_for_value(value: Any) -> str:
    state = _clean_state(value)
    return state if state else "UNKNOWN"


def _state_for_rep(rep: dict[str, Any] | None) -> str:
    return _state_for_value((rep or {}).get("state"))


def _first_state(row: dict[str, Any], *fields: str) -> str:
    for field in fields:
        state = _clean_state(row.get(field))
        if state:
            return state
    return ""


def _identity_state_by_campaign_fieldrep(source: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    state_by_rep: dict[str, tuple[tuple[str, str, str], str]] = {}
    state_fields = (
        "state",
        "state_normalized",
        "campaign_fieldrep_state",
        "user_management_state",
        "portal_state",
        "field_rep_state",
    )
    for identity in source.get("inclinic_field_rep_identity_v2", []):
        if not _row_is_current(identity):
            continue
        rep_id = _field(identity, "campaign_fieldrep_id", "field_rep_uuid")
        state = _first_state(identity, *state_fields)
        if not rep_id or not state:
            continue
        key = normalize_key(rep_id)
        freshness = (
            _clean(identity.get("source_updated_at")),
            _clean(identity.get("migrated_at")),
            _clean(identity.get("source_created_at")),
        )
        current = state_by_rep.get(key)
        if current is None or freshness >= current[0]:
            state_by_rep[key] = (freshness, state)
    return {key: state for key, (_, state) in state_by_rep.items()}


def _field_rep_state_fallback_by_id() -> dict[str, str]:
    if not _legacy_v2_fallback_enabled():
        return {}
    fallback: dict[str, str] = {}
    source_tables = (
        ("raw_server1", "campaign_fieldrep", "id", ("state",)),
        ("silver", "dim_field_rep", "id", ("state", "state_normalized")),
    )
    for schema, table, id_column, state_fields in source_tables:
        try:
            if not table_exists(schema, table):
                continue
            for row in fetch_table(schema, table):
                rep_id = _field(row, id_column)
                state = _first_state(row, *state_fields)
                if rep_id and state:
                    fallback.setdefault(normalize_key(rep_id), state)
        except Exception:
            continue
    return fallback


RAW_V2_SOURCE_TABLES = {
    RAW_V2_MASTER_SCHEMA: {
        "campaign_v2": "campaign_v2",
        "field_rep_v2": "field_rep_v2",
        "campaign_field_rep_assignment_v2": "campaign_field_rep_assignment_v2",
        "doctor_field_rep_roster_bridge_v2": "doctor_field_rep_roster_bridge_v2",
    },
    RAW_V2_INCLINIC_SCHEMA: {
        "inclinic_assigned_doctor_roster_v2": "inclinic_assigned_doctor_roster_v2",
        "inclinic_collateral_v2": "inclinic_collateral_v2",
        "inclinic_campaign_collateral_v2": "inclinic_campaign_collateral_v2",
        "inclinic_campaign_field_rep_assignment_v2": "inclinic_campaign_field_rep_assignment_v2",
        "inclinic_collateral_transaction_v2": "inclinic_collateral_transaction_v2",
        "inclinic_share_event_v2": "inclinic_share_event_v2",
        "inclinic_field_rep_identity_v2": "inclinic_field_rep_identity_v2",
        "inclinic_campaign_v2": "campaign_management_campaign",
        "migration_exception_v2": "migration_exception_v2",
    },
}

REQUIRED_V2_SOURCE_KEYS = (
    "campaign_v2",
    "field_rep_v2",
    "campaign_field_rep_assignment_v2",
    "doctor_field_rep_roster_bridge_v2",
    "campaign_management_campaign",
    "inclinic_assigned_doctor_roster_v2",
    "inclinic_collateral_v2",
    "inclinic_campaign_collateral_v2",
    "inclinic_campaign_field_rep_assignment_v2",
    "inclinic_collateral_transaction_v2",
    "inclinic_share_event_v2",
)

RAW_V2_KEY_CANDIDATES: dict[str, tuple[tuple[str, ...], ...]] = {
    "campaign_v2": (("campaign_uuid",), ("id",), ("source_table", "source_pk_value")),
    "field_rep_v2": (("current_campaign_fieldrep_id",), ("id",), ("source_table", "source_pk_value"), ("field_rep_uuid",)),
    "campaign_field_rep_assignment_v2": (
        ("campaign_field_rep_assignment_uuid",),
        ("id",),
        ("campaign_uuid", "field_rep_uuid"),
        ("source_table", "source_pk_value"),
    ),
    "doctor_field_rep_roster_bridge_v2": (
        ("doctor_field_rep_roster_bridge_uuid",),
        ("campaign_uuid", "doctor_phone_normalized", "brand_supplied_field_rep_id"),
        ("source_table", "source_pk_value"),
    ),
    "inclinic_assigned_doctor_roster_v2": (
        ("doctor_field_rep_roster_bridge_uuid",),
        ("campaign_uuid", "doctor_phone_normalized", "brand_supplied_field_rep_id"),
        ("source_table", "source_pk_value"),
    ),
    "inclinic_collateral_v2": (("collateral_uuid",), ("old_id",), ("source_table", "source_pk_value")),
    "inclinic_campaign_collateral_v2": (("old_id",), ("source_table", "source_pk_value"), ("campaign_collateral_uuid",)),
    "inclinic_campaign_field_rep_assignment_v2": (
        ("assignment_uuid",),
        ("old_id",),
        ("campaign_uuid", "field_rep_uuid"),
        ("source_table", "source_pk_value"),
    ),
    "inclinic_collateral_transaction_v2": (
        ("transaction_uuid",),
        ("old_id",),
        ("old_transaction_id",),
        ("source_table", "source_pk_value"),
    ),
    "inclinic_share_event_v2": (("share_event_uuid",), ("old_id",), ("source_table", "source_pk_value")),
    "inclinic_field_rep_identity_v2": (
        ("inclinic_field_rep_identity_id",),
        ("field_rep_uuid", "source_column", "source_value_normalized"),
        ("source_table", "source_pk_value"),
    ),
    "inclinic_campaign_v2": (("inclinic_campaign_uuid",), ("brand_campaign_id",), ("id",), ("source_table", "source_pk_value")),
    "migration_exception_v2": (("exception_id",), ("source_table", "source_pk_value", "issue_code")),
}


def _columns_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    return list(dict.fromkeys(key for row in rows for key in row.keys())) or ["id"]


def _raw_v2_row_key(table: str, row: dict[str, Any]) -> tuple[str, ...]:
    for candidate in RAW_V2_KEY_CANDIDATES.get(table, ()):
        values = tuple(_clean(row.get(column)) for column in candidate)
        if all(values):
            return (*candidate, *values)
    columns = sorted(row.keys())
    return ("payload", _stable_payload_fingerprint(row, columns))


def _merge_raw_v2_rows(raw_table: str, incoming_rows: list[dict[str, Any]], existing_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in existing_rows:
        merged[_raw_v2_row_key(raw_table, row)] = row
    for row in incoming_rows:
        merged[_raw_v2_row_key(raw_table, row)] = row
    return list(merged.values())


def _load_source_from_raw_v2() -> dict[str, list[dict[str, Any]]]:
    return {
        "campaign_v2": fetch_table(RAW_V2_MASTER_SCHEMA, "campaign_v2"),
        "field_rep_v2": fetch_table(RAW_V2_MASTER_SCHEMA, "field_rep_v2"),
        "campaign_field_rep_assignment_v2": fetch_table(RAW_V2_MASTER_SCHEMA, "campaign_field_rep_assignment_v2"),
        "doctor_field_rep_roster_bridge_v2": fetch_table(RAW_V2_MASTER_SCHEMA, "doctor_field_rep_roster_bridge_v2"),
        "inclinic_assigned_doctor_roster_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_assigned_doctor_roster_v2"),
        "inclinic_collateral_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_collateral_v2"),
        "inclinic_campaign_collateral_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_campaign_collateral_v2"),
        "inclinic_campaign_field_rep_assignment_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_campaign_field_rep_assignment_v2"),
        "inclinic_collateral_transaction_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_collateral_transaction_v2"),
        "inclinic_share_event_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_share_event_v2"),
        "inclinic_field_rep_identity_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_field_rep_identity_v2"),
        "campaign_management_campaign": fetch_table(RAW_V2_INCLINIC_SCHEMA, "inclinic_campaign_v2"),
        "migration_exception_v2": fetch_table(RAW_V2_INCLINIC_SCHEMA, "migration_exception_v2") if table_exists(RAW_V2_INCLINIC_SCHEMA, "migration_exception_v2") else [],
    }


def _load_source_from_mysql_v2() -> dict[str, list[dict[str, Any]]]:
    rfa = settings.MYSQL_SERVER_1
    inclinic = settings.MYSQL_SERVER_2
    if _legacy_v2_fallback_enabled():
        auth_rows = _fetch_optional_table(rfa, RFA_DEFAULT_DB, "auth_user")
        field_rep_rows = _merge_field_rep_sources(
            _fetch_table(rfa, RFA_DEFAULT_DB, "field_rep_v2"),
            _fetch_optional_table(rfa, RFA_DEFAULT_DB, "campaign_fieldrep"),
            auth_rows,
        )
    else:
        field_rep_rows = _fetch_table(rfa, RFA_DEFAULT_DB, "field_rep_v2")
    inclinic_campaign_rows = (
        _fetch_optional_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_campaign_v2")
        or _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "campaign_management_campaign")
    )
    if _legacy_v2_fallback_enabled():
        campaign_collateral_rows = _merge_campaign_collateral_sources(
            _fetch_optional_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_campaign_collateral_v2"),
            _fetch_optional_table(inclinic, INCLINIC_DEFAULT_DB, "collateral_management_campaigncollateral"),
            inclinic_campaign_rows,
        )
    else:
        campaign_collateral_rows = _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_campaign_collateral_v2")
    return {
        "campaign_v2": _fetch_table(rfa, RFA_DEFAULT_DB, "campaign_v2"),
        "field_rep_v2": field_rep_rows,
        "campaign_field_rep_assignment_v2": _fetch_table(rfa, RFA_DEFAULT_DB, "campaign_field_rep_assignment_v2"),
        "doctor_field_rep_roster_bridge_v2": _fetch_table(rfa, RFA_DEFAULT_DB, "doctor_field_rep_roster_bridge_v2"),
        "inclinic_assigned_doctor_roster_v2": _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_assigned_doctor_roster_v2"),
        "inclinic_collateral_v2": _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_collateral_v2"),
        "inclinic_campaign_collateral_v2": campaign_collateral_rows,
        "inclinic_campaign_field_rep_assignment_v2": _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_campaign_field_rep_assignment_v2"),
        "inclinic_collateral_transaction_v2": _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_collateral_transaction_v2"),
        "inclinic_share_event_v2": _fetch_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_share_event_v2"),
        "inclinic_field_rep_identity_v2": _fetch_optional_table(inclinic, INCLINIC_DEFAULT_DB, "inclinic_field_rep_identity_v2"),
        "campaign_management_campaign": inclinic_campaign_rows,
        "migration_exception_v2": _fetch_optional_table(inclinic, INCLINIC_DEFAULT_DB, "migration_exception_v2"),
    }


def refresh_raw_v2_from_source(run_id: str) -> dict[str, int]:
    source = _load_source_from_mysql_v2()
    counts: dict[str, int] = {}
    with transaction.atomic():
        for schema, table_map in RAW_V2_SOURCE_TABLES.items():
            for raw_table, source_key in table_map.items():
                incoming_rows = source.get(source_key, [])
                existing_rows = fetch_table(schema, raw_table) if table_exists(schema, raw_table) else []
                rows = _merge_raw_v2_rows(raw_table, incoming_rows, existing_rows)
                replace_table(schema, raw_table, _columns_for_rows(rows), rows)
                counts[f"{schema}.{raw_table}"] = len(rows)
                counts[f"{schema}.{raw_table}__incoming"] = len(incoming_rows)
                counts[f"{schema}.{raw_table}__preserved_existing"] = max(len(rows) - len(incoming_rows), 0)
    return counts


def _load_source() -> dict[str, list[dict[str, Any]]]:
    if table_exists(RAW_V2_MASTER_SCHEMA, "campaign_v2") and table_exists(RAW_V2_INCLINIC_SCHEMA, "inclinic_collateral_transaction_v2"):
        return _load_source_from_raw_v2()
    return _load_source_from_mysql_v2()


def _apply_campaign_privacy_to_source(source: dict[str, list[dict[str, Any]]], allowlist: set[str]) -> dict[str, list[dict[str, Any]]]:
    if not allowlist:
        return source

    output = {key: list(rows) for key, rows in source.items()}
    campaign_fields_by_source = {
        "campaign_v2": ("legacy_campaign_id", "id", "campaign_id", "brand_campaign_id"),
        "campaign_management_campaign": ("brand_campaign_id", "id", "campaign_id", "legacy_campaign_id"),
        "campaign_field_rep_assignment_v2": ("legacy_campaign_id", "campaign_id"),
        "doctor_field_rep_roster_bridge_v2": ("legacy_campaign_id", "campaign_id"),
        "inclinic_assigned_doctor_roster_v2": ("legacy_campaign_id", "campaign_id"),
        "inclinic_campaign_collateral_v2": ("legacy_campaign_id", "campaign_id", "old_campaign_id", "brand_campaign_id"),
        "inclinic_campaign_field_rep_assignment_v2": ("legacy_campaign_id", "campaign_id", "old_campaign_id", "brand_campaign_id"),
        "inclinic_collateral_transaction_v2": ("legacy_campaign_id", "old_brand_campaign_id", "brand_campaign_id", "campaign_id"),
        "inclinic_share_event_v2": ("legacy_campaign_id", "old_brand_campaign_id", "brand_campaign_id", "campaign_id"),
    }
    for source_key, fields in campaign_fields_by_source.items():
        output[source_key] = filter_rows_by_campaign_fields(output.get(source_key, []), allowlist, fields)

    allowed_rep_ids: set[str] = set()
    for source_key in ("campaign_field_rep_assignment_v2", "inclinic_campaign_field_rep_assignment_v2", "doctor_field_rep_roster_bridge_v2", "inclinic_assigned_doctor_roster_v2"):
        for row in output.get(source_key, []):
            for field in ("field_rep_id", "campaign_fieldrep_id", "current_campaign_fieldrep_id", "registered_by_id", "old_field_rep_id"):
                value = _field(row, field)
                if value:
                    allowed_rep_ids.add(value)

    if allowed_rep_ids:
        output["field_rep_v2"] = [
            row
            for row in output.get("field_rep_v2", [])
            if _field(row, "current_campaign_fieldrep_id", "id") in allowed_rep_ids
        ]
        output["inclinic_field_rep_identity_v2"] = [
            row
            for row in output.get("inclinic_field_rep_identity_v2", [])
            if _field(row, "campaign_fieldrep_id") in allowed_rep_ids
        ]
    else:
        output["field_rep_v2"] = []
        output["inclinic_field_rep_identity_v2"] = []

    return output


def _row_visible_for_person_privacy(
    row: dict[str, Any],
    person_rules: list[dict[str, Any]],
    *,
    campaign_fields: tuple[str, ...],
    email_fields: tuple[str, ...] = (),
    phone_fields: tuple[str, ...] = (),
) -> bool:
    return row_visible_by_person_privacy(
        row,
        person_rules,
        campaign_fields=campaign_fields,
        email_fields=email_fields,
        phone_fields=phone_fields,
    )


def _apply_person_privacy_to_source(source: dict[str, list[dict[str, Any]]], person_rules: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not person_rules:
        return source

    output = {key: list(rows) for key, rows in source.items()}
    doctor_campaign_fields = ("legacy_campaign_id", "old_brand_campaign_id", "brand_campaign_id", "campaign_id")
    for source_key, phone_fields in {
        "doctor_field_rep_roster_bridge_v2": ("doctor_phone_normalized", "doctor_phone_raw"),
        "inclinic_assigned_doctor_roster_v2": ("doctor_phone_normalized", "doctor_phone_raw"),
        "inclinic_share_event_v2": ("doctor_phone_normalized", "old_doctor_identifier"),
        "inclinic_collateral_transaction_v2": ("doctor_phone_normalized", "old_doctor_number"),
    }.items():
        output[source_key] = [
            row
            for row in output.get(source_key, [])
            if _row_visible_for_person_privacy(
                row,
                person_rules,
                campaign_fields=doctor_campaign_fields,
                email_fields=("doctor_email", "old_doctor_email", "email"),
                phone_fields=phone_fields,
            )
        ]

    restricted_rep_allowed_campaigns: dict[str, set[str]] = defaultdict(set)
    for row in output.get("field_rep_v2", []):
        rep_id = _field(row, "current_campaign_fieldrep_id", "id")
        allowed = person_privacy_allowed_campaigns_for_row(
            row,
            person_rules,
            email_fields=("primary_email", "email", "field_rep_email"),
            phone_fields=("phone_number", "primary_phone_raw", "primary_phone_normalized"),
        )
        if rep_id and allowed is not None:
            restricted_rep_allowed_campaigns[rep_id].update(allowed)

    for row in output.get("inclinic_field_rep_identity_v2", []):
        rep_id = _field(row, "campaign_fieldrep_id")
        allowed = person_privacy_allowed_campaigns_for_row(
            row,
            person_rules,
            email_fields=("email_normalized", "user_management_email", "auth_user_email", "source_value", "source_value_normalized"),
            phone_fields=("phone_normalized", "campaign_fieldrep_phone_number"),
        )
        if rep_id and allowed is not None:
            restricted_rep_allowed_campaigns[rep_id].update(allowed)

    if restricted_rep_allowed_campaigns:
        rep_scoped_source_keys = (
            "campaign_field_rep_assignment_v2",
            "inclinic_campaign_field_rep_assignment_v2",
            "doctor_field_rep_roster_bridge_v2",
            "inclinic_assigned_doctor_roster_v2",
            "inclinic_share_event_v2",
            "inclinic_collateral_transaction_v2",
        )
        for source_key in rep_scoped_source_keys:
            filtered_rows = []
            for row in output.get(source_key, []):
                rep_id = _field(row, "field_rep_id", "campaign_fieldrep_id", "current_campaign_fieldrep_id", "registered_by_id", "old_field_rep_id")
                allowed_campaigns = restricted_rep_allowed_campaigns.get(rep_id)
                if allowed_campaigns and not filter_rows_by_campaign_fields([row], allowed_campaigns, ("legacy_campaign_id", "campaign_id", "old_campaign_id", "old_brand_campaign_id", "brand_campaign_id")):
                    continue
                filtered_rows.append(row)
            output[source_key] = filtered_rows

        visible_restricted_rep_ids: set[str] = set()
        for source_key in ("campaign_field_rep_assignment_v2", "inclinic_campaign_field_rep_assignment_v2"):
            for row in output.get(source_key, []):
                rep_id = _field(row, "field_rep_id", "campaign_fieldrep_id", "current_campaign_fieldrep_id", "registered_by_id", "old_field_rep_id")
                if rep_id in restricted_rep_allowed_campaigns:
                    visible_restricted_rep_ids.add(rep_id)

        output["field_rep_v2"] = [
            row
            for row in output.get("field_rep_v2", [])
            if _field(row, "current_campaign_fieldrep_id", "id") not in restricted_rep_allowed_campaigns
            or _field(row, "current_campaign_fieldrep_id", "id") in visible_restricted_rep_ids
        ]
        output["inclinic_field_rep_identity_v2"] = [
            row
            for row in output.get("inclinic_field_rep_identity_v2", [])
            if _field(row, "campaign_fieldrep_id") not in restricted_rep_allowed_campaigns
            or _field(row, "campaign_fieldrep_id") in visible_restricted_rep_ids
        ]

    return output


def _apply_raw_visibility_to_source(source: dict[str, list[dict[str, Any]]], raw_visibility_rules: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not raw_visibility_rules:
        return source

    output = {key: list(rows) for key, rows in source.items()}

    def remove_hidden(source_key: str, hidden_ids: set[str], fields: tuple[str, ...]) -> None:
        if not hidden_ids:
            return
        output[source_key] = [
            row for row in output.get(source_key, []) if not row_matches_raw_visibility_ids(row, hidden_ids, fields)
        ]

    def retain_visible(source_key: str, keep_ids: set[str], fields: tuple[str, ...]) -> None:
        if not keep_ids:
            return
        output[source_key] = [
            row for row in output.get(source_key, []) if row_matches_raw_visibility_ids(row, keep_ids, fields)
        ]

    campaign_ids = raw_visibility_entity_ids(raw_visibility_rules, "campaign", system_key="inclinic")
    if campaign_ids:
        for source_key, fields in {
            "campaign_v2": ("legacy_campaign_id", "id", "campaign_id", "campaign_uuid", "brand_campaign_id", "source_pk_value"),
            "campaign_management_campaign": ("id", "brand_campaign_id", "campaign_id", "campaign_uuid", "source_pk_value"),
            "campaign_field_rep_assignment_v2": ("legacy_campaign_id", "campaign_id", "campaign_uuid"),
            "doctor_field_rep_roster_bridge_v2": ("legacy_campaign_id", "campaign_id", "campaign_uuid"),
            "inclinic_assigned_doctor_roster_v2": ("legacy_campaign_id", "campaign_id", "campaign_uuid"),
            "inclinic_campaign_collateral_v2": ("legacy_campaign_id", "campaign_id", "old_campaign_id", "brand_campaign_id", "campaign_uuid"),
            "inclinic_campaign_field_rep_assignment_v2": ("legacy_campaign_id", "campaign_id", "old_campaign_id", "brand_campaign_id", "campaign_uuid"),
            "inclinic_collateral_transaction_v2": ("legacy_campaign_id", "old_brand_campaign_id", "brand_campaign_id", "campaign_id", "campaign_uuid"),
            "inclinic_share_event_v2": ("legacy_campaign_id", "old_brand_campaign_id", "brand_campaign_id", "campaign_id", "campaign_uuid"),
        }.items():
            remove_hidden(source_key, campaign_ids, fields)

    field_rep_ids = raw_visibility_entity_ids(raw_visibility_rules, "field_rep", system_key="inclinic")
    if field_rep_ids:
        for source_key, fields in {
            "field_rep_v2": (
                "current_campaign_fieldrep_id",
                "id",
                "field_rep_uuid",
                "current_brand_supplied_field_rep_id",
                "brand_supplied_field_rep_id",
                "source_pk_value",
            ),
            "inclinic_field_rep_identity_v2": (
                "campaign_fieldrep_id",
                "field_rep_uuid",
                "source_value",
                "source_value_normalized",
                "source_pk_value",
            ),
            "campaign_field_rep_assignment_v2": ("field_rep_id", "campaign_fieldrep_id", "field_rep_uuid", "source_pk_value"),
            "doctor_field_rep_roster_bridge_v2": ("field_rep_id", "campaign_fieldrep_id", "old_field_rep_id", "field_rep_uuid"),
            "inclinic_assigned_doctor_roster_v2": ("field_rep_id", "campaign_fieldrep_id", "old_field_rep_id", "field_rep_uuid"),
            "inclinic_campaign_field_rep_assignment_v2": ("field_rep_id", "campaign_fieldrep_id", "old_field_rep_id", "field_rep_uuid"),
            "inclinic_collateral_transaction_v2": (
                "field_rep_id",
                "campaign_fieldrep_id",
                "old_field_rep_id",
                "brand_supplied_field_rep_id",
                "old_field_rep_unique_id",
                "field_rep_uuid",
            ),
            "inclinic_share_event_v2": (
                "field_rep_id",
                "campaign_fieldrep_id",
                "old_field_rep_id",
                "brand_supplied_field_rep_id",
                "old_field_rep_unique_id",
                "field_rep_uuid",
            ),
        }.items():
            remove_hidden(source_key, field_rep_ids, fields)

    doctor_ids = raw_visibility_entity_ids(raw_visibility_rules, "doctor", system_key="inclinic")
    if doctor_ids:
        for source_key, fields in {
            "doctor_field_rep_roster_bridge_v2": (
                "doctor_phone_normalized",
                "doctor_phone_raw",
                "doctor_uuid",
                "inclinic_doctor_uuid",
                "source_pk_value",
            ),
            "inclinic_assigned_doctor_roster_v2": (
                "doctor_phone_normalized",
                "doctor_phone_raw",
                "doctor_uuid",
                "inclinic_doctor_uuid",
                "source_pk_value",
            ),
            "inclinic_collateral_transaction_v2": (
                "doctor_phone_normalized",
                "old_doctor_number",
                "doctor_uuid",
                "inclinic_doctor_uuid",
                "old_doctor_unique_id",
            ),
            "inclinic_share_event_v2": (
                "doctor_phone_normalized",
                "old_doctor_identifier",
                "doctor_uuid",
                "inclinic_doctor_uuid",
            ),
        }.items():
            remove_hidden(source_key, doctor_ids, fields)

    collateral_keep_ids = raw_visibility_keep_only_ids(raw_visibility_rules, "collateral", system_key="inclinic")
    if collateral_keep_ids:
        for source_key, fields in {
            "inclinic_collateral_v2": ("old_id", "collateral_uuid", "source_pk_value"),
            "inclinic_campaign_collateral_v2": (
                "old_collateral_id",
                "collateral_uuid",
            ),
            "inclinic_collateral_transaction_v2": (
                "old_collateral_id",
                "collateral_uuid",
                "collateral_id",
                "source_pk_value",
            ),
            "inclinic_share_event_v2": (
                "old_collateral_id",
                "collateral_uuid",
                "collateral_id",
                "source_pk_value",
            ),
        }.items():
            retain_visible(source_key, collateral_keep_ids, fields)
    else:
        collateral_ids = raw_visibility_entity_ids(raw_visibility_rules, "collateral", system_key="inclinic")
        if collateral_ids:
            for source_key, fields in {
                "inclinic_collateral_v2": ("old_id", "collateral_uuid", "source_pk_value"),
                "inclinic_campaign_collateral_v2": (
                    "old_collateral_id",
                    "collateral_uuid",
                    "old_id",
                    "campaign_collateral_uuid",
                    "source_pk_value",
                ),
                "inclinic_collateral_transaction_v2": (
                    "old_collateral_id",
                    "collateral_uuid",
                    "collateral_id",
                    "source_pk_value",
                ),
                "inclinic_share_event_v2": (
                    "old_collateral_id",
                    "collateral_uuid",
                    "collateral_id",
                    "source_pk_value",
                ),
            }.items():
                remove_hidden(source_key, collateral_ids, fields)

    share_ids = raw_visibility_entity_ids(raw_visibility_rules, "share", system_key="inclinic")
    remove_hidden("inclinic_share_event_v2", share_ids, ("old_id", "share_event_uuid", "source_pk_value"))

    transaction_ids = raw_visibility_entity_ids(raw_visibility_rules, "transaction", system_key="inclinic")
    remove_hidden(
        "inclinic_collateral_transaction_v2",
        transaction_ids,
        ("old_id", "transaction_uuid", "old_transaction_id", "source_pk_value"),
    )

    return output


def _validate_required_v2_source_counts(source: dict[str, list[dict[str, Any]]]) -> None:
    empty_required_tables = [
        source_key
        for source_key in REQUIRED_V2_SOURCE_KEYS
        if len(source.get(source_key, [])) <= 0
    ]
    if empty_required_tables:
        raise RuntimeError(
            "InClinic V2 source safety check failed before silver/gold rebuild. "
            "Required V2 source tables returned zero rows: "
            f"{', '.join(empty_required_tables)}. Existing InClinic reporting tables were not replaced."
        )


def _rep_brand_id_by_campaign_fieldrep(source: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for rep in _current_field_rep_source_rows(source):
        rep_id = _field(rep, "current_campaign_fieldrep_id", "id")
        brand_id = _field(rep, "current_brand_supplied_field_rep_id", "brand_supplied_field_rep_id")
        if rep_id and brand_id:
            mapping[normalize_key(rep_id)] = brand_id
    for identity in source.get("inclinic_field_rep_identity_v2", []):
        rep_id = _field(identity, "campaign_fieldrep_id")
        brand_id = _field(identity, "brand_supplied_field_rep_id")
        if rep_id and brand_id:
            mapping.setdefault(normalize_key(rep_id), brand_id)
    return mapping


def _current_field_rep_source_rows(source: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in source.get("field_rep_v2", []):
        if not _row_is_current(row):
            continue
        key = _field_rep_merge_key(row)
        existing = rows_by_key.get(key)
        if existing is None or _source_freshness_key(row) >= _source_freshness_key(existing):
            rows_by_key[key] = row
    return list(rows_by_key.values())


def _rule_campaign_matches(rule: ReportingCorrectionRule, campaign_id: str) -> bool:
    return normalize_key(rule.campaign_id) == normalize_key(campaign_id)


def _rule_phone_matches(rule: ReportingCorrectionRule, *values: Any) -> bool:
    rule_digits = _source_digits(rule.doctor_phone)
    rule_last10 = normalize_phone(rule.doctor_phone_normalized or rule.doctor_phone)
    for value in values:
        digits = _source_digits(value)
        last10 = normalize_phone(value)
        if rule_digits and digits and rule_digits == digits:
            return True
        if rule_last10 and last10 and rule_last10 == last10:
            return True
    return False


def _brand_rep_matches(rule_brand_id: str, row_brand_id: str, row_rep_id: str, brand_by_rep_id: dict[str, str]) -> bool:
    expected = normalize_key(rule_brand_id)
    if not expected:
        return True
    candidates = [
        normalize_key(row_brand_id),
        normalize_key(row_rep_id),
        normalize_key(brand_by_rep_id.get(normalize_key(row_rep_id), "")),
    ]
    return expected in candidates


def _row_brand_rep_id(row: dict[str, Any], brand_by_rep_id: dict[str, str], *rep_id_fields: str) -> str:
    for field in ("brand_supplied_field_rep_id", "current_brand_supplied_field_rep_id", "old_field_rep_unique_id", "field_rep_unique_id"):
        value = _field(row, field)
        if value:
            return value
    for field in rep_id_fields:
        rep_id = _field(row, field)
        brand_id = brand_by_rep_id.get(normalize_key(rep_id), "")
        if brand_id:
            return brand_id
    return ""


def _affected_brand_ids(rule: ReportingCorrectionRule) -> set[str]:
    values = [rule.field_rep_brand_supplied_id, rule.affected_field_rep_brand_supplied_ids]
    out: set[str] = set()
    for raw in values:
        for item in re.split(r"[,/\s]+", _clean(raw)):
            normalized = normalize_key(item)
            if normalized:
                out.add(normalized)
    return out


def _should_exclude_roster_row(
    row: dict[str, Any],
    rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> bool:
    campaign_id = _field(row, "legacy_campaign_id")
    rep_id = _field(row, "campaign_fieldrep_id")
    brand_id = _row_brand_rep_id(row, brand_by_rep_id, "campaign_fieldrep_id")
    phone_values = [
        row.get("doctor_phone_raw"),
        row.get("doctor_phone_normalized"),
        row.get("old_doctor_number"),
    ]
    doctor_name = _field(row, "doctor_name_raw", "doctor_name_normalized")
    for rule in rules:
        if not _rule_campaign_matches(rule, campaign_id) or not _rule_phone_matches(rule, *phone_values):
            continue
        if rule.rule_type == RULE_KEEP_DOCTOR_WITH_REP:
            expected = normalize_key(rule.expected_field_rep_brand_supplied_id)
            actual = normalize_key(brand_id)
            affected = _affected_brand_ids(rule)
            if expected and actual != expected and (not affected or actual in affected):
                return True
        if rule.rule_type == RULE_EXCLUDE_INVALID_PHONE:
            if not _brand_rep_matches(rule.field_rep_brand_supplied_id, brand_id, rep_id, brand_by_rep_id):
                continue
            if rule.doctor_name and doctor_name and normalize_name(rule.doctor_name) != normalize_name(doctor_name):
                continue
            return True
    return False


def _should_exclude_activity_row(
    *,
    campaign_id: str,
    rep_id: str,
    brand_id: str,
    doctor_name: str,
    phone_values: list[Any],
    rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> bool:
    for rule in rules:
        if rule.rule_type != RULE_EXCLUDE_INVALID_PHONE:
            continue
        if not _rule_campaign_matches(rule, campaign_id) or not _rule_phone_matches(rule, *phone_values):
            continue
        if not _brand_rep_matches(rule.field_rep_brand_supplied_id, brand_id, rep_id, brand_by_rep_id):
            continue
        if rule.doctor_name and doctor_name and normalize_name(rule.doctor_name) != normalize_name(doctor_name):
            continue
        return True
    return False


def _campaign_rows(source: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    local_by_brand = {
        _clean(row.get("brand_campaign_id")): row
        for row in source["campaign_management_campaign"]
        if _clean(row.get("brand_campaign_id"))
    }
    rows: list[dict[str, Any]] = []
    for campaign in source["campaign_v2"]:
        if not _row_is_current(campaign) or not _truthy(campaign.get("system_ic")):
            continue
        campaign_id = _field(campaign, "legacy_campaign_id", "id")
        local = local_by_brand.get(campaign_id, {})
        rows.append(
            {
                "id": campaign_id,
                "brand_campaign_id": campaign_id,
                "campaign_uuid": _clean(campaign.get("campaign_uuid")),
                "name": _field(local, "name") or _field(campaign, "name"),
                "brand_id": _clean(campaign.get("brand_id")),
                "brand_name": _field(local, "brand_name") or _field(campaign, "name"),
                "company_name": _field(local, "company_name") or _field(local, "brand_name") or _field(campaign, "name"),
                "company_logo": _clean(local.get("company_logo")),
                "start_date": _field(local, "start_date") or _field(campaign, "start_date"),
                "end_date": _field(local, "end_date") or _field(campaign, "end_date"),
                "status": _field(local, "status") or _field(campaign, "status"),
                "num_doctors": _field(local, "num_doctors") or _field(campaign, "num_doctors_supported"),
                "num_doctors_supported": _field(campaign, "num_doctors_supported") or _field(local, "num_doctors"),
                "created_at": _field(campaign, "created_at"),
                "updated_at": _field(campaign, "updated_at"),
                "source_system": "rfa_master+inclinic",
            }
        )
    return rows


def _field_rep_rows(
    source: dict[str, list[dict[str, Any]]],
    now: str,
    state_fallback_by_rep: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    identity_state_by_rep = _identity_state_by_campaign_fieldrep(source)
    state_fallback_by_rep = state_fallback_by_rep or {}
    for rep in _current_field_rep_source_rows(source):
        rep_id = _field(rep, "current_campaign_fieldrep_id", "id")
        if not rep_id:
            continue
        rep_key = normalize_key(rep_id)
        state = (
            _first_state(rep, "state", "state_normalized", "current_state", "field_rep_state", "campaign_fieldrep_state")
            or identity_state_by_rep.get(rep_key, "")
            or state_fallback_by_rep.get(rep_key, "")
        )
        rows.append(
            {
                "id": rep_id,
                "full_name": _field(rep, "display_name", "full_name"),
                "phone_number": _clean(rep.get("phone_number")),
                "brand_supplied_field_rep_id": _field(rep, "current_brand_supplied_field_rep_id", "brand_supplied_field_rep_id"),
                "is_active": "true" if _truthy(rep.get("is_active")) else "false",
                "password_hash": _clean(rep.get("password_hash")),
                "created_at": _field(rep, "created_at"),
                "updated_at": _field(rep, "updated_at"),
                "brand_id": _clean(rep.get("brand_id")),
                "user_id": _clean(rep.get("user_id")),
                "state": state,
                "field_rep_phone_normalized": _phone(rep.get("phone_number")),
                "field_rep_email_best": _clean(rep.get("primary_email")),
                "state_normalized": _state_for_value(state),
                "is_active_flag": "true" if _truthy(rep.get("is_active")) else "false",
                "created_at_ts": _field(rep, "created_at"),
                "updated_at_ts": _field(rep, "updated_at"),
                "campaign_id": "",
                "source_table": "field_rep_v2",
                "source_field_rep_id": rep_id,
                "_silver_updated_at": now,
                "_dq_status": _field(rep, "verification_status") or "PASS",
                "_dq_errors": "",
            }
        )
    return rows


def _assignment_rows(source: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for assignment in source["campaign_field_rep_assignment_v2"]:
        if not _row_is_current(assignment):
            continue
        campaign_id = _field(assignment, "legacy_campaign_id", "campaign_id")
        rep_id = _field(assignment, "field_rep_id", "campaign_fieldrep_id")
        if not campaign_id or not rep_id:
            continue
        key = (campaign_id, rep_id)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "id": _field(assignment, "legacy_campaign_fieldrep_id", "id", "campaign_field_rep_assignment_uuid"),
                "campaign_id": campaign_id,
                "field_rep_id": rep_id,
                "created_at": _field(assignment, "assigned_at", "created_at"),
                "state": _clean(assignment.get("state")),
                "assignment_status": _field(assignment, "assignment_status") or "active",
            }
        )
    return rows


def _collateral_rows(source: dict[str, list[dict[str, Any]]], now: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for collateral in source["inclinic_collateral_v2"]:
        if not _row_is_current(collateral):
            continue
        collateral_id = _field(collateral, "old_id", "collateral_uuid")
        title = _field(collateral, "old_title", "collateral_uuid")
        has_content = bool(_field(collateral, "old_file", "old_vimeo_url"))
        rows.append(
            {
                "id": collateral_id,
                "type": _field(collateral, "old_type", "content_type_normalized"),
                "title": title,
                "file": _clean(collateral.get("old_file")),
                "vimeo_url": _clean(collateral.get("old_vimeo_url")),
                "content_id": _clean(collateral.get("old_content_id")),
                "upload_date": _field(collateral, "old_upload_date"),
                "is_active": "true" if _truthy(collateral.get("old_is_active")) else "false",
                "created_at": _field(collateral, "old_created_at", "source_created_at"),
                "updated_at": _field(collateral, "old_updated_at", "source_updated_at"),
                "banner_1": _clean(collateral.get("old_banner_1")),
                "banner_2": _clean(collateral.get("old_banner_2")),
                "campaign_id": _clean(collateral.get("old_campaign_id")),
                "created_by_id": _clean(collateral.get("old_created_by_id")),
                "description": _clean(collateral.get("old_description")),
                "purpose": _clean(collateral.get("old_purpose")),
                "doctor_name": _clean(collateral.get("old_doctor_name")),
                "webinar_date": _field(collateral, "old_webinar_date"),
                "webinar_description": _clean(collateral.get("old_webinar_description")),
                "webinar_title": _clean(collateral.get("old_webinar_title")),
                "webinar_url": _clean(collateral.get("old_webinar_url")),
                "is_active_flag": "true" if _truthy(collateral.get("old_is_active")) else "false",
                "upload_date_ts": _field(collateral, "old_upload_date"),
                "created_at_ts": _field(collateral, "old_created_at", "source_created_at"),
                "updated_at_ts": _field(collateral, "old_updated_at", "source_updated_at"),
                "webinar_date_dt": _field(collateral, "old_webinar_date"),
                "collateral_display_name": title,
                "content_missing_flag": "0" if has_content else "1",
                "_silver_updated_at": now,
            }
        )
    return rows


def _campaign_uuid_to_legacy(source: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table in (
        "inclinic_campaign_field_rep_assignment_v2",
        "inclinic_assigned_doctor_roster_v2",
        "inclinic_collateral_transaction_v2",
        "inclinic_share_event_v2",
    ):
        for row in source[table]:
            campaign_uuid = _clean(row.get("campaign_uuid"))
            legacy = _clean(row.get("legacy_campaign_id"))
            if campaign_uuid and legacy and legacy.lower() != "null":
                mapping[campaign_uuid] = legacy
    for row in source["campaign_v2"]:
        campaign_uuid = _clean(row.get("campaign_uuid"))
        legacy = _field(row, "legacy_campaign_id", "id")
        if campaign_uuid and legacy:
            mapping.setdefault(campaign_uuid, legacy)
    return mapping


def _campaign_lookup(source: dict[str, list[dict[str, Any]]]) -> tuple[dict[str, str], dict[str, str]]:
    local_to_brand = {
        _clean(row.get("id")): _clean(row.get("brand_campaign_id"))
        for row in source["campaign_management_campaign"]
        if _clean(row.get("id")) and _clean(row.get("brand_campaign_id"))
    }
    normalized_to_brand: dict[str, str] = {}
    for row in source["campaign_v2"]:
        campaign_id = _field(row, "legacy_campaign_id", "id")
        if campaign_id:
            normalized_to_brand[_norm(campaign_id)] = campaign_id
    for row in source["campaign_management_campaign"]:
        brand_id = _clean(row.get("brand_campaign_id"))
        if brand_id:
            normalized_to_brand[_norm(brand_id)] = brand_id
    return local_to_brand, normalized_to_brand


def _resolve_campaign_id(source: dict[str, list[dict[str, Any]]], *values: Any) -> str:
    local_to_brand, normalized_to_brand = _campaign_lookup(source)
    for value in values:
        raw = _clean(value)
        if not raw:
            continue
        if raw in local_to_brand:
            return local_to_brand[raw]
        normalized = _norm(raw)
        if normalized in normalized_to_brand:
            return normalized_to_brand[normalized]
        if len(normalized) >= 24:
            return raw
    return ""


def _schedule_rows(source: dict[str, list[dict[str, Any]]], dim_collateral: list[dict[str, Any]], now: str) -> list[dict[str, Any]]:
    legacy_by_uuid = _campaign_uuid_to_legacy(source)
    collateral_by_uuid = _build_indexes(source["inclinic_collateral_v2"], "collateral_uuid")
    collateral_dim_by_id = {row["id"]: row for row in dim_collateral}
    rows: list[dict[str, Any]] = []
    source_rows_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in source["inclinic_campaign_collateral_v2"]:
        if not _row_is_current(row):
            continue
        key = _campaign_collateral_merge_key(row)
        existing = source_rows_by_key.get(key)
        if existing is None or _source_freshness_key(row) >= _source_freshness_key(existing):
            source_rows_by_key[key] = row

    for row in source_rows_by_key.values():
        legacy_campaign_id = legacy_by_uuid.get(_clean(row.get("campaign_uuid")), "") or _resolve_campaign_id(
            source,
            row.get("legacy_campaign_id"),
            row.get("brand_campaign_id"),
            row.get("old_campaign_id"),
            row.get("campaign_id"),
        )
        old_collateral_id = _field(row, "old_collateral_id")
        if not old_collateral_id:
            collateral_source = collateral_by_uuid.get(_clean(row.get("collateral_uuid")), {})
            old_collateral_id = _field(collateral_source, "old_id")
        if not legacy_campaign_id or not old_collateral_id:
            continue
        collateral = collateral_dim_by_id.get(old_collateral_id, {})
        start = _field(row, "old_start_date")
        end = _field(row, "old_end_date")
        rows.append(
            {
                "id": _field(row, "old_id", "campaign_collateral_uuid"),
                "start_date": start,
                "end_date": end,
                "created_at": _field(row, "old_created_at", "source_created_at"),
                "updated_at": _field(row, "old_updated_at", "source_updated_at"),
                "campaign_id": legacy_campaign_id,
                "collateral_id": old_collateral_id,
                "schedule_start_ts": start,
                "schedule_end_ts": end,
                "schedule_start_date": start[:10] if start else "",
                "schedule_end_date": end[:10] if end else "",
                "schedule_missing_flag": "1" if not start or not end else "0",
                "campaign_id_resolved": legacy_campaign_id,
                "collateral_type": _clean(collateral.get("type")),
                "collateral_title": _clean(collateral.get("title")),
                "_silver_updated_at": now,
            }
        )
    return rows


def _doctor_key(campaign_id: str, phone: str, fallback: str) -> str:
    return _md5(campaign_id, phone) if campaign_id and phone else _md5(campaign_id, fallback)


def _assigned_roster_rows(
    source: dict[str, list[dict[str, Any]]],
    correction_rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for table_name in ("inclinic_assigned_doctor_roster_v2", "doctor_field_rep_roster_bridge_v2"):
        for row in source.get(table_name, []):
            if not _row_is_current(row):
                continue
            if _should_exclude_roster_row(row, correction_rules, brand_by_rep_id):
                continue
            campaign_id = _field(row, "legacy_campaign_id")
            rep_id = _field(row, "campaign_fieldrep_id")
            phone = _phone(row.get("doctor_phone_normalized") or row.get("doctor_phone_raw"))
            if not campaign_id or not rep_id or not phone:
                continue
            key = (campaign_id, rep_id, phone)
            if key in rows and rows[key].get("source_table") == "inclinic_assigned_doctor_roster_v2":
                continue
            rows[key] = {**row, "_reporting_source_table": table_name}
    return list(rows.values())


def _doctor_rows(
    source: dict[str, list[dict[str, Any]]],
    dim_field_rep_by_id: dict[str, dict[str, Any]],
    now: str,
    correction_rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}

    def add(row: dict[str, Any], source_name: str, campaign_id: str, rep_id: str, phone: str, name: str, doctor_uuid: str = "") -> None:
        if not campaign_id:
            return
        key_value = _doctor_key(campaign_id, phone, doctor_uuid or name)
        if not key_value:
            return
        key = (key_value, rep_id)
        if key in rows and rows[key]["source"] == "assigned_roster":
            return
        rep = dim_field_rep_by_id.get(rep_id)
        rows[key] = {
            "id": doctor_uuid or key_value,
            "name": name or "Unknown Doctor",
            "phone": phone,
            "rep_id": rep_id,
            "source": source_name,
            "doctor_phone_normalized": phone,
            "doctor_identity_source": "campaign_phone" if phone else source_name,
            "doctor_identity_key": key_value,
            "rep_id_normalized": rep_id,
            "field_rep_id_resolved": rep_id,
            "state_normalized": _state_for_rep(rep),
            "_silver_updated_at": now,
            "_dq_status": "PASS",
            "_dq_errors": "",
        }

    for roster in _assigned_roster_rows(source, correction_rules, brand_by_rep_id):
        add(
            roster,
            "assigned_roster",
            _field(roster, "legacy_campaign_id"),
            _field(roster, "campaign_fieldrep_id"),
            _phone(roster.get("doctor_phone_normalized") or roster.get("doctor_phone_raw")),
            _field(roster, "doctor_name_raw", "doctor_name_normalized"),
            _field(roster, "doctor_uuid", "inclinic_doctor_uuid"),
        )

    for tx in source["inclinic_collateral_transaction_v2"]:
        if not _row_is_current(tx):
            continue
        campaign_id = _resolve_campaign_id(source, tx.get("legacy_campaign_id"), tx.get("old_brand_campaign_id"))
        rep_id = _field(tx, "campaign_fieldrep_id", "old_field_rep_id")
        phone = _phone(tx.get("doctor_phone_normalized") or tx.get("old_doctor_number"))
        if _should_exclude_activity_row(
            campaign_id=campaign_id,
            rep_id=rep_id,
            brand_id=_row_brand_rep_id(tx, brand_by_rep_id, "campaign_fieldrep_id", "old_field_rep_id"),
            doctor_name=_field(tx, "old_doctor_name"),
            phone_values=[tx.get("old_doctor_number"), tx.get("doctor_phone_normalized")],
            rules=correction_rules,
            brand_by_rep_id=brand_by_rep_id,
        ):
            continue
        add(
            tx,
            "collateral_transaction",
            campaign_id,
            rep_id,
            phone,
            _field(tx, "old_doctor_name"),
            _field(tx, "doctor_uuid", "inclinic_doctor_uuid", "old_doctor_unique_id"),
        )

    for share in source["inclinic_share_event_v2"]:
        if not _row_is_current(share):
            continue
        campaign_id = _resolve_campaign_id(source, share.get("legacy_campaign_id"), share.get("old_brand_campaign_id"))
        rep_id = _field(share, "campaign_fieldrep_id", "old_field_rep_id")
        phone = _phone(share.get("doctor_phone_normalized") or share.get("old_doctor_identifier"))
        if _should_exclude_activity_row(
            campaign_id=campaign_id,
            rep_id=rep_id,
            brand_id=_row_brand_rep_id(share, brand_by_rep_id, "campaign_fieldrep_id", "old_field_rep_id"),
            doctor_name="",
            phone_values=[share.get("old_doctor_identifier"), share.get("doctor_phone_normalized")],
            rules=correction_rules,
            brand_by_rep_id=brand_by_rep_id,
        ):
            continue
        add(
            share,
            "share_event",
            campaign_id,
            rep_id,
            phone,
            "",
            _field(share, "doctor_uuid", "inclinic_doctor_uuid"),
        )

    return list(rows.values())


def _share_rows(
    source: dict[str, list[dict[str, Any]]],
    run_id: str,
    now: str,
    correction_rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for share in source["inclinic_share_event_v2"]:
        if not _row_is_current(share):
            continue
        campaign_id = _resolve_campaign_id(source, share.get("legacy_campaign_id"), share.get("old_brand_campaign_id"))
        collateral_id = _field(share, "old_collateral_id", "collateral_uuid")
        phone = _phone(share.get("doctor_phone_normalized") or share.get("old_doctor_identifier"))
        rep_id = _resolve_share_rep_id(share, campaign_id, {}, {})
        if _should_exclude_activity_row(
            campaign_id=campaign_id,
            rep_id=rep_id,
            brand_id=_row_brand_rep_id(share, brand_by_rep_id, "campaign_fieldrep_id", "old_field_rep_id"),
            doctor_name="",
            phone_values=[share.get("old_doctor_identifier"), share.get("doctor_phone_normalized")],
            rules=correction_rules,
            brand_by_rep_id=brand_by_rep_id,
        ):
            continue
        share_id = _field(share, "old_id", "share_event_uuid")
        rows.append(
            {
                "id": share_id,
                "share_channel": _field(share, "old_share_channel", "share_channel_normalized"),
                "share_timestamp": _field(share, "shared_at", "old_share_timestamp"),
                "message_text": _clean(share.get("old_message_text")),
                "created_at": _field(share, "old_created_at", "source_created_at"),
                "updated_at": _field(share, "old_updated_at", "source_updated_at"),
                "short_link_id": _clean(share.get("old_short_link_id")),
                "collateral_id": collateral_id,
                "doctor_identifier": _field(share, "old_doctor_identifier", "doctor_phone_normalized"),
                "brand_campaign_id": campaign_id,
                "field_rep_email": _field(share, "old_field_rep_email", "field_rep_email_normalized"),
                "field_rep_id": rep_id,
                "_ingestion_run_id": run_id,
                "_ingested_at": now,
                "_source_server": "inclinic_live",
                "_source_table": "inclinic_share_event_v2",
                "_extract_started_at": now,
                "_extract_ended_at": now,
                "_record_hash": _md5("share", share_id, campaign_id, collateral_id, phone, rep_id),
                "_is_deleted": "false",
                "_dq_status": _field(share, "verification_status") or "PASS",
                "_dq_errors": "",
                "doctor_identifier_normalized": phone,
                "doctor_identity_key": _doctor_key(campaign_id, phone, _field(share, "doctor_uuid", "share_event_uuid")),
                "reached_event_ts": _field(share, "shared_at", "old_share_timestamp", "old_created_at"),
                "share_timestamp_ts": _field(share, "shared_at", "old_share_timestamp"),
                "created_at_ts": _field(share, "old_created_at", "source_created_at"),
                "updated_at_ts": _field(share, "old_updated_at", "source_updated_at"),
                "_silver_updated_at": now,
                "_as_of_run_id": run_id,
            }
        )
    return rows


def _transaction_rows(
    source: dict[str, list[dict[str, Any]]],
    run_id: str,
    now: str,
    correction_rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for tx in source["inclinic_collateral_transaction_v2"]:
        if not _row_is_current(tx):
            continue
        campaign_id = _resolve_campaign_id(source, tx.get("legacy_campaign_id"), tx.get("old_brand_campaign_id"))
        collateral_id = _field(tx, "old_collateral_id", "collateral_uuid")
        phone = _phone(tx.get("doctor_phone_normalized") or tx.get("old_doctor_number"))
        rep_id = _field(tx, "campaign_fieldrep_id", "old_field_rep_id")
        if _should_exclude_activity_row(
            campaign_id=campaign_id,
            rep_id=rep_id,
            brand_id=_row_brand_rep_id(tx, brand_by_rep_id, "campaign_fieldrep_id", "old_field_rep_id"),
            doctor_name=_field(tx, "old_doctor_name"),
            phone_values=[tx.get("old_doctor_number"), tx.get("doctor_phone_normalized")],
            rules=correction_rules,
            brand_by_rep_id=brand_by_rep_id,
        ):
            continue
        tx_id = _field(tx, "old_id", "transaction_uuid")
        transaction_id = _field(tx, "old_transaction_id") or _md5("tx", campaign_id, collateral_id, phone, rep_id, tx_id)
        has_viewed = _truthy(tx.get("old_has_viewed")) or bool(_field(tx, "old_viewed_at", "old_first_viewed_at"))
        downloaded_pdf = _truthy(tx.get("old_downloaded_pdf"))
        pdf_completed = _truthy(tx.get("old_pdf_completed"))
        video_gt_50 = _truthy(tx.get("old_video_view_gt_50")) or (_num(tx.get("old_video_watch_percentage")).isdigit() and int(_num(tx.get("old_video_watch_percentage"))) >= 50)
        opened_ts = _event_date(tx, "old_first_viewed_at", "old_viewed_at")
        video_ts = _event_date(tx, "old_video_gt_50_at", "old_video_100_at", "old_last_viewed_at", "old_updated_at") if video_gt_50 else ""
        pdf_ts = _event_date(tx, "old_viewed_last_page_at", "old_updated_at") if downloaded_pdf or pdf_completed else ""
        doctor_identity_key = _doctor_key(campaign_id, phone, _field(tx, "doctor_uuid", "transaction_uuid"))
        rows.append(
            {
                "id": tx_id,
                "transaction_id": transaction_id,
                "source_transaction_id": _field(tx, "old_transaction_id"),
                "brand_campaign_id": campaign_id,
                "field_rep_id": rep_id,
                "field_rep_unique_id": _field(tx, "brand_supplied_field_rep_id", "old_field_rep_unique_id"),
                "doctor_name": _field(tx, "old_doctor_name"),
                "doctor_number": _field(tx, "old_doctor_number", "doctor_phone_normalized"),
                "doctor_unique_id": _field(tx, "old_doctor_unique_id", "doctor_uuid", "inclinic_doctor_uuid"),
                "collateral_id": collateral_id,
                "transaction_date": _field(tx, "old_transaction_date"),
                "has_viewed": "true" if has_viewed else "false",
                "downloaded_pdf": "true" if downloaded_pdf else "false",
                "pdf_completed": "true" if pdf_completed else "false",
                "video_view_lt_50": _num(tx.get("old_video_view_lt_50")),
                "video_view_gt_50": "true" if video_gt_50 else "false",
                "video_completed": "true" if _truthy(tx.get("old_video_completed")) else "false",
                "pdf_total_pages": _num(tx.get("old_pdf_total_pages")),
                "last_video_percentage": _num(tx.get("old_last_video_percentage")),
                "pdf_last_page": _num(tx.get("old_pdf_last_page")),
                "doctor_viewer_engagement_id": _clean(tx.get("old_doctor_viewer_engagement_id")),
                "share_management_engagement_id": _clean(tx.get("old_share_management_engagement_id")),
                "video_tracking_last_event_id": _clean(tx.get("old_video_tracking_last_event_id")),
                "created_at": _field(tx, "old_created_at", "source_created_at"),
                "updated_at": _field(tx, "old_updated_at", "source_updated_at"),
                "sent_at": _field(tx, "old_sent_at"),
                "viewed_at": _field(tx, "old_viewed_at"),
                "first_viewed_at": _field(tx, "old_first_viewed_at"),
                "viewed_last_page_at": _field(tx, "old_viewed_last_page_at"),
                "video_lt_50_at": _field(tx, "old_video_lt_50_at"),
                "video_gt_50_at": _field(tx, "old_video_gt_50_at"),
                "video_100_at": _field(tx, "old_video_100_at"),
                "last_viewed_at": _field(tx, "old_last_viewed_at"),
                "dv_engagement_id": _clean(tx.get("old_dv_engagement_id")),
                "field_rep_email": _clean(tx.get("old_field_rep_email")),
                "share_channel": _clean(tx.get("old_share_channel")),
                "sm_engagement_id": _clean(tx.get("old_sm_engagement_id")),
                "video_watch_percentage": _num(tx.get("old_video_watch_percentage")),
                "_ingestion_run_id": run_id,
                "_ingested_at": now,
                "_source_server": "inclinic_live",
                "_source_table": "inclinic_collateral_transaction_v2",
                "_extract_started_at": now,
                "_extract_ended_at": now,
                "_record_hash": _md5("tx", tx_id, transaction_id, campaign_id, collateral_id, phone, rep_id),
                "_is_deleted": "false",
                "_dq_status": _field(tx, "verification_status") or "PASS",
                "_dq_errors": _field(tx, "field_rep_identifier_consistency_status"),
                "doctor_phone_normalized": phone,
                "doctor_identity_key": doctor_identity_key,
                "transaction_identity_key": _md5("txid", transaction_id),
                "doctor_master_id_resolved": _field(tx, "doctor_uuid", "inclinic_doctor_uuid"),
                "field_rep_master_id_resolved": rep_id,
                "brand_supplied_field_rep_id_resolved": _field(tx, "brand_supplied_field_rep_id", "old_field_rep_unique_id"),
                "has_viewed_flag": "1" if has_viewed else "0",
                "downloaded_pdf_flag": "1" if downloaded_pdf else "0",
                "pdf_completed_flag": "1" if pdf_completed else "0",
                "video_view_gt_50_flag": "1" if video_gt_50 else "0",
                "last_video_percentage_num": _num(tx.get("old_last_video_percentage")),
                "video_watch_percentage_num": _num(tx.get("old_video_watch_percentage")),
                "pdf_last_page_num": _num(tx.get("old_pdf_last_page")),
                "pdf_total_pages_num": _num(tx.get("old_pdf_total_pages")),
                "created_at_ts": _field(tx, "old_created_at", "source_created_at"),
                "updated_at_ts": _field(tx, "old_updated_at", "source_updated_at"),
                "transaction_date_ts": _field(tx, "old_transaction_date"),
                "sent_at_ts": _field(tx, "old_sent_at"),
                "viewed_at_ts": _field(tx, "old_viewed_at"),
                "first_viewed_at_ts": _field(tx, "old_first_viewed_at"),
                "viewed_last_page_at_ts": _field(tx, "old_viewed_last_page_at"),
                "video_lt_50_at_ts": _field(tx, "old_video_lt_50_at"),
                "video_gt_50_at_ts": _field(tx, "old_video_gt_50_at"),
                "video_100_at_ts": _field(tx, "old_video_100_at"),
                "last_viewed_at_ts": _field(tx, "old_last_viewed_at"),
                "reached_event_ts": _event_date(tx, "old_sent_at", "old_transaction_date", "old_created_at"),
                "opened_event_ts": opened_ts,
                "video_gt_50_event_ts": video_ts,
                "pdf_download_event_ts": pdf_ts,
                "_silver_updated_at": now,
                "_as_of_run_id": run_id,
            }
        )
    return rows


def _bridge_base_rows(
    source: dict[str, list[dict[str, Any]]],
    dim_field_rep_by_id: dict[str, dict[str, Any]],
    now: str,
    correction_rules: list[ReportingCorrectionRule],
    brand_by_rep_id: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for roster in _assigned_roster_rows(source, correction_rules, brand_by_rep_id):
        campaign_id = _field(roster, "legacy_campaign_id")
        rep_id = _field(roster, "campaign_fieldrep_id")
        phone = _phone(roster.get("doctor_phone_normalized") or roster.get("doctor_phone_raw"))
        doctor_key = _doctor_key(campaign_id, phone, _field(roster, "doctor_uuid", "source_pk_value"))
        key = (campaign_id, rep_id, doctor_key)
        if not campaign_id or not rep_id or not doctor_key or key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "brand_campaign_id": campaign_id,
                "doctor_identity_key": doctor_key,
                "doctor_master_id_resolved": _field(roster, "doctor_uuid", "inclinic_doctor_uuid"),
                "field_rep_id_resolved": rep_id,
                "state_normalized": _state_for_rep(dim_field_rep_by_id.get(rep_id)),
                "inclusion_reason": "ASSIGNED_DOCTOR_ROSTER_V2",
                "_silver_updated_at": now,
                "_dq_status": _field(roster, "verification_status") or "PASS",
                "_dq_errors": _field(roster, "match_status"),
            }
        )
    return rows


def _map_rows(
    fact_tx: list[dict[str, Any]],
    fact_share: list[dict[str, Any]],
    bridge_rows: list[dict[str, Any]],
    now: str,
) -> list[dict[str, Any]]:
    campaign_ids = {row["brand_campaign_id"] for row in fact_tx if row.get("brand_campaign_id")}
    campaign_ids.update(row["brand_campaign_id"] for row in fact_share if row.get("brand_campaign_id"))
    campaign_ids.update(row["brand_campaign_id"] for row in bridge_rows if row.get("brand_campaign_id"))
    return [
        {
            "brand_campaign_id": campaign_id,
            "campaign_id_resolved": campaign_id,
            "distinct_campaign_id_count": "1",
            "_dq_status": "PASS",
            "_dq_errors": "",
            "_silver_updated_at": now,
        }
        for campaign_id in sorted(campaign_ids)
    ]


def _email_key(value: Any) -> str:
    return _clean(value).lower()


def _field_rep_identity_priority(row: dict[str, Any]) -> tuple[int, int, str]:
    source_table = _clean(row.get("source_table")).lower()
    source_column = _clean(row.get("source_column")).lower()
    source_rank = {
        "user_management_user": 0,
        "campaign_fieldrep": 10,
        "auth_user": 20,
    }.get(source_table, 50)
    column_rank = {
        "email": 0,
        "field_id": 10,
        "id": 20,
    }.get(source_column, 50)
    return source_rank, column_rank, _field(row, "campaign_fieldrep_id")


def _share_email_resolution_context(source: dict[str, list[dict[str, Any]]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, set[str]]]:
    identities_by_email: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for identity in source.get("inclinic_field_rep_identity_v2", []):
        if not _row_is_current(identity):
            continue
        rep_id = _field(identity, "campaign_fieldrep_id")
        if not rep_id:
            continue
        for email in (
            identity.get("email_normalized"),
            identity.get("user_management_email"),
            identity.get("auth_user_email"),
        ):
            key = _email_key(email)
            if key:
                identities_by_email[key].append(identity)

    for key, rows in identities_by_email.items():
        identities_by_email[key] = sorted(rows, key=_field_rep_identity_priority)

    assigned_reps_by_campaign: dict[str, set[str]] = defaultdict(set)
    for table in ("campaign_field_rep_assignment_v2", "inclinic_campaign_field_rep_assignment_v2"):
        for assignment in source.get(table, []):
            if not _row_is_current(assignment):
                continue
            campaign_id = _field(assignment, "legacy_campaign_id", "campaign_id")
            rep_id = _field(assignment, "field_rep_id", "campaign_fieldrep_id")
            if campaign_id and rep_id:
                assigned_reps_by_campaign[campaign_id].add(rep_id)
    return identities_by_email, assigned_reps_by_campaign


def _resolve_share_rep_id(
    share: dict[str, Any],
    campaign_id: str,
    identities_by_email: dict[str, list[dict[str, Any]]],
    assigned_reps_by_campaign: dict[str, set[str]],
) -> str:
    return _field(share, "campaign_fieldrep_id", "old_field_rep_id")


def _first_seen_rows(fact_tx: list[dict[str, Any]], fact_share: list[dict[str, Any]], now: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, str]] = {}

    def min_ts(current: str, candidate: str) -> str:
        if not candidate:
            return current
        if not current:
            return candidate
        return min(current, candidate)

    for tx in fact_tx:
        key = (tx["brand_campaign_id"], tx["collateral_id"], tx["doctor_identity_key"])
        if not all(key):
            continue
        row = grouped.setdefault(
            key,
            {
                "brand_campaign_id": key[0],
                "collateral_id": key[1],
                "doctor_identity_key": key[2],
                "reached_first_ts": "",
                "opened_first_ts": "",
                "video_gt_50_first_ts": "",
                "pdf_download_first_ts": "",
                "last_activity_ts": "",
                "_silver_updated_at": now,
            },
        )
        row["reached_first_ts"] = min_ts(row["reached_first_ts"], tx.get("reached_event_ts", ""))
        row["opened_first_ts"] = min_ts(row["opened_first_ts"], tx.get("opened_event_ts", ""))
        row["video_gt_50_first_ts"] = min_ts(row["video_gt_50_first_ts"], tx.get("video_gt_50_event_ts", ""))
        row["pdf_download_first_ts"] = min_ts(row["pdf_download_first_ts"], tx.get("pdf_download_event_ts", ""))
        row["last_activity_ts"] = max(row["last_activity_ts"], tx.get("updated_at_ts", ""), tx.get("last_viewed_at_ts", ""))

    for share in fact_share:
        key = (share["brand_campaign_id"], share["collateral_id"], share["doctor_identity_key"])
        if not all(key):
            continue
        row = grouped.setdefault(
            key,
            {
                "brand_campaign_id": key[0],
                "collateral_id": key[1],
                "doctor_identity_key": key[2],
                "reached_first_ts": "",
                "opened_first_ts": "",
                "video_gt_50_first_ts": "",
                "pdf_download_first_ts": "",
                "last_activity_ts": "",
                "_silver_updated_at": now,
            },
        )
        row["reached_first_ts"] = min_ts(row["reached_first_ts"], share.get("reached_event_ts", ""))
        row["last_activity_ts"] = max(row["last_activity_ts"], share.get("updated_at_ts", ""))

    return list(grouped.values())


def _bronze_relation_kind(table: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
            """,
            [BRONZE_COMPAT_SCHEMA, table],
        )
        row = cursor.fetchone()
    return str(row[0]) if row else ""


def _archive_existing_bronze_relation(table: str, relkind: str) -> None:
    ensure_schema(BRONZE_LEGACY_ARCHIVE_SCHEMA)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    archive_name = f"{table}__legacy_{timestamp}"
    archive_command = "ALTER MATERIALIZED VIEW" if relkind == "m" else "ALTER TABLE"
    source_ref = f"{_qident(BRONZE_COMPAT_SCHEMA)}.{_qident(table)}"
    renamed_ref = f"{_qident(BRONZE_COMPAT_SCHEMA)}.{_qident(archive_name)}"
    execute(f"{archive_command} {source_ref} RENAME TO {_qident(archive_name)};")
    execute(
        f"{archive_command} {renamed_ref} "
        f"SET SCHEMA {_qident(BRONZE_LEGACY_ARCHIVE_SCHEMA)};"
    )


def _prepare_bronze_compat_relation(table: str) -> None:
    relkind = _bronze_relation_kind(table)
    if not relkind:
        return
    if relkind == "v":
        execute(f"DROP VIEW {_qident(BRONZE_COMPAT_SCHEMA)}.{_qident(table)} CASCADE;")
        return
    if relkind in {"r", "p", "f", "m"}:
        _archive_existing_bronze_relation(table, relkind)
        return
    raise RuntimeError(
        f"Cannot create V2 bronze compatibility view for bronze.{table}: "
        f"unexpected relation type '{relkind}'. Existing object was not changed."
    )


def _create_bronze_views() -> None:
    ensure_schema(BRONZE_COMPAT_SCHEMA)
    for table in BRONZE_COMPAT_TABLES:
        _prepare_bronze_compat_relation(table)

    execute(
        """
        CREATE VIEW bronze.campaign_fieldrep AS
        SELECT
            id,
            full_name,
            phone_number,
            brand_supplied_field_rep_id,
            is_active,
            password_hash,
            created_at,
            updated_at,
            brand_id,
            user_id,
            state
        FROM silver.dim_field_rep;
        """
    )
    execute(
        """
        CREATE VIEW bronze.campaign_campaignfieldrep AS
        SELECT id, field_rep_id, created_at, campaign_id, state
        FROM silver.dim_campaign_field_rep_assignment;
        """
    )
    execute(
        """
        CREATE VIEW bronze.campaign_campaign AS
        SELECT
            updated_at,
            'false'::text AS system_rfa,
            'false'::text AS system_pe,
            'true'::text AS system_ic,
            status,
            start_date,
            ''::text AS register_message,
            num_doctors_supported,
            name,
            id,
            end_date,
            ''::text AS doctor_recruitment_link,
            created_at,
            ''::text AS contact_person_phone,
            ''::text AS contact_person_name,
            ''::text AS contact_person_email,
            ''::text AS brand_manager_password_encrypted,
            ''::text AS brand_manager_login_token,
            ''::text AS brand_manager_login_link,
            ''::text AS brand_manager_email,
            brand_id,
            ''::text AS banner_target_url,
            ''::text AS banner_small_url,
            ''::text AS banner_small_key,
            ''::text AS banner_large_url,
            ''::text AS banner_large_key,
            ''::text AS add_to_campaign_message
        FROM silver.dim_campaign;
        """
    )
    execute(
        """
        CREATE VIEW bronze.campaign_management_campaign AS
        SELECT
            id,
            name,
            brand_name,
            start_date,
            end_date,
            ''::text AS description,
            status,
            created_at,
            updated_at,
            ''::text AS created_by_id,
            brand_campaign_id,
            ''::text AS brand_logo,
            company_logo,
            company_name,
            ''::text AS contract,
            ''::text AS incharge_contact,
            ''::text AS incharge_designation,
            ''::text AS incharge_name,
            ''::text AS items_per_clinic_per_year,
            num_doctors,
            ''::text AS printing_excel,
            ''::text AS printing_required
        FROM silver.dim_campaign;
        """
    )
    execute(
        """
        CREATE VIEW bronze.collateral_management_collateral AS
        SELECT
            id,
            type,
            title,
            file,
            vimeo_url,
            content_id,
            upload_date,
            is_active,
            created_at,
            updated_at,
            banner_1,
            banner_2,
            campaign_id,
            created_by_id,
            description,
            purpose,
            doctor_name,
            webinar_date,
            webinar_description,
            webinar_title,
            webinar_url
        FROM silver.dim_collateral;
        """
    )
    execute(
        """
        CREATE VIEW bronze.collateral_management_campaigncollateral AS
        SELECT id, start_date, end_date, created_at, updated_at, campaign_id, collateral_id
        FROM silver.bridge_campaign_collateral_schedule;
        """
    )


def _drop_bronze_views() -> None:
    ensure_schema(BRONZE_COMPAT_SCHEMA)
    for table in BRONZE_COMPAT_TABLES:
        if _bronze_relation_kind(table) == "v":
            execute(f"DROP VIEW {_qident(BRONZE_COMPAT_SCHEMA)}.{_qident(table)} CASCADE;")


def _record_dq_issues(run_id: str, source: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    issues: dict[str, int] = defaultdict(int)
    for row in source["migration_exception_v2"]:
        if _clean(row.get("resolution_status")).lower() == "open":
            issues[_field(row, "issue_code") or "OPEN_EXCEPTION"] += 1
    for tx in source["inclinic_collateral_transaction_v2"]:
        status = _clean(tx.get("field_rep_identifier_consistency_status")).lower()
        if status in {"conflict", "missing"}:
            issues[f"transaction_field_rep_identifier_{status}"] += 1
    for issue, count in issues.items():
        execute(
            """
            INSERT INTO control.dq_issue_log
            (run_id, layer, table_name, issue_type, issue_count, issue_sample, created_at)
            VALUES (%s, 'source_v2', 'inclinic', %s, %s, '', NOW()::text)
            """,
            [run_id, issue, str(count)],
        )
    return dict(issues)


def build_v2_reporting(run_id: str) -> dict[str, Any]:
    now = _now()
    source = _load_source()
    _validate_required_v2_source_counts(source)
    privacy_allowlist = active_campaign_privacy_allowlist()
    source = _apply_campaign_privacy_to_source(source, privacy_allowlist)
    person_privacy_rules = active_person_privacy_rules()
    source = _apply_person_privacy_to_source(source, person_privacy_rules)
    raw_visibility_rules = active_raw_visibility_rules(system_key="inclinic")
    source = _apply_raw_visibility_to_source(source, raw_visibility_rules)

    ensure_schema("silver")
    _drop_bronze_views()
    correction_rules = active_reporting_correction_rules()
    brand_by_rep_id = _rep_brand_id_by_campaign_fieldrep(source)
    campaign_rows = _campaign_rows(source)
    field_rep_rows = _field_rep_rows(source, now, _field_rep_state_fallback_by_id())
    field_rep_by_id = {row["id"]: row for row in field_rep_rows}
    assignment_rows = _assignment_rows(source)
    collateral_rows = _collateral_rows(source, now)
    schedule_rows = _schedule_rows(source, collateral_rows, now)
    doctor_rows = _doctor_rows(source, field_rep_by_id, now, correction_rules, brand_by_rep_id)
    share_rows = _share_rows(source, run_id, now, correction_rules, brand_by_rep_id)
    transaction_rows = _transaction_rows(source, run_id, now, correction_rules, brand_by_rep_id)
    bridge_rows = _bridge_base_rows(source, field_rep_by_id, now, correction_rules, brand_by_rep_id)
    map_rows = _map_rows(transaction_rows, share_rows, bridge_rows, now)
    action_rows = _first_seen_rows(transaction_rows, share_rows, now)

    table_specs = [
        ("dim_campaign", DIM_CAMPAIGN_COLUMNS, campaign_rows),
        ("dim_field_rep", DIM_FIELD_REP_COLUMNS, field_rep_rows),
        ("dim_campaign_field_rep_assignment", CAMPAIGN_FIELD_REP_COLUMNS, assignment_rows),
        ("dim_doctor", DIM_DOCTOR_COLUMNS, doctor_rows),
        ("dim_collateral", DIM_COLLATERAL_COLUMNS, collateral_rows),
        ("bridge_campaign_collateral_schedule", SCHEDULE_COLUMNS, schedule_rows),
        ("fact_share_log", FACT_SHARE_COLUMNS, share_rows),
        ("fact_collateral_transaction", FACT_TRANSACTION_COLUMNS, transaction_rows),
        ("map_brand_campaign_to_campaign", MAP_CAMPAIGN_COLUMNS, map_rows),
        ("bridge_brand_campaign_doctor_base", BRIDGE_BASE_COLUMNS, bridge_rows),
        ("doctor_action_first_seen", ACTION_FIRST_SEEN_COLUMNS, action_rows),
    ]

    preservation_counts: dict[str, dict[str, int]] = {}
    for table, columns, rows in table_specs:
        archived = _archive_replaced_reporting_rows("silver", table, columns, rows, run_id=run_id, now=now)
        if archived["archived"]:
            preservation_counts[f"silver.{table}"] = archived
        replace_table("silver", table, columns, rows)

    _create_bronze_views()
    issues = _record_dq_issues(run_id, source)

    return {
        "counts": {
            "silver.dim_campaign": len(campaign_rows),
            "silver.dim_field_rep": len(field_rep_rows),
            "silver.dim_campaign_field_rep_assignment": len(assignment_rows),
            "silver.dim_doctor": len(doctor_rows),
            "silver.dim_collateral": len(collateral_rows),
            "silver.bridge_campaign_collateral_schedule": len(schedule_rows),
            "silver.fact_share_log": len(share_rows),
            "silver.fact_collateral_transaction": len(transaction_rows),
            "silver.map_brand_campaign_to_campaign": len(map_rows),
            "silver.bridge_brand_campaign_doctor_base": len(bridge_rows),
            "silver.doctor_action_first_seen": len(action_rows),
            "ops.reporting_data_correction_rule_active": len(correction_rules),
            "ops.reporting_campaign_privacy_allowlist_active": len(privacy_allowlist),
            "ops.reporting_person_privacy_rule_active": len(person_privacy_rules),
            "ops.reporting_raw_visibility_rule_active": len(raw_visibility_rules),
        },
        "preservation_counts": preservation_counts,
        "issues": issues,
    }
