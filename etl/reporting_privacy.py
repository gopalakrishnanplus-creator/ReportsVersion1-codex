from __future__ import annotations

import re
import uuid
from typing import Any

from django.db import connection


PRIVACY_SCHEMA = "ops"
PRIVACY_TABLE = "reporting_campaign_privacy_allowlist"
PERSON_PRIVACY_TABLE = "reporting_person_privacy_rule"
RAW_VISIBILITY_TABLE = "reporting_raw_visibility_rule"

SYSTEM_LABELS = {
    "inclinic": "InClinic Reporting",
    "sapa": "SAPA / RFA",
    "pe": "Patient Education",
}

RAW_VISIBILITY_ENTITY_LABELS = {
    "activity": "Activity / event",
    "campaign": "Campaign",
    "collateral": "Collateral / content",
    "content": "Content / video",
    "doctor": "Doctor / patient",
    "field_rep": "Field representative",
    "patient": "Patient",
    "share": "Share",
    "transaction": "Transaction",
}

RAW_VISIBILITY_TABLE_OPTIONS: tuple[dict[str, Any], ...] = (
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_inclinic",
        "table_name": "inclinic_collateral_v2",
        "label": "InClinic collateral master",
        "entity_type": "collateral",
        "identifier_fields": ("old_id", "collateral_uuid", "source_pk_value"),
        "downstream_effect": "Hides collateral tiles, campaign collateral schedule rows, share rows, transaction rows, and first-seen actions.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_server2",
        "table_name": "collateral_management_collateral",
        "label": "Legacy InClinic collateral master",
        "entity_type": "collateral",
        "identifier_fields": ("id",),
        "downstream_effect": "Hides the collateral and linked InClinic schedules, shares, and transactions.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_inclinic",
        "table_name": "inclinic_campaign_collateral_v2",
        "label": "InClinic campaign-to-collateral link",
        "entity_type": "collateral",
        "identifier_fields": ("old_collateral_id", "collateral_uuid", "source_pk_value"),
        "downstream_effect": "Hides linked campaign schedule rows plus downstream share and transaction rows for that collateral.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_server2",
        "table_name": "collateral_management_campaigncollateral",
        "label": "Legacy campaign-to-collateral link",
        "entity_type": "collateral",
        "identifier_fields": ("collateral_id", "id"),
        "downstream_effect": "Hides linked campaign schedule rows plus downstream share and transaction rows for that collateral.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_inclinic",
        "table_name": "inclinic_share_event_v2",
        "label": "InClinic share events",
        "entity_type": "share",
        "identifier_fields": ("old_id", "share_event_uuid", "source_pk_value"),
        "downstream_effect": "Hides selected share events and derived reach/first-seen metrics.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_inclinic",
        "table_name": "inclinic_collateral_transaction_v2",
        "label": "InClinic collateral transactions",
        "entity_type": "transaction",
        "identifier_fields": ("old_id", "transaction_uuid", "old_transaction_id", "source_pk_value"),
        "downstream_effect": "Hides selected transaction events and derived sent/viewed/video/PDF metrics.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_master",
        "table_name": "campaign_v2",
        "label": "InClinic campaign master",
        "entity_type": "campaign",
        "identifier_fields": ("legacy_campaign_id", "id", "campaign_uuid"),
        "downstream_effect": "Hides the campaign and all campaign-scoped InClinic rows.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_master",
        "table_name": "field_rep_v2",
        "label": "InClinic field rep master",
        "entity_type": "field_rep",
        "identifier_fields": ("current_campaign_fieldrep_id", "id", "field_rep_uuid", "brand_supplied_field_rep_id"),
        "downstream_effect": "Hides the field rep, assignments, doctors, shares, and transactions linked to that rep.",
    },
    {
        "system_key": "inclinic",
        "schema_name": "raw_v2_inclinic",
        "table_name": "inclinic_assigned_doctor_roster_v2",
        "label": "InClinic assigned doctors",
        "entity_type": "doctor",
        "identifier_fields": ("doctor_phone_normalized", "doctor_uuid", "source_pk_value"),
        "downstream_effect": "Hides the doctor/patient identity and related InClinic activity rows.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "campaign_campaign_raw",
        "label": "SAPA / RFA campaign master",
        "entity_type": "campaign",
        "identifier_fields": ("id", "brand_campaign_id"),
        "downstream_effect": "Hides the campaign and campaign-scoped SAPA/RFA doctors, activity, and gold metrics.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "campaign_fieldrep_raw",
        "label": "SAPA / RFA field rep master",
        "entity_type": "field_rep",
        "identifier_fields": ("id", "brand_supplied_field_rep_id", "user_id"),
        "downstream_effect": "Hides the field rep and linked SAPA/RFA doctors and activity.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "campaign_doctor_raw",
        "label": "SAPA / RFA campaign doctors",
        "entity_type": "doctor",
        "identifier_fields": ("doctor_id", "id", "email", "phone"),
        "downstream_effect": "Hides the doctor/patient identity and related SAPA/RFA activity.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "redflags_doctor_raw",
        "label": "SAPA / RFA red-flag doctor master",
        "entity_type": "doctor",
        "identifier_fields": ("doctor_id", "email", "whatsapp_no", "clinic_phone"),
        "downstream_effect": "Hides the doctor/patient identity and related SAPA/RFA activity.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "redflags_patientsubmission_raw",
        "label": "SAPA red-flag patient submissions",
        "entity_type": "patient",
        "identifier_fields": ("record_id", "patient_id", "doctor_id"),
        "downstream_effect": "Hides matching screening, red-flag, follow-up, reminder, and video-view rows.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "gnd_gndpatientsubmission_raw",
        "label": "SAPA GND patient submissions",
        "entity_type": "patient",
        "identifier_fields": ("id", "patient_id", "doctor_id"),
        "downstream_effect": "Hides matching screening, red-flag, follow-up, reminder, and video-view rows.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "redflags_patientvideo_raw",
        "label": "SAPA patient videos",
        "entity_type": "content",
        "identifier_fields": ("id", "red_flag_id", "patient_video_url"),
        "downstream_effect": "Hides matching patient education content and related video-view rows.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "gnd_gndpatientvideo_raw",
        "label": "SAPA GND patient videos",
        "entity_type": "content",
        "identifier_fields": ("id", "red_flag_id", "patient_video_url"),
        "downstream_effect": "Hides matching GND patient education content and related video-view rows.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "redflags_redflag_raw",
        "label": "SAPA red-flag catalog",
        "entity_type": "content",
        "identifier_fields": ("red_flag_id", "doctor_video_url"),
        "downstream_effect": "Hides matching red-flag content from facts and video metrics.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "gnd_gndredflag_raw",
        "label": "SAPA GND red-flag catalog",
        "entity_type": "content",
        "identifier_fields": ("red_flag_id", "doctor_video_url"),
        "downstream_effect": "Hides matching GND red-flag content from facts and video metrics.",
    },
    {
        "system_key": "sapa",
        "schema_name": "raw_sapa_mysql",
        "table_name": "rfa_activity_event_raw",
        "label": "SAPA / RFA activity events",
        "entity_type": "activity",
        "identifier_fields": ("activity_event_uuid", "source_event_id", "source_pk_value"),
        "downstream_effect": "Hides selected activity events and derived legacy facts.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_master",
        "table_name": "campaign_campaign_raw",
        "label": "PE campaign master",
        "entity_type": "campaign",
        "identifier_fields": ("id", "campaign_uuid"),
        "downstream_effect": "Hides the PE campaign and campaign-scoped enrollments, shares, playback, and banner clicks.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "publisher_campaign_raw",
        "label": "PE publisher campaign",
        "entity_type": "campaign",
        "identifier_fields": ("campaign_id", "campaign_uuid", "pe_campaign_uuid"),
        "downstream_effect": "Hides the PE campaign and campaign-scoped content, shares, playback, and banner clicks.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_master",
        "table_name": "campaign_fieldrep_raw",
        "label": "PE field rep master",
        "entity_type": "field_rep",
        "identifier_fields": ("id", "brand_supplied_field_rep_id", "user_id"),
        "downstream_effect": "Hides the field rep and linked PE enrollment/base rows.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_master",
        "table_name": "campaign_doctor_raw",
        "label": "PE campaign doctors",
        "entity_type": "doctor",
        "identifier_fields": ("id", "doctor_id", "email", "phone"),
        "downstream_effect": "Hides the doctor/patient identity and related PE shares/playback/banner rows.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_master",
        "table_name": "redflags_doctor_raw",
        "label": "PE doctor master",
        "entity_type": "doctor",
        "identifier_fields": ("doctor_id", "email", "whatsapp_no", "clinic_phone"),
        "downstream_effect": "Hides the doctor/patient identity and related PE shares/playback/banner rows.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "sharing_shareactivity_raw",
        "label": "PE share activity",
        "entity_type": "share",
        "identifier_fields": ("id", "public_id", "share_event_uuid"),
        "downstream_effect": "Hides selected shares plus linked playback, funnel, and video-view rows.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "sharing_shareplaybackevent_raw",
        "label": "PE playback events",
        "entity_type": "share",
        "identifier_fields": ("share_id", "share_public_id", "share_event_uuid", "id"),
        "downstream_effect": "Hides selected playback events and related video-view rollups.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "sharing_sharebannerclickevent_raw",
        "label": "PE banner click events",
        "entity_type": "activity",
        "identifier_fields": ("id", "source_banner_click_id", "banner_id"),
        "downstream_effect": "Hides selected banner click events and banner-attributed metrics.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "catalog_video_raw",
        "label": "PE video catalog",
        "entity_type": "content",
        "identifier_fields": ("id", "code", "video_code"),
        "downstream_effect": "Hides selected content from campaign content, shares, playback, and video-view rows.",
    },
    {
        "system_key": "pe",
        "schema_name": "raw_pe_portal",
        "table_name": "catalog_videocluster_raw",
        "label": "PE video bundle catalog",
        "entity_type": "content",
        "identifier_fields": ("id", "code", "video_cluster_code"),
        "downstream_effect": "Hides selected content bundles from campaign content, shares, playback, and video-view rows.",
    },
)


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def normalize_campaign_id(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "", _clean(value)).lower()


def normalize_email(value: Any) -> str:
    return _clean(value).lower()


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value))
    return digits[-10:] if len(digits) >= 10 else digits


def normalize_record_identifier(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "", _clean(value)).lower()


def _raw_visibility_option(system_key: str, schema_name: str, table_name: str) -> dict[str, Any] | None:
    system_key = _clean(system_key).lower()
    schema_name = _clean(schema_name)
    table_name = _clean(table_name)
    for option in RAW_VISIBILITY_TABLE_OPTIONS:
        if (
            option["system_key"] == system_key
            and option["schema_name"] == schema_name
            and option["table_name"] == table_name
        ):
            return dict(option)
    return None


def list_raw_visibility_table_options(system_key: str | None = None) -> list[dict[str, Any]]:
    selected_system = _clean(system_key).lower()
    options = []
    for option in RAW_VISIBILITY_TABLE_OPTIONS:
        if selected_system and option["system_key"] != selected_system:
            continue
        enriched = dict(option)
        enriched["system_label"] = SYSTEM_LABELS.get(enriched["system_key"], enriched["system_key"])
        enriched["entity_label"] = RAW_VISIBILITY_ENTITY_LABELS.get(enriched["entity_type"], enriched["entity_type"])
        enriched["identifier_help"] = ", ".join(enriched["identifier_fields"])
        enriched["value"] = f"{enriched['system_key']}||{enriched['schema_name']}||{enriched['table_name']}"
        options.append(enriched)
    return sorted(options, key=lambda item: (item["system_label"], item["label"]))


def ensure_campaign_privacy_table() -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(PRIVACY_SCHEMA)}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_qident(PRIVACY_SCHEMA)}.{_qident(PRIVACY_TABLE)} (
                rule_id TEXT PRIMARY KEY,
                campaign_id TEXT NOT NULL,
                campaign_id_normalized TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS reporting_campaign_privacy_allowlist_active_idx
            ON {_qident(PRIVACY_SCHEMA)}.{_qident(PRIVACY_TABLE)}
            (is_active, campaign_id_normalized)
            """
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_qident(PRIVACY_SCHEMA)}.{_qident(PERSON_PRIVACY_TABLE)} (
                rule_id TEXT PRIMARY KEY,
                person_label TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '',
                email_normalized TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                phone_normalized TEXT NOT NULL DEFAULT '',
                allowed_campaign_id TEXT NOT NULL,
                allowed_campaign_id_normalized TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS reporting_person_privacy_rule_active_idx
            ON {_qident(PRIVACY_SCHEMA)}.{_qident(PERSON_PRIVACY_TABLE)}
            (is_active, allowed_campaign_id_normalized, email_normalized, phone_normalized)
            """
        )
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_qident(PRIVACY_SCHEMA)}.{_qident(RAW_VISIBILITY_TABLE)} (
                rule_id TEXT PRIMARY KEY,
                system_key TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_identifier TEXT NOT NULL,
                record_identifier_normalized TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_by TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS reporting_raw_visibility_rule_active_idx
            ON {_qident(PRIVACY_SCHEMA)}.{_qident(RAW_VISIBILITY_TABLE)}
            (is_active, system_key, entity_type, record_identifier_normalized)
            """
        )


def create_campaign_privacy_allowlist_rule(*, campaign_id: str, reason: str, created_by: str) -> str:
    ensure_campaign_privacy_table()
    campaign_id = _clean(campaign_id)
    campaign_id_normalized = normalize_campaign_id(campaign_id)
    reason = _clean(reason)
    if not campaign_id_normalized:
        raise ValueError("Campaign ID is required.")
    if len(reason) < 8:
        raise ValueError("A clear reason is required.")

    rule_id = uuid.uuid4().hex
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {_qident(PRIVACY_SCHEMA)}.{_qident(PRIVACY_TABLE)}
            (rule_id, campaign_id, campaign_id_normalized, reason, created_by)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [rule_id, campaign_id, campaign_id_normalized, reason, _clean(created_by)],
        )
    return rule_id


def deactivate_campaign_privacy_allowlist_rule(rule_id: str) -> bool:
    ensure_campaign_privacy_table()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {_qident(PRIVACY_SCHEMA)}.{_qident(PRIVACY_TABLE)}
            SET is_active = FALSE, updated_at = NOW()
            WHERE rule_id = %s
            """,
            [_clean(rule_id)],
        )
        return cursor.rowcount > 0


def list_campaign_privacy_allowlist_rules(include_inactive: bool = True) -> list[dict[str, Any]]:
    ensure_campaign_privacy_table()
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT rule_id, campaign_id, campaign_id_normalized, reason, is_active, created_by,
                   created_at::text AS created_at, updated_at::text AS updated_at
            FROM {_qident(PRIVACY_SCHEMA)}.{_qident(PRIVACY_TABLE)}
            {where_sql}
            ORDER BY is_active DESC, created_at DESC
            """
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def active_campaign_privacy_allowlist() -> set[str]:
    rows = list_campaign_privacy_allowlist_rules(include_inactive=False)
    return {
        normalized
        for row in rows
        for normalized in [normalize_campaign_id(row.get("campaign_id_normalized") or row.get("campaign_id"))]
        if normalized
    }


def create_person_privacy_rule(
    *,
    person_label: str,
    email: str,
    phone: str,
    allowed_campaign_id: str,
    reason: str,
    created_by: str,
) -> str:
    ensure_campaign_privacy_table()
    person_label = _clean(person_label)
    email = _clean(email)
    phone = _clean(phone)
    email_normalized = normalize_email(email)
    phone_normalized = normalize_phone(phone)
    allowed_campaign_id = _clean(allowed_campaign_id)
    allowed_campaign_id_normalized = normalize_campaign_id(allowed_campaign_id)
    reason = _clean(reason)
    if not email_normalized and not phone_normalized:
        raise ValueError("Email or phone is required.")
    if not allowed_campaign_id_normalized:
        raise ValueError("Allowed campaign ID is required.")
    if len(reason) < 8:
        raise ValueError("A clear reason is required.")

    rule_id = uuid.uuid4().hex
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {_qident(PRIVACY_SCHEMA)}.{_qident(PERSON_PRIVACY_TABLE)}
            (rule_id, person_label, email, email_normalized, phone, phone_normalized,
             allowed_campaign_id, allowed_campaign_id_normalized, reason, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                rule_id,
                person_label,
                email,
                email_normalized,
                phone,
                phone_normalized,
                allowed_campaign_id,
                allowed_campaign_id_normalized,
                reason,
                _clean(created_by),
            ],
        )
    return rule_id


def deactivate_person_privacy_rule(rule_id: str) -> bool:
    ensure_campaign_privacy_table()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {_qident(PRIVACY_SCHEMA)}.{_qident(PERSON_PRIVACY_TABLE)}
            SET is_active = FALSE, updated_at = NOW()
            WHERE rule_id = %s
            """,
            [_clean(rule_id)],
        )
        return cursor.rowcount > 0


def list_person_privacy_rules(include_inactive: bool = True) -> list[dict[str, Any]]:
    ensure_campaign_privacy_table()
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT rule_id, person_label, email, email_normalized, phone, phone_normalized,
                   allowed_campaign_id, allowed_campaign_id_normalized, reason, is_active, created_by,
                   created_at::text AS created_at, updated_at::text AS updated_at
            FROM {_qident(PRIVACY_SCHEMA)}.{_qident(PERSON_PRIVACY_TABLE)}
            {where_sql}
            ORDER BY is_active DESC, created_at DESC
            """
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def active_person_privacy_rules() -> list[dict[str, Any]]:
    return list_person_privacy_rules(include_inactive=False)


def create_raw_visibility_rule(
    *,
    system_key: str,
    schema_name: str,
    table_name: str,
    record_identifier: str,
    reason: str,
    created_by: str,
) -> str:
    ensure_campaign_privacy_table()
    option = _raw_visibility_option(system_key, schema_name, table_name)
    if option is None:
        raise ValueError("Select a supported RAW table for reporting visibility.")
    record_identifier = _clean(record_identifier)
    normalized = normalize_record_identifier(record_identifier)
    reason = _clean(reason)
    if not normalized:
        raise ValueError("Record identifier is required.")
    if len(reason) < 8:
        raise ValueError("A clear reason is required.")

    rule_id = uuid.uuid4().hex
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {_qident(PRIVACY_SCHEMA)}.{_qident(RAW_VISIBILITY_TABLE)}
            (rule_id, system_key, schema_name, table_name, record_identifier,
             record_identifier_normalized, entity_type, reason, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                rule_id,
                option["system_key"],
                option["schema_name"],
                option["table_name"],
                record_identifier,
                normalized,
                option["entity_type"],
                reason,
                _clean(created_by),
            ],
        )
    return rule_id


def deactivate_raw_visibility_rule(rule_id: str) -> bool:
    ensure_campaign_privacy_table()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {_qident(PRIVACY_SCHEMA)}.{_qident(RAW_VISIBILITY_TABLE)}
            SET is_active = FALSE, updated_at = NOW()
            WHERE rule_id = %s
            """,
            [_clean(rule_id)],
        )
        return cursor.rowcount > 0


def list_raw_visibility_rules(include_inactive: bool = True) -> list[dict[str, Any]]:
    ensure_campaign_privacy_table()
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT rule_id, system_key, schema_name, table_name, record_identifier,
                   record_identifier_normalized, entity_type, reason, is_active, created_by,
                   created_at::text AS created_at, updated_at::text AS updated_at
            FROM {_qident(PRIVACY_SCHEMA)}.{_qident(RAW_VISIBILITY_TABLE)}
            {where_sql}
            ORDER BY is_active DESC, created_at DESC
            """
        )
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    labels_by_table = {
        (option["system_key"], option["schema_name"], option["table_name"]): option
        for option in list_raw_visibility_table_options()
    }
    for row in rows:
        option = labels_by_table.get((row["system_key"], row["schema_name"], row["table_name"]), {})
        row["system_label"] = SYSTEM_LABELS.get(row["system_key"], row["system_key"])
        row["table_label"] = option.get("label") or f"{row['schema_name']}.{row['table_name']}"
        row["entity_label"] = RAW_VISIBILITY_ENTITY_LABELS.get(row["entity_type"], row["entity_type"])
        row["downstream_effect"] = option.get("downstream_effect", "")
    return rows


def active_raw_visibility_rules(system_key: str | None = None) -> list[dict[str, Any]]:
    rows = list_raw_visibility_rules(include_inactive=False)
    selected_system = _clean(system_key).lower()
    if not selected_system:
        return rows
    return [row for row in rows if row.get("system_key") == selected_system]


def raw_visibility_entity_ids(
    rules: list[dict[str, Any]],
    entity_type: str,
    *,
    system_key: str | None = None,
) -> set[str]:
    selected_system = _clean(system_key).lower()
    selected_entity = _clean(entity_type).lower()
    return {
        normalized
        for row in rules
        if row.get("is_active", True)
        and _clean(row.get("entity_type")).lower() == selected_entity
        and (not selected_system or _clean(row.get("system_key")).lower() == selected_system)
        for normalized in [
            normalize_record_identifier(row.get("record_identifier_normalized") or row.get("record_identifier"))
        ]
        if normalized
    }


def row_matches_raw_visibility_ids(
    row: dict[str, Any],
    hidden_ids: set[str],
    fields: tuple[str, ...],
) -> bool:
    if not hidden_ids:
        return False
    return any(normalize_record_identifier(row.get(field)) in hidden_ids for field in fields if _clean(row.get(field)))


def campaign_allowed_by_allowlist(value: Any, allowlist: set[str]) -> bool:
    if not allowlist:
        return True
    return normalize_campaign_id(value) in allowlist


def row_allowed_by_campaign_fields(row: dict[str, Any], allowlist: set[str], fields: tuple[str, ...]) -> bool:
    if not allowlist:
        return True
    return any(campaign_allowed_by_allowlist(row.get(field), allowlist) for field in fields if _clean(row.get(field)))


def filter_rows_by_campaign_fields(rows: list[dict[str, Any]], allowlist: set[str], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    if not allowlist:
        return rows
    return [row for row in rows if row_allowed_by_campaign_fields(row, allowlist, fields)]


def person_privacy_matching_rules(
    row: dict[str, Any],
    rules: list[dict[str, Any]],
    *,
    email_fields: tuple[str, ...] = (),
    phone_fields: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    if not rules:
        return []
    row_emails = {
        normalized
        for field in email_fields
        for normalized in [normalize_email(row.get(field))]
        if normalized
    }
    row_phones = {
        normalized
        for field in phone_fields
        for normalized in [normalize_phone(row.get(field))]
        if normalized
    }
    matches = []
    for rule in rules:
        rule_email = normalize_email(rule.get("email_normalized") or rule.get("email"))
        rule_phone = normalize_phone(rule.get("phone_normalized") or rule.get("phone"))
        if (rule_email and rule_email in row_emails) or (rule_phone and rule_phone in row_phones):
            matches.append(rule)
    return matches


def person_privacy_allowed_campaigns_for_row(
    row: dict[str, Any],
    rules: list[dict[str, Any]],
    *,
    email_fields: tuple[str, ...] = (),
    phone_fields: tuple[str, ...] = (),
) -> set[str] | None:
    matches = person_privacy_matching_rules(row, rules, email_fields=email_fields, phone_fields=phone_fields)
    if not matches:
        return None
    return {
        normalized
        for rule in matches
        for normalized in [normalize_campaign_id(rule.get("allowed_campaign_id_normalized") or rule.get("allowed_campaign_id"))]
        if normalized
    }


def row_visible_by_person_privacy(
    row: dict[str, Any],
    rules: list[dict[str, Any]],
    *,
    campaign_fields: tuple[str, ...],
    email_fields: tuple[str, ...] = (),
    phone_fields: tuple[str, ...] = (),
) -> bool:
    allowed_campaigns = person_privacy_allowed_campaigns_for_row(
        row,
        rules,
        email_fields=email_fields,
        phone_fields=phone_fields,
    )
    if allowed_campaigns is None:
        return True
    row_campaigns = {
        normalized
        for field in campaign_fields
        for normalized in [normalize_campaign_id(row.get(field))]
        if normalized
    }
    return bool(row_campaigns & allowed_campaigns)
