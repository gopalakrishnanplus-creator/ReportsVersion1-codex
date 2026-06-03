from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

from django.db import connection


CORRECTION_SCHEMA = "ops"
CORRECTION_TABLE = "reporting_data_correction_rule"

RULE_KEEP_DOCTOR_WITH_REP = "keep_doctor_with_field_rep"
RULE_EXCLUDE_INVALID_PHONE = "exclude_invalid_doctor_phone"
ACTIVE_RULE_TYPES = {RULE_KEEP_DOCTOR_WITH_REP, RULE_EXCLUDE_INVALID_PHONE}


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", _clean(value))
    return digits[-10:] if len(digits) >= 10 else digits


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "", _clean(value)).lower()


def normalize_name(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "", _clean(value)).lower()


@dataclass(frozen=True)
class ReportingCorrectionRule:
    correction_id: str
    rule_type: str
    system_name: str
    campaign_id: str
    doctor_phone: str
    doctor_phone_normalized: str
    doctor_name: str
    field_rep_brand_supplied_id: str
    expected_field_rep_brand_supplied_id: str
    affected_field_rep_brand_supplied_ids: str
    reason: str
    created_by: str


def ensure_reporting_correction_table() -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(CORRECTION_SCHEMA)}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_qident(CORRECTION_SCHEMA)}.{_qident(CORRECTION_TABLE)} (
                correction_id TEXT PRIMARY KEY,
                rule_type TEXT NOT NULL,
                system_name TEXT NOT NULL DEFAULT 'inclinic',
                campaign_id TEXT NOT NULL,
                doctor_phone TEXT NOT NULL,
                doctor_phone_normalized TEXT NOT NULL,
                doctor_name TEXT NOT NULL DEFAULT '',
                field_rep_brand_supplied_id TEXT NOT NULL DEFAULT '',
                expected_field_rep_brand_supplied_id TEXT NOT NULL DEFAULT '',
                affected_field_rep_brand_supplied_ids TEXT NOT NULL DEFAULT '',
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
            CREATE INDEX IF NOT EXISTS reporting_correction_active_idx
            ON {_qident(CORRECTION_SCHEMA)}.{_qident(CORRECTION_TABLE)}
            (is_active, rule_type, campaign_id, doctor_phone_normalized)
            """
        )


def create_reporting_correction_rule(
    *,
    rule_type: str,
    campaign_id: str,
    doctor_phone: str,
    doctor_name: str = "",
    field_rep_brand_supplied_id: str = "",
    expected_field_rep_brand_supplied_id: str = "",
    affected_field_rep_brand_supplied_ids: str = "",
    reason: str,
    created_by: str,
    system_name: str = "inclinic",
) -> str:
    ensure_reporting_correction_table()
    rule_type = _clean(rule_type)
    if rule_type not in ACTIVE_RULE_TYPES:
        raise ValueError("Choose a valid reporting correction type.")
    campaign_id = _clean(campaign_id)
    doctor_phone = _clean(doctor_phone)
    phone_normalized = normalize_phone(doctor_phone)
    reason = _clean(reason)
    if not campaign_id:
        raise ValueError("Campaign ID is required.")
    if not phone_normalized:
        raise ValueError("Doctor phone is required.")
    if len(reason) < 8:
        raise ValueError("A clear correction reason is required.")
    if rule_type == RULE_KEEP_DOCTOR_WITH_REP and not _clean(expected_field_rep_brand_supplied_id):
        raise ValueError("Expected ASM / brand-supplied field rep ID is required.")
    if rule_type == RULE_EXCLUDE_INVALID_PHONE and not (_clean(field_rep_brand_supplied_id) or _clean(doctor_name)):
        raise ValueError("For invalid-phone exclusions, provide Field Rep ID or Doctor Name to avoid a broad phone-only rule.")

    correction_id = uuid.uuid4().hex
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {_qident(CORRECTION_SCHEMA)}.{_qident(CORRECTION_TABLE)}
            (
                correction_id, rule_type, system_name, campaign_id, doctor_phone, doctor_phone_normalized,
                doctor_name, field_rep_brand_supplied_id, expected_field_rep_brand_supplied_id,
                affected_field_rep_brand_supplied_ids, reason, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                correction_id,
                rule_type,
                _clean(system_name) or "inclinic",
                campaign_id,
                doctor_phone,
                phone_normalized,
                _clean(doctor_name),
                _clean(field_rep_brand_supplied_id),
                _clean(expected_field_rep_brand_supplied_id),
                _clean(affected_field_rep_brand_supplied_ids),
                reason,
                _clean(created_by),
            ],
        )
    return correction_id


def deactivate_reporting_correction_rule(correction_id: str) -> bool:
    ensure_reporting_correction_table()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {_qident(CORRECTION_SCHEMA)}.{_qident(CORRECTION_TABLE)}
            SET is_active = FALSE, updated_at = NOW()
            WHERE correction_id = %s
            """,
            [_clean(correction_id)],
        )
        return cursor.rowcount > 0


def list_reporting_correction_rules(include_inactive: bool = True) -> list[dict[str, Any]]:
    ensure_reporting_correction_table()
    where_sql = "" if include_inactive else "WHERE is_active = TRUE"
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT correction_id, rule_type, system_name, campaign_id, doctor_phone, doctor_phone_normalized,
                   doctor_name, field_rep_brand_supplied_id, expected_field_rep_brand_supplied_id,
                   affected_field_rep_brand_supplied_ids, reason, is_active, created_by,
                   created_at::text AS created_at, updated_at::text AS updated_at
            FROM {_qident(CORRECTION_SCHEMA)}.{_qident(CORRECTION_TABLE)}
            {where_sql}
            ORDER BY is_active DESC, created_at DESC
            """
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def active_reporting_correction_rules() -> list[ReportingCorrectionRule]:
    rows = list_reporting_correction_rules(include_inactive=False)
    rules: list[ReportingCorrectionRule] = []
    for row in rows:
        rule_type = _clean(row.get("rule_type"))
        if rule_type not in ACTIVE_RULE_TYPES:
            continue
        rules.append(
            ReportingCorrectionRule(
                correction_id=_clean(row.get("correction_id")),
                rule_type=rule_type,
                system_name=_clean(row.get("system_name")) or "inclinic",
                campaign_id=_clean(row.get("campaign_id")),
                doctor_phone=_clean(row.get("doctor_phone")),
                doctor_phone_normalized=normalize_phone(row.get("doctor_phone_normalized") or row.get("doctor_phone")),
                doctor_name=_clean(row.get("doctor_name")),
                field_rep_brand_supplied_id=_clean(row.get("field_rep_brand_supplied_id")),
                expected_field_rep_brand_supplied_id=_clean(row.get("expected_field_rep_brand_supplied_id")),
                affected_field_rep_brand_supplied_ids=_clean(row.get("affected_field_rep_brand_supplied_ids")),
                reason=_clean(row.get("reason")),
                created_by=_clean(row.get("created_by")),
            )
        )
    return rules
