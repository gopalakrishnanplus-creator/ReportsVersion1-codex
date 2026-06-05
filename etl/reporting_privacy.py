from __future__ import annotations

import re
import uuid
from typing import Any

from django.db import connection


PRIVACY_SCHEMA = "ops"
PRIVACY_TABLE = "reporting_campaign_privacy_allowlist"


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def normalize_campaign_id(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "", _clean(value)).lower()


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
