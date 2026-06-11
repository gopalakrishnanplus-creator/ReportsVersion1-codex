from __future__ import annotations

import html
import json
import re
import textwrap
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from django.contrib import messages
from django.db import connection
from django.db.utils import DatabaseError, OperationalError, ProgrammingError
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse

from etl.utils.specs import SOURCE_TABLE_SPECS
from reporting.access import absolute_url, access_email_history, authenticate_session, build_report_access, send_access_email, validate_credentials
from reporting.campaign_performance import CampaignPerformanceNotFound, _configured_system_keys, _resolve_campaign_reference, _system_report_path


UNMAPPED_ACTIVITY_FIELD_REP_ID = "__unmapped_activity__"


def _fetch_dicts(sql: str, params=None):
    with connection.cursor() as cursor:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _fetch_dicts_with_timeout(sql: str, params=None, timeout_ms: int = 12000):
    with connection.cursor() as cursor:
        cursor.execute("SET statement_timeout = %s", [int(timeout_ms)])
        try:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            try:
                cursor.execute("SET statement_timeout = 0")
            except DatabaseError:
                connection.close_if_unusable_or_obsolete()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _row_has_week_data(row: dict[str, Any]) -> bool:
    metrics = (
        _to_float(row.get("doctors_reached_unique")),
        _to_float(row.get("doctors_opened_unique")),
        _to_float(row.get("video_viewed_50_unique")),
        _to_float(row.get("pdf_download_unique")),
        _to_float(row.get("doctors_consumed_unique")),
    )
    return any(v > 0 for v in metrics)


def _safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return (num / den) * 100.0


def _capped_pct(num: float, den: float) -> float:
    return min(_safe_pct(num, den), 100.0)


def _weekly_doctor_base(total_doctors: float) -> float:
    return total_doctors / 4.0 if total_doctors else 0.0


def _health_color(score: float) -> str:
    if score <= 40:
        return "red"
    if score < 60:
        return "yellow"
    return "green"


def _health_label(score: float) -> str:
    if score <= 40:
        return "Low"
    if score < 60:
        return "Medium"
    return "Good"


def _clean_display_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"null", "none", "n/a", "na", "-", "brand"}:
        return None
    return text


def _engagement_health_score(reached: float, opened: float, consumed: float, total_doctors: float) -> float:
    reached_pct = _capped_pct(reached, total_doctors)
    opened_pct = _capped_pct(opened, reached)
    consumed_pct = _capped_pct(consumed, opened)
    return (reached_pct + opened_pct + consumed_pct) / 3.0


def _weekly_engagement_health_score(reached: float, opened: float, consumed: float, total_doctors: float) -> float:
    reached_pct = _capped_pct(reached, _weekly_doctor_base(total_doctors))
    opened_pct = _capped_pct(opened, reached)
    consumed_pct = _capped_pct(consumed, opened)
    return (reached_pct + opened_pct + consumed_pct) / 3.0


def _state_weekly_health_score(reached: float, opened: float, consumed: float, total_state: float) -> float:
    reached_pct = _capped_pct(reached, _weekly_doctor_base(total_state))
    opened_pct = _capped_pct(opened, reached)
    consumed_pct = _capped_pct(consumed, opened)
    return (reached_pct + opened_pct + consumed_pct) / 3.0


def _apply_weekly_v2_fields(row: dict[str, Any], total_doctors: float | None = None) -> dict[str, Any]:
    total = _to_float(total_doctors if total_doctors is not None else row.get("total_doctors_in_campaign"))
    reached = _to_float(row.get("doctors_reached_unique"))
    opened = _to_float(row.get("doctors_opened_unique"))
    consumed = _to_float(row.get("doctors_consumed_unique"))
    row["total_doctors_in_campaign"] = total
    row["weekly_doctor_base"] = _weekly_doctor_base(total)
    row["weekly_reached_pct"] = _capped_pct(reached, row["weekly_doctor_base"]) / 100.0
    row["weekly_opened_pct"] = _capped_pct(opened, reached) / 100.0
    row["weekly_consumption_pct"] = _capped_pct(consumed, opened) / 100.0
    row["weekly_health_score"] = _weekly_engagement_health_score(reached, opened, consumed, total)
    row["health_color"] = _health_color(row["weekly_health_score"]).title()
    row["insufficient_data_flag"] = 1 if total <= 0 else 0
    return row


def _first_display_word(value: Any) -> str:
    text = _clean_display_text(value) or ""
    return text.split()[0] if text.split() else ""


def _format_schedule_date(value: Any) -> str | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(txt[:19], fmt).strftime("%b %d, %Y")
        except ValueError:
            continue
    return txt


def _parse_schedule_date(value: Any):
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(txt[:19], fmt).date()
        except ValueError:
            continue
    return None


def _normalize_campaign_id(value: Any) -> str:
    return str(value or "").strip()


def _normalize_lookup_key(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(value or "").strip().lower())


def _normalized_sql(column_sql: str) -> str:
    return f"lower(regexp_replace(COALESCE(btrim({column_sql}), ''), '[^a-zA-Z0-9]', '', 'g'))"


INDIAN_STATE_DISPLAY_BY_KEY = {
    "andamanandnicobarislands": "Andaman and Nicobar Islands",
    "andamanandnicobar": "Andaman and Nicobar Islands",
    "andhrapradesh": "Andhra Pradesh",
    "arunachalpradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "chattisgarh": "Chhattisgarh",
    "dadraandnagarhavelianddamananddiu": "Dadra and Nagar Haveli and Daman and Diu",
    "dadraandnagarhaveli": "Dadra and Nagar Haveli and Daman and Diu",
    "damananddiu": "Dadra and Nagar Haveli and Daman and Diu",
    "delhi": "Delhi",
    "delhincr": "Delhi",
    "newdelhi": "Delhi",
    "nctdelhi": "Delhi",
    "nationalcapitalterritoryofdelhi": "Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachalpradesh": "Himachal Pradesh",
    "jammuandkashmir": "Jammu & Kashmir",
    "jammukashmir": "Jammu & Kashmir",
    "jk": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhyapradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "orissa": "Odisha",
    "odisa": "Odisha",
    "puducherry": "Puducherry",
    "pondicherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamilnadu": "Tamil Nadu",
    "telangana": "Telangana",
    "telengana": "Telangana",
    "telingana": "Telangana",
    "tripura": "Tripura",
    "uttarpradesh": "Uttar Pradesh",
    "up": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "uttrakhand": "Uttarakhand",
    "westbengal": "West Bengal",
}

UNKNOWN_STATE_KEYS = {
    "",
    "null",
    "none",
    "unknown",
    "unitedkingdom",
    "uk",
}


def _valid_state_sql(column_sql: str) -> str:
    state_key_sql = f"lower(regexp_replace(COALESCE(btrim({column_sql}), ''), '[^a-zA-Z0-9]', '', 'g'))"
    unknown_values = ", ".join(f"'{key}'" for key in sorted(UNKNOWN_STATE_KEYS))
    state_cases = " ".join(
        f"WHEN {state_key_sql} = '{key}' THEN '{label}'"
        for key, label in INDIAN_STATE_DISPLAY_BY_KEY.items()
    )
    return f"CASE WHEN {state_key_sql} IN ({unknown_values}) THEN NULL {state_cases} ELSE NULL END"


def _display_state_sql(column_sql: str) -> str:
    state_key_sql = f"lower(regexp_replace(COALESCE(btrim({column_sql}), ''), '[^a-zA-Z0-9]', '', 'g'))"
    unknown_values = ", ".join(f"'{key}'" for key in sorted(UNKNOWN_STATE_KEYS))
    return f"CASE WHEN {state_key_sql} IN ({unknown_values}) THEN NULL ELSE NULLIF(btrim({column_sql}), '') END"


def _canonical_state_name(value: Any) -> str | None:
    state_key = re.sub(r"[^a-zA-Z0-9]", "", str(value or "").strip().lower())
    return INDIAN_STATE_DISPLAY_BY_KEY.get(state_key)


def _is_unknown_state(value: Any) -> bool:
    return _canonical_state_name(value) is None


def _display_state_name(value: Any) -> str:
    return _canonical_state_name(value) or "Unknown"


def _state_sort_key(item: dict[str, Any]) -> tuple[int, str]:
    state = item.get("state")
    return (1 if _is_unknown_state(state) else 0, str(state or "").strip().lower())


def _state_attention_rank_key(item: dict[str, Any]) -> tuple[float, int, str]:
    return (_to_float(item.get("health_score")), *_state_sort_key(item))


def _state_attention_card_rows(state_attention: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    return list(state_attention[:limit])


def _aggregate_weekly_metric_rows(rows: list[dict[str, Any]], total_doctors: float) -> dict[str, Any]:
    if not rows:
        return {}
    week_starts = [row.get("week_start_date") for row in rows if row.get("week_start_date")]
    week_ends = [row.get("week_end_date") for row in rows if row.get("week_end_date")]
    aggregate = {
        "brand_campaign_id": rows[0].get("brand_campaign_id"),
        "week_index": 0,
        "week_start_date": min(week_starts) if week_starts else None,
        "week_end_date": max(week_ends) if week_ends else None,
        "doctors_reached_unique": sum(_to_float(row.get("doctors_reached_unique")) for row in rows),
        "doctors_opened_unique": sum(_to_float(row.get("doctors_opened_unique")) for row in rows),
        "video_viewed_50_unique": sum(_to_float(row.get("video_viewed_50_unique")) for row in rows),
        "pdf_download_unique": sum(_to_float(row.get("pdf_download_unique")) for row in rows),
        "doctors_consumed_unique": sum(_to_float(row.get("doctors_consumed_unique")) for row in rows),
        "total_doctors_in_campaign": total_doctors or _to_float(rows[-1].get("total_doctors_in_campaign")),
    }
    return _apply_weekly_v2_fields(aggregate)


def _placeholders(values: list[Any]) -> str:
    return ", ".join(["%s"] * len(values))


def _unique_non_empty(values: list[Any]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text.lower() == "null":
            continue
        if text not in seen:
            seen.add(text)
            output.append(text)
    return output


def _campaign_brand_variants(selected_campaign: str) -> list[str]:
    lookup_key = _normalize_lookup_key(selected_campaign)
    if not lookup_key:
        return [selected_campaign]
    try:
        rows = _fetch_dicts(
            f"""
            SELECT DISTINCT brand_campaign_id
            FROM (
                SELECT brand_campaign_id
                FROM gold_global.campaign_registry
                WHERE {_normalized_sql('brand_campaign_id')} = %s
                UNION ALL
                SELECT brand_campaign_id
                FROM silver.map_brand_campaign_to_campaign
                WHERE {_normalized_sql('brand_campaign_id')} = %s
                UNION ALL
                SELECT brand_campaign_id
                FROM bronze.campaign_management_campaign
                WHERE {_normalized_sql('brand_campaign_id')} = %s
            ) variants
            WHERE COALESCE(NULLIF(btrim(brand_campaign_id), ''), '') <> ''
            """,
            [lookup_key, lookup_key, lookup_key],
        )
    except (ProgrammingError, OperationalError):
        rows = []
    return _unique_non_empty([selected_campaign, *[row.get("brand_campaign_id") for row in rows]])


def _campaign_key_placeholders(selected_campaign: str, brand_campaign_variants: list[str]) -> tuple[list[str], str]:
    keys = _unique_non_empty([_normalize_lookup_key(selected_campaign), *[_normalize_lookup_key(v) for v in brand_campaign_variants]])
    if not keys:
        keys = [_normalize_lookup_key(selected_campaign)]
    return keys, _placeholders(keys)


def _candidate_campaign_ids_cte(brand_key_placeholders: str) -> str:
    return f"""
        candidate_campaign_ids AS (
            SELECT DISTINCT candidate_campaign_id
            FROM (
                SELECT NULLIF(btrim(%s), '') AS candidate_campaign_id
                UNION ALL
                SELECT NULLIF(btrim(m.campaign_id_resolved), '')
                FROM silver.map_brand_campaign_to_campaign m
                WHERE {_normalized_sql('m.brand_campaign_id')} IN ({brand_key_placeholders})
                UNION ALL
                SELECT NULLIF(btrim(cm.id::text), '')
                FROM bronze.campaign_management_campaign cm
                WHERE {_normalized_sql('cm.brand_campaign_id')} IN ({brand_key_placeholders})
                UNION ALL
                SELECT NULLIF(btrim(cc.id::text), '')
                FROM bronze.campaign_campaign cc
                WHERE {_normalized_sql('cc.id::text')} = %s
            ) candidates
            WHERE candidate_campaign_id IS NOT NULL
        )
    """


def _optional_table_exists(schema: str, table: str) -> bool:
    try:
        return _table_exists(schema, table)
    except Exception:
        return False


def _field_rep_alias_sql_parts() -> tuple[str, str, list[str]]:
    has_auth_user = _optional_table_exists("bronze", "auth_user")
    has_local_user = _optional_table_exists("bronze", "user_management_user")
    has_legacy_rep = _optional_table_exists("bronze", "sharing_management_fieldrepresentative")

    joins: list[str] = []
    select_parts: list[str] = []
    key_columns = [
        "auth_email_key",
        "auth_username_key",
        "local_user_id_key",
        "local_field_id_key",
        "local_email_key",
        "local_username_key",
        "legacy_rep_id_key",
        "legacy_field_id_key",
        "legacy_email_key",
        "legacy_gmail_key",
        "legacy_whatsapp_key",
    ]

    if has_auth_user:
        joins.append("LEFT JOIN bronze.auth_user au ON au.id::text = cfr.user_id::text")
        select_parts.extend(
            [
                f"{_normalized_sql('au.id::text')} AS auth_user_id_key",
                f"{_normalized_sql('au.email')} AS auth_email_key",
                f"{_normalized_sql('au.username')} AS auth_username_key",
            ]
        )
        auth_email_match = f" OR ({_normalized_sql('uu.email')} <> '' AND {_normalized_sql('uu.email')} = {_normalized_sql('au.email')})"
        auth_username_match = f" OR ({_normalized_sql('uu.username')} <> '' AND {_normalized_sql('uu.username')} = {_normalized_sql('au.username')})"
        legacy_auth_email_match = (
            f" OR ({_normalized_sql('sfr.email')} <> '' AND {_normalized_sql('sfr.email')} = {_normalized_sql('au.email')})"
            f" OR ({_normalized_sql('sfr.gmail')} <> '' AND {_normalized_sql('sfr.gmail')} = {_normalized_sql('au.email')})"
        )
    else:
        select_parts.extend(
            [
                f"{_normalized_sql('cfr.user_id::text')} AS auth_user_id_key",
                "''::text AS auth_email_key",
                "''::text AS auth_username_key",
            ]
        )
        auth_email_match = ""
        auth_username_match = ""
        legacy_auth_email_match = ""

    if has_local_user:
        joins.append(
            f"""
            LEFT JOIN bronze.user_management_user uu
              ON ({_normalized_sql('uu.field_id')} <> '' AND {_normalized_sql('uu.field_id')} = {_normalized_sql('cfr.brand_supplied_field_rep_id')})
              {auth_email_match}
              {auth_username_match}
            """
        )
        select_parts.extend(
            [
                f"{_normalized_sql('uu.id::text')} AS local_user_id_key",
                f"{_normalized_sql('uu.field_id')} AS local_field_id_key",
                f"{_normalized_sql('uu.email')} AS local_email_key",
                f"{_normalized_sql('uu.username')} AS local_username_key",
            ]
        )
    else:
        select_parts.extend(
            [
                "''::text AS local_user_id_key",
                "''::text AS local_field_id_key",
                "''::text AS local_email_key",
                "''::text AS local_username_key",
            ]
        )

    if has_legacy_rep:
        joins.append(
            f"""
            LEFT JOIN bronze.sharing_management_fieldrepresentative sfr
              ON ({_normalized_sql('sfr.field_id')} <> '' AND {_normalized_sql('sfr.field_id')} = {_normalized_sql('cfr.brand_supplied_field_rep_id')})
              {legacy_auth_email_match}
            """
        )
        select_parts.extend(
            [
                f"{_normalized_sql('sfr.id::text')} AS legacy_rep_id_key",
                f"{_normalized_sql('sfr.field_id')} AS legacy_field_id_key",
                f"{_normalized_sql('sfr.email')} AS legacy_email_key",
                f"{_normalized_sql('sfr.gmail')} AS legacy_gmail_key",
                f"{_normalized_sql('sfr.whatsapp_number')} AS legacy_whatsapp_key",
            ]
        )
    else:
        select_parts.extend(
            [
                "''::text AS legacy_rep_id_key",
                "''::text AS legacy_field_id_key",
                "''::text AS legacy_email_key",
                "''::text AS legacy_gmail_key",
                "''::text AS legacy_whatsapp_key",
            ]
        )

    return "\n".join(joins), ",\n                " + ",\n                ".join(select_parts), key_columns


def _active_reporting_correction_rules_cte() -> str:
    if not _table_exists("ops", "reporting_data_correction_rule"):
        return """
        active_reporting_correction_rules AS (
            SELECT
                ''::text AS rule_type,
                ''::text AS campaign_key,
                ''::text AS doctor_phone_digits,
                ''::text AS doctor_phone_last10,
                ''::text AS doctor_name_key,
                ''::text AS field_rep_brand_supplied_key,
                ''::text AS expected_field_rep_brand_supplied_key,
                ''::text AS affected_field_rep_brand_supplied_ids
            WHERE FALSE
        )
        """
    return f"""
        active_reporting_correction_rules AS (
            SELECT
                rule_type,
                {_normalized_sql('campaign_id')} AS campaign_key,
                regexp_replace(COALESCE(doctor_phone, ''), '[^0-9]', '', 'g') AS doctor_phone_digits,
                right(regexp_replace(COALESCE(NULLIF(doctor_phone_normalized, ''), doctor_phone, ''), '[^0-9]', '', 'g'), 10) AS doctor_phone_last10,
                {_normalized_sql('doctor_name')} AS doctor_name_key,
                {_normalized_sql('field_rep_brand_supplied_id')} AS field_rep_brand_supplied_key,
                {_normalized_sql('expected_field_rep_brand_supplied_id')} AS expected_field_rep_brand_supplied_key,
                COALESCE(affected_field_rep_brand_supplied_ids, '') AS affected_field_rep_brand_supplied_ids
            FROM ops.reporting_data_correction_rule
            WHERE is_active = TRUE
              AND COALESCE(NULLIF(btrim(system_name), ''), 'inclinic') = 'inclinic'
              AND rule_type IN ('keep_doctor_with_field_rep', 'exclude_invalid_doctor_phone')
        )
        """


def _current_schedule_rows(selected_campaign: str) -> list[dict[str, Any]]:
    lookup_key = _normalize_lookup_key(selected_campaign)
    rows = _fetch_dicts(
        f"""
        WITH campaign_row AS (
            SELECT
                cm.id,
                cm.brand_campaign_id,
                cm.name AS campaign_name,
                cm.brand_name,
                cm.company_name,
                cm.company_logo,
                CASE
                    WHEN cm.start_date IS NULL OR btrim(cm.start_date) = '' OR lower(btrim(cm.start_date)) = 'null' THEN NULL
                    ELSE cm.start_date::date
                END AS campaign_start_date,
                CASE
                    WHEN cm.end_date IS NULL OR btrim(cm.end_date) = '' OR lower(btrim(cm.end_date)) = 'null' THEN NULL
                    ELSE cm.end_date::date
                END AS campaign_end_date,
                COALESCE(NULLIF(cm.updated_at, ''), NULLIF(cm.created_at, '')) AS source_updated_at,
                CASE
                    WHEN {_normalized_sql('cm.brand_campaign_id')} = %s THEN 1
                    WHEN {_normalized_sql('cm.id::text')} = %s THEN 2
                    ELSE 3
                END AS campaign_match_rank
            FROM bronze.campaign_management_campaign cm
            LEFT JOIN silver.map_brand_campaign_to_campaign m
              ON {_normalized_sql('m.brand_campaign_id')} = %s
            WHERE
                {_normalized_sql('cm.brand_campaign_id')} = %s
                OR {_normalized_sql('cm.id::text')} = %s
                OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
        ),
        campaign_source AS (
            SELECT *
            FROM campaign_row
        ),
        raw_schedule_candidates AS (
            SELECT
                sc.collateral_id,
                COALESCE(
                    CASE
                        WHEN sc.schedule_start_date IS NULL
                          OR btrim(sc.schedule_start_date::text) = ''
                          OR lower(btrim(sc.schedule_start_date::text)) = 'null'
                        THEN NULL
                        ELSE sc.schedule_start_date::date
                    END,
                    cs.campaign_start_date
                ) AS schedule_start_date,
                COALESCE(
                    CASE
                        WHEN sc.schedule_end_date IS NULL
                          OR btrim(sc.schedule_end_date::text) = ''
                          OR lower(btrim(sc.schedule_end_date::text)) = 'null'
                        THEN NULL
                        ELSE sc.schedule_end_date::date
                    END,
                    cs.campaign_end_date
                ) AS schedule_end_date,
                cs.campaign_start_date,
                cs.campaign_end_date,
                sc.collateral_title,
                COALESCE(
                    NULLIF(btrim(cs.brand_name), ''),
                    NULLIF(btrim(cs.company_name), ''),
                    NULLIF(btrim(cs.campaign_name), '')
                ) AS brand_name,
                CASE
                    WHEN cs.company_logo IS NULL OR btrim(cs.company_logo) = '' OR lower(btrim(cs.company_logo)) = 'null'
                    THEN NULL
                    ELSE cs.company_logo
                END AS company_logo,
                cs.campaign_name,
                cs.campaign_match_rank,
                cs.source_updated_at
            FROM campaign_source cs
            LEFT JOIN silver.bridge_campaign_collateral_schedule sc
              ON sc.campaign_id_resolved::text = cs.id::text
        ),
        schedule_candidates AS (
            SELECT
                *,
                CASE
                    WHEN schedule_start_date <= CURRENT_DATE
                     AND schedule_end_date >= CURRENT_DATE THEN 0
                    WHEN schedule_start_date <= CURRENT_DATE THEN 1
                    WHEN schedule_start_date > CURRENT_DATE THEN 2
                    ELSE 3
                END AS schedule_rank
            FROM raw_schedule_candidates
        )
        SELECT *
        FROM schedule_candidates
        ORDER BY
            schedule_rank,
            CASE WHEN schedule_start_date <= CURRENT_DATE THEN schedule_start_date END DESC NULLS LAST,
            CASE WHEN schedule_start_date > CURRENT_DATE THEN schedule_start_date END ASC NULLS LAST,
            schedule_end_date DESC NULLS LAST,
            campaign_match_rank ASC,
            source_updated_at DESC NULLS LAST,
            collateral_id DESC NULLS LAST
        LIMIT 50
        """,
        [lookup_key, lookup_key, lookup_key, lookup_key, lookup_key],
    )
    return rows


def _campaign_display_name(selected_campaign: str, brand_campaign_variants: list[str]) -> str | None:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    try:
        rows = _fetch_dicts(
            f"""
            WITH candidates AS (
                SELECT
                    cm.brand_name,
                    cm.company_name,
                    cm.name AS campaign_name,
                    COALESCE(NULLIF(cm.updated_at, ''), NULLIF(cm.created_at, '')) AS sort_ts
                FROM bronze.campaign_management_campaign cm
                WHERE {_normalized_sql('cm.brand_campaign_id')} IN ({brand_placeholders})
                   OR {_normalized_sql('cm.id::text')} IN ({brand_placeholders})
                UNION ALL
                SELECT
                    NULL::text AS brand_name,
                    NULL::text AS company_name,
                    cc.name AS campaign_name,
                    COALESCE(NULLIF(cc.updated_at, ''), NULLIF(cc.created_at, '')) AS sort_ts
                FROM bronze.campaign_campaign cc
                JOIN silver.map_brand_campaign_to_campaign m
                  ON NULLIF(btrim(m.campaign_id_resolved), '') = cc.id::text
                WHERE {_normalized_sql('m.brand_campaign_id')} IN ({brand_placeholders})
                   OR {_normalized_sql('cc.id::text')} IN ({brand_placeholders})
            )
            SELECT brand_name, company_name, campaign_name
            FROM candidates
            ORDER BY sort_ts DESC NULLS LAST
            LIMIT 20
            """,
            [*brand_keys, *brand_keys, *brand_keys, *brand_keys],
        )
    except (ProgrammingError, OperationalError):
        return None

    for row in rows:
        for key in ("brand_name", "company_name", "campaign_name"):
            label = _clean_display_text(row.get(key))
            if label:
                return label
    return None


def _assigned_doctor_count(selected_campaign: str, brand_campaign_variants: list[str]) -> int:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    candidate_cte = _candidate_campaign_ids_cte(brand_placeholders)
    alias_joins, alias_selects, _alias_key_columns = _field_rep_alias_sql_parts()
    params = [selected_campaign, *brand_keys, *brand_keys, _normalize_lookup_key(selected_campaign)]
    rows = _fetch_dicts(
        f"""
        WITH {candidate_cte},
        assigned_reps AS (
            SELECT DISTINCT
                ccf.field_rep_id::text AS field_rep_id,
                {_normalized_sql('ccf.field_rep_id::text')} AS internal_rep_key,
                {_normalized_sql('cfr.brand_supplied_field_rep_id')} AS external_rep_key
                {alias_selects}
            FROM bronze.campaign_campaignfieldrep ccf
            JOIN candidate_campaign_ids ci
              ON {_normalized_sql('ccf.campaign_id')} = {_normalized_sql('ci.candidate_campaign_id')}
            LEFT JOIN bronze.campaign_fieldrep cfr
              ON cfr.id::text = ccf.field_rep_id::text
            {alias_joins}
        ),
        assigned_rep_keys AS (
            SELECT field_rep_id, internal_rep_key AS rep_key, 40 AS match_rank
            FROM assigned_reps
            WHERE internal_rep_key <> ''
        ),
        campaign_roster_doctors AS (
            SELECT DISTINCT ark.field_rep_id, b.doctor_identity_key
            FROM silver.bridge_brand_campaign_doctor_base b
            JOIN assigned_rep_keys ark
              ON {_normalized_sql('b.field_rep_id_resolved')} = ark.rep_key
            WHERE {_normalized_sql('b.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(b.doctor_identity_key, ''), '') <> ''
        ),
        global_assigned_doctors AS (
            SELECT DISTINCT ar.field_rep_id, d.doctor_identity_key
            FROM assigned_reps ar
            JOIN silver.dim_doctor d
              ON d.field_rep_id_resolved = ar.field_rep_id
            WHERE COALESCE(NULLIF(d.doctor_identity_key, ''), '') <> ''
        ),
        assigned_doctors AS (
            SELECT field_rep_id, doctor_identity_key
            FROM campaign_roster_doctors
            UNION ALL
            SELECT field_rep_id, doctor_identity_key
            FROM global_assigned_doctors
            WHERE NOT EXISTS (
                SELECT 1
                FROM campaign_roster_doctors roster
                WHERE roster.field_rep_id = global_assigned_doctors.field_rep_id
            )
        ),
        declared_counts AS (
            SELECT MAX(
                CASE
                    WHEN cm.num_doctors ~ '^[0-9]+(\\.[0-9]+)?$' THEN cm.num_doctors::numeric
                    ELSE NULL
                END
            ) AS declared_total
            FROM bronze.campaign_management_campaign cm
            WHERE {_normalized_sql('cm.brand_campaign_id')} IN ({brand_placeholders})
        ),
        supported_counts AS (
            SELECT MAX(
                CASE
                    WHEN cc.num_doctors_supported ~ '^[0-9]+(\\.[0-9]+)?$' THEN cc.num_doctors_supported::numeric
                    ELSE NULL
                END
            ) AS supported_total
            FROM bronze.campaign_campaign cc
            JOIN candidate_campaign_ids ci
              ON {_normalized_sql('cc.id::text')} = {_normalized_sql('ci.candidate_campaign_id')}
        )
        SELECT
            CASE
                WHEN COALESCE((SELECT COUNT(*) FROM assigned_doctors), 0) > 0
                THEN COALESCE((SELECT COUNT(*) FROM assigned_doctors), 0)
                ELSE GREATEST(
                    COALESCE((SELECT declared_total FROM declared_counts), 0),
                    COALESCE((SELECT supported_total FROM supported_counts), 0)
                )
            END::int AS assigned_total
        """,
        [*params, *brand_keys, *brand_keys],
    )
    return _to_int(rows[0].get("assigned_total")) if rows else 0


def _weekly_rows_for_current_collateral(
    selected_campaign: str,
    brand_campaign_variants: list[str],
    current_collateral_ids: list[str],
    total_doctors: int,
    schedule_start_date: Any = None,
    schedule_end_date: Any = None,
) -> list[dict[str, Any]]:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    collateral_filter = ""
    params: list[Any] = [*brand_keys]
    if current_collateral_ids:
        collateral_filter = f"AND a.collateral_id::text IN ({_placeholders(current_collateral_ids)})"
        params.extend(current_collateral_ids)

    params.extend([schedule_start_date, schedule_end_date, schedule_start_date, schedule_start_date, selected_campaign])
    rows = _fetch_dicts(
        f"""
        WITH source_events AS (
            SELECT
                COALESCE(NULLIF(a.doctor_identity_key,''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key,
                CASE WHEN a.reached_first_ts IS NULL OR btrim(a.reached_first_ts) = '' OR lower(btrim(a.reached_first_ts)) = 'null' THEN NULL ELSE a.reached_first_ts::date END AS reached_first_date,
                CASE WHEN a.opened_first_ts IS NULL OR btrim(a.opened_first_ts) = '' OR lower(btrim(a.opened_first_ts)) = 'null' THEN NULL ELSE a.opened_first_ts::date END AS opened_first_date,
                CASE WHEN a.video_gt_50_first_ts IS NULL OR btrim(a.video_gt_50_first_ts) = '' OR lower(btrim(a.video_gt_50_first_ts)) = 'null' THEN NULL ELSE a.video_gt_50_first_ts::date END AS video_gt_50_first_date,
                CASE WHEN a.pdf_download_first_ts IS NULL OR btrim(a.pdf_download_first_ts) = '' OR lower(btrim(a.pdf_download_first_ts)) = 'null' THEN NULL ELSE a.pdf_download_first_ts::date END AS pdf_download_first_date
            FROM silver.doctor_action_first_seen a
            WHERE {_normalized_sql('a.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter}
        ),
        fact_normalized AS (
            SELECT
                doctor_key,
                reached_first_date,
                opened_first_date,
                video_gt_50_first_date,
                pdf_download_first_date,
                (
                    SELECT MIN(activity_date)
                    FROM (
                        VALUES
                            (reached_first_date),
                            (opened_first_date),
                            (video_gt_50_first_date),
                            (pdf_download_first_date)
                    ) AS dates(activity_date)
                    WHERE activity_date IS NOT NULL
                ) AS first_activity_date
            FROM source_events
        ),
        activity_bounds AS (
            SELECT
                MIN(first_activity_date) AS first_activity_date,
                MAX(first_activity_date) AS last_activity_date
            FROM fact_normalized
        ),
        schedule_bounds AS (
            SELECT
                COALESCE(%s::date, first_activity_date, CURRENT_DATE)::date AS schedule_start_date,
                GREATEST(
                    LEAST(COALESCE(%s::date, last_activity_date, %s::date, CURRENT_DATE)::date, CURRENT_DATE),
                    COALESCE(%s::date, first_activity_date, CURRENT_DATE)::date
                ) AS schedule_end_date
            FROM activity_bounds
        ),
        weeks AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY week_start)::int AS week_index,
                week_start::date AS week_start_date,
                LEAST((week_start + interval '6 day')::date, b.schedule_end_date)::date AS week_end_date
            FROM schedule_bounds b
            CROSS JOIN LATERAL generate_series(
                b.schedule_start_date,
                b.schedule_end_date,
                interval '7 day'
            ) AS gs(week_start)
        ),
        agg AS (
            SELECT
                w.week_index,
                w.week_start_date,
                w.week_end_date,
                COUNT(DISTINCT f.doctor_key) FILTER (
                    WHERE f.reached_first_date BETWEEN w.week_start_date AND w.week_end_date
                       OR f.opened_first_date BETWEEN w.week_start_date AND w.week_end_date
                       OR f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date
                       OR f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date
                ) AS doctors_reached_unique,
                COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.opened_first_date BETWEEN w.week_start_date AND w.week_end_date) AS doctors_opened_unique,
                COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date) AS video_viewed_50_unique,
                COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date) AS pdf_download_unique,
                COUNT(DISTINCT f.doctor_key) FILTER (
                    WHERE (f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date)
                       OR (f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date)
                ) AS doctors_consumed_unique
            FROM weeks w
            LEFT JOIN fact_normalized f ON TRUE
            GROUP BY w.week_index, w.week_start_date, w.week_end_date
        )
        SELECT
            %s::text AS brand_campaign_id,
            week_index,
            week_start_date,
            week_end_date,
            doctors_reached_unique,
            doctors_opened_unique,
            video_viewed_50_unique,
            pdf_download_unique,
            doctors_consumed_unique,
            %s::numeric AS total_doctors_in_campaign,
            (%s::numeric / 4.0) AS weekly_doctor_base,
            LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_reached_unique::numeric / NULLIF((%s::numeric / 4.0),0) END, 1.0) AS weekly_reached_pct,
            CASE WHEN doctors_reached_unique=0 THEN 0 ELSE LEAST(doctors_opened_unique::numeric / doctors_reached_unique, 1.0) END AS weekly_opened_pct,
            CASE WHEN doctors_opened_unique=0 THEN 0 ELSE LEAST(doctors_consumed_unique::numeric / doctors_opened_unique, 1.0) END AS weekly_consumption_pct,
            (
                LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_reached_unique::numeric / NULLIF((%s::numeric / 4.0),0) END, 1.0)
                + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE LEAST(doctors_opened_unique::numeric / doctors_reached_unique, 1.0) END
                + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE LEAST(doctors_consumed_unique::numeric / doctors_opened_unique, 1.0) END
            ) / 3.0 * 100 AS weekly_health_score
        FROM agg
        ORDER BY week_index
        """,
        [*params, *([total_doctors] * 6)],
    )
    for row in rows:
        _apply_weekly_v2_fields(row, total_doctors)
    return rows


def _current_collateral_period_metrics(
    selected_campaign: str,
    brand_campaign_variants: list[str],
    current_collateral_ids: list[str],
    period_start: Any,
    period_end: Any,
) -> dict[str, Any]:
    if not period_start or not period_end:
        return {}
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    collateral_filter = ""
    params: list[Any] = [period_start, period_end, *brand_keys]
    if current_collateral_ids:
        collateral_filter = f"AND a.collateral_id::text IN ({_placeholders(current_collateral_ids)})"
        params.extend(current_collateral_ids)
    rows = _fetch_dicts(
        f"""
        WITH period AS (
            SELECT %s::date AS period_start, %s::date AS period_end
        ),
        source_events AS (
            SELECT
                COALESCE(NULLIF(a.doctor_identity_key,''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key,
                CASE WHEN a.reached_first_ts IS NULL OR btrim(a.reached_first_ts) = '' OR lower(btrim(a.reached_first_ts)) = 'null' THEN NULL ELSE a.reached_first_ts::date END AS reached_first_date,
                CASE WHEN a.opened_first_ts IS NULL OR btrim(a.opened_first_ts) = '' OR lower(btrim(a.opened_first_ts)) = 'null' THEN NULL ELSE a.opened_first_ts::date END AS opened_first_date,
                CASE WHEN a.video_gt_50_first_ts IS NULL OR btrim(a.video_gt_50_first_ts) = '' OR lower(btrim(a.video_gt_50_first_ts)) = 'null' THEN NULL ELSE a.video_gt_50_first_ts::date END AS video_gt_50_first_date,
                CASE WHEN a.pdf_download_first_ts IS NULL OR btrim(a.pdf_download_first_ts) = '' OR lower(btrim(a.pdf_download_first_ts)) = 'null' THEN NULL ELSE a.pdf_download_first_ts::date END AS pdf_download_first_date
            FROM silver.doctor_action_first_seen a
            WHERE {_normalized_sql('a.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter}
        )
        SELECT
            COUNT(DISTINCT doctor_key) FILTER (
                WHERE reached_first_date BETWEEN period_start AND period_end
                   OR opened_first_date BETWEEN period_start AND period_end
                   OR video_gt_50_first_date BETWEEN period_start AND period_end
                   OR pdf_download_first_date BETWEEN period_start AND period_end
            ) AS doctors_reached_unique,
            COUNT(DISTINCT doctor_key) FILTER (WHERE opened_first_date BETWEEN period_start AND period_end) AS doctors_opened_unique,
            COUNT(DISTINCT doctor_key) FILTER (WHERE video_gt_50_first_date BETWEEN period_start AND period_end) AS video_viewed_50_unique,
            COUNT(DISTINCT doctor_key) FILTER (WHERE pdf_download_first_date BETWEEN period_start AND period_end) AS pdf_download_unique,
            COUNT(DISTINCT doctor_key) FILTER (
                WHERE video_gt_50_first_date BETWEEN period_start AND period_end
                   OR pdf_download_first_date BETWEEN period_start AND period_end
            ) AS doctors_consumed_unique
        FROM source_events
        CROSS JOIN period
        """,
        params,
    )
    return rows[0] if rows else {}


def _collateral_health_rows(
    selected_campaign: str,
    brand_campaign_variants: list[str],
) -> list[dict[str, Any]]:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    return _fetch_dicts_with_timeout(
        f"""
        WITH source_events AS (
            SELECT
                a.collateral_id::text AS collateral_id,
                a.brand_campaign_id::text AS brand_campaign_id,
                COALESCE(NULLIF(a.doctor_identity_key,''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key,
                NULLIF(a.reached_first_ts, '') AS reached_first_ts,
                NULLIF(a.opened_first_ts, '') AS opened_first_ts,
                NULLIF(a.video_gt_50_first_ts, '') AS video_gt_50_first_ts,
                NULLIF(a.pdf_download_first_ts, '') AS pdf_download_first_ts
            FROM silver.doctor_action_first_seen a
            WHERE {_normalized_sql('a.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(btrim(a.collateral_id::text), ''), '') <> ''
        ),
        collateral_stats AS (
            SELECT
                se.collateral_id,
                COALESCE(MAX(NULLIF(c.collateral_title, '')), 'Collateral ' || se.collateral_id) AS collateral_title,
                COUNT(DISTINCT doctor_key) FILTER (
                    WHERE reached_first_ts IS NOT NULL
                       OR opened_first_ts IS NOT NULL
                       OR video_gt_50_first_ts IS NOT NULL
                       OR pdf_download_first_ts IS NOT NULL
                ) AS reached,
                COUNT(DISTINCT doctor_key) FILTER (WHERE opened_first_ts IS NOT NULL) AS opened,
                COUNT(DISTINCT doctor_key) FILTER (WHERE video_gt_50_first_ts IS NOT NULL) AS video,
                COUNT(DISTINCT doctor_key) FILTER (WHERE pdf_download_first_ts IS NOT NULL) AS pdf,
                COUNT(DISTINCT doctor_key) FILTER (
                    WHERE video_gt_50_first_ts IS NOT NULL OR pdf_download_first_ts IS NOT NULL
                ) AS consumed
            FROM source_events se
            LEFT JOIN silver.bridge_campaign_collateral_schedule c
              ON c.collateral_id::text = se.collateral_id
             AND (
                  {_normalized_sql('c.campaign_id_resolved')} = {_normalized_sql('se.brand_campaign_id')}
                  OR {_normalized_sql('c.campaign_id')} = {_normalized_sql('se.brand_campaign_id')}
             )
            GROUP BY se.collateral_id
        )
        SELECT
            collateral_id,
            collateral_title,
            reached,
            opened,
            video,
            pdf,
            consumed,
            CASE WHEN reached = 0 THEN 0 ELSE ROUND((opened::numeric / reached) * 100, 2) END AS opened_pct,
            CASE WHEN opened = 0 THEN 0 ELSE ROUND((video::numeric / opened) * 100, 2) END AS video_pct,
            CASE WHEN opened = 0 THEN 0 ELSE ROUND((pdf::numeric / opened) * 100, 2) END AS pdf_pct
        FROM collateral_stats
        ORDER BY collateral_id
        """,
        brand_keys,
    )


def _field_rep_insight_rows(
    selected_campaign: str,
    brand_campaign_variants: list[str],
    current_collateral_ids: list[str],
    period_start: Any = None,
    period_end: Any = None,
    include_doctor_details: bool = True,
) -> list[dict[str, Any]]:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    candidate_cte = _candidate_campaign_ids_cte(brand_placeholders)
    correction_rules_cte = _active_reporting_correction_rules_cte()
    collateral_filter_action = ""
    if current_collateral_ids:
        collateral_placeholders = _placeholders(current_collateral_ids)
        collateral_filter_action = f"AND a.collateral_id::text IN ({collateral_placeholders})"

    if include_doctor_details:
        assigned_doctors_json_sql = """
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'name', doctor_name,
                            'phone', COALESCE(doctor_phone, ''),
                            'doctor_key', doctor_identity_key
                        )
                        ORDER BY doctor_name, COALESCE(doctor_phone, ''), doctor_identity_key
                    ),
                    '[]'::jsonb
                ) AS assigned_doctors_json
        """
        activity_doctors_json_sql = """
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'name', doctor_name,
                            'phone', COALESCE(doctor_phone, ''),
                            'doctor_key', doctor_key,
                            'source_field_rep_id', COALESCE(source_field_rep_id, ''),
                            'source_field_rep_email', COALESCE(source_field_rep_email, ''),
                            'source_brand_rep_id', COALESCE(source_brand_rep_id, ''),
                            'evidence_source', COALESCE(evidence_source, '')
                        )
                        ORDER BY doctor_name, COALESCE(doctor_phone, ''), doctor_key
                    ) FILTER (WHERE sent_flag = 1),
                    '[]'::jsonb
                ) AS sent_doctors_json,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'name', doctor_name,
                            'phone', COALESCE(doctor_phone, ''),
                            'doctor_key', doctor_key,
                            'source_field_rep_id', COALESCE(source_field_rep_id, ''),
                            'source_field_rep_email', COALESCE(source_field_rep_email, ''),
                            'source_brand_rep_id', COALESCE(source_brand_rep_id, ''),
                            'evidence_source', COALESCE(evidence_source, '')
                        )
                        ORDER BY doctor_name, COALESCE(doctor_phone, ''), doctor_key
                    ) FILTER (WHERE viewed_flag = 1),
                    '[]'::jsonb
                ) AS viewed_doctors_json,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'name', doctor_name,
                            'phone', COALESCE(doctor_phone, ''),
                            'doctor_key', doctor_key,
                            'source_field_rep_id', COALESCE(source_field_rep_id, ''),
                            'source_field_rep_email', COALESCE(source_field_rep_email, ''),
                            'source_brand_rep_id', COALESCE(source_brand_rep_id, ''),
                            'evidence_source', COALESCE(evidence_source, '')
                        )
                        ORDER BY doctor_name, COALESCE(doctor_phone, ''), doctor_key
                    ) FILTER (WHERE video_flag = 1),
                    '[]'::jsonb
                ) AS video_doctors_json,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'name', doctor_name,
                            'phone', COALESCE(doctor_phone, ''),
                            'doctor_key', doctor_key,
                            'source_field_rep_id', COALESCE(source_field_rep_id, ''),
                            'source_field_rep_email', COALESCE(source_field_rep_email, ''),
                            'source_brand_rep_id', COALESCE(source_brand_rep_id, ''),
                            'evidence_source', COALESCE(evidence_source, '')
                        )
                        ORDER BY doctor_name, COALESCE(doctor_phone, ''), doctor_key
                    ) FILTER (WHERE pdf_flag = 1),
                    '[]'::jsonb
                ) AS pdf_doctors_json
        """
    else:
        assigned_doctors_json_sql = "'[]'::jsonb AS assigned_doctors_json"
        activity_doctors_json_sql = """
                '[]'::jsonb AS sent_doctors_json,
                '[]'::jsonb AS viewed_doctors_json,
                '[]'::jsonb AS video_doctors_json,
                '[]'::jsonb AS pdf_doctors_json
        """

    alias_joins, alias_selects, alias_key_columns = _field_rep_alias_sql_parts()
    alias_key_rank = {
        "auth_email_key": 10,
        "local_email_key": 10,
        "legacy_email_key": 10,
        "legacy_gmail_key": 10,
        "auth_user_id_key": 20,
        "local_user_id_key": 30,
        "legacy_rep_id_key": 35,
        "internal_rep_key": 40,
        "external_rep_key": 45,
        "local_field_id_key": 50,
        "legacy_field_id_key": 50,
        "auth_username_key": 60,
        "local_username_key": 60,
        "legacy_whatsapp_key": 70,
    }
    alias_key_type = {
        "auth_email_key": "email",
        "local_email_key": "email",
        "legacy_email_key": "email",
        "legacy_gmail_key": "email",
        "auth_user_id_key": "auth_user_id",
        "local_user_id_key": "local_user_id",
        "legacy_rep_id_key": "legacy_rep_id",
        "internal_rep_key": "campaign_fieldrep_id",
        "external_rep_key": "brand_field_id",
        "local_field_id_key": "brand_field_id",
        "legacy_field_id_key": "brand_field_id",
        "auth_username_key": "username",
        "local_username_key": "username",
        "legacy_whatsapp_key": "phone",
    }
    alias_key_unions = "\n            ".join(
        f"""
            UNION
            SELECT field_rep_id, {column} AS rep_key, '{alias_key_type.get(column, "alias")}'::text AS key_type, {alias_key_rank.get(column, 90)} AS match_rank
            FROM raw_assigned_reps
            WHERE {column} <> ''
        """.rstrip()
        for column in alias_key_columns
    )

    params = [
        selected_campaign,
        *brand_keys,
        *brand_keys,
        _normalize_lookup_key(selected_campaign),
        *brand_keys,
        period_start,
        period_end,
        *brand_keys,
        *brand_keys,
        *brand_keys,
        *brand_keys,
        *current_collateral_ids,
    ]
    return _fetch_dicts(
        f"""
        WITH {candidate_cte},
        {correction_rules_cte},
        raw_assigned_reps AS (
            SELECT DISTINCT
                ccf.field_rep_id::text AS field_rep_id,
                NULLIF(btrim(cfr.brand_supplied_field_rep_id), '') AS field_rep_brand_supplied_id,
                COALESCE(
                    NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''),
                    NULLIF(btrim(ccf.field_rep_id::text), '')
                ) AS field_rep_display_id,
                COALESCE(
                    NULLIF(btrim(cfr.full_name), ''),
                    NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''),
                    NULLIF(btrim(ccf.field_rep_id::text), ''),
                    'Unknown Field Rep'
                ) AS field_rep_name,
                {_normalized_sql('ccf.field_rep_id::text')} AS internal_rep_key,
                {_normalized_sql('cfr.brand_supplied_field_rep_id')} AS external_rep_key,
                COALESCE(
                    {_valid_state_sql('cfr.state')},
                    {_display_state_sql('cfr.state')},
                    'UNKNOWN'
                ) AS state_normalized
                {alias_selects}
            FROM bronze.campaign_campaignfieldrep ccf
            JOIN candidate_campaign_ids ci
              ON {_normalized_sql('ccf.campaign_id')} = {_normalized_sql('ci.candidate_campaign_id')}
            LEFT JOIN bronze.campaign_fieldrep cfr
              ON cfr.id::text = ccf.field_rep_id::text
            {alias_joins}
        ),
        assigned_reps AS (
            SELECT
                field_rep_id,
                COALESCE(
                    MAX(NULLIF(field_rep_display_id, '')),
                    field_rep_id
                ) AS field_rep_display_id,
                COALESCE(
                    MAX(NULLIF(field_rep_name, '')),
                    MAX(NULLIF(field_rep_display_id, '')),
                    field_rep_id,
                    'Unknown Field Rep'
                ) AS field_rep_name,
                COALESCE(
                    MAX(NULLIF(field_rep_brand_supplied_id, '')),
                    MAX(NULLIF(field_rep_display_id, '')),
                    field_rep_id
                ) AS field_rep_brand_supplied_id,
                COALESCE(
                    MAX(NULLIF(state_normalized, '')) FILTER (WHERE state_normalized <> 'UNKNOWN'),
                    'UNKNOWN'
                ) AS state_normalized
            FROM raw_assigned_reps
            GROUP BY field_rep_id
        ),
        assigned_rep_keys AS (
            SELECT field_rep_id, auth_user_id_key AS rep_key, 'auth_user_id'::text AS key_type, 20 AS match_rank
            FROM raw_assigned_reps
            WHERE auth_user_id_key <> ''
            UNION
            SELECT field_rep_id, local_user_id_key AS rep_key, 'local_user_id'::text AS key_type, 30 AS match_rank
            FROM raw_assigned_reps
            WHERE local_user_id_key <> ''
            UNION
            SELECT field_rep_id, legacy_rep_id_key AS rep_key, 'legacy_rep_id'::text AS key_type, 35 AS match_rank
            FROM raw_assigned_reps
            WHERE legacy_rep_id_key <> ''
            UNION
            SELECT field_rep_id, internal_rep_key AS rep_key, 'campaign_fieldrep_id'::text AS key_type, 40 AS match_rank
            FROM raw_assigned_reps
            WHERE internal_rep_key <> ''
            UNION
            SELECT field_rep_id, external_rep_key AS rep_key, 'brand_field_id'::text AS key_type, 45 AS match_rank
            FROM raw_assigned_reps
            WHERE external_rep_key <> ''
            {alias_key_unions}
        ),
        doctor_identity_lookup AS (
            SELECT DISTINCT ON (d.doctor_identity_key)
                d.doctor_identity_key,
                d.name,
                d.phone,
                d.doctor_phone_normalized,
                d._silver_updated_at
            FROM silver.dim_doctor d
            WHERE COALESCE(NULLIF(d.doctor_identity_key, ''), '') <> ''
            ORDER BY
                d.doctor_identity_key,
                CASE
                    WHEN NULLIF(btrim(d.name), '') IS NULL
                      OR lower(btrim(d.name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END,
                d.id DESC
        ),
        doctor_id_lookup AS (
            SELECT DISTINCT ON (d.id::text)
                d.id::text AS doctor_id,
                d.name,
                d.phone,
                d.doctor_phone_normalized,
                d._silver_updated_at
            FROM silver.dim_doctor d
            WHERE COALESCE(NULLIF(d.id::text, ''), '') <> ''
            ORDER BY
                d.id::text,
                CASE
                    WHEN NULLIF(btrim(d.name), '') IS NULL
                      OR lower(btrim(d.name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END,
                d.id DESC
        ),
        campaign_roster_matches AS (
            SELECT DISTINCT ON (ark.field_rep_id, b.doctor_identity_key)
                ark.field_rep_id,
                b.doctor_identity_key,
                COALESCE(NULLIF(btrim(d_bridge.name), ''), 'Unknown Doctor') AS doctor_name,
                NULLIF(btrim(COALESCE(d_bridge.phone, d_bridge.doctor_phone_normalized)), '') AS doctor_phone,
                NULLIF(btrim(COALESCE(d_bridge._silver_updated_at, b._silver_updated_at)), '') AS doctor_updated_at,
                0 AS source_rank,
                ark.match_rank AS match_rank,
                CASE
                    WHEN NULLIF(btrim(d_bridge.name), '') IS NULL
                      OR lower(btrim(d_bridge.name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END AS name_rank
            FROM silver.bridge_brand_campaign_doctor_base b
            JOIN assigned_rep_keys ark
              ON {_normalized_sql('b.field_rep_id_resolved')} = ark.rep_key
             AND ark.key_type = 'campaign_fieldrep_id'
            JOIN assigned_reps ar_rule
              ON ar_rule.field_rep_id = ark.field_rep_id
            LEFT JOIN doctor_identity_lookup d_identity
              ON d_identity.doctor_identity_key = b.doctor_identity_key
            LEFT JOIN doctor_id_lookup d_master
              ON COALESCE(NULLIF(b.doctor_master_id_resolved, ''), '') <> ''
             AND d_master.doctor_id = b.doctor_master_id_resolved
            LEFT JOIN LATERAL (
                SELECT
                    COALESCE(NULLIF(btrim(d_identity.name), ''), NULLIF(btrim(d_master.name), '')) AS name,
                    COALESCE(NULLIF(btrim(d_identity.phone), ''), NULLIF(btrim(d_master.phone), '')) AS phone,
                    COALESCE(NULLIF(btrim(d_identity.doctor_phone_normalized), ''), NULLIF(btrim(d_master.doctor_phone_normalized), '')) AS doctor_phone_normalized,
                    COALESCE(NULLIF(btrim(d_identity._silver_updated_at), ''), NULLIF(btrim(d_master._silver_updated_at), '')) AS _silver_updated_at
            ) d_bridge ON TRUE
            WHERE {_normalized_sql('b.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(b.doctor_identity_key, ''), '') <> ''
              AND NOT EXISTS (
                  SELECT 1
                  FROM active_reporting_correction_rules rule
                  WHERE rule.campaign_key = {_normalized_sql('b.brand_campaign_id')}
                    AND (
                        (
                            rule.doctor_phone_digits <> ''
                            AND regexp_replace(COALESCE(d_bridge.phone, d_bridge.doctor_phone_normalized, ''), '[^0-9]', '', 'g') = rule.doctor_phone_digits
                        )
                        OR (
                            rule.doctor_phone_last10 <> ''
                            AND right(regexp_replace(COALESCE(d_bridge.phone, d_bridge.doctor_phone_normalized, ''), '[^0-9]', '', 'g'), 10) = rule.doctor_phone_last10
                        )
                    )
                    AND (
                        (
                            rule.rule_type = 'keep_doctor_with_field_rep'
                            AND rule.expected_field_rep_brand_supplied_key <> ''
                            AND NOT EXISTS (
                                SELECT 1
                                FROM assigned_rep_keys expected_scope
                                WHERE expected_scope.field_rep_id = ar_rule.field_rep_id
                                  AND expected_scope.rep_key = rule.expected_field_rep_brand_supplied_key
                            )
                            AND (
                                COALESCE(NULLIF(btrim(rule.affected_field_rep_brand_supplied_ids), ''), '') = ''
                                OR EXISTS (
                                    SELECT 1
                                    FROM regexp_split_to_table(rule.affected_field_rep_brand_supplied_ids, '[,/\\s]+') AS affected(value)
                                    JOIN assigned_rep_keys affected_scope
                                      ON affected_scope.field_rep_id = ar_rule.field_rep_id
                                     AND affected_scope.rep_key = lower(regexp_replace(affected.value, '[^a-zA-Z0-9]+', '', 'g'))
                                    WHERE lower(regexp_replace(affected.value, '[^a-zA-Z0-9]+', '', 'g')) <> ''
                                )
                            )
                        )
                        OR (
                            rule.rule_type = 'exclude_invalid_doctor_phone'
                            AND (
                                rule.field_rep_brand_supplied_key = ''
                                OR EXISTS (
                                    SELECT 1
                                    FROM assigned_rep_keys invalid_scope
                                    WHERE invalid_scope.field_rep_id = ar_rule.field_rep_id
                                      AND invalid_scope.rep_key = rule.field_rep_brand_supplied_key
                                )
                            )
                            AND (
                                rule.doctor_name_key = ''
                                OR rule.doctor_name_key = {_normalized_sql("COALESCE(d_bridge.name, '')")}
                            )
                        )
                    )
              )
            ORDER BY
                ark.field_rep_id,
                b.doctor_identity_key,
                ark.match_rank,
                CASE
                    WHEN NULLIF(btrim(d_bridge.name), '') IS NULL
                      OR lower(btrim(d_bridge.name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END,
                NULLIF(btrim(COALESCE(d_bridge._silver_updated_at, b._silver_updated_at)), '') DESC NULLS LAST,
                ark.field_rep_id
        ),
        global_doctor_matches AS (
            SELECT
                ar.field_rep_id,
                d.doctor_identity_key,
                COALESCE(NULLIF(btrim(d.name), ''), 'Unknown Doctor') AS doctor_name,
                NULLIF(btrim(COALESCE(d.phone, d.doctor_phone_normalized)), '') AS doctor_phone,
                NULLIF(btrim(d._silver_updated_at), '') AS doctor_updated_at,
                1 AS source_rank,
                100 AS match_rank,
                CASE
                    WHEN NULLIF(btrim(d.name), '') IS NULL
                      OR lower(btrim(d.name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END AS name_rank
            FROM assigned_reps ar
            LEFT JOIN silver.dim_doctor d
              ON d.field_rep_id_resolved = ar.field_rep_id
            WHERE COALESCE(NULLIF(d.doctor_identity_key, ''), '') <> ''
        ),
        raw_assigned_doctor_matches AS (
            SELECT
                field_rep_id,
                doctor_identity_key,
                doctor_name,
                doctor_phone,
                doctor_updated_at,
                source_rank,
                match_rank,
                name_rank
            FROM campaign_roster_matches
        ),
        assigned_doctor_matches AS (
            SELECT DISTINCT ON (field_rep_id, doctor_identity_key)
                field_rep_id,
                doctor_identity_key,
                doctor_name,
                doctor_phone
            FROM raw_assigned_doctor_matches
            ORDER BY
                field_rep_id,
                doctor_identity_key,
                source_rank,
                match_rank,
                name_rank,
                doctor_updated_at DESC NULLS LAST,
                field_rep_id
        ),
        assigned_doctor_rows AS (
            SELECT
                field_rep_id,
                doctor_identity_key,
                MIN(doctor_name) AS doctor_name,
                MIN(doctor_phone) AS doctor_phone
            FROM assigned_doctor_matches
            GROUP BY
                field_rep_id,
                doctor_identity_key
        ),
        assigned_doctors AS (
            SELECT
                field_rep_id,
                COUNT(DISTINCT doctor_identity_key) AS total_doctors_assigned,
                {assigned_doctors_json_sql}
            FROM assigned_doctor_rows
            GROUP BY field_rep_id
        ),
        activity_period AS (
            SELECT %s::date AS period_start, %s::date AS period_end
        ),
        transaction_doctor_lookup AS (
            SELECT DISTINCT ON ({_normalized_sql('tx.brand_campaign_id')}, tx.collateral_id::text, tx.doctor_identity_key)
                {_normalized_sql('tx.brand_campaign_id')} AS brand_campaign_key,
                tx.collateral_id::text AS collateral_id,
                tx.doctor_identity_key,
                tx.doctor_name,
                tx.doctor_number
            FROM silver.fact_collateral_transaction tx
            WHERE {_normalized_sql('tx.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(tx.doctor_identity_key, ''), '') <> ''
            ORDER BY
                {_normalized_sql('tx.brand_campaign_id')},
                tx.collateral_id::text,
                tx.doctor_identity_key,
                CASE
                    WHEN NULLIF(btrim(tx.doctor_name), '') IS NULL
                      OR lower(btrim(tx.doctor_name)) IN ('unknown doctor', 'unknown', 'null', 'none')
                    THEN 1 ELSE 0
                END,
                COALESCE(tx.updated_at_ts, tx.created_at_ts, tx.transaction_date_ts) DESC NULLS LAST,
                tx.id DESC
        ),
        rep_evidence_candidates AS (
            SELECT
                {_normalized_sql('tx.brand_campaign_id')} AS brand_campaign_key,
                tx.collateral_id::text AS collateral_id,
                tx.doctor_identity_key AS doctor_key,
                NULLIF({_normalized_sql('tx.field_rep_email')}, '') AS email_rep_key,
                COALESCE(
                    NULLIF({_normalized_sql('tx.field_rep_master_id_resolved')}, ''),
                    NULLIF({_normalized_sql('tx.field_rep_id')}, '')
                ) AS master_rep_key,
                COALESCE(
                    NULLIF({_normalized_sql('tx.brand_supplied_field_rep_id_resolved')}, ''),
                    NULLIF({_normalized_sql('tx.field_rep_unique_id')}, '')
                ) AS brand_rep_key,
                NULLIF({_normalized_sql('tx.field_rep_id')}, '') AS numeric_rep_key,
                COALESCE(NULLIF(btrim(tx.field_rep_id), ''), '') AS source_field_rep_id,
                COALESCE(NULLIF(btrim(tx.field_rep_email), ''), '') AS source_field_rep_email,
                COALESCE(
                    NULLIF(btrim(tx.brand_supplied_field_rep_id_resolved), ''),
                    NULLIF(btrim(tx.field_rep_unique_id), ''),
                    ''
                ) AS source_brand_rep_id,
                'collateral_transaction'::text AS evidence_source,
                10 AS source_rank,
                COALESCE(
                    NULLIF(tx.opened_event_ts, ''),
                    NULLIF(tx.video_gt_50_event_ts, ''),
                    NULLIF(tx.pdf_download_event_ts, ''),
                    NULLIF(tx.reached_event_ts, ''),
                    NULLIF(tx.updated_at_ts, ''),
                    NULLIF(tx.created_at_ts, ''),
                    NULLIF(tx.transaction_date_ts, '')
                ) AS evidence_ts,
                tx.id::text AS source_id
            FROM silver.fact_collateral_transaction tx
            WHERE {_normalized_sql('tx.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(tx.doctor_identity_key, ''), '') <> ''
              AND COALESCE(
                    NULLIF({_normalized_sql('tx.field_rep_master_id_resolved')}, ''),
                    NULLIF({_normalized_sql('tx.field_rep_id')}, ''),
                    NULLIF({_normalized_sql('tx.brand_supplied_field_rep_id_resolved')}, ''),
                    NULLIF({_normalized_sql('tx.field_rep_unique_id')}, ''),
                    NULLIF({_normalized_sql('tx.field_rep_email')}, '')
                  ) IS NOT NULL
              AND lower(btrim(COALESCE(tx._dq_errors, ''))) NOT IN ('missing', 'conflict', 'ambiguous')
            UNION ALL
            SELECT
                {_normalized_sql('s.brand_campaign_id')} AS brand_campaign_key,
                s.collateral_id::text AS collateral_id,
                s.doctor_identity_key AS doctor_key,
                NULLIF({_normalized_sql('s.field_rep_email')}, '') AS email_rep_key,
                NULLIF({_normalized_sql('s.field_rep_id::text')}, '') AS master_rep_key,
                ''::text AS brand_rep_key,
                ''::text AS numeric_rep_key,
                COALESCE(NULLIF(btrim(s.field_rep_id::text), ''), '') AS source_field_rep_id,
                COALESCE(NULLIF(btrim(s.field_rep_email), ''), '') AS source_field_rep_email,
                ''::text AS source_brand_rep_id,
                'share_log'::text AS evidence_source,
                20 AS source_rank,
                COALESCE(
                    NULLIF(s.share_timestamp_ts, ''),
                    NULLIF(s.updated_at_ts, ''),
                    NULLIF(s.created_at_ts, ''),
                    NULLIF(s.reached_event_ts, '')
                ) AS evidence_ts,
                s.id::text AS source_id
            FROM silver.fact_share_log s
            WHERE {_normalized_sql('s.brand_campaign_id')} IN ({brand_placeholders})
              AND COALESCE(NULLIF(s.doctor_identity_key, ''), '') <> ''
              AND COALESCE(
                    NULLIF({_normalized_sql('s.field_rep_id::text')}, ''),
                    NULLIF({_normalized_sql('s.field_rep_email')}, '')
                  ) IS NOT NULL
        ),
        rep_evidence_latest AS (
            SELECT DISTINCT ON (brand_campaign_key, collateral_id, doctor_key)
                brand_campaign_key,
                collateral_id,
                doctor_key,
                email_rep_key,
                master_rep_key,
                brand_rep_key,
                numeric_rep_key,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source
            FROM rep_evidence_candidates
            ORDER BY
                brand_campaign_key,
                collateral_id,
                doctor_key,
                source_rank,
                evidence_ts DESC NULLS LAST,
                source_id DESC NULLS LAST
        ),
        action_dates AS (
            SELECT
                a.brand_campaign_id,
                a.collateral_id,
                'action:' || COALESCE(a.brand_campaign_id, '') || ':' || COALESCE(a.collateral_id, '') || ':' ||
                    COALESCE(NULLIF(a.doctor_identity_key, ''), a.brand_campaign_id || ':' || a.collateral_id) AS activity_row_id,
                COALESCE(NULLIF(a.doctor_identity_key, ''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key,
                COALESCE(
                    NULLIF(btrim(d_action.name), ''),
                    NULLIF(btrim(tx_action.doctor_name), '')
                ) AS doctor_name,
                COALESCE(
                    NULLIF(btrim(d_action.phone), ''),
                    NULLIF(btrim(d_action.doctor_phone_normalized), ''),
                    NULLIF(btrim(tx_action.doctor_number), ''),
                    ''
                ) AS doctor_phone,
                CASE WHEN a.reached_first_ts IS NULL OR btrim(a.reached_first_ts) = '' OR lower(btrim(a.reached_first_ts)) = 'null' THEN NULL ELSE a.reached_first_ts::date END AS reached_first_date,
                CASE WHEN a.opened_first_ts IS NULL OR btrim(a.opened_first_ts) = '' OR lower(btrim(a.opened_first_ts)) = 'null' THEN NULL ELSE a.opened_first_ts::date END AS opened_first_date,
                CASE WHEN a.video_gt_50_first_ts IS NULL OR btrim(a.video_gt_50_first_ts) = '' OR lower(btrim(a.video_gt_50_first_ts)) = 'null' THEN NULL ELSE a.video_gt_50_first_ts::date END AS video_gt_50_first_date,
                CASE WHEN a.pdf_download_first_ts IS NULL OR btrim(a.pdf_download_first_ts) = '' OR lower(btrim(a.pdf_download_first_ts)) = 'null' THEN NULL ELSE a.pdf_download_first_ts::date END AS pdf_download_first_date
            FROM silver.doctor_action_first_seen a
            LEFT JOIN doctor_identity_lookup d_action
              ON d_action.doctor_identity_key = a.doctor_identity_key
            LEFT JOIN transaction_doctor_lookup tx_action
              ON tx_action.brand_campaign_key = {_normalized_sql('a.brand_campaign_id')}
             AND tx_action.collateral_id = a.collateral_id::text
             AND tx_action.doctor_identity_key = a.doctor_identity_key
            WHERE {_normalized_sql('a.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_action}
        ),
        activity_source AS (
            SELECT
                ad.brand_campaign_id,
                ad.activity_row_id,
                ad.doctor_key,
                ad.doctor_name,
                ad.doctor_phone,
                COALESCE(rep_evidence.email_rep_key, '') AS email_rep_key,
                COALESCE(rep_evidence.master_rep_key, '') AS master_rep_key,
                COALESCE(rep_evidence.brand_rep_key, '') AS brand_rep_key,
                COALESCE(rep_evidence.numeric_rep_key, '') AS numeric_rep_key,
                COALESCE(rep_evidence.source_field_rep_id, '') AS source_field_rep_id,
                COALESCE(rep_evidence.source_field_rep_email, '') AS source_field_rep_email,
                COALESCE(rep_evidence.source_brand_rep_id, '') AS source_brand_rep_id,
                COALESCE(rep_evidence.evidence_source, '') AS evidence_source,
                CASE
                    WHEN (
                        ad.reached_first_date IS NOT NULL
                        AND (p.period_start IS NULL OR ad.reached_first_date >= p.period_start)
                        AND (p.period_end IS NULL OR ad.reached_first_date <= p.period_end)
                    ) OR (
                        ad.opened_first_date IS NOT NULL
                        AND (p.period_start IS NULL OR ad.opened_first_date >= p.period_start)
                        AND (p.period_end IS NULL OR ad.opened_first_date <= p.period_end)
                    ) OR (
                        ad.video_gt_50_first_date IS NOT NULL
                        AND (p.period_start IS NULL OR ad.video_gt_50_first_date >= p.period_start)
                        AND (p.period_end IS NULL OR ad.video_gt_50_first_date <= p.period_end)
                    ) OR (
                        ad.pdf_download_first_date IS NOT NULL
                        AND (p.period_start IS NULL OR ad.pdf_download_first_date >= p.period_start)
                        AND (p.period_end IS NULL OR ad.pdf_download_first_date <= p.period_end)
                    )
                    THEN 1 ELSE 0
                END AS sent_flag,
                CASE
                    WHEN ad.opened_first_date IS NOT NULL
                     AND (p.period_start IS NULL OR ad.opened_first_date >= p.period_start)
                     AND (p.period_end IS NULL OR ad.opened_first_date <= p.period_end)
                    THEN 1 ELSE 0
                END AS viewed_flag,
                CASE
                    WHEN ad.video_gt_50_first_date IS NOT NULL
                     AND (p.period_start IS NULL OR ad.video_gt_50_first_date >= p.period_start)
                     AND (p.period_end IS NULL OR ad.video_gt_50_first_date <= p.period_end)
                    THEN 1 ELSE 0
                END AS video_flag,
                CASE
                    WHEN ad.pdf_download_first_date IS NOT NULL
                     AND (p.period_start IS NULL OR ad.pdf_download_first_date >= p.period_start)
                     AND (p.period_end IS NULL OR ad.pdf_download_first_date <= p.period_end)
                    THEN 1 ELSE 0
                END AS pdf_flag
            FROM action_dates ad
            CROSS JOIN activity_period p
            LEFT JOIN rep_evidence_latest rep_evidence
              ON rep_evidence.brand_campaign_key = {_normalized_sql('ad.brand_campaign_id')}
             AND rep_evidence.collateral_id = ad.collateral_id::text
             AND rep_evidence.doctor_key = ad.doctor_key
            WHERE (
                ad.reached_first_date IS NOT NULL
                AND (p.period_start IS NULL OR ad.reached_first_date >= p.period_start)
                AND (p.period_end IS NULL OR ad.reached_first_date <= p.period_end)
            ) OR (
                ad.opened_first_date IS NOT NULL
                AND (p.period_start IS NULL OR ad.opened_first_date >= p.period_start)
                AND (p.period_end IS NULL OR ad.opened_first_date <= p.period_end)
            ) OR (
                ad.video_gt_50_first_date IS NOT NULL
                AND (p.period_start IS NULL OR ad.video_gt_50_first_date >= p.period_start)
                AND (p.period_end IS NULL OR ad.video_gt_50_first_date <= p.period_end)
            ) OR (
                ad.pdf_download_first_date IS NOT NULL
                AND (p.period_start IS NULL OR ad.pdf_download_first_date >= p.period_start)
                AND (p.period_end IS NULL OR ad.pdf_download_first_date <= p.period_end)
            )
        ),
        activity_key_candidates AS (
            SELECT
                brand_campaign_id,
                activity_row_id,
                doctor_key,
                doctor_name,
                doctor_phone,
                sent_flag,
                viewed_flag,
                video_flag,
                pdf_flag,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source,
                email_rep_key AS rep_key,
                'email'::text AS key_type,
                10 AS source_rank
            FROM activity_source
            WHERE COALESCE(email_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, master_rep_key, 'campaign_fieldrep_id'::text, 20
            FROM activity_source
            WHERE COALESCE(master_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, brand_rep_key, 'brand_field_id'::text, 30
            FROM activity_source
            WHERE COALESCE(brand_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, numeric_rep_key, 'local_user_id'::text, 50
            FROM activity_source
            WHERE COALESCE(numeric_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, numeric_rep_key, 'auth_user_id'::text, 50
            FROM activity_source
            WHERE COALESCE(numeric_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, numeric_rep_key, 'legacy_rep_id'::text, 50
            FROM activity_source
            WHERE COALESCE(numeric_rep_key, '') <> ''
            UNION ALL
            SELECT brand_campaign_id, activity_row_id, doctor_key, doctor_name, doctor_phone, sent_flag, viewed_flag, video_flag, pdf_flag, source_field_rep_id, source_field_rep_email, source_brand_rep_id, evidence_source, numeric_rep_key, 'campaign_fieldrep_id'::text, 50
            FROM activity_source
            WHERE COALESCE(numeric_rep_key, '') <> ''
        ),
        activity_candidate_matches AS (
            SELECT
                ark.field_rep_id,
                akc.brand_campaign_id,
                akc.activity_row_id,
                akc.doctor_key,
                akc.doctor_name,
                akc.doctor_phone,
                akc.sent_flag,
                akc.viewed_flag,
                akc.video_flag,
                akc.pdf_flag,
                akc.source_field_rep_id,
                akc.source_field_rep_email,
                akc.source_brand_rep_id,
                akc.evidence_source,
                akc.source_rank,
                ark.match_rank,
                akc.key_type
            FROM activity_key_candidates akc
            JOIN assigned_rep_keys ark
              ON ark.rep_key = akc.rep_key
             AND ark.key_type = akc.key_type
            JOIN assigned_reps ar_rule
              ON ar_rule.field_rep_id = ark.field_rep_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM active_reporting_correction_rules rule
                WHERE rule.campaign_key = {_normalized_sql('akc.brand_campaign_id')}
                  AND (
                      (
                          rule.doctor_phone_digits <> ''
                          AND regexp_replace(COALESCE(akc.doctor_phone, akc.doctor_key, ''), '[^0-9]', '', 'g') = rule.doctor_phone_digits
                      )
                      OR (
                          rule.doctor_phone_last10 <> ''
                          AND right(regexp_replace(COALESCE(akc.doctor_phone, akc.doctor_key, ''), '[^0-9]', '', 'g'), 10) = rule.doctor_phone_last10
                      )
                  )
                  AND (
                      (
                          rule.rule_type = 'keep_doctor_with_field_rep'
                          AND rule.expected_field_rep_brand_supplied_key <> ''
                          AND NOT EXISTS (
                              SELECT 1
                              FROM assigned_rep_keys expected_scope
                              WHERE expected_scope.field_rep_id = ar_rule.field_rep_id
                                AND expected_scope.rep_key = rule.expected_field_rep_brand_supplied_key
                          )
                          AND (
                              COALESCE(NULLIF(btrim(rule.affected_field_rep_brand_supplied_ids), ''), '') = ''
                              OR EXISTS (
                                  SELECT 1
                                  FROM regexp_split_to_table(rule.affected_field_rep_brand_supplied_ids, '[,/\\s]+') AS affected(value)
                                  JOIN assigned_rep_keys affected_scope
                                    ON affected_scope.field_rep_id = ar_rule.field_rep_id
                                   AND affected_scope.rep_key = lower(regexp_replace(affected.value, '[^a-zA-Z0-9]+', '', 'g'))
                                  WHERE lower(regexp_replace(affected.value, '[^a-zA-Z0-9]+', '', 'g')) <> ''
                              )
                          )
                      )
                      OR (
                          rule.rule_type = 'exclude_invalid_doctor_phone'
                          AND (
                              rule.field_rep_brand_supplied_key = ''
                              OR EXISTS (
                                  SELECT 1
                                  FROM assigned_rep_keys invalid_scope
                                  WHERE invalid_scope.field_rep_id = ar_rule.field_rep_id
                                    AND invalid_scope.rep_key = rule.field_rep_brand_supplied_key
                              )
                          )
                          AND (
                              rule.doctor_name_key = ''
                              OR rule.doctor_name_key = {_normalized_sql("COALESCE(akc.doctor_name, '')")}
                          )
                      )
                  )
            )
        ),
        best_activity_candidate_source AS (
            SELECT
                activity_row_id,
                MIN(source_rank) AS best_source_rank
            FROM activity_candidate_matches
            GROUP BY activity_row_id
        ),
        unambiguous_activity_matches AS (
            SELECT
                m.activity_row_id,
                MIN(m.field_rep_id) AS field_rep_id,
                b.best_source_rank
            FROM activity_candidate_matches m
            JOIN best_activity_candidate_source b
              ON b.activity_row_id = m.activity_row_id
             AND b.best_source_rank = m.source_rank
            GROUP BY m.activity_row_id, b.best_source_rank
            HAVING COUNT(DISTINCT m.field_rep_id) = 1
        ),
        direct_matched_activity AS (
            SELECT DISTINCT ON (m.activity_row_id)
                u.field_rep_id,
                m.activity_row_id,
                m.doctor_key,
                m.doctor_name,
                m.doctor_phone,
                m.sent_flag,
                m.viewed_flag,
                m.video_flag,
                m.pdf_flag,
                m.source_field_rep_id,
                m.source_field_rep_email,
                m.source_brand_rep_id,
                m.evidence_source
            FROM activity_candidate_matches m
            JOIN unambiguous_activity_matches u
              ON u.activity_row_id = m.activity_row_id
             AND u.field_rep_id = m.field_rep_id
             AND u.best_source_rank = m.source_rank
            ORDER BY
                m.activity_row_id,
                m.match_rank,
                m.key_type,
                m.field_rep_id
        ),
        rule_corrected_activity AS (
            SELECT DISTINCT
                ar_expected.field_rep_id,
                src.activity_row_id,
                src.doctor_key,
                src.doctor_name,
                src.doctor_phone,
                src.sent_flag,
                src.viewed_flag,
                src.video_flag,
                src.pdf_flag,
                src.source_field_rep_id,
                src.source_field_rep_email,
                src.source_brand_rep_id,
                CASE
                    WHEN COALESCE(NULLIF(src.evidence_source, ''), '') = '' THEN 'reporting_correction_rule'
                    ELSE src.evidence_source || ', reporting_correction_rule'
                END AS evidence_source
            FROM activity_source src
            JOIN active_reporting_correction_rules rule
              ON rule.rule_type = 'keep_doctor_with_field_rep'
             AND rule.campaign_key = {_normalized_sql('src.brand_campaign_id')}
             AND rule.expected_field_rep_brand_supplied_key <> ''
             AND (
                (
                    rule.doctor_phone_digits <> ''
                    AND regexp_replace(COALESCE(src.doctor_phone, src.doctor_key, ''), '[^0-9]', '', 'g') = rule.doctor_phone_digits
                )
                OR (
                    rule.doctor_phone_last10 <> ''
                    AND right(regexp_replace(COALESCE(src.doctor_phone, src.doctor_key, ''), '[^0-9]', '', 'g'), 10) = rule.doctor_phone_last10
                )
             )
            JOIN assigned_reps ar_expected
              ON EXISTS (
                SELECT 1
                FROM assigned_rep_keys expected_scope
                WHERE expected_scope.field_rep_id = ar_expected.field_rep_id
                  AND expected_scope.rep_key = rule.expected_field_rep_brand_supplied_key
              )
        ),
        doctor_matched_activity AS (
            SELECT
                NULL::text AS field_rep_id,
                src.activity_row_id,
                src.doctor_key,
                src.doctor_name,
                src.doctor_phone,
                src.sent_flag,
                src.viewed_flag,
                src.video_flag,
                src.pdf_flag,
                src.source_field_rep_id,
                src.source_field_rep_email,
                src.source_brand_rep_id,
                src.evidence_source
            FROM activity_source src
            WHERE FALSE
        ),
        unmatched_activity AS (
            SELECT
                '{UNMAPPED_ACTIVITY_FIELD_REP_ID}'::text AS field_rep_id,
                src.activity_row_id,
                src.doctor_key,
                src.doctor_name,
                src.doctor_phone,
                src.sent_flag,
                src.viewed_flag,
                src.video_flag,
                src.pdf_flag,
                src.source_field_rep_id,
                src.source_field_rep_email,
                src.source_brand_rep_id,
                src.evidence_source
            FROM activity_source src
            WHERE NOT EXISTS (
                SELECT 1
                FROM direct_matched_activity dm
                WHERE dm.activity_row_id = src.activity_row_id
            )
              AND NOT EXISTS (
                SELECT 1
                FROM doctor_matched_activity dmatch
                WHERE dmatch.activity_row_id = src.activity_row_id
            )
              AND NOT EXISTS (
                SELECT 1
                FROM rule_corrected_activity rca
                WHERE rca.activity_row_id = src.activity_row_id
            )
              AND NOT EXISTS (
                SELECT 1
                FROM active_reporting_correction_rules rule
                WHERE rule.campaign_key = {_normalized_sql('src.brand_campaign_id')}
                  AND rule.rule_type IN ('keep_doctor_with_field_rep', 'exclude_invalid_doctor_phone')
                  AND (
                    (
                        rule.doctor_phone_digits <> ''
                        AND regexp_replace(COALESCE(src.doctor_phone, src.doctor_key, ''), '[^0-9]', '', 'g') = rule.doctor_phone_digits
                    )
                    OR (
                        rule.doctor_phone_last10 <> ''
                        AND right(regexp_replace(COALESCE(src.doctor_phone, src.doctor_key, ''), '[^0-9]', '', 'g'), 10) = rule.doctor_phone_last10
                    )
                  )
                  AND (
                    rule.rule_type = 'keep_doctor_with_field_rep'
                    OR rule.doctor_name_key = ''
                    OR rule.doctor_name_key = {_normalized_sql("COALESCE(src.doctor_name, '')")}
                  )
            )
        ),
        reporting_reps AS (
            SELECT
                field_rep_id,
                field_rep_display_id,
                field_rep_name,
                state_normalized
            FROM assigned_reps
            UNION ALL
            SELECT
                '{UNMAPPED_ACTIVITY_FIELD_REP_ID}'::text AS field_rep_id,
                'UNMAPPED_ACTIVITY'::text AS field_rep_display_id,
                'Unmapped Activity'::text AS field_rep_name,
                'UNKNOWN'::text AS state_normalized
            WHERE EXISTS (SELECT 1 FROM unmatched_activity)
        ),
        matched_activity AS (
            SELECT
                field_rep_id,
                activity_row_id,
                doctor_key,
                doctor_name,
                doctor_phone,
                sent_flag,
                viewed_flag,
                video_flag,
                pdf_flag,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source
            FROM direct_matched_activity
            UNION ALL
            SELECT
                field_rep_id,
                activity_row_id,
                doctor_key,
                doctor_name,
                doctor_phone,
                sent_flag,
                viewed_flag,
                video_flag,
                pdf_flag,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source
            FROM doctor_matched_activity
            UNION ALL
            SELECT
                field_rep_id,
                activity_row_id,
                doctor_key,
                doctor_name,
                doctor_phone,
                sent_flag,
                viewed_flag,
                video_flag,
                pdf_flag,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source
            FROM rule_corrected_activity
            UNION ALL
            SELECT
                field_rep_id,
                activity_row_id,
                doctor_key,
                doctor_name,
                doctor_phone,
                sent_flag,
                viewed_flag,
                video_flag,
                pdf_flag,
                source_field_rep_id,
                source_field_rep_email,
                source_brand_rep_id,
                evidence_source
            FROM unmatched_activity
        ),
        activity_doctor_rows AS (
            SELECT
                ma.field_rep_id,
                ma.doctor_key,
                COALESCE(
                    MIN(NULLIF(NULLIF(ma.doctor_name, ''), 'Unknown Doctor')),
                    MIN(NULLIF(NULLIF(ad.doctor_name, ''), 'Unknown Doctor')),
                    ''
                ) AS doctor_name,
                COALESCE(
                    MIN(NULLIF(ma.doctor_phone, '')),
                    MIN(NULLIF(ad.doctor_phone, '')),
                    ''
                ) AS doctor_phone,
                STRING_AGG(DISTINCT NULLIF(ma.source_field_rep_id, ''), ', ' ORDER BY NULLIF(ma.source_field_rep_id, '')) AS source_field_rep_id,
                STRING_AGG(DISTINCT NULLIF(ma.source_field_rep_email, ''), ', ' ORDER BY NULLIF(ma.source_field_rep_email, '')) AS source_field_rep_email,
                STRING_AGG(DISTINCT NULLIF(ma.source_brand_rep_id, ''), ', ' ORDER BY NULLIF(ma.source_brand_rep_id, '')) AS source_brand_rep_id,
                STRING_AGG(DISTINCT NULLIF(ma.evidence_source, ''), ', ' ORDER BY NULLIF(ma.evidence_source, '')) AS evidence_source,
                MAX(ma.sent_flag) AS sent_flag,
                MAX(ma.viewed_flag) AS viewed_flag,
                MAX(ma.video_flag) AS video_flag,
                MAX(ma.pdf_flag) AS pdf_flag
            FROM matched_activity ma
            LEFT JOIN assigned_doctor_rows ad
              ON ad.field_rep_id = ma.field_rep_id
             AND (
                ad.doctor_identity_key = ma.doctor_key
                OR (
                    COALESCE(NULLIF(ad.doctor_phone, ''), '') <> ''
                    AND ad.doctor_phone = ma.doctor_key
                )
             )
            GROUP BY ma.field_rep_id, ma.doctor_key
        ),
        activity_for_rep AS (
            SELECT
                field_rep_id,
                COUNT(*) FILTER (WHERE sent_flag = 1) AS doctors_sent,
                COUNT(*) FILTER (WHERE viewed_flag = 1) AS doctors_viewed,
                COUNT(*) FILTER (WHERE video_flag = 1) AS doctors_video_played,
                COUNT(*) FILTER (WHERE pdf_flag = 1) AS doctors_pdf_downloaded,
                {activity_doctors_json_sql}
            FROM activity_doctor_rows
            GROUP BY field_rep_id
        )
        SELECT
            COALESCE(NULLIF(ar.field_rep_display_id, ''), ar.field_rep_id) AS field_rep_id,
            ar.field_rep_name,
            ar.state_normalized,
            COALESCE(ad.total_doctors_assigned, 0)::int AS total_doctors_assigned,
            COALESCE(ad.assigned_doctors_json, '[]'::jsonb)::text AS assigned_doctors_json,
            COALESCE(ab.doctors_sent, 0)::int AS doctors_sent,
            COALESCE(ab.sent_doctors_json, '[]'::jsonb)::text AS sent_doctors_json,
            COALESCE(ab.doctors_viewed, 0)::int AS doctors_viewed,
            COALESCE(ab.viewed_doctors_json, '[]'::jsonb)::text AS viewed_doctors_json,
            COALESCE(ab.doctors_video_played, 0)::int AS doctors_video_played,
            COALESCE(ab.video_doctors_json, '[]'::jsonb)::text AS video_doctors_json,
            COALESCE(ab.doctors_pdf_downloaded, 0)::int AS doctors_pdf_downloaded,
            COALESCE(ab.pdf_doctors_json, '[]'::jsonb)::text AS pdf_doctors_json,
            CASE
                WHEN COALESCE(ad.total_doctors_assigned, 0) = 0
                 AND (
                    COALESCE(ab.doctors_sent, 0) > 0
                    OR COALESCE(ab.doctors_viewed, 0) > 0
                    OR COALESCE(ab.doctors_video_played, 0) > 0
                    OR COALESCE(ab.doctors_pdf_downloaded, 0) > 0
                 )
                THEN 'No campaign doctor roster match; engagement comes from consolidated doctor action metrics.'
                WHEN COALESCE(ab.doctors_sent, 0) > COALESCE(ad.total_doctors_assigned, 0)
                THEN 'Engagement exceeds campaign roster matches; check doctor roster or rep mapping.'
                ELSE ''
            END AS assignment_note,
            (ar.field_rep_id = '{UNMAPPED_ACTIVITY_FIELD_REP_ID}') AS is_unmapped_activity
        FROM reporting_reps ar
        LEFT JOIN assigned_doctors ad ON ad.field_rep_id = ar.field_rep_id
        LEFT JOIN activity_for_rep ab ON ab.field_rep_id = ar.field_rep_id
        ORDER BY
            COALESCE(ab.doctors_sent, 0) DESC,
            COALESCE(ad.total_doctors_assigned, 0) DESC,
            ar.field_rep_name
        """,
        params,
    )


def _state_attention_source_rows(
    selected_campaign: str,
    brand_campaign_variants: list[str],
    selected_schema: str,
    latest_week: dict[str, Any],
    bridge_base_exists: bool,
    current_collateral_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    candidate_cte = _candidate_campaign_ids_cte(brand_placeholders)
    current_collateral_ids = current_collateral_ids or []
    collateral_filter_action = ""
    collateral_filter_tx = ""
    collateral_filter_share = ""
    if current_collateral_ids:
        collateral_placeholders = _placeholders(current_collateral_ids)
        collateral_filter_action = f"AND a.collateral_id::text IN ({collateral_placeholders})"
        collateral_filter_tx = f"AND tx.collateral_id::text IN ({collateral_placeholders})"
        collateral_filter_share = f"AND s.collateral_id::text IN ({collateral_placeholders})"
    bridge_params = brand_keys if bridge_base_exists else []
    alias_joins, alias_selects, alias_key_columns = _field_rep_alias_sql_parts()
    alias_state_unions = "\n                    ".join(
        f"""
                    UNION
                    SELECT {column} AS rep_key, state_normalized
                    FROM raw_rep_state_campaign
                    WHERE {column} <> ''
                      AND state_normalized IS NOT NULL
        """.rstrip()
        for column in alias_key_columns
    )
    bridge_base_sql = (
        f"""
        bridge_base AS (
            SELECT DISTINCT ON (b.brand_campaign_id, b.doctor_identity_key)
              b.brand_campaign_id,
              b.doctor_identity_key,
              b.field_rep_id_resolved,
              b.state_normalized
            FROM silver.bridge_brand_campaign_doctor_base b
            WHERE {_normalized_sql('b.brand_campaign_id')} IN ({brand_placeholders})
            ORDER BY
              b.brand_campaign_id,
              b.doctor_identity_key,
              CASE WHEN {_valid_state_sql('b.state_normalized')} IS NULL THEN 1 ELSE 0 END
        )
        """
        if bridge_base_exists
        else """
        bridge_base AS (
            SELECT
              NULL::text AS brand_campaign_id,
              NULL::text AS doctor_identity_key,
              NULL::text AS field_rep_id_resolved,
              NULL::text AS state_normalized
            WHERE FALSE
        )
        """
    )
    _ = selected_schema
    return _fetch_dicts_with_timeout(
        f"""
        WITH {candidate_cte},
        raw_rep_state_campaign AS (
            SELECT DISTINCT
              lower(regexp_replace(btrim(ccf.field_rep_id::text), '[^a-zA-Z0-9]', '', 'g')) AS internal_rep_key,
              {_normalized_sql('cfr.brand_supplied_field_rep_id')} AS external_rep_key,
              {_valid_state_sql('cfr.state')} AS state_normalized
              {alias_selects}
            FROM bronze.campaign_campaignfieldrep ccf
            JOIN candidate_campaign_ids ci
              ON lower(regexp_replace(NULLIF(btrim(ccf.campaign_id), ''), '[^a-zA-Z0-9]', '', 'g'))
               = lower(regexp_replace(ci.candidate_campaign_id, '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN bronze.campaign_fieldrep cfr
              ON cfr.id::text = ccf.field_rep_id::text
            {alias_joins}
            WHERE cfr.state IS NOT NULL
              AND btrim(cfr.state) <> ''
              AND lower(btrim(cfr.state)) NOT IN ('null', 'none', 'unknown')
              AND {_valid_state_sql('cfr.state')} IS NOT NULL
        ),
        rep_state_campaign AS (
            SELECT internal_rep_key AS rep_key, state_normalized
            FROM raw_rep_state_campaign
            WHERE internal_rep_key <> ''
              AND state_normalized IS NOT NULL
            UNION
            SELECT external_rep_key AS rep_key, state_normalized
            FROM raw_rep_state_campaign
            WHERE external_rep_key <> ''
              AND state_normalized IS NOT NULL
            {alias_state_unions}
        ),
        rep_state_global AS (
            SELECT DISTINCT
              lower(regexp_replace(COALESCE(NULLIF(btrim(brand_supplied_field_rep_id), ''), btrim(id::text)), '[^a-zA-Z0-9]', '', 'g')) AS rep_key,
              {_valid_state_sql('state')} AS state_normalized
            FROM bronze.campaign_fieldrep
            WHERE state IS NOT NULL
              AND btrim(state) <> ''
              AND lower(btrim(state)) NOT IN ('null', 'none', 'unknown')
              AND {_valid_state_sql('state')} IS NOT NULL
        ),
        roster_candidates AS (
            SELECT DISTINCT
              d.doctor_identity_key AS doctor_key,
              COALESCE(
                {_valid_state_sql('rsc.state_normalized')},
                {_valid_state_sql('d.state_normalized')},
                'UNKNOWN'
              ) AS state_normalized
            FROM rep_state_campaign rsc
            JOIN silver.dim_doctor d
              ON {_normalized_sql('d.rep_id_normalized')} = rsc.rep_key
              OR {_normalized_sql('d.field_rep_id_resolved')} = rsc.rep_key
            WHERE COALESCE(NULLIF(d.doctor_identity_key, ''), '') <> ''
        ),
        roster_base AS (
            SELECT DISTINCT ON (doctor_key)
              doctor_key,
              state_normalized
            FROM roster_candidates
            ORDER BY
              doctor_key,
              CASE WHEN state_normalized = 'UNKNOWN' THEN 1 ELSE 0 END,
              state_normalized
        ),
        source_events AS (
            SELECT
              a.brand_campaign_id,
              a.collateral_id,
              COALESCE(NULLIF(a.doctor_identity_key, ''), a.brand_campaign_id || ':' || a.collateral_id) AS doctor_key,
              NULLIF(a.doctor_identity_key, '') AS doctor_identity_key,
              CASE WHEN a.reached_first_ts IS NULL OR btrim(a.reached_first_ts) = '' OR lower(btrim(a.reached_first_ts)) = 'null' THEN NULL ELSE a.reached_first_ts::date END AS reached_first_date,
              CASE WHEN a.opened_first_ts IS NULL OR btrim(a.opened_first_ts) = '' OR lower(btrim(a.opened_first_ts)) = 'null' THEN NULL ELSE a.opened_first_ts::date END AS opened_first_date,
              CASE WHEN a.video_gt_50_first_ts IS NULL OR btrim(a.video_gt_50_first_ts) = '' OR lower(btrim(a.video_gt_50_first_ts)) = 'null' THEN NULL ELSE a.video_gt_50_first_ts::date END AS video_gt_50_first_date,
              CASE WHEN a.pdf_download_first_ts IS NULL OR btrim(a.pdf_download_first_ts) = '' OR lower(btrim(a.pdf_download_first_ts)) = 'null' THEN NULL ELSE a.pdf_download_first_ts::date END AS pdf_download_first_date
            FROM silver.doctor_action_first_seen a
            WHERE {_normalized_sql('a.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_action}
        ),
        share_rep_id_email_map AS (
            SELECT DISTINCT ON ({_normalized_sql('s.field_rep_id::text')})
              {_normalized_sql('s.field_rep_id::text')} AS source_rep_id_key,
              s.field_rep_email AS mapped_email
            FROM silver.fact_share_log s
            WHERE {_normalized_sql('s.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_share}
              AND {_normalized_sql('s.field_rep_id::text')} <> ''
              AND {_normalized_sql('s.field_rep_email')} <> ''
            ORDER BY
              {_normalized_sql('s.field_rep_id::text')},
              COALESCE(s.updated_at_ts, s.created_at_ts, s.share_timestamp_ts) DESC NULLS LAST,
              s.id DESC
        ),
        tx_rep AS (
            SELECT DISTINCT ON (tx.brand_campaign_id, tx.collateral_id, tx.doctor_identity_key)
              tx.brand_campaign_id,
              tx.collateral_id,
              tx.doctor_identity_key,
              COALESCE(
                NULLIF(btrim(linked_share.field_rep_email), ''),
                NULLIF(btrim(rep_email_map.mapped_email), ''),
                NULLIF(btrim(tx.field_rep_master_id_resolved), ''),
                NULLIF(btrim(tx.brand_supplied_field_rep_id_resolved), ''),
                NULLIF(btrim(tx.field_rep_email), ''),
                tx.field_rep_id::text
              ) AS field_rep_id_resolved
            FROM silver.fact_collateral_transaction tx
            LEFT JOIN silver.fact_share_log linked_share
              ON {_normalized_sql('linked_share.brand_campaign_id')} = {_normalized_sql('tx.brand_campaign_id')}
             AND linked_share.collateral_id::text = tx.collateral_id::text
             AND linked_share.id::text = COALESCE(
                 NULLIF(btrim(tx.sm_engagement_id), ''),
                 NULLIF(btrim(tx.share_management_engagement_id), '')
             )
            LEFT JOIN share_rep_id_email_map rep_email_map
              ON rep_email_map.source_rep_id_key = {_normalized_sql('tx.field_rep_id')}
            WHERE {_normalized_sql('tx.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_tx}
              AND COALESCE(NULLIF(btrim(tx.field_rep_id), ''), NULL) IS NOT NULL
            ORDER BY tx.brand_campaign_id, tx.collateral_id, tx.doctor_identity_key, COALESCE(tx.updated_at_ts, tx.created_at_ts, tx.transaction_date_ts) DESC, tx.id DESC
        ),
        share_rep AS (
            SELECT DISTINCT ON (s.brand_campaign_id, s.collateral_id, s.doctor_identity_key)
              s.brand_campaign_id,
              s.collateral_id,
              s.doctor_identity_key,
              COALESCE(NULLIF(btrim(s.field_rep_email), ''), s.field_rep_id::text) AS field_rep_id_resolved
            FROM silver.fact_share_log s
            WHERE {_normalized_sql('s.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_share}
              AND COALESCE(NULLIF(btrim(s.field_rep_id::text), ''), NULL) IS NOT NULL
            ORDER BY s.brand_campaign_id, s.collateral_id, s.doctor_identity_key, COALESCE(s.updated_at_ts, s.created_at_ts, s.share_timestamp_ts) DESC, s.id DESC
        ),
        {bridge_base_sql},
        event_enriched AS (
            SELECT
              se.doctor_key,
              COALESCE(
                {_valid_state_sql('rb.state_normalized')},
                {_valid_state_sql('base.state_normalized')},
                {_valid_state_sql('rsc_tx.state_normalized')},
                {_valid_state_sql('rsc_share.state_normalized')},
                {_valid_state_sql('rsg_tx.state_normalized')},
                {_valid_state_sql('rsg_share.state_normalized')},
                {_valid_state_sql('fr_tx.state_normalized')},
                {_valid_state_sql('fr_share.state_normalized')},
                {_valid_state_sql('d.state_normalized')},
                'UNKNOWN'
              ) AS state_normalized,
              COALESCE(
                se.reached_first_date,
                se.opened_first_date,
                se.video_gt_50_first_date,
                se.pdf_download_first_date
              ) AS effective_reached_date,
              se.opened_first_date,
              se.video_gt_50_first_date,
              se.pdf_download_first_date
            FROM source_events se
            LEFT JOIN roster_base rb
              ON rb.doctor_key = se.doctor_key
            LEFT JOIN silver.dim_doctor d
              ON d.doctor_identity_key = se.doctor_identity_key
            LEFT JOIN bridge_base base
              ON {_normalized_sql('base.brand_campaign_id')} = {_normalized_sql('se.brand_campaign_id')}
             AND base.doctor_identity_key = se.doctor_identity_key
            LEFT JOIN tx_rep tx
              ON tx.brand_campaign_id = se.brand_campaign_id
             AND tx.collateral_id = se.collateral_id
             AND tx.doctor_identity_key = se.doctor_identity_key
            LEFT JOIN share_rep sr
              ON sr.brand_campaign_id = se.brand_campaign_id
             AND sr.collateral_id = se.collateral_id
             AND sr.doctor_identity_key = se.doctor_identity_key
            LEFT JOIN rep_state_campaign rsc_tx
              ON rsc_tx.rep_key = lower(regexp_replace(NULLIF(btrim(tx.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN rep_state_campaign rsc_share
              ON rsc_share.rep_key = lower(regexp_replace(NULLIF(btrim(sr.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN rep_state_global rsg_tx
              ON rsg_tx.rep_key = lower(regexp_replace(NULLIF(btrim(tx.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN rep_state_global rsg_share
              ON rsg_share.rep_key = lower(regexp_replace(NULLIF(btrim(sr.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN silver.dim_field_rep fr_tx
              ON lower(regexp_replace(COALESCE(NULLIF(btrim(fr_tx.source_field_rep_id), ''), btrim(fr_tx.id::text)), '[^a-zA-Z0-9]', '', 'g'))
               = lower(regexp_replace(NULLIF(btrim(tx.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
            LEFT JOIN silver.dim_field_rep fr_share
              ON lower(regexp_replace(COALESCE(NULLIF(btrim(fr_share.source_field_rep_id), ''), btrim(fr_share.id::text)), '[^a-zA-Z0-9]', '', 'g'))
               = lower(regexp_replace(NULLIF(btrim(sr.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
        ),
        denominator_base AS (
            SELECT doctor_key, state_normalized
            FROM roster_base
            UNION ALL
            SELECT ee.doctor_key, ee.state_normalized
            FROM event_enriched ee
            WHERE NOT EXISTS (
                SELECT 1 FROM roster_base rb WHERE rb.doctor_key = ee.doctor_key
            )
        ),
        state_universe AS (
            SELECT DISTINCT state_normalized FROM rep_state_campaign
            UNION
            SELECT DISTINCT state_normalized
            FROM denominator_base
            WHERE state_normalized IS NOT NULL
            UNION
            SELECT DISTINCT state_normalized
            FROM event_enriched
            WHERE state_normalized IS NOT NULL
        ),
        event_agg AS (
            SELECT
              state_normalized,
              COUNT(DISTINCT doctor_key) FILTER (
                WHERE effective_reached_date IS NOT NULL
                  AND effective_reached_date BETWEEN %s::date AND %s::date
              ) AS reached,
              COUNT(DISTINCT doctor_key) FILTER (
                WHERE opened_first_date IS NOT NULL
                  AND opened_first_date BETWEEN %s::date AND %s::date
              ) AS opened,
              COUNT(DISTINCT doctor_key) FILTER (
                WHERE (
                    video_gt_50_first_date IS NOT NULL
                    AND video_gt_50_first_date BETWEEN %s::date AND %s::date
                )
                OR (
                    pdf_download_first_date IS NOT NULL
                    AND pdf_download_first_date BETWEEN %s::date AND %s::date
                )
              ) AS consumed
            FROM event_enriched
            GROUP BY 1
        ),
        denominator_agg AS (
            SELECT
              state_normalized,
              COUNT(DISTINCT doctor_key) AS total_state
            FROM denominator_base
            GROUP BY 1
        )
        SELECT
          su.state_normalized,
          COALESCE(ea.reached, 0) AS reached,
          COALESCE(ea.opened, 0) AS opened,
          COALESCE(ea.consumed, 0) AS consumed,
          COALESCE(da.total_state, COALESCE(ea.reached, 0), 0) AS total_state
        FROM state_universe su
        LEFT JOIN event_agg ea ON ea.state_normalized = su.state_normalized
        LEFT JOIN denominator_agg da ON da.state_normalized = su.state_normalized
        ORDER BY
          CASE
            WHEN COALESCE(ea.reached,0)=0 OR COALESCE(da.total_state,0)=0 THEN 0
            ELSE ((LEAST((COALESCE(ea.reached,0) / NULLIF((COALESCE(da.total_state,0) / 4.0),0)),1.0)
              + (COALESCE(ea.opened,0) / NULLIF(COALESCE(ea.reached,0),0))
              + (COALESCE(ea.consumed,0) / NULLIF(COALESCE(ea.opened,0),0))) / 3.0) * 100
          END ASC,
          su.state_normalized ASC
        """,
        [
            selected_campaign,
            *brand_keys,
            *brand_keys,
            _normalize_lookup_key(selected_campaign),
            *brand_keys,
            *current_collateral_ids,
            *brand_keys,
            *current_collateral_ids,
            *brand_keys,
            *current_collateral_ids,
            *brand_keys,
            *current_collateral_ids,
            *bridge_params,
            latest_week.get("week_start_date"),
            latest_week.get("week_end_date"),
            latest_week.get("week_start_date"),
            latest_week.get("week_end_date"),
            latest_week.get("week_start_date"),
            latest_week.get("week_end_date"),
            latest_week.get("week_start_date"),
            latest_week.get("week_end_date"),
        ],
        timeout_ms=12000,
    )


def _build_media_logo_url(company_logo_path: Any) -> str | None:
    raw = str(company_logo_path or "").strip()
    if not raw or raw.lower() == "null":
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://inclinic.inditech.co.in/media/{raw.lstrip('/')}"


def _format_field_rep_summary(field_rep_insights: list[dict[str, Any]], total_doctors: int = 0) -> dict[str, int]:
    assigned_rep_rows = [row for row in field_rep_insights if not row.get("is_unmapped_activity")]
    return {
        "total_reps": len(assigned_rep_rows),
        "total_doctors_assigned": sum(_to_int(row.get("total_doctors_assigned")) for row in field_rep_insights),
        "doctors_sent": sum(_to_int(row.get("doctors_sent")) for row in field_rep_insights),
        "doctors_viewed": sum(_to_int(row.get("doctors_viewed")) for row in field_rep_insights),
        "doctors_video_played": sum(_to_int(row.get("doctors_video_played")) for row in field_rep_insights),
        "doctors_pdf_downloaded": sum(_to_int(row.get("doctors_pdf_downloaded")) for row in field_rep_insights),
        "assignment_issue_count": sum(1 for row in field_rep_insights if row.get("assignment_note")),
    }


def _safe_filename_part(value: Any, fallback: str = "download") -> str:
    text = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value or "").strip()).strip("_")
    return text or fallback


def _export_filename(prefix: str, context: dict[str, Any], extension: str, extra: Any = "") -> str:
    campaign = _safe_filename_part(context.get("selected_campaign"), "campaign")
    week = context.get("selected_week")
    suffix_parts = [campaign]
    if extra:
        suffix_parts.append(_safe_filename_part(extra, "detail"))
    suffix_parts.append(f"week_{week}" if week else "all_weeks")
    timestamp = datetime.now().strftime("%Y%m%d")
    return f"{_safe_filename_part(prefix)}_{'_'.join(suffix_parts)}_{timestamp}.{extension.lstrip('.')}"


def _json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _excel_cell(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _excel_table(headers: list[str], rows: list[list[Any]]) -> str:
    header_html = "".join(f"<th>{_excel_cell(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{_excel_cell(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f'<table border="1"><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>'


def _excel_response(filename: str, sections: list[tuple[str, list[str], list[list[Any]]]]) -> HttpResponse:
    section_html = []
    for title, headers, rows in sections:
        section_html.append(f"<h2>{_excel_cell(title)}</h2>")
        section_html.append(_excel_table(headers, rows))
        section_html.append("<br>")
    workbook = (
        "<html><head><meta charset=\"UTF-8\"></head><body>"
        + "".join(section_html)
        + "</body></html>"
    )
    response = HttpResponse(workbook, content_type="application/vnd.ms-excel; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


FIELD_REP_SUMMARY_EXPORT_HEADERS = [
    "Field Rep ID",
    "Field Representative",
    "State",
    "Doctors Assigned",
    "Collateral Sent",
    "Viewed",
    "Video Played",
    "PDF / Collateral Saved",
]


FIELD_REP_DOCTOR_EXPORT_HEADERS = [
    "Field Rep ID",
    "Field Representative",
    "State",
    "Metric",
    "S. No.",
    "Doctor Name",
    "Doctor Number",
    "Doctor Key",
]


FIELD_REP_DOCTOR_METRICS = [
    ("Doctors Assigned", "assigned_doctors_json"),
    ("Collateral Sent", "sent_doctors_json"),
    ("Viewed", "viewed_doctors_json"),
    ("Video Played", "video_doctors_json"),
    ("PDF / Collateral Saved", "pdf_doctors_json"),
]


FIELD_REP_DOCTOR_METRIC_KEYS = {
    "assigned": ("Doctors Assigned", "assigned_doctors_json"),
    "sent": ("Collateral Sent", "sent_doctors_json"),
    "viewed": ("Viewed", "viewed_doctors_json"),
    "video": ("Video Played", "video_doctors_json"),
    "pdf": ("PDF / Collateral Saved", "pdf_doctors_json"),
}


def _field_rep_detail_url(campaign_id: str, week_filter: int | None = None, collateral_id: str | None = None) -> str:
    url = reverse("campaign-field-rep-insights-detail", kwargs={"brand_campaign_id": campaign_id})
    params = {}
    if week_filter:
        params["week"] = str(week_filter)
    if collateral_id:
        params["collateral_id"] = str(collateral_id)
    return f"{url}?{urlencode(params)}" if params else url


def _field_rep_doctor_detail_payload(
    context: dict[str, Any],
    rep_id: str,
    metric_key: str,
) -> tuple[dict[str, Any], int]:
    metric_label, json_key = FIELD_REP_DOCTOR_METRIC_KEYS.get(metric_key, FIELD_REP_DOCTOR_METRIC_KEYS["assigned"])
    normalized_rep_id = str(rep_id or "").strip()
    for row in context.get("field_rep_insights") or []:
        if str(row.get("field_rep_id") or "").strip() != normalized_rep_id:
            continue
        doctors = _json_list(row.get(json_key))
        return (
            {
                "field_rep_id": row.get("field_rep_id", ""),
                "field_rep_name": row.get("field_rep_name", ""),
                "state": row.get("state_normalized", "UNKNOWN") or "UNKNOWN",
                "metric_key": metric_key,
                "metric_label": metric_label,
                "doctor_count": len(doctors),
                "doctors": doctors,
            },
            200,
        )
    return (
        {
            "field_rep_id": normalized_rep_id,
            "metric_key": metric_key,
            "metric_label": metric_label,
            "doctor_count": 0,
            "doctors": [],
            "error": "Field representative was not found for this campaign.",
        },
        404,
    )


def _field_rep_summary_export_rows(field_rep_insights: list[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            row.get("field_rep_id", ""),
            row.get("field_rep_name", ""),
            row.get("state_normalized", "UNKNOWN") or "UNKNOWN",
            _to_int(row.get("total_doctors_assigned")),
            _to_int(row.get("doctors_sent")),
            _to_int(row.get("doctors_viewed")),
            _to_int(row.get("doctors_video_played")),
            _to_int(row.get("doctors_pdf_downloaded")),
        ]
        for row in field_rep_insights
    ]


def _field_rep_doctor_detail_export_rows(field_rep_insights: list[dict[str, Any]]) -> list[list[Any]]:
    detail_rows: list[list[Any]] = []
    for row in field_rep_insights:
        for metric_label, json_key in FIELD_REP_DOCTOR_METRICS:
            for index, doctor in enumerate(_json_list(row.get(json_key)), start=1):
                detail_rows.append(
                    [
                        row.get("field_rep_id", ""),
                        row.get("field_rep_name", ""),
                        row.get("state_normalized", "UNKNOWN") or "UNKNOWN",
                        metric_label,
                        index,
                        doctor.get("name", ""),
                        doctor.get("phone", ""),
                        doctor.get("doctor_key", ""),
                    ]
                )
    return detail_rows


def _field_rep_insights_excel_response(
    context: dict[str, Any],
    filename_prefix: str = "field_rep_insights",
    filename_extra: Any = "",
) -> HttpResponse:
    field_rep_insights = context.get("field_rep_insights") or []
    filename = _export_filename(filename_prefix, context, "xls", filename_extra)
    return _excel_response(
        filename,
        [
            (
                "Field Representative Summary",
                FIELD_REP_SUMMARY_EXPORT_HEADERS,
                _field_rep_summary_export_rows(field_rep_insights),
            ),
            (
                "Doctor Details",
                FIELD_REP_DOCTOR_EXPORT_HEADERS,
                _field_rep_doctor_detail_export_rows(field_rep_insights),
            ),
        ],
    )


def _is_missing_doctor_name(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"", "unknown doctor", "unknown", "null", "none", "-"}


def _manual_mapping_export_rows(field_rep_insights: list[dict[str, Any]]) -> list[list[Any]]:
    assigned_keys_by_rep: dict[str, set[str]] = {}
    detail_records: list[dict[str, Any]] = []
    for row in field_rep_insights:
        rep_id = str(row.get("field_rep_id") or "")
        rep_name = str(row.get("field_rep_name") or "")
        for metric_label, json_key in FIELD_REP_DOCTOR_METRICS:
            for doctor in _json_list(row.get(json_key)):
                doctor_key = str(doctor.get("doctor_key") or "")
                doctor_phone = str(doctor.get("phone") or "")
                effective_key = doctor_key or doctor_phone
                record = {
                    "rep_id": rep_id,
                    "rep_name": rep_name,
                    "metric": metric_label,
                    "doctor_name": doctor.get("name", ""),
                    "doctor_phone": doctor_phone,
                    "doctor_key": doctor_key,
                    "effective_key": effective_key,
                    "source_field_rep_id": doctor.get("source_field_rep_id", ""),
                    "source_field_rep_email": doctor.get("source_field_rep_email", ""),
                    "source_brand_rep_id": doctor.get("source_brand_rep_id", ""),
                    "evidence_source": doctor.get("evidence_source", ""),
                }
                detail_records.append(record)
                if metric_label == "Doctors Assigned" and effective_key:
                    assigned_keys_by_rep.setdefault(rep_id, set()).add(effective_key)

    correction_rows_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for record in detail_records:
        rep_id = record["rep_id"]
        doctor_key = record["doctor_key"]
        doctor_phone = record["doctor_phone"]
        effective_key = record["effective_key"]
        assigned_keys = assigned_keys_by_rep.get(rep_id, set())
        is_unmapped_rep = rep_id in {"UNMAPPED_ACTIVITY", UNMAPPED_ACTIVITY_FIELD_REP_ID}
        is_activity_metric = record["metric"] != "Doctors Assigned"
        is_outside_roster = is_activity_metric and not is_unmapped_rep and bool(effective_key) and effective_key not in assigned_keys
        is_unknown_name = _is_missing_doctor_name(record["doctor_name"])
        if not is_unmapped_rep and not is_unknown_name and not is_outside_roster:
            continue

        issue = "Doctor name missing or unknown"
        if is_unmapped_rep:
            issue = "Unmapped or ambiguous field-rep activity"
        elif is_outside_roster:
            issue = "Activity doctor is not in assigned roster for this rep"

        key = (rep_id, doctor_key, doctor_phone, issue)
        existing = correction_rows_by_key.setdefault(
            key,
            {
                "issue": issue,
                "rep_id": rep_id,
                "rep_name": record["rep_name"],
                "metrics": set(),
                "doctor_name": record["doctor_name"],
                "doctor_phone": doctor_phone,
                "doctor_key": doctor_key,
                "source_field_rep_id": record.get("source_field_rep_id", ""),
                "source_field_rep_email": record.get("source_field_rep_email", ""),
                "source_brand_rep_id": record.get("source_brand_rep_id", ""),
                "evidence_source": record.get("evidence_source", ""),
            },
        )
        existing["metrics"].add(record["metric"])

    return [
        [
            index,
            row["issue"],
            row["rep_id"],
            row["rep_name"],
            ", ".join(sorted(row["metrics"])),
            row["doctor_name"],
            row["doctor_phone"],
            row["doctor_key"],
            row.get("source_field_rep_id", ""),
            row.get("source_field_rep_email", ""),
            row.get("source_brand_rep_id", ""),
            row.get("evidence_source", ""),
            "",
            "",
            "",
            row["doctor_phone"],
        ]
        for index, row in enumerate(correction_rows_by_key.values(), start=1)
    ]


def _manual_mapping_excel_response(context: dict[str, Any]) -> HttpResponse:
    filename = _export_filename("doctors_requiring_manual_mapping", context, "xls")
    return _excel_response(
        filename,
        [
            (
                "Doctors Requiring Manual Mapping",
                [
                    "S. No.",
                    "Issue",
                    "Current Field Rep ID",
                    "Current Field Representative",
                    "Metric(s)",
                    "Current Doctor Name",
                    "Current Doctor Number",
                    "Doctor Key",
                    "Source Field Rep ID",
                    "Source Field Rep Email",
                    "Source Brand Supplied Field Rep ID",
                    "Source Evidence Table",
                    "Correct Field Rep Brand Supplied ID",
                    "Correct Field Rep Name / Email",
                    "Correct Doctor Name",
                    "Correct Doctor Number",
                ],
                _manual_mapping_export_rows(context.get("field_rep_insights") or []),
            )
        ],
    )


def _pdf_escape(value: Any) -> str:
    text = str(value if value is not None else "")
    text = text.encode("latin-1", "replace").decode("latin-1")
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_pages(lines: list[str], title: str, width: int = 92, max_lines: int = 52) -> list[list[str]]:
    wrapped: list[str] = []
    for line in lines:
        raw = str(line if line is not None else "")
        if not raw:
            wrapped.append("")
            continue
        wrapped.extend(textwrap.wrap(raw, width=width, replace_whitespace=True, drop_whitespace=True) or [""])

    pages: list[list[str]] = []
    current: list[str] = []
    for line in wrapped:
        if len(current) >= max_lines:
            pages.append(current)
            current = []
        current.append(line)
    if current or not pages:
        pages.append(current)
    return [[title, "", *page] for page in pages]


def _build_pdf_bytes(title: str, lines: list[str]) -> bytes:
    pages = _pdf_pages(lines, title)
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    page_refs: list[str] = []
    for page in pages:
        page_obj_id = len(objects) + 1
        content_obj_id = page_obj_id + 1
        page_refs.append(f"{page_obj_id} 0 R")

        content_lines = ["BT", "/F1 15 Tf", "50 805 Td"]
        first_line = True
        for line_index, line in enumerate(page):
            if not first_line:
                content_lines.append("0 -14 Td")
            if line_index == 2:
                content_lines.append("/F1 10 Tf")
            content_lines.append(f"({_pdf_escape(line)}) Tj")
            first_line = False
        content_lines.append("ET")
        content = "\n".join(content_lines).encode("latin-1", "replace")
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_obj_id} 0 R >>"
        ).encode("latin-1")
        content_obj = b"<< /Length " + str(len(content)).encode("ascii") + b" >>\nstream\n" + content + b"\nendstream"
        objects.extend([page_obj, content_obj])

    objects[1] = f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(page_refs)} >>".encode("latin-1")

    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("ascii")
    )
    return bytes(pdf)


def _pdf_response(filename: str, title: str, lines: list[str]) -> HttpResponse:
    response = HttpResponse(_build_pdf_bytes(title, lines), content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def _campaign_pdf_lines(context: dict[str, Any]) -> list[str]:
    lines = [
        f"Brand: {context.get('brand_name') or ''}",
        f"Campaign: {context.get('selected_campaign') or ''}",
        f"Collateral: {context.get('collateral_name') or ''}",
        f"Schedule: {context.get('schedule_text') or ''}",
        f"Period: {context.get('week_of') or 'All Weeks'}",
        "",
        "Key Metrics",
        f"Campaign Health: {context.get('campaign_health', 0)}/100",
        f"Weekly Campaign Health: {context.get('weekly_health', 0)}/100",
        f"Doctors Reached (Unique): {context.get('kpi_reached', 0)} ({context.get('kpi_reached_pct', 0)}%)",
        f"Doctors Opened (Unique): {context.get('kpi_opened', 0)} ({context.get('kpi_opened_pct', 0)}%)",
        f"Video Viewed (Unique): {context.get('kpi_video', 0)} (>50% viewed: {context.get('kpi_video_pct', 0)}%)",
        f"PDF Downloads (Unique): {context.get('kpi_pdf', 0)} ({context.get('kpi_pdf_pct', 0)}%)",
        "",
        "Field Representative Summary",
    ]
    summary = context.get("field_rep_summary") or {}
    lines.extend(
        [
            f"Field Reps: {summary.get('total_reps', 0)}",
            f"Doctors Assigned: {summary.get('total_doctors_assigned', 0)}",
            f"Collateral Sent: {summary.get('doctors_sent', 0)}",
            f"Viewed: {summary.get('doctors_viewed', 0)}",
            f"Video Played: {summary.get('doctors_video_played', 0)}",
            f"PDF / Collateral Saved: {summary.get('doctors_pdf_downloaded', 0)}",
            "",
            "Field Rep Breakdown",
            "Field Rep ID | Name | State | Assigned | Sent | Viewed | Video | Saved",
        ]
    )
    for row in context.get("field_rep_insights") or []:
        lines.append(
            " | ".join(
                [
                    str(row.get("field_rep_id", "")),
                    str(row.get("field_rep_name", "")),
                    str(row.get("state_normalized", "UNKNOWN") or "UNKNOWN"),
                    str(_to_int(row.get("total_doctors_assigned"))),
                    str(_to_int(row.get("doctors_sent"))),
                    str(_to_int(row.get("doctors_viewed"))),
                    str(_to_int(row.get("doctors_video_played"))),
                    str(_to_int(row.get("doctors_pdf_downloaded"))),
                ]
            )
        )
    if context.get("error_message"):
        lines.extend(["", f"Data issue: {context.get('error_message')}"])
    return lines


def _collateral_display_name(row: dict[str, Any], fallback: str = "Collateral") -> str:
    return _clean_display_text(row.get("collateral_title")) or _clean_display_text(row.get("campaign_name")) or fallback


def _collateral_display_start(row: dict[str, Any]) -> Any:
    return row.get("campaign_start_date") or row.get("schedule_start_date")


def _collateral_display_end(row: dict[str, Any]) -> Any:
    return row.get("campaign_end_date") or row.get("schedule_end_date")


def _collateral_status_label(row: dict[str, Any]) -> str:
    start = _parse_schedule_date(_collateral_display_start(row))
    end = _parse_schedule_date(_collateral_display_end(row))
    today = datetime.now().date()
    if start and start > today:
        return "Upcoming"
    if end and end < today:
        return "Past"
    return "Current"


def _format_collateral_options(
    schedule_rows: list[dict[str, Any]],
    selected_campaign: str,
    current_collateral_id: str | None,
    selected_week: int | None = None,
) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in schedule_rows:
        collateral_id = str(row.get("collateral_id") or "").strip()
        if not collateral_id or collateral_id in seen:
            continue
        status_label = _collateral_status_label(row)
        if status_label == "Upcoming":
            continue
        seen.add(collateral_id)
        start = _format_schedule_date(_collateral_display_start(row))
        end = _format_schedule_date(_collateral_display_end(row))
        params = {"collateral_id": collateral_id}
        if selected_week:
            params["week"] = str(selected_week)
        options.append(
            {
                "collateral_id": collateral_id,
                "name": _collateral_display_name(row, f"Collateral {collateral_id}"),
                "schedule_text": f"{start} - {end}" if start and end else "Schedule unavailable",
                "url": f"{reverse('campaign-overview-specific', kwargs={'brand_campaign_id': selected_campaign})}?{urlencode(params)}",
                "status_label": "Selected" if collateral_id == str(current_collateral_id or "") else status_label,
                "is_selected": "true" if collateral_id == str(current_collateral_id or "") else "false",
            }
        )
    status_order = {"Selected": -1, "Current": 0, "Past": 1}
    return sorted(options, key=lambda item: (status_order.get(item["status_label"], 9), item["name"].lower()))


def _has_inclinic_campaign_access(request: HttpRequest, normalized_campaign_id: str) -> bool:
    access = build_report_access("inclinic", normalized_campaign_id)
    return bool(request.session.get(access.session_key) or request.session.get(f"auth_{normalized_campaign_id}"))


def _campaign_list() -> list[dict[str, Any]]:
    """Return campaign list for menu page.

    If ETL/GOLD tables are not created yet (fresh deploy), return an empty list
    instead of crashing the entire dashboard route.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('gold_global.campaign_registry')")
            registry_exists = cursor.fetchone()[0] is not None
        if not registry_exists:
            return []

        return _fetch_dicts(
            """
            WITH campaign_candidates AS (
                SELECT
                  r.brand_campaign_id,
                  r.gold_schema_name,
                  MIN(
                    CASE
                        WHEN cm.name IS NULL OR btrim(cm.name) = '' OR lower(btrim(cm.name)) = 'null'
                        THEN NULL
                        ELSE cm.name
                    END
                  ) AS cm_campaign_name,
                  MIN(
                    CASE
                        WHEN cc.name IS NULL OR btrim(cc.name) = '' OR lower(btrim(cc.name)) = 'null'
                        THEN NULL
                        ELSE cc.name
                    END
                  ) AS cc_campaign_name
                FROM gold_global.campaign_registry r
                LEFT JOIN silver.map_brand_campaign_to_campaign m ON m.brand_campaign_id = r.brand_campaign_id
                LEFT JOIN bronze.campaign_campaign cc
                  ON cc.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                LEFT JOIN bronze.campaign_management_campaign cm
                  ON regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(r.brand_campaign_id)), '-', '', 'g')
                  OR cm.id::text = btrim(r.brand_campaign_id)
                  OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                GROUP BY r.brand_campaign_id, r.gold_schema_name
            )
            SELECT
              brand_campaign_id,
              gold_schema_name,
              COALESCE(cm_campaign_name, cc_campaign_name) AS campaign_name
            FROM campaign_candidates
            WHERE COALESCE(cm_campaign_name, cc_campaign_name) IS NOT NULL
              AND lower(COALESCE(cm_campaign_name, cc_campaign_name)) NOT LIKE 'test%'
              AND lower(COALESCE(cm_campaign_name, cc_campaign_name)) NOT LIKE '%dummy%'
            ORDER BY COALESCE(cm_campaign_name, cc_campaign_name)
            """
        )
    except (ProgrammingError, OperationalError):
        return []


def _campaign_performance_link_rows(request: HttpRequest) -> list[dict[str, Any]]:
    try:
        source_rows: list[dict[str, Any]] = []
        if _table_exists("raw_sapa_mysql", "campaign_campaign_raw"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      id::text AS campaign_id,
                      COALESCE(NULLIF(btrim(name), ''), id::text) AS campaign_name,
                      CASE WHEN lower(COALESCE(system_rfa, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_rfa,
                      CASE WHEN lower(COALESCE(system_ic, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_ic,
                      CASE WHEN lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_pe,
                      NULLIF(btrim(brand_manager_login_link), '') AS brand_manager_login_link,
                      0 AS source_priority
                    FROM raw_sapa_mysql.campaign_campaign_raw
                    WHERE
                      lower(COALESCE(system_rfa, '')) IN ('1', 'true', 't', 'yes')
                      OR lower(COALESCE(system_ic, '')) IN ('1', 'true', 't', 'yes')
                      OR lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes')
                    """
                )
            )
        if _table_exists("bronze", "campaign_campaign"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      id::text AS campaign_id,
                      COALESCE(NULLIF(btrim(name), ''), id::text) AS campaign_name,
                      CASE WHEN lower(COALESCE(system_rfa, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_rfa,
                      CASE WHEN lower(COALESCE(system_ic, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_ic,
                      CASE WHEN lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_pe,
                      NULLIF(btrim(brand_manager_login_link), '') AS brand_manager_login_link,
                      10 AS source_priority
                    FROM bronze.campaign_campaign
                    WHERE
                      lower(COALESCE(system_rfa, '')) IN ('1', 'true', 't', 'yes')
                      OR lower(COALESCE(system_ic, '')) IN ('1', 'true', 't', 'yes')
                      OR lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes')
                    """
                )
            )
        if _table_exists("raw_pe_master", "campaign_campaign_raw"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      id::text AS campaign_id,
                      COALESCE(NULLIF(btrim(name), ''), id::text) AS campaign_name,
                      FALSE AS system_rfa,
                      FALSE AS system_ic,
                      CASE WHEN lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_pe,
                      ''::text AS brand_manager_login_link,
                      20 AS source_priority
                    FROM raw_pe_master.campaign_campaign_raw
                    WHERE lower(COALESCE(system_pe, '')) IN ('1', 'true', 't', 'yes')
                    """
                )
            )
        if _table_exists("gold_global", "campaign_registry"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      COALESCE(NULLIF(btrim(campaign_id_resolved), ''), brand_campaign_id)::text AS campaign_id,
                      COALESCE(NULLIF(btrim(campaign_id_resolved), ''), brand_campaign_id)::text AS campaign_name,
                      FALSE AS system_rfa,
                      TRUE AS system_ic,
                      FALSE AS system_pe,
                      ''::text AS brand_manager_login_link,
                      30 AS source_priority
                    FROM gold_global.campaign_registry
                    """
                )
            )
        if _table_exists("gold_pe_global", "campaign_registry"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      COALESCE(NULLIF(btrim(campaign_id_original), ''), campaign_id_normalized)::text AS campaign_id,
                      COALESCE(NULLIF(btrim(campaign_name), ''), campaign_id_original, campaign_id_normalized)::text AS campaign_name,
                      FALSE AS system_rfa,
                      FALSE AS system_ic,
                      TRUE AS system_pe,
                      ''::text AS brand_manager_login_link,
                      40 AS source_priority
                    FROM gold_pe_global.campaign_registry
                    """
                )
            )
        if _table_exists("gold_sapa_global", "campaign_registry"):
            source_rows.extend(
                _fetch_dicts(
                    """
                    SELECT
                      campaign_key::text AS campaign_id,
                      COALESCE(NULLIF(btrim(campaign_label), ''), campaign_key)::text AS campaign_name,
                      TRUE AS system_rfa,
                      FALSE AS system_ic,
                      FALSE AS system_pe,
                      ''::text AS brand_manager_login_link,
                      50 AS source_priority
                    FROM gold_sapa_global.campaign_registry
                    """
                )
            )
        if not source_rows:
            return []
    except (ProgrammingError, OperationalError):
        return []

    rows_by_key: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        campaign_id = _normalize_campaign_id(row.get("campaign_id"))
        lookup_key = _normalize_lookup_key(campaign_id)
        if not lookup_key:
            continue
        current = rows_by_key.get(lookup_key)
        row_priority = _to_int(row.get("source_priority"), 100)
        if current is None or row_priority < _to_int(current.get("source_priority"), 100):
            current = dict(row)
            current["campaign_id"] = campaign_id
            rows_by_key[lookup_key] = current
        else:
            current["system_rfa"] = bool(current.get("system_rfa") or row.get("system_rfa"))
            current["system_ic"] = bool(current.get("system_ic") or row.get("system_ic"))
            current["system_pe"] = bool(current.get("system_pe") or row.get("system_pe"))
            if not current.get("brand_manager_login_link") and row.get("brand_manager_login_link"):
                current["brand_manager_login_link"] = row.get("brand_manager_login_link")

    output = []
    for row in sorted(rows_by_key.values(), key=lambda item: ((item.get("campaign_name") or item.get("campaign_id") or "").lower(), item.get("campaign_id") or "")):
        campaign_id = _normalize_campaign_id(row.get("campaign_id"))
        try:
            reference = _resolve_campaign_reference(campaign_id)
        except CampaignPerformanceNotFound:
            reference = None
        system_keys = _configured_system_keys(reference) if reference else []
        if not system_keys:
            if row.get("system_rfa"):
                system_keys.append("rfa")
            if row.get("system_ic"):
                system_keys.append("in_clinic")
            if row.get("system_pe"):
                system_keys.append("patient_education")
        systems = []
        for key in system_keys:
            if key == "rfa":
                systems.append("RFA")
            elif key == "in_clinic":
                systems.append("InClinic")
            elif key == "patient_education":
                systems.append("PE")

        system_report_links = []
        brand_campaign_id = str(reference.brand_campaign_id if reference else "").strip()
        if "in_clinic" in system_keys:
            path = _system_report_path("in_clinic", reference) if reference else ""
            in_clinic_report_url = absolute_url(request, path) if path else ""
            system_report_links.append(
                {
                    "label": "InClinic Report",
                    "url": in_clinic_report_url,
                    "status": "Not mapped" if not in_clinic_report_url else "",
                }
            )
        if "patient_education" in system_keys:
            path = _system_report_path("patient_education", reference) if reference else ""
            pe_report_url = absolute_url(request, path) if path else ""
            system_report_links.append(
                {
                    "label": "PE Report",
                    "url": pe_report_url,
                    "status": "Not mapped" if not pe_report_url else "",
                }
            )
        if "rfa" in system_keys:
            path = _system_report_path("rfa", reference) if reference else f"/sapa-growth/campaign/{campaign_id}/"
            rfa_report_url = absolute_url(request, path) if path else ""
            system_report_links.append(
                {
                    "label": "RFA Report",
                    "url": rfa_report_url,
                    "status": "Not mapped" if not rfa_report_url else "",
                }
            )

        output.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": row.get("campaign_name") or campaign_id,
                "brand_campaign_id": brand_campaign_id,
                "selected_systems": systems,
                "selected_systems_label": ", ".join(systems) if systems else "-",
                "performance_page_url": absolute_url(request, f"/campaign-performance/{campaign_id}/"),
                "performance_api_url": absolute_url(request, f"/reporting/api/campaign-performance/{campaign_id}/"),
                "legacy_brand_route_url": absolute_url(request, f"/campaign/{brand_campaign_id}/performance/") if brand_campaign_id else "",
                "system_report_links": system_report_links,
                "brand_manager_login_link": row.get("brand_manager_login_link") or "",
            }
        )
    return output



def _table_exists(schema: str, table: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", [f"{schema}.{table}"])
        return cursor.fetchone()[0] is not None


def _table_count(schema: str, table: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
        return int(cursor.fetchone()[0])


def _build_debug_snapshot() -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "layers": [],
        "latest_run": None,
        "errors": [],
    }

    try:
        layer_specs = {
            "raw_server1": list(SOURCE_TABLE_SPECS.get("mysql_server_1", {}).keys()),
            "raw_server2": list(SOURCE_TABLE_SPECS.get("mysql_server_2", {}).keys()),
            "bronze": list(SOURCE_TABLE_SPECS.get("mysql_server_1", {}).keys()) + list(SOURCE_TABLE_SPECS.get("mysql_server_2", {}).keys()),
            "silver": [
                "dim_field_rep",
                "dim_doctor",
                "dim_collateral",
                "bridge_campaign_collateral_schedule",
                "fact_collateral_transaction",
                "map_brand_campaign_to_campaign",
                "bridge_brand_campaign_doctor_base",
                "doctor_action_first_seen",
            ],
            "gold_global": [
                "campaign_registry",
                "campaign_health_history",
                "benchmark_last_10_campaigns",
            ],
            "control": ["etl_run_log"],
        }

        for schema, tables in layer_specs.items():
            schema_rows = []
            for table in tables:
                try:
                    exists = _table_exists(schema, table)
                    row_count = _table_count(schema, table) if exists else 0
                    schema_rows.append({
                        "table": table,
                        "exists": exists,
                        "row_count": row_count,
                    })
                except Exception as exc:
                    schema_rows.append({
                        "table": table,
                        "exists": False,
                        "row_count": 0,
                        "error": str(exc),
                    })
                    snapshot["errors"].append(f"{schema}.{table}: {exc}")

            snapshot["layers"].append({"schema": schema, "tables": schema_rows})

        # Count campaign schemas for quick GOLD visibility
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.schemata
                WHERE schema_name LIKE 'gold_campaign_%'
            """)
            snapshot["gold_campaign_schema_count"] = int(cursor.fetchone()[0])

        # Latest ETL run metadata
        if _table_exists("control", "etl_run_log"):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id, started_at, ended_at, status, notes
                    FROM control.etl_run_log
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
                if row:
                    parsed_notes = None
                    notes_errors = []
                    notes_value = row[4]
                    if notes_value:
                        try:
                            parsed_notes = json.loads(notes_value)
                            notes_errors = list((parsed_notes.get("errors") or {}).items())[:20]
                        except (TypeError, ValueError):
                            parsed_notes = None

                    snapshot["latest_run"] = {
                        "run_id": row[0],
                        "started_at": row[1],
                        "ended_at": row[2],
                        "status": row[3],
                        "notes": notes_value,
                        "notes_summary": (parsed_notes or {}).get("summary"),
                        "notes_errors": notes_errors,
                    }

        # Per-campaign GOLD table diagnostics
        snapshot["campaign_schema_tables"] = []
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name LIKE 'gold_campaign_%'
                ORDER BY schema_name
                """
            )
            campaign_schemas = [r[0] for r in cursor.fetchall()]

        for schema_name in campaign_schemas:
            tables = []
            for table_name in ["fact_doctor_collateral_latest", "kpi_weekly_summary", "weekly_action_items"]:
                try:
                    exists = _table_exists(schema_name, table_name)
                    row_count = _table_count(schema_name, table_name) if exists else 0
                    tables.append({
                        "table": table_name,
                        "exists": exists,
                        "row_count": row_count,
                    })
                except Exception as exc:
                    tables.append({
                        "table": table_name,
                        "exists": False,
                        "row_count": 0,
                        "error": str(exc),
                    })
                    snapshot["errors"].append(f"{schema_name}.{table_name}: {exc}")

            snapshot["campaign_schema_tables"].append({
                "schema": schema_name,
                "tables": tables,
            })

    except Exception as exc:
        snapshot["errors"].append(str(exc))

    return snapshot

def menu_page(request: HttpRequest) -> HttpResponse:
    campaigns = []
    for row in _campaign_list():
        item = dict(row)
        normalized_campaign_id = _normalize_campaign_id(item.get("brand_campaign_id"))
        item["login_href"] = f"/campaign/{normalized_campaign_id}/login/"
        item["access_href"] = f"/campaign/{normalized_campaign_id}/access/"
        item["email_href"] = f"/campaign/{normalized_campaign_id}/send-access-email/"
        campaigns.append(item)
    return render(request, "dashboard/menu.html", {"campaigns": campaigns})


def reports_home(request: HttpRequest) -> HttpResponse:
    report_cards = [
        {
            "label": "In-Clinic Sharing",
            "description": "Campaign menu, secure login flow, access email history, and the legacy collateral dashboard.",
            "href": "/inclinic/",
        },
        {
            "label": "Patient Education",
            "description": "Campaign launcher, PE login flow, filter-aware dashboards, and campaign access email management.",
            "href": "/pe-reports/",
        },
        {
            "label": "SAPA Growth Clinic",
            "description": "Campaign launcher with separate secure login, access email management, and RFA campaign dashboards.",
            "href": "/sapa-growth/",
        },
        {
            "label": "Campaign Performance Links",
            "description": "Copy campaign-performance page and API URLs so you can embed them into your main Brand Manager system.",
            "href": "/campaign-performance/links/",
        },
    ]
    return render(request, "dashboard/home.html", {"report_cards": report_cards})


def etl_debug_page(request: HttpRequest) -> HttpResponse:
    debug_snapshot = _build_debug_snapshot()
    return render(request, "dashboard/debug.html", {"debug_snapshot": debug_snapshot})


def campaign_performance_links_page(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "dashboard/campaign_performance_links.html",
        {"link_rows": _campaign_performance_link_rows(request)},
    )


def campaign_performance_page(
    request: HttpRequest,
    campaign_id: str | None = None,
    brand_campaign_id: str | None = None,
) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(campaign_id or brand_campaign_id)
    campaigns = {_normalize_campaign_id(c["brand_campaign_id"]): c for c in _campaign_list()}
    campaign = campaigns.get(normalized_campaign_id, {"brand_campaign_id": normalized_campaign_id, "campaign_name": normalized_campaign_id})
    return render(
        request,
        "dashboard/campaign_performance.html",
        {
            "campaign": campaign,
            "campaign_id": normalized_campaign_id,
            "campaign_name": campaign.get("campaign_name") or normalized_campaign_id,
            "performance_api_url": f"/reporting/api/campaign-performance-page/{normalized_campaign_id}/",
        },
    )


def campaign_login(request: HttpRequest, brand_campaign_id: str) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    campaigns = {_normalize_campaign_id(c["brand_campaign_id"]): c for c in _campaign_list()}
    campaign = campaigns.get(normalized_campaign_id)
    if not campaign:
        return redirect("menu")

    access = build_report_access("inclinic", normalized_campaign_id)
    if request.session.get(access.session_key) or request.session.get(f"auth_{normalized_campaign_id}"):
        return redirect("campaign-overview-specific", brand_campaign_id=normalized_campaign_id)

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        if validate_credentials("inclinic", normalized_campaign_id, username, password):
            authenticate_session(request, "inclinic", normalized_campaign_id)
            request.session[f"auth_{normalized_campaign_id}"] = True
            return redirect("campaign-overview-specific", brand_campaign_id=normalized_campaign_id)
        error_message = "Invalid brand credentials"

    return render(
        request,
        "dashboard/login.html",
        {
            "campaign": campaign,
            "error_message": error_message,
        },
    )


def campaign_access_page(request: HttpRequest, brand_campaign_id: str) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    campaigns = {_normalize_campaign_id(c["brand_campaign_id"]): c for c in _campaign_list()}
    campaign = campaigns.get(normalized_campaign_id)
    if not campaign:
        return redirect("menu")
    return render(
        request,
        "dashboard/access.html",
        {
            "campaign": campaign,
            "history_rows": access_email_history("inclinic", normalized_campaign_id),
        },
    )


def send_access_email_view(request: HttpRequest, brand_campaign_id: str) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    campaigns = {_normalize_campaign_id(c["brand_campaign_id"]): c for c in _campaign_list()}
    campaign = campaigns.get(normalized_campaign_id)
    if request.method != "POST":
        return redirect("campaign-access", brand_campaign_id=normalized_campaign_id)
    if not campaign:
        messages.error(request, "That campaign is not available for In-Clinic access.")
        return redirect("menu")

    recipient_email = request.POST.get("recipient_email", "")
    access = build_report_access("inclinic", normalized_campaign_id)
    try:
        send_access_email(
            report_key="inclinic",
            recipient_email=recipient_email,
            access_url=absolute_url(request, f"/campaign/{normalized_campaign_id}/login/"),
            report_name=str(campaign.get("campaign_name") or normalized_campaign_id),
            scope_label="Campaign",
            scope_id=normalized_campaign_id,
            username=access.username,
            password=access.password,
            brand_name=str(campaign.get("campaign_name") or ""),
        )
    except Exception as exc:
        messages.error(request, f"In-Clinic access email could not be sent: {exc}")
        return redirect("campaign-access", brand_campaign_id=normalized_campaign_id)

    messages.success(request, f"In-Clinic access email sent to {(recipient_email or '').strip()}.")
    return redirect("campaign-access", brand_campaign_id=normalized_campaign_id)


def _build_report_context(
    selected_campaign: str,
    week_filter: int | None = None,
    selected_collateral_id: str | None = None,
    include_field_rep_doctor_details: bool = True,
    include_state_attention: bool = True,
) -> dict[str, Any]:
    selected_schema = None
    all_weekly_rows: list[dict[str, Any]] = []
    weekly_rows: list[dict[str, Any]] = []
    data_weekly_rows: list[dict[str, Any]] = []
    active_week_values: set[int] = set()
    error_message = None
    state_attention: list[dict[str, Any]] = []
    state_attention_card: list[dict[str, Any]] = []
    schedule_text = "Schedule unavailable"
    collateral_name = "N/A"
    brand_name = "Apex"
    brand_logo_text = "apex"
    company_logo_url = None
    field_rep_insights: list[dict[str, Any]] = []
    old_collaterals: list[dict[str, str]] = []
    current_field_rep_collateral_ids: list[str] = []
    field_rep_summary = {
        "total_reps": 0,
        "total_doctors_assigned": 0,
        "doctors_sent": 0,
        "doctors_viewed": 0,
        "doctors_video_played": 0,
        "doctors_pdf_downloaded": 0,
        "assignment_issue_count": 0,
    }

    action_panel = {
        "primary_issue": "No issue detected",
        "who_should_act": "Field Team Lead",
        "actions": ["Continue current execution and monitor weekly movement."],
    }
    collateral_cards = {"current": {}, "best": {}, "benchmark": {}}
    collateral_comparison_ids: set[str] = set()
    show_collateral_comparison_extras = False
    requested_collateral_id = str(selected_collateral_id or "").strip()

    context_metrics = {
        "campaign_health": 0.0,
        "campaign_wow": 0.0,
        "campaign_benchmark_label": "Insufficient Data",
        "campaign_color": "red",
        "campaign_score_available": False,
        "weekly_health": 0.0,
        "weekly_wow": 0.0,
        "weekly_benchmark_label": "Insufficient Data",
        "weekly_color": "red",
        "weekly_score_available": False,
        "kpi_reached": 0,
        "kpi_opened": 0,
        "kpi_video": 0,
        "kpi_pdf": 0,
        "kpi_reached_pct": 0,
        "kpi_opened_pct": 0,
        "kpi_video_pct": 0,
        "kpi_pdf_pct": 0,
        "week_of": "Week -",
    }

    try:
        requested_campaign = _normalize_campaign_id(selected_campaign)
        selected_campaign = requested_campaign
        lookup_key = _normalize_lookup_key(requested_campaign)
        schema_rows = _fetch_dicts(
            f"""
            SELECT brand_campaign_id, gold_schema_name
            FROM gold_global.campaign_registry
            WHERE {_normalized_sql('brand_campaign_id')} = %s
            ORDER BY
                CASE WHEN btrim(brand_campaign_id) = btrim(%s) THEN 0 ELSE 1 END,
                last_seen_ts DESC NULLS LAST,
                brand_campaign_id
            LIMIT 1
            """,
            [lookup_key, requested_campaign],
        )
        if not schema_rows:
            return {"error_message": f"Campaign schema not found for {requested_campaign}", **context_metrics}

        selected_campaign = _normalize_campaign_id(schema_rows[0]["brand_campaign_id"])
        selected_schema = schema_rows[0]["gold_schema_name"]
        brand_campaign_variants = _campaign_brand_variants(requested_campaign)
        brand_campaign_variants = _unique_non_empty([selected_campaign, *brand_campaign_variants])

        schedule_rows = _current_schedule_rows(requested_campaign)
        collateral_comparison_ids.update(_unique_non_empty(row.get("collateral_id") for row in schedule_rows))
        show_collateral_comparison_extras = len(collateral_comparison_ids) > 1
        current_collateral_ids: list[str] = []
        schedule_start_raw = None
        schedule_end_raw = None
        if schedule_rows:
            primary_schedule = next(
                (row for row in schedule_rows if str(row.get("collateral_id") or "").strip() == requested_collateral_id),
                schedule_rows[0],
            )
            current_collateral_id = str(primary_schedule.get("collateral_id") or "").strip() or None
            primary_rank = primary_schedule.get("schedule_rank")
            display_start_raw = primary_schedule.get("campaign_start_date") or primary_schedule.get("schedule_start_date")
            display_end_raw = primary_schedule.get("campaign_end_date") or primary_schedule.get("schedule_end_date")
            schedule_start_raw = display_start_raw
            schedule_end_raw = display_end_raw
            if requested_collateral_id and current_collateral_id:
                current_collateral_ids = [current_collateral_id]
            else:
                current_collateral_ids = _unique_non_empty(
                    [
                        row.get("collateral_id")
                        for row in schedule_rows
                        if row.get("schedule_rank") == primary_rank
                        and row.get("schedule_start_date") == primary_schedule.get("schedule_start_date")
                        and row.get("schedule_end_date") == primary_schedule.get("schedule_end_date")
                    ]
                )
            current_field_rep_collateral_ids = current_collateral_ids
            old_collaterals = _format_collateral_options(schedule_rows, requested_campaign, current_collateral_id, week_filter)
            start = _format_schedule_date(display_start_raw)
            end = _format_schedule_date(display_end_raw)
            if start and end:
                schedule_text = f"{start} - {end}"
            collateral_name = primary_schedule.get("collateral_title") or collateral_name
            brand_name = (
                _clean_display_text(primary_schedule.get("brand_name"))
                or _clean_display_text(primary_schedule.get("campaign_name"))
                or brand_name
            )
            company_logo_url = _build_media_logo_url(primary_schedule.get("company_logo"))

        try:
            assigned_total_doctors = _assigned_doctor_count(requested_campaign, brand_campaign_variants)
        except DatabaseError as exc:
            assigned_total_doctors = 0
            error_message = error_message or f"Assigned doctor totals are temporarily unavailable: {exc}"

        try:
            field_rep_insights = _field_rep_insight_rows(
                requested_campaign,
                brand_campaign_variants,
                current_field_rep_collateral_ids,
                schedule_start_raw,
                schedule_end_raw,
                include_doctor_details=include_field_rep_doctor_details,
            )
        except DatabaseError as exc:
            field_rep_insights = []
            error_message = error_message or f"Field representative insights are temporarily unavailable: {exc}"
        field_rep_assigned_total = sum(_to_int(row.get("total_doctors_assigned")) for row in field_rep_insights)
        roster_total_doctors = assigned_total_doctors or field_rep_assigned_total
        unmapped_activity_doctors = sum(
            _to_int(row.get("doctors_sent")) for row in field_rep_insights if row.get("is_unmapped_activity")
        )
        reporting_total_doctors = max(roster_total_doctors, field_rep_assigned_total + unmapped_activity_doctors)
        try:
            all_weekly_rows = _weekly_rows_for_current_collateral(
                requested_campaign,
                brand_campaign_variants,
                current_collateral_ids,
                reporting_total_doctors,
                schedule_start_raw,
                schedule_end_raw,
            )
        except DatabaseError as exc:
            all_weekly_rows = []
            error_message = error_message or f"Campaign weekly metrics are temporarily unavailable: {exc}"
        if not current_collateral_ids and not any(_row_has_week_data(row) for row in all_weekly_rows):
            try:
                fallback_weekly_rows = _fetch_dicts(f"SELECT * FROM {selected_schema}.kpi_weekly_summary ORDER BY week_index")
            except DatabaseError:
                fallback_weekly_rows = []
            if fallback_weekly_rows:
                all_weekly_rows = fallback_weekly_rows
        for row in all_weekly_rows:
            _apply_weekly_v2_fields(row, reporting_total_doctors or _to_float(row.get("total_doctors_in_campaign")))
        data_weekly_rows = [r for r in all_weekly_rows if _row_has_week_data(r)]
        metric_weekly_rows = data_weekly_rows or all_weekly_rows

        week_values = sorted({_to_int(r.get("week_index")) for r in all_weekly_rows if _to_int(r.get("week_index")) > 0})
        active_week_values = {_to_int(r.get("week_index")) for r in data_weekly_rows if _to_int(r.get("week_index")) > 0}
        if week_filter and week_filter not in week_values:
            week_filter = None

        if week_filter:
            weekly_rows = [r for r in all_weekly_rows if _to_int(r.get("week_index")) == week_filter]
        else:
            weekly_rows = list(all_weekly_rows)

        metric_rows = weekly_rows if week_filter else metric_weekly_rows

        if not company_logo_url:
            try:
                fallback_logo = _fetch_dicts(
                    """
                    WITH matched_campaign AS (
                        SELECT
                            cm.company_logo,
                            ROW_NUMBER() OVER (
                                ORDER BY
                                    CASE
                                        WHEN regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g') THEN 1
                                        WHEN cm.id::text = btrim(%s) THEN 2
                                        ELSE 3
                                    END,
                                    cm.id DESC
                            ) AS rn
                        FROM bronze.campaign_management_campaign cm
                        LEFT JOIN silver.map_brand_campaign_to_campaign m
                          ON regexp_replace(lower(btrim(m.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                        WHERE
                            regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                            OR cm.id::text = btrim(%s)
                            OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                    )
                    SELECT company_logo
                    FROM matched_campaign
                    WHERE rn = 1
                    """,
                    [selected_campaign, selected_campaign, selected_campaign, selected_campaign, selected_campaign],
                )
            except DatabaseError:
                fallback_logo = []
            if fallback_logo:
                company_logo_url = _build_media_logo_url(fallback_logo[0].get("company_logo"))

        if collateral_name in {"", "N/A", "Collateral"}:
            try:
                fallback_collateral = _fetch_dicts(
                    """
                    SELECT MIN(NULLIF(c.title, '')) AS collateral_title
                    FROM silver.fact_collateral_transaction t
                    LEFT JOIN bronze.collateral_management_collateral c ON c.id = t.collateral_id
                    WHERE t.brand_campaign_id = %s
                    """,
                    [selected_campaign],
                )
            except DatabaseError:
                fallback_collateral = []
            if fallback_collateral:
                collateral_name = fallback_collateral[0].get("collateral_title") or collateral_name

        if metric_rows:
            latest_week = metric_rows[-1] if week_filter else _aggregate_weekly_metric_rows(metric_rows, reporting_total_doctors)
            latest_data_week = (data_weekly_rows or all_weekly_rows or metric_rows)[-1]
            total_doctors = _to_float(latest_week.get("total_doctors_in_campaign"))

            try:
                period_metrics = _current_collateral_period_metrics(
                    requested_campaign,
                    brand_campaign_variants,
                    current_collateral_ids,
                    latest_week.get("week_start_date"),
                    latest_week.get("week_end_date"),
                )
            except DatabaseError:
                period_metrics = {}
            if period_metrics:
                latest_week = {**latest_week, **period_metrics}
                _apply_weekly_v2_fields(latest_week, total_doctors)

            latest_reached = _to_float(latest_week.get("doctors_reached_unique"))
            latest_opened = _to_float(latest_week.get("doctors_opened_unique"))
            latest_video = _to_float(latest_week.get("video_viewed_50_unique"))
            latest_pdf = _to_float(latest_week.get("pdf_download_unique"))
            latest_consumed = _to_float(latest_week.get("doctors_consumed_unique"))

            reached_pct_total = _safe_pct(latest_reached, total_doctors)
            opened_pct_reached = _safe_pct(latest_opened, latest_reached)
            video_pct_opened = _safe_pct(latest_video, latest_opened)
            pdf_pct_opened = _safe_pct(latest_pdf, latest_opened)
            consumed_pct_opened = _safe_pct(latest_consumed, latest_opened)

            current_week_idx = _to_int(
                latest_week.get("week_index") if week_filter else latest_data_week.get("week_index"),
                1,
            )
            prev_week = None
            if week_filter and current_week_idx > 1:
                prev_candidates = [r for r in all_weekly_rows if _to_int(r.get("week_index")) == current_week_idx - 1]
                prev_week = prev_candidates[-1] if prev_candidates else None

            health_rows = data_weekly_rows or metric_rows
            try:
                collateral_health_source = _collateral_health_rows(requested_campaign, brand_campaign_variants)
            except DatabaseError:
                collateral_health_source = []
            collateral_comparison_ids.update(_unique_non_empty(row.get("collateral_id") for row in collateral_health_source))
            show_collateral_comparison_extras = len(collateral_comparison_ids) > 1
            collateral_scores = [
                _engagement_health_score(
                    _to_float(row.get("reached")),
                    _to_float(row.get("opened")),
                    _to_float(row.get("consumed")),
                    total_doctors,
                )
                for row in collateral_health_source
            ]
            campaign_health = (
                sum(collateral_scores) / len(collateral_scores)
                if collateral_scores
                else _engagement_health_score(latest_reached, latest_opened, latest_consumed, total_doctors)
            )
            weekly_health = _weekly_engagement_health_score(latest_reached, latest_opened, latest_consumed, total_doctors)

            previous_health_rows = [
                r for r in health_rows
                if _to_int(r.get("week_index")) < current_week_idx
            ]
            previous_campaign_health = _engagement_health_score(
                sum(_to_float(r.get("doctors_reached_unique")) for r in previous_health_rows),
                sum(_to_float(r.get("doctors_opened_unique")) for r in previous_health_rows),
                sum(_to_float(r.get("doctors_consumed_unique")) for r in previous_health_rows),
                total_doctors,
            ) if previous_health_rows else campaign_health
            wow_campaign = campaign_health - previous_campaign_health
            wow_weekly = (
                weekly_health
                - _weekly_engagement_health_score(
                    _to_float(prev_week.get("doctors_reached_unique")),
                    _to_float(prev_week.get("doctors_opened_unique")),
                    _to_float(prev_week.get("doctors_consumed_unique")),
                    total_doctors,
                )
                if week_filter and prev_week
                else 0.0
            )

            bridge_base_exists = _table_exists("silver", "bridge_brand_campaign_doctor_base")
            if include_state_attention:
                try:
                    state_rows = _state_attention_source_rows(
                        requested_campaign,
                        brand_campaign_variants,
                        selected_schema,
                        latest_week,
                        bridge_base_exists,
                        current_collateral_ids,
                    )
                except DatabaseError:
                    state_rows = []
            else:
                state_rows = []

            state_buckets: dict[str, dict[str, float]] = {}
            for row in state_rows:
                state = _display_state_name(row.get("state_normalized"))
                bucket = state_buckets.setdefault(state, {"reached": 0.0, "opened": 0.0, "consumed": 0.0, "total_state": 0.0})
                bucket["reached"] += _to_float(row.get("reached"))
                bucket["opened"] += _to_float(row.get("opened"))
                bucket["consumed"] += _to_float(row.get("consumed"))
                bucket["total_state"] += _to_float(row.get("total_state"))

            state_attention = []
            for state, counts in state_buckets.items():
                reached = counts["reached"]
                opened = counts["opened"]
                consumed = counts["consumed"]
                total_state = counts["total_state"]
                reached_pct = _capped_pct(reached, _weekly_doctor_base(total_state))
                open_pct = _capped_pct(opened, reached)
                consumed_pct = _capped_pct(consumed, opened)
                state_health = _state_weekly_health_score(reached, opened, consumed, total_state)
                label = _health_label(state_health)
                state_attention.append(
                    {
                        "state": state,
                        "open_pct": round(open_pct, 1),
                        "reached_pct": round(reached_pct, 1),
                        "consumed_pct": round(consumed_pct, 1),
                        "health_score": round(state_health, 1),
                        "label": label,
                    }
                )
            state_attention.sort(key=_state_attention_rank_key)
            state_attention_card = _state_attention_card_rows(state_attention)

            weakest = min(
                [
                    ("OPEN", opened_pct_reached),
                    ("CONSUMPTION", consumed_pct_opened),
                    ("REACH", reached_pct_total),
                ],
                key=lambda x: x[1],
            )[0]

            if weakest == "OPEN":
                action_panel = {
                    "primary_issue": f"Low Open Rate in {len(state_attention)} States",
                    "who_should_act": "Field Team Lead",
                    "actions": [
                        "Improve pitch and preview text to increase engagement.",
                        "Resend campaign to unopened doctors with updated messaging.",
                    ],
                }
            elif weakest == "CONSUMPTION":
                action_panel = {
                    "primary_issue": "Low Consumption Conversion",
                    "who_should_act": "Content + Field Team",
                    "actions": [
                        "Improve content hook and opening CTA for stronger consumption.",
                        "Prioritize follow-up with doctors who opened but did not consume.",
                    ],
                }
            else:
                action_panel = {
                    "primary_issue": "Low Reach Coverage",
                    "who_should_act": "Field Team Lead",
                    "actions": [
                        "Increase resend cadence for unreached doctor cohorts.",
                        "Ensure field reps cover low reach clusters first.",
                    ],
                }

            for row in collateral_health_source:
                reached = _to_float(row.get("reached"))
                opened = _to_float(row.get("opened"))
                consumed = _to_float(row.get("consumed"))
                row["reached_pct"] = _safe_pct(reached, total_doctors)
                row["opened_pct"] = _safe_pct(opened, reached)
                row["video_pct"] = _safe_pct(_to_float(row.get("video")), opened)
                row["pdf_pct"] = _safe_pct(_to_float(row.get("pdf")), opened)
                row["health_score"] = _engagement_health_score(reached, opened, consumed, total_doctors)

            best_collateral = max(
                collateral_health_source,
                key=lambda r: (
                    _to_float(r.get("reached")),
                    _to_float(r.get("opened")),
                    _to_float(r.get("consumed")),
                    _to_float(r.get("video")) + _to_float(r.get("pdf")),
                    _to_float(r.get("health_score")),
                ),
            ) if collateral_health_source else {}
            try:
                bench_rows = _fetch_dicts(
                    """
                    SELECT avg_campaign_health_score
                    FROM gold_global.benchmark_last_10_campaigns
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """
                )
            except DatabaseError:
                bench_rows = []
            benchmark_health = _to_float(bench_rows[0]["avg_campaign_health_score"]) if bench_rows else 0.0

            collateral_cards["current"] = {
                "title": collateral_name,
                "reached": _to_int(latest_reached),
                "opened": _to_int(latest_opened),
                "video": _to_int(latest_video),
                "pdf": _to_int(latest_pdf),
                "reached_pct": round(reached_pct_total, 1),
                "opened_pct": round(opened_pct_reached, 1),
                "video_pct": round(video_pct_opened, 1),
                "pdf_pct": round(pdf_pct_opened, 1),
            }
            collateral_cards["best"] = {
                "title": best_collateral.get("collateral_title") or "Best Collateral",
                "reached": _to_int(_to_float(best_collateral.get("reached"))),
                "opened": _to_int(_to_float(best_collateral.get("opened"))),
                "video": _to_int(_to_float(best_collateral.get("video"))),
                "pdf": _to_int(_to_float(best_collateral.get("pdf"))),
                "reached_pct": round(_to_float(best_collateral.get("reached_pct")), 1),
                "opened_pct": round(_to_float(best_collateral.get("opened_pct")), 1),
                "video_pct": round(_to_float(best_collateral.get("video_pct")), 1),
                "pdf_pct": round(_to_float(best_collateral.get("pdf_pct")), 1),
            }
            benchmark_metric_rows = []
            if bridge_base_exists:
                try:
                    benchmark_metric_rows = _fetch_dicts(
                        """
                        WITH recent_campaigns AS (
                            SELECT DISTINCT brand_campaign_id
                            FROM gold_global.campaign_health_history
                            ORDER BY brand_campaign_id DESC
                            LIMIT 10
                        ),
                        campaign_doctor_base AS (
                            SELECT b.brand_campaign_id, COUNT(DISTINCT b.doctor_identity_key) AS total_doctors
                            FROM silver.bridge_brand_campaign_doctor_base b
                            JOIN recent_campaigns r ON r.brand_campaign_id = b.brand_campaign_id
                            GROUP BY b.brand_campaign_id
                        ),
                        campaign_actions AS (
                            SELECT
                                a.brand_campaign_id,
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.reached_first_ts,'') IS NOT NULL) AS reached,
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.opened_first_ts,'') IS NOT NULL) AS opened,
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.video_gt_50_first_ts,'') IS NOT NULL) AS video,
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.pdf_download_first_ts,'') IS NOT NULL) AS pdf,
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (
                                    WHERE NULLIF(a.video_gt_50_first_ts,'') IS NOT NULL
                                       OR NULLIF(a.pdf_download_first_ts,'') IS NOT NULL
                                ) AS consumed
                            FROM silver.doctor_action_first_seen a
                            JOIN recent_campaigns r ON r.brand_campaign_id = a.brand_campaign_id
                            GROUP BY a.brand_campaign_id
                        ),
                        campaign_stats AS (
                            SELECT
                                x.brand_campaign_id,
                                x.reached,
                                x.opened,
                                x.video,
                                x.pdf,
                                CASE WHEN d.total_doctors=0 THEN 0 ELSE (x.reached::numeric / d.total_doctors) * 100 END AS reached_pct,
                                CASE WHEN x.reached=0 THEN 0 ELSE (x.opened::numeric / x.reached) * 100 END AS opened_pct,
                                CASE WHEN x.opened=0 THEN 0 ELSE (x.video::numeric / x.opened) * 100 END AS video_pct,
                                CASE WHEN x.opened=0 THEN 0 ELSE (x.pdf::numeric / x.opened) * 100 END AS pdf_pct,
                                (
                                  CASE WHEN d.total_doctors=0 THEN 0 ELSE LEAST(x.reached::numeric / d.total_doctors, 1.0) END
                                  + CASE WHEN x.reached=0 THEN 0 ELSE LEAST(x.opened::numeric / x.reached, 1.0) END
                                  + CASE WHEN x.opened=0 THEN 0 ELSE LEAST(x.consumed::numeric / x.opened, 1.0) END
                                ) / 3.0 * 100 AS health_score
                            FROM campaign_actions x
                            JOIN campaign_doctor_base d ON d.brand_campaign_id = x.brand_campaign_id
                        )
                        SELECT *
                        FROM campaign_stats
                        ORDER BY health_score DESC, reached DESC, opened DESC
                        LIMIT 1
                        """
                    )
                except DatabaseError:
                    benchmark_metric_rows = []
            bm = benchmark_metric_rows[0] if benchmark_metric_rows else {}
            benchmark_reached_pct = round(_to_float(bm.get("reached_pct")), 1)
            benchmark_opened_pct = round(_to_float(bm.get("opened_pct")), 1)
            benchmark_video_pct = round(_to_float(bm.get("video_pct")), 1)
            benchmark_pdf_pct = round(_to_float(bm.get("pdf_pct")), 1)

            collateral_cards["benchmark"] = {
                "title": "Benchmark Best (Last 10 Campaigns)",
                "reached": _to_int(bm.get("reached")),
                "opened": _to_int(bm.get("opened")),
                "video": _to_int(bm.get("video")),
                "pdf": _to_int(bm.get("pdf")),
                "reached_pct": benchmark_reached_pct,
                "opened_pct": benchmark_opened_pct,
                "video_pct": benchmark_video_pct,
                "pdf_pct": benchmark_pdf_pct,
                "benchmark_health": round(_to_float(bm.get("health_score"), benchmark_health), 1),
            }

            context_metrics = {
                "campaign_health": round(campaign_health, 1),
                "campaign_wow": round(wow_campaign, 1),
                "campaign_benchmark_label": "Above Average" if campaign_health >= benchmark_health else "Below Average",
                "campaign_color": _health_color(campaign_health),
                "campaign_score_available": total_doctors > 0,
                "weekly_health": round(weekly_health, 1),
                "weekly_wow": round(wow_weekly, 1),
                "weekly_benchmark_label": "Average" if 40 <= weekly_health < 60 else ("Good" if weekly_health >= 60 else "Low"),
                "weekly_color": _health_color(weekly_health),
                "weekly_score_available": total_doctors > 0,
                "kpi_reached": _to_int(latest_reached),
                "kpi_opened": _to_int(latest_opened),
                "kpi_video": _to_int(latest_video),
                "kpi_pdf": _to_int(latest_pdf),
                "kpi_reached_pct": round(reached_pct_total, 1),
                "kpi_opened_pct": round(opened_pct_reached, 1),
                "kpi_video_pct": round(video_pct_opened, 1),
                "kpi_pdf_pct": round(pdf_pct_opened, 1),
                "week_of": (
                    f"Week {current_week_idx} ({latest_week.get('week_start_date')} to {latest_week.get('week_end_date')})"
                    if week_filter
                    else f"All Weeks ({latest_week.get('week_start_date')} to {latest_week.get('week_end_date')})"
                ),
            }

        field_rep_summary = _format_field_rep_summary(field_rep_insights, roster_total_doctors)

    except Exception as exc:
        error_message = str(exc)

    trend_source_rows = weekly_rows if week_filter else data_weekly_rows
    trend_labels = [f"Week {r.get('week_index')}" for r in trend_source_rows]
    reached_pct_series = [
        _capped_pct(_to_float(r.get("doctors_reached_unique")), _weekly_doctor_base(_to_float(r.get("total_doctors_in_campaign"))))
        for r in trend_source_rows
    ]
    opened_pct_series = [
        _capped_pct(_to_float(r.get("doctors_opened_unique")), _to_float(r.get("doctors_reached_unique")))
        for r in trend_source_rows
    ]
    pdf_pct_series = [
        _capped_pct(_to_float(r.get("pdf_download_unique")), _to_float(r.get("doctors_opened_unique")))
        for r in trend_source_rows
    ]
    video_pct_series = [
        _capped_pct(_to_float(r.get("video_viewed_50_unique")), _to_float(r.get("doctors_opened_unique")))
        for r in trend_source_rows
    ]

    week_options = [
        {
            "value": week_index,
            "label": f"Week {week_index}{' *' if week_index in active_week_values else ''}",
            "has_data": week_index in active_week_values,
        }
        for week_index in sorted({_to_int(r.get("week_index")) for r in all_weekly_rows if _to_int(r.get("week_index")) > 0})
    ]

    if selected_campaign:
        metadata_name = _campaign_display_name(selected_campaign, brand_campaign_variants) if "brand_campaign_variants" in locals() else None
        brand_name = metadata_name if brand_name == "Apex" and metadata_name else (_clean_display_text(brand_name) or metadata_name or "Apex")
        brand_logo_text = _first_display_word(brand_name) or brand_name.strip()

    return {
        "selected_campaign": selected_campaign,
        "brand_name": brand_name,
        "brand_logo_text": brand_logo_text,
        "company_logo_url": company_logo_url,
        "selected_schema": selected_schema,
        "weekly_rows": weekly_rows,
        "error_message": error_message,
        "schedule_text": schedule_text,
        "collateral_name": collateral_name,
        "state_attention": state_attention,
        "state_attention_card": state_attention_card,
        "action_panel": action_panel,
        "field_rep_insights": field_rep_insights,
        "field_rep_summary": field_rep_summary,
        "old_collaterals": old_collaterals,
        "current_field_rep_collateral_id": current_field_rep_collateral_ids[0] if current_field_rep_collateral_ids else "",
        "selected_collateral_id": current_field_rep_collateral_ids[0] if requested_collateral_id and current_field_rep_collateral_ids else "",
        "field_rep_detail_url": _field_rep_detail_url(
            selected_campaign,
            week_filter,
            current_field_rep_collateral_ids[0] if requested_collateral_id and current_field_rep_collateral_ids else None,
        ),
        "collateral_cards": collateral_cards,
        "show_collateral_comparison_extras": show_collateral_comparison_extras,
        "trend_labels": trend_labels,
        "reached_pct_series": [round(v, 1) for v in reached_pct_series],
        "opened_pct_series": [round(v, 1) for v in opened_pct_series],
        "pdf_pct_series": [round(v, 1) for v in pdf_pct_series],
        "video_pct_series": [round(v, 1) for v in video_pct_series],
        "week_options": week_options,
        "selected_week": week_filter,
        **context_metrics,
    }


def _build_collateral_field_rep_context(
    selected_campaign: str,
    collateral_id: str,
    include_field_rep_doctor_details: bool = True,
) -> dict[str, Any]:
    requested_campaign = _normalize_campaign_id(selected_campaign)
    selected_collateral_id = str(collateral_id or "").strip()
    context: dict[str, Any] = {
        "selected_campaign": requested_campaign,
        "selected_collateral_id": selected_collateral_id,
        "brand_name": "Apex",
        "brand_logo_text": "apex",
        "company_logo_url": None,
        "collateral_name": f"Collateral {selected_collateral_id}" if selected_collateral_id else "Collateral",
        "schedule_text": "Schedule unavailable",
        "field_rep_insights": [],
        "field_rep_summary": _format_field_rep_summary([]),
        "old_collaterals": [],
        "field_rep_detail_url": _field_rep_detail_url(requested_campaign, collateral_id=selected_collateral_id),
        "error_message": None,
    }
    try:
        lookup_key = _normalize_lookup_key(requested_campaign)
        schema_rows = _fetch_dicts(
            f"""
            SELECT brand_campaign_id, gold_schema_name
            FROM gold_global.campaign_registry
            WHERE {_normalized_sql('brand_campaign_id')} = %s
            ORDER BY
                CASE WHEN btrim(brand_campaign_id) = btrim(%s) THEN 0 ELSE 1 END,
                last_seen_ts DESC NULLS LAST,
                brand_campaign_id
            LIMIT 1
            """,
            [lookup_key, requested_campaign],
        )
        if not schema_rows:
            context["error_message"] = f"Campaign schema not found for {requested_campaign}"
            return context

        selected_campaign_resolved = _normalize_campaign_id(schema_rows[0]["brand_campaign_id"])
        brand_campaign_variants = _unique_non_empty([selected_campaign_resolved, *_campaign_brand_variants(requested_campaign)])
        schedule_rows = _current_schedule_rows(requested_campaign)
        selected_row = next((row for row in schedule_rows if str(row.get("collateral_id") or "").strip() == selected_collateral_id), {})
        if selected_row:
            context["collateral_name"] = _collateral_display_name(selected_row, context["collateral_name"])
            start = _format_schedule_date(_collateral_display_start(selected_row))
            end = _format_schedule_date(_collateral_display_end(selected_row))
            if start and end:
                context["schedule_text"] = f"{start} - {end}"
            context["brand_name"] = (
                _clean_display_text(selected_row.get("brand_name"))
                or _clean_display_text(selected_row.get("campaign_name"))
                or context["brand_name"]
            )
            context["company_logo_url"] = _build_media_logo_url(selected_row.get("company_logo"))
        metadata_name = _campaign_display_name(selected_campaign_resolved, brand_campaign_variants)
        context["brand_name"] = _clean_display_text(context["brand_name"]) or metadata_name or "Apex"
        context["brand_logo_text"] = _first_display_word(context["brand_name"]) or context["brand_name"].strip()
        context["old_collaterals"] = _format_collateral_options(schedule_rows, requested_campaign, selected_collateral_id)

        field_rep_insights = _field_rep_insight_rows(
            requested_campaign,
            brand_campaign_variants,
            [selected_collateral_id],
            selected_row.get("schedule_start_date") if selected_row else None,
            selected_row.get("schedule_end_date") if selected_row else None,
            include_doctor_details=include_field_rep_doctor_details,
        )
        total_doctors = _assigned_doctor_count(requested_campaign, brand_campaign_variants)
        context["field_rep_insights"] = field_rep_insights
        context["field_rep_summary"] = _format_field_rep_summary(field_rep_insights, total_doctors)
    except Exception as exc:
        context["error_message"] = str(exc)
    return context


def campaign_overview(request: HttpRequest, brand_campaign_id: str | None = None):
    if not brand_campaign_id:
        return redirect("menu")

    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None

    context = _build_report_context(
        normalized_campaign_id,
        week_filter,
        selected_collateral_id=collateral_id,
        include_field_rep_doctor_details=False,
    )
    return render(request, "dashboard/overview.html", context)


def field_rep_doctor_details(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return JsonResponse({"error": "Authentication required."}, status=403)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    rep_id = request.GET.get("rep_id", "")
    metric_key = request.GET.get("metric", "assigned")
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None
    if collateral_id:
        context = _build_collateral_field_rep_context(
            normalized_campaign_id,
            collateral_id,
            include_field_rep_doctor_details=True,
        )
    else:
        context_kwargs = {
            "include_field_rep_doctor_details": True,
            "include_state_attention": False,
        }
        if collateral_id:
            context_kwargs["selected_collateral_id"] = collateral_id
        context = _build_report_context(normalized_campaign_id, week_filter, **context_kwargs)
    payload, status = _field_rep_doctor_detail_payload(context, rep_id, metric_key)
    return JsonResponse(payload, status=status)


def export_report(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None
    context = _build_report_context(
        normalized_campaign_id,
        week_filter,
        selected_collateral_id=collateral_id,
        include_field_rep_doctor_details=False,
        include_state_attention=False,
    )
    filename = _export_filename("in_clinic_report", context, "pdf")
    title = f"In-Clinic Sharing Report - {context.get('brand_name') or normalized_campaign_id}"
    return _pdf_response(filename, title, _campaign_pdf_lines(context))


def export_field_rep_insights(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None
    context = _build_report_context(
        normalized_campaign_id,
        week_filter,
        selected_collateral_id=collateral_id,
        include_state_attention=False,
    )
    return _field_rep_insights_excel_response(context)


def export_unmapped_doctors(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None
    context = _build_report_context(
        normalized_campaign_id,
        week_filter,
        selected_collateral_id=collateral_id,
        include_state_attention=False,
    )
    return _manual_mapping_excel_response(context)


def export_collateral_field_rep_insights(request: HttpRequest, brand_campaign_id: str, collateral_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    context = _build_collateral_field_rep_context(normalized_campaign_id, collateral_id)
    return _field_rep_insights_excel_response(
        context,
        filename_prefix="field_rep_insights_collateral",
        filename_extra=f"collateral_{collateral_id}",
    )


def campaign_field_rep_collateral_insights(request: HttpRequest, brand_campaign_id: str, collateral_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)
    context = _build_collateral_field_rep_context(
        normalized_campaign_id,
        collateral_id,
        include_field_rep_doctor_details=False,
    )
    return render(request, "dashboard/field_rep_collateral_insights.html", context)


def campaign_state_list(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not _has_inclinic_campaign_access(request, normalized_campaign_id):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)
    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    collateral_id = str(request.GET.get("collateral_id") or "").strip() or None
    context = _build_report_context(
        normalized_campaign_id,
        week_filter,
        selected_collateral_id=collateral_id,
        include_field_rep_doctor_details=False,
    )
    return render(request, "dashboard/state_list.html", context)
