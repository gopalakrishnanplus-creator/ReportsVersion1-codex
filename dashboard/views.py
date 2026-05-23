from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from django.contrib import messages
from django.db import connection
from django.db.utils import OperationalError, ProgrammingError
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from etl.utils.specs import SOURCE_TABLE_SPECS
from reporting.access import absolute_url, access_email_history, authenticate_session, build_report_access, send_access_email, validate_credentials
from reporting.campaign_performance import CampaignPerformanceNotFound, _resolve_campaign_reference


def _fetch_dicts(sql: str, params=None):
    with connection.cursor() as cursor:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


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


def _health_color(score: float) -> str:
    if score < 40:
        return "red"
    if score < 60:
        return "yellow"
    return "green"


def _clean_display_text(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"null", "none", "n/a", "na", "-", "brand"}:
        return None
    return text


def _engagement_health_score(reached: float, opened: float, consumed: float, total_doctors: float) -> float:
    reached_component = min(_safe_pct(reached, total_doctors), 100.0) / 100.0
    opened_component = min(_safe_pct(opened, total_doctors), 100.0) / 100.0
    consumed_component = min(_safe_pct(consumed, total_doctors), 100.0) / 100.0
    return ((reached_component * 0.5) + (opened_component * 0.25) + (consumed_component * 0.25)) * 100.0


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


def _normalize_campaign_id(value: Any) -> str:
    return str(value or "").strip()


def _normalize_lookup_key(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(value or "").strip().lower())


def _normalized_sql(column_sql: str) -> str:
    return f"lower(regexp_replace(COALESCE(btrim({column_sql}), ''), '[^a-zA-Z0-9]', '', 'g'))"


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
        select_parts.extend(["''::text AS auth_email_key", "''::text AS auth_username_key"])
        auth_email_match = ""
        auth_username_match = ""
        legacy_auth_email_match = ""

    if has_local_user:
        joins.append(
            f"""
            LEFT JOIN bronze.user_management_user uu
              ON ({_normalized_sql('uu.field_id')} <> '' AND {_normalized_sql('uu.field_id')} = {_normalized_sql('cfr.brand_supplied_field_rep_id')})
              OR ({_normalized_sql('uu.id::text')} <> '' AND {_normalized_sql('uu.id::text')} = {_normalized_sql('ccf.field_rep_id::text')})
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


def _current_schedule_rows(selected_campaign: str) -> list[dict[str, Any]]:
    lookup_key = _normalize_lookup_key(selected_campaign)
    return _fetch_dicts(
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
        schedule_candidates AS (
            SELECT
                sc.collateral_id,
                COALESCE(sc.schedule_start_date, cs.campaign_start_date) AS schedule_start_date,
                COALESCE(sc.schedule_end_date, cs.campaign_end_date) AS schedule_end_date,
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
                cs.source_updated_at,
                CASE
                    WHEN COALESCE(sc.schedule_start_date, cs.campaign_start_date) <= CURRENT_DATE
                     AND COALESCE(sc.schedule_end_date, cs.campaign_end_date) >= CURRENT_DATE THEN 0
                    WHEN COALESCE(sc.schedule_start_date, cs.campaign_start_date) <= CURRENT_DATE THEN 1
                    WHEN COALESCE(sc.schedule_start_date, cs.campaign_start_date) > CURRENT_DATE THEN 2
                    ELSE 3
                END AS schedule_rank
            FROM campaign_source cs
            LEFT JOIN silver.bridge_campaign_collateral_schedule sc
              ON sc.campaign_id_resolved::text = cs.id::text
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
    alias_joins, alias_selects, alias_key_columns = _field_rep_alias_sql_parts()
    alias_key_unions = "\n                ".join(
        f"UNION ALL SELECT {column} AS rep_key FROM assigned_reps" for column in alias_key_columns
    )
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
            SELECT DISTINCT rep_key
            FROM (
                SELECT internal_rep_key AS rep_key FROM assigned_reps
                UNION ALL SELECT external_rep_key AS rep_key FROM assigned_reps
                {alias_key_unions}
            ) keys
            WHERE rep_key <> ''
        ),
        assigned_doctors AS (
            SELECT DISTINCT d.doctor_identity_key
            FROM assigned_rep_keys ar
            JOIN silver.dim_doctor d
              ON {_normalized_sql('d.rep_id_normalized')} = ar.rep_key
              OR {_normalized_sql('d.field_rep_id_resolved')} = ar.rep_key
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
        SELECT GREATEST(
            COALESCE((SELECT COUNT(*) FROM assigned_doctors), 0),
            COALESCE((SELECT declared_total FROM declared_counts), 0),
            COALESCE((SELECT supported_total FROM supported_counts), 0)
        )::int AS assigned_total
        """,
        [*params, *brand_keys],
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
    return _fetch_dicts(
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
            (%s::numeric / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric) AS weekly_doctor_base,
            LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_reached_unique::numeric / NULLIF(%s::numeric,0) END, 1.0) AS weekly_reached_pct,
            CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END AS weekly_opened_pct,
            CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END AS weekly_consumption_pct,
            (
                LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_reached_unique::numeric / NULLIF(%s::numeric,0) END, 1.0) * 0.5
                + LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_opened_unique::numeric / NULLIF(%s::numeric,0) END, 1.0) * 0.25
                + LEAST(CASE WHEN %s::numeric=0 THEN 0 ELSE doctors_consumed_unique::numeric / NULLIF(%s::numeric,0) END, 1.0) * 0.25
            ) * 100 AS weekly_health_score
        FROM agg
        ORDER BY week_index
        """,
        [*params, *([total_doctors] * 10)],
    )


def _field_rep_insight_rows(
    selected_campaign: str,
    brand_campaign_variants: list[str],
    current_collateral_ids: list[str],
) -> list[dict[str, Any]]:
    brand_keys, brand_placeholders = _campaign_key_placeholders(selected_campaign, brand_campaign_variants)
    candidate_cte = _candidate_campaign_ids_cte(brand_placeholders)
    collateral_filter_tx = ""
    collateral_filter_share = ""
    if current_collateral_ids:
        collateral_placeholders = _placeholders(current_collateral_ids)
        collateral_filter_tx = f"AND tx.collateral_id::text IN ({collateral_placeholders})"
        collateral_filter_share = f"AND s.collateral_id::text IN ({collateral_placeholders})"

    alias_joins, alias_selects, alias_key_columns = _field_rep_alias_sql_parts()
    alias_key_unions = "\n            ".join(
        f"""
            UNION
            SELECT field_rep_id, {column} AS rep_key
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
        *current_collateral_ids,
        *brand_keys,
        *current_collateral_ids,
    ]
    return _fetch_dicts(
        f"""
        WITH {candidate_cte},
        raw_assigned_reps AS (
            SELECT DISTINCT
                ccf.field_rep_id::text AS field_rep_id,
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
                COALESCE(NULLIF(initcap(btrim(cfr.state)), ''), 'UNKNOWN') AS state_normalized
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
                    MAX(NULLIF(state_normalized, '')),
                    'UNKNOWN'
                ) AS state_normalized
            FROM raw_assigned_reps
            GROUP BY field_rep_id
        ),
        assigned_rep_keys AS (
            SELECT field_rep_id, internal_rep_key AS rep_key
            FROM raw_assigned_reps
            WHERE internal_rep_key <> ''
            UNION
            SELECT field_rep_id, external_rep_key AS rep_key
            FROM raw_assigned_reps
            WHERE external_rep_key <> ''
            {alias_key_unions}
        ),
        assigned_doctors AS (
            SELECT
                ark.field_rep_id,
                COUNT(DISTINCT d.doctor_identity_key) AS total_doctors_assigned
            FROM assigned_rep_keys ark
            LEFT JOIN silver.dim_doctor d
              ON (
                  {_normalized_sql('d.rep_id_normalized')} = ark.rep_key
                  OR {_normalized_sql('d.field_rep_id_resolved')} = ark.rep_key
              )
            GROUP BY ark.field_rep_id
        ),
        activity AS (
            SELECT
                {_normalized_sql('tx.field_rep_id')} AS rep_key,
                tx.doctor_identity_key,
                1 AS sent_flag,
                CASE WHEN tx.has_viewed_flag = '1' OR NULLIF(tx.opened_event_ts, '') IS NOT NULL THEN 1 ELSE 0 END AS viewed_flag,
                CASE
                    WHEN tx.video_view_gt_50_flag = '1'
                      OR COALESCE(tx.last_video_percentage_num, 0) > 0
                      OR COALESCE(tx.video_watch_percentage_num, 0) > 0
                      OR NULLIF(tx.video_lt_50_at_ts, '') IS NOT NULL
                      OR NULLIF(tx.video_gt_50_at_ts, '') IS NOT NULL
                      OR NULLIF(tx.video_100_at_ts, '') IS NOT NULL
                    THEN 1 ELSE 0
                END AS video_flag,
                CASE WHEN tx.downloaded_pdf_flag = '1' OR NULLIF(tx.pdf_download_event_ts, '') IS NOT NULL THEN 1 ELSE 0 END AS pdf_flag
            FROM silver.fact_collateral_transaction tx
            WHERE {_normalized_sql('tx.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_tx}
            UNION ALL
            SELECT
                {_normalized_sql('s.field_rep_id::text')} AS rep_key,
                s.doctor_identity_key,
                1 AS sent_flag,
                0 AS viewed_flag,
                0 AS video_flag,
                0 AS pdf_flag
            FROM silver.fact_share_log s
            WHERE {_normalized_sql('s.brand_campaign_id')} IN ({brand_placeholders})
              {collateral_filter_share}
        ),
        activity_for_rep AS (
            SELECT
                ark.field_rep_id,
                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE a.sent_flag = 1) AS doctors_sent,
                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE a.viewed_flag = 1) AS doctors_viewed,
                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE a.video_flag = 1) AS doctors_video_played,
                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE a.pdf_flag = 1) AS doctors_pdf_downloaded
            FROM assigned_rep_keys ark
            LEFT JOIN activity a ON a.rep_key = ark.rep_key
            GROUP BY ark.field_rep_id
        )
        SELECT
            COALESCE(NULLIF(ar.field_rep_display_id, ''), ar.field_rep_id) AS field_rep_id,
            ar.field_rep_name,
            ar.state_normalized,
            COALESCE(ad.total_doctors_assigned, 0)::int AS total_doctors_assigned,
            COALESCE(ab.doctors_sent, 0)::int AS doctors_sent,
            COALESCE(ab.doctors_viewed, 0)::int AS doctors_viewed,
            COALESCE(ab.doctors_video_played, 0)::int AS doctors_video_played,
            COALESCE(ab.doctors_pdf_downloaded, 0)::int AS doctors_pdf_downloaded,
            CASE
                WHEN COALESCE(ad.total_doctors_assigned, 0) = 0
                 AND (
                    COALESCE(ab.doctors_sent, 0) > 0
                    OR COALESCE(ab.doctors_viewed, 0) > 0
                    OR COALESCE(ab.doctors_video_played, 0) > 0
                    OR COALESCE(ab.doctors_pdf_downloaded, 0) > 0
                 )
                THEN 'No campaign doctor roster match; engagement comes from share/transaction logs.'
                WHEN COALESCE(ab.doctors_sent, 0) > COALESCE(ad.total_doctors_assigned, 0)
                THEN 'Engagement exceeds campaign roster matches; check doctor roster or rep mapping.'
                ELSE ''
            END AS assignment_note
        FROM assigned_reps ar
        LEFT JOIN assigned_doctors ad ON ad.field_rep_id = ar.field_rep_id
        LEFT JOIN activity_for_rep ab ON ab.field_rep_id = ar.field_rep_id
        ORDER BY
            COALESCE(ab.doctors_sent, 0) DESC,
            COALESCE(ad.total_doctors_assigned, 0) DESC,
            ar.field_rep_name
        """,
        params,
    )


def _build_media_logo_url(company_logo_path: Any) -> str | None:
    raw = str(company_logo_path or "").strip()
    if not raw or raw.lower() == "null":
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://inclinic.inditech.co.in/media/{raw.lstrip('/')}"


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
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('bronze.campaign_campaign')")
            campaign_table_exists = cursor.fetchone()[0] is not None
            cursor.execute("SELECT to_regclass('silver.map_brand_campaign_to_campaign')")
            mapping_table_exists = cursor.fetchone()[0] is not None
        if not campaign_table_exists:
            return []

        mapping_join = ""
        mapping_select = "NULL::text AS brand_campaign_id"
        if mapping_table_exists:
            mapping_join = """
                LEFT JOIN (
                    SELECT campaign_id_resolved, MIN(brand_campaign_id) AS brand_campaign_id
                    FROM silver.map_brand_campaign_to_campaign
                    GROUP BY campaign_id_resolved
                ) mapped
                  ON mapped.campaign_id_resolved = cc.id::text
            """
            mapping_select = "NULLIF(btrim(mapped.brand_campaign_id), '') AS brand_campaign_id"

        rows = _fetch_dicts(
            f"""
            SELECT
              cc.id::text AS campaign_id,
              COALESCE(NULLIF(btrim(cc.name), ''), cc.id::text) AS campaign_name,
              CASE WHEN lower(COALESCE(cc.system_rfa, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_rfa,
              CASE WHEN lower(COALESCE(cc.system_ic, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_ic,
              CASE WHEN lower(COALESCE(cc.system_pe, '')) IN ('1', 'true', 't', 'yes') THEN TRUE ELSE FALSE END AS system_pe,
              NULLIF(btrim(cc.brand_manager_login_link), '') AS brand_manager_login_link,
              {mapping_select}
            FROM bronze.campaign_campaign cc
            {mapping_join}
            WHERE
              lower(COALESCE(cc.system_rfa, '')) IN ('1', 'true', 't', 'yes')
              OR lower(COALESCE(cc.system_ic, '')) IN ('1', 'true', 't', 'yes')
              OR lower(COALESCE(cc.system_pe, '')) IN ('1', 'true', 't', 'yes')
            ORDER BY COALESCE(NULLIF(btrim(cc.name), ''), cc.id::text)
            """
        )
    except (ProgrammingError, OperationalError):
        return []

    output = []
    for row in rows:
        systems = []
        if row.get("system_rfa"):
            systems.append("RFA")
        if row.get("system_ic"):
            systems.append("InClinic")
        if row.get("system_pe"):
            systems.append("PE")
        campaign_id = _normalize_campaign_id(row.get("campaign_id"))
        brand_campaign_id = str(row.get("brand_campaign_id") or "").strip()
        in_clinic_report_url = absolute_url(request, f"/campaign/{brand_campaign_id}/") if row.get("system_ic") and brand_campaign_id else ""
        pe_report_url = ""
        if (row.get("system_ic") and not in_clinic_report_url) or row.get("system_pe"):
            try:
                reference = _resolve_campaign_reference(campaign_id)
            except CampaignPerformanceNotFound:
                reference = None
            if row.get("system_ic") and not in_clinic_report_url and reference and reference.brand_campaign_id:
                in_clinic_report_url = absolute_url(request, f"/campaign/{reference.brand_campaign_id}/")
            if row.get("system_pe") and reference and reference.pe_campaign_id:
                pe_report_url = absolute_url(request, f"/pe-reports/campaign/{reference.pe_campaign_id}/")

        system_report_links = []
        if row.get("system_ic"):
            system_report_links.append(
                {
                    "label": "InClinic Report",
                    "url": in_clinic_report_url,
                    "status": "Not mapped" if not in_clinic_report_url else "",
                }
            )
        if row.get("system_pe"):
            system_report_links.append(
                {
                    "label": "PE Report",
                    "url": pe_report_url,
                    "status": "Not mapped" if not pe_report_url else "",
                }
            )
        if row.get("system_rfa"):
            system_report_links.append(
                {
                    "label": "RFA Report",
                    "url": "",
                    "status": "Not available yet",
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


def campaign_performance_page(request: HttpRequest, campaign_id: str) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(campaign_id)
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


def _build_report_context(selected_campaign: str, week_filter: int | None = None) -> dict[str, Any]:
    selected_schema = None
    all_weekly_rows: list[dict[str, Any]] = []
    weekly_rows: list[dict[str, Any]] = []
    data_weekly_rows: list[dict[str, Any]] = []
    active_week_values: set[int] = set()
    error_message = None
    state_attention: list[dict[str, Any]] = []
    schedule_text = "Schedule unavailable"
    collateral_name = "N/A"
    brand_name = "Apex"
    brand_logo_text = "apex"
    company_logo_url = None
    field_rep_insights: list[dict[str, Any]] = []
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

    context_metrics = {
        "campaign_health": 0.0,
        "campaign_wow": 0.0,
        "campaign_benchmark_label": "Insufficient Data",
        "campaign_color": "red",
        "weekly_health": 0.0,
        "weekly_wow": 0.0,
        "weekly_benchmark_label": "Insufficient Data",
        "weekly_color": "red",
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
        current_collateral_ids: list[str] = []
        schedule_start_raw = None
        schedule_end_raw = None
        if schedule_rows:
            primary_schedule = schedule_rows[0]
            primary_rank = primary_schedule.get("schedule_rank")
            schedule_start_raw = primary_schedule.get("schedule_start_date")
            schedule_end_raw = primary_schedule.get("schedule_end_date")
            current_collateral_ids = _unique_non_empty(
                [
                    row.get("collateral_id")
                    for row in schedule_rows
                    if row.get("schedule_rank") == primary_rank
                    and row.get("schedule_start_date") == schedule_start_raw
                    and row.get("schedule_end_date") == schedule_end_raw
                ]
            )
            start = _format_schedule_date(primary_schedule.get("schedule_start_date"))
            end = _format_schedule_date(primary_schedule.get("schedule_end_date"))
            if start and end:
                schedule_text = f"{start} - {end}"
            collateral_name = primary_schedule.get("collateral_title") or collateral_name
            brand_name = (
                _clean_display_text(primary_schedule.get("brand_name"))
                or _clean_display_text(primary_schedule.get("campaign_name"))
                or brand_name
            )
            company_logo_url = _build_media_logo_url(primary_schedule.get("company_logo"))

        assigned_total_doctors = _assigned_doctor_count(requested_campaign, brand_campaign_variants)
        field_rep_insights = _field_rep_insight_rows(requested_campaign, brand_campaign_variants, current_collateral_ids)
        field_rep_assigned_total = sum(_to_int(row.get("total_doctors_assigned")) for row in field_rep_insights)
        reporting_total_doctors = assigned_total_doctors or field_rep_assigned_total
        all_weekly_rows = _weekly_rows_for_current_collateral(
            requested_campaign,
            brand_campaign_variants,
            current_collateral_ids,
            reporting_total_doctors,
            schedule_start_raw,
            schedule_end_raw,
        )
        if not current_collateral_ids and not any(_row_has_week_data(row) for row in all_weekly_rows):
            fallback_weekly_rows = _fetch_dicts(f"SELECT * FROM {selected_schema}.kpi_weekly_summary ORDER BY week_index")
            if fallback_weekly_rows:
                all_weekly_rows = fallback_weekly_rows
        if reporting_total_doctors:
            for row in all_weekly_rows:
                row["total_doctors_in_campaign"] = reporting_total_doctors
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
            if fallback_logo:
                company_logo_url = _build_media_logo_url(fallback_logo[0].get("company_logo"))

        if collateral_name in {"", "N/A", "Collateral"}:
            fallback_collateral = _fetch_dicts(
                """
                SELECT MIN(NULLIF(c.title, '')) AS collateral_title
                FROM silver.fact_collateral_transaction t
                LEFT JOIN bronze.collateral_management_collateral c ON c.id = t.collateral_id
                WHERE t.brand_campaign_id = %s
                """,
                [selected_campaign],
            )
            if fallback_collateral:
                collateral_name = fallback_collateral[0].get("collateral_title") or collateral_name

        if metric_rows:
            latest_week = metric_rows[-1]
            total_doctors = _to_float(latest_week.get("total_doctors_in_campaign"))

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

            current_week_idx = _to_int(latest_week.get("week_index"), 1)
            prev_week = None
            if current_week_idx > 1:
                prev_candidates = [r for r in all_weekly_rows if _to_int(r.get("week_index")) == current_week_idx - 1]
                prev_week = prev_candidates[-1] if prev_candidates else None

            health_rows = data_weekly_rows or metric_rows
            campaign_reached = sum(_to_float(r.get("doctors_reached_unique")) for r in health_rows)
            campaign_opened = sum(_to_float(r.get("doctors_opened_unique")) for r in health_rows)
            campaign_consumed = sum(_to_float(r.get("doctors_consumed_unique")) for r in health_rows)
            campaign_health = _engagement_health_score(campaign_reached, campaign_opened, campaign_consumed, total_doctors)
            weekly_health = _engagement_health_score(latest_reached, latest_opened, latest_consumed, total_doctors)

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
                - _engagement_health_score(
                    _to_float(prev_week.get("doctors_reached_unique")),
                    _to_float(prev_week.get("doctors_opened_unique")),
                    _to_float(prev_week.get("doctors_consumed_unique")),
                    total_doctors,
                )
                if prev_week
                else 0.0
            )

            bridge_base_exists = _table_exists("silver", "bridge_brand_campaign_doctor_base")
            base_state_sql = "NULLIF(btrim(base.state_normalized), '')," if bridge_base_exists else ""
            base_join_sql = (
                """
                    LEFT JOIN silver.bridge_brand_campaign_doctor_base base
                      ON base.brand_campaign_id = f.brand_campaign_id
                     AND base.doctor_identity_key = f.doctor_identity_key
                """
                if bridge_base_exists
                else ""
            )
            try:
                state_rows = _fetch_dicts(
                f"""
                WITH campaign_ref AS (
                    SELECT DISTINCT candidate_campaign_id
                    FROM (
                        SELECT NULLIF(btrim(%s), '') AS candidate_campaign_id
                        UNION ALL
                        SELECT NULLIF(btrim(m.campaign_id_resolved), '') AS candidate_campaign_id
                        FROM silver.map_brand_campaign_to_campaign m
                        WHERE lower(regexp_replace(btrim(m.brand_campaign_id), '[^a-zA-Z0-9]', '', 'g'))
                              = lower(regexp_replace(btrim(%s), '[^a-zA-Z0-9]', '', 'g'))
                        UNION ALL
                        SELECT NULLIF(btrim(cc.id::text), '') AS candidate_campaign_id
                        FROM bronze.campaign_campaign cc
                        WHERE lower(regexp_replace(btrim(cc.id::text), '[^a-zA-Z0-9]', '', 'g'))
                              = lower(regexp_replace(btrim(%s), '[^a-zA-Z0-9]', '', 'g'))
                    ) q
                    WHERE candidate_campaign_id IS NOT NULL
                ),
                rep_state_campaign AS (
                    SELECT DISTINCT
                      lower(regexp_replace(btrim(ccf.field_rep_id::text), '[^a-zA-Z0-9]', '', 'g')) AS rep_key,
                      initcap(btrim(cfr.state)) AS state_normalized
                    FROM bronze.campaign_campaignfieldrep ccf
                    JOIN campaign_ref cr
                      ON lower(regexp_replace(NULLIF(btrim(ccf.campaign_id), ''), '[^a-zA-Z0-9]', '', 'g'))
                       = lower(regexp_replace(cr.candidate_campaign_id, '[^a-zA-Z0-9]', '', 'g'))
                    LEFT JOIN bronze.campaign_fieldrep cfr
                      ON cfr.id::text = ccf.field_rep_id::text
                    WHERE cfr.state IS NOT NULL
                      AND btrim(cfr.state) <> ''
                      AND lower(btrim(cfr.state)) <> 'null'
                ),
                rep_state_global AS (
                    SELECT DISTINCT
                      lower(regexp_replace(COALESCE(NULLIF(btrim(brand_supplied_field_rep_id), ''), btrim(id::text)), '[^a-zA-Z0-9]', '', 'g')) AS rep_key,
                      initcap(btrim(state)) AS state_normalized
                    FROM bronze.campaign_fieldrep
                    WHERE state IS NOT NULL
                      AND btrim(state) <> ''
                      AND lower(btrim(state)) <> 'null'
                ),
                tx_rep AS (
                    SELECT DISTINCT ON (brand_campaign_id, doctor_identity_key)
                      brand_campaign_id,
                      doctor_identity_key,
                      field_rep_id::text AS field_rep_id_resolved
                    FROM silver.fact_collateral_transaction
                    WHERE COALESCE(NULLIF(btrim(field_rep_id), ''), NULL) IS NOT NULL
                    ORDER BY brand_campaign_id, doctor_identity_key, COALESCE(updated_at_ts, created_at_ts, transaction_date_ts) DESC, id DESC
                ),
                fact_enriched AS (
                    SELECT
                      f.doctor_identity_key,
                      COALESCE(
                        NULLIF(btrim(f.state_normalized), ''),
                        {base_state_sql}
                        NULLIF(btrim(d.state_normalized), ''),
                        NULLIF(btrim(fr.state_normalized), ''),
                        NULLIF(btrim(rsc_fact.state_normalized), ''),
                        NULLIF(btrim(rsg_fact.state_normalized), ''),
                        NULLIF(btrim(rsc_tx.state_normalized), ''),
                        NULLIF(btrim(rsg_tx.state_normalized), ''),
                        'UNKNOWN'
                      ) AS state_normalized,
                      f.reached_first_ts,
                      f.opened_first_ts
                    FROM {selected_schema}.fact_doctor_collateral_latest f
                    {base_join_sql}
                    LEFT JOIN silver.dim_doctor d
                      ON d.doctor_identity_key = f.doctor_identity_key
                    LEFT JOIN silver.dim_field_rep fr
                      ON lower(regexp_replace(COALESCE(NULLIF(btrim(fr.source_field_rep_id), ''), btrim(fr.id::text)), '[^a-zA-Z0-9]', '', 'g'))
                       = lower(regexp_replace(NULLIF(btrim(f.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
                    LEFT JOIN tx_rep tx
                      ON tx.brand_campaign_id = f.brand_campaign_id
                     AND tx.doctor_identity_key = f.doctor_identity_key
                    LEFT JOIN rep_state_campaign rsc_fact
                      ON rsc_fact.rep_key = lower(regexp_replace(NULLIF(btrim(f.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
                    LEFT JOIN rep_state_global rsg_fact
                      ON rsg_fact.rep_key = lower(regexp_replace(NULLIF(btrim(f.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
                    LEFT JOIN rep_state_campaign rsc_tx
                      ON rsc_tx.rep_key = lower(regexp_replace(NULLIF(btrim(tx.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
                    LEFT JOIN rep_state_global rsg_tx
                      ON rsg_tx.rep_key = lower(regexp_replace(NULLIF(btrim(tx.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
                ),
                state_universe AS (
                    SELECT DISTINCT state_normalized FROM rep_state_campaign
                    UNION
                    SELECT DISTINCT state_normalized FROM fact_enriched WHERE state_normalized <> 'UNKNOWN'
                ),
                agg AS (
                    SELECT
                      state_normalized,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE reached_first_ts IS NOT NULL
                          AND reached_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS reached,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE opened_first_ts IS NOT NULL
                          AND opened_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS opened,
                      COUNT(DISTINCT doctor_identity_key) AS total_state
                    FROM fact_enriched
                    GROUP BY 1
                )
                SELECT
                  su.state_normalized,
                  COALESCE(a.reached, 0) AS reached,
                  COALESCE(a.opened, 0) AS opened,
                  COALESCE(a.total_state, 0) AS total_state
                FROM state_universe su
                LEFT JOIN agg a ON a.state_normalized = su.state_normalized
                ORDER BY
                  CASE
                    WHEN COALESCE(a.reached,0)=0 OR COALESCE(a.total_state,0)=0 THEN 0
                    ELSE ((LEAST((COALESCE(a.reached,0) / NULLIF((COALESCE(a.total_state,0)/4.0),0)),1.0)
                      + (COALESCE(a.opened,0) / NULLIF(COALESCE(a.reached,0),0))
                      + (COALESCE(a.opened,0) / NULLIF(COALESCE(a.opened,0),0))) / 3.0) * 100
                  END ASC,
                  su.state_normalized ASC
                """,
                [
                    selected_campaign,
                    selected_campaign,
                    selected_campaign,
                    latest_week.get("week_start_date"),
                    latest_week.get("week_end_date"),
                    latest_week.get("week_start_date"),
                    latest_week.get("week_end_date"),
                ],
                )
            except (ProgrammingError, OperationalError):
                state_rows = []

            state_attention = []
            for row in state_rows:
                reached = _to_float(row.get("reached"))
                opened = _to_float(row.get("opened"))
                total_state = _to_float(row.get("total_state"))
                reached_pct = min(_safe_pct(reached, total_state / 4.0 if total_state else 0), 100.0)
                open_pct = _safe_pct(opened, reached)
                state_health = ((reached_pct / 100.0) + (open_pct / 100.0) + (open_pct / 100.0)) / 3.0 * 100
                label = "Low" if state_health < 40 else "Medium" if state_health < 60 else "Good"
                state_attention.append(
                    {
                        "state": row.get("state_normalized"),
                        "open_pct": round(open_pct, 1),
                        "reached_pct": round(reached_pct, 1),
                        "label": label,
                    }
                )

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

            weekly_best = max(
                health_rows,
                key=lambda r: _engagement_health_score(
                    _to_float(r.get("doctors_reached_unique")),
                    _to_float(r.get("doctors_opened_unique")),
                    _to_float(r.get("doctors_consumed_unique")),
                    total_doctors,
                ),
            )
            bench_rows = _fetch_dicts(
                """
                SELECT avg_campaign_health_score
                FROM gold_global.benchmark_last_10_campaigns
                ORDER BY as_of_date DESC
                LIMIT 1
                """
            )
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
                "title": f"Week {weekly_best.get('week_index')} Best",
                "reached": _to_int(_to_float(weekly_best.get("doctors_reached_unique"))),
                "opened": _to_int(_to_float(weekly_best.get("doctors_opened_unique"))),
                "video": _to_int(_to_float(weekly_best.get("video_viewed_50_unique"))),
                "pdf": _to_int(_to_float(weekly_best.get("pdf_download_unique"))),
                "reached_pct": round(_safe_pct(_to_float(weekly_best.get("doctors_reached_unique")), total_doctors), 1),
                "opened_pct": round(_safe_pct(_to_float(weekly_best.get("doctors_opened_unique")), _to_float(weekly_best.get("doctors_reached_unique"))), 1),
                "video_pct": round(_safe_pct(_to_float(weekly_best.get("video_viewed_50_unique")), _to_float(weekly_best.get("doctors_opened_unique"))), 1),
                "pdf_pct": round(_safe_pct(_to_float(weekly_best.get("pdf_download_unique")), _to_float(weekly_best.get("doctors_opened_unique"))), 1),
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
                                COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.pdf_download_first_ts,'') IS NOT NULL) AS pdf
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
                                  CASE WHEN d.total_doctors=0 THEN 0 ELSE LEAST(x.reached::numeric / d.total_doctors, 1.0) END * 0.5
                                  + CASE WHEN d.total_doctors=0 THEN 0 ELSE LEAST(x.opened::numeric / d.total_doctors, 1.0) END * 0.25
                                  + CASE WHEN d.total_doctors=0 THEN 0 ELSE LEAST((GREATEST(x.video, x.pdf))::numeric / d.total_doctors, 1.0) END * 0.25
                                ) * 100 AS health_score
                            FROM campaign_actions x
                            JOIN campaign_doctor_base d ON d.brand_campaign_id = x.brand_campaign_id
                        )
                        SELECT *
                        FROM campaign_stats
                        ORDER BY health_score DESC, reached DESC, opened DESC
                        LIMIT 1
                        """
                    )
                except (ProgrammingError, OperationalError):
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
                "weekly_health": round(weekly_health, 1),
                "weekly_wow": round(wow_weekly, 1),
                "weekly_benchmark_label": "Average" if 40 <= weekly_health < 60 else ("Good" if weekly_health >= 60 else "Low"),
                "weekly_color": _health_color(weekly_health),
                "kpi_reached": _to_int(latest_reached),
                "kpi_opened": _to_int(latest_opened),
                "kpi_video": _to_int(latest_video),
                "kpi_pdf": _to_int(latest_pdf),
                "kpi_reached_pct": round(reached_pct_total, 1),
                "kpi_opened_pct": round(opened_pct_reached, 1),
                "kpi_video_pct": round(video_pct_opened, 1),
                "kpi_pdf_pct": round(pdf_pct_opened, 1),
                "week_of": f"Week {current_week_idx} ({latest_week.get('week_start_date')} to {latest_week.get('week_end_date')})",
            }

        field_rep_summary = {
            "total_reps": len(field_rep_insights),
            "total_doctors_assigned": reporting_total_doctors,
            "doctors_sent": sum(_to_int(row.get("doctors_sent")) for row in field_rep_insights),
            "doctors_viewed": sum(_to_int(row.get("doctors_viewed")) for row in field_rep_insights),
            "doctors_video_played": sum(_to_int(row.get("doctors_video_played")) for row in field_rep_insights),
            "doctors_pdf_downloaded": sum(_to_int(row.get("doctors_pdf_downloaded")) for row in field_rep_insights),
            "assignment_issue_count": sum(1 for row in field_rep_insights if row.get("assignment_note")),
        }

    except Exception as exc:
        error_message = str(exc)

    trend_source_rows = weekly_rows if week_filter else data_weekly_rows
    trend_labels = [f"Week {r.get('week_index')}" for r in trend_source_rows]
    reached_pct_series = [_safe_pct(_to_float(r.get("doctors_reached_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in trend_source_rows]
    opened_pct_series = [_safe_pct(_to_float(r.get("doctors_opened_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in trend_source_rows]
    pdf_pct_series = [_safe_pct(_to_float(r.get("pdf_download_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in trend_source_rows]
    video_pct_series = [_safe_pct(_to_float(r.get("video_viewed_50_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in trend_source_rows]

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
        "action_panel": action_panel,
        "field_rep_insights": field_rep_insights,
        "field_rep_summary": field_rep_summary,
        "collateral_cards": collateral_cards,
        "trend_labels": trend_labels,
        "reached_pct_series": [round(v, 1) for v in reached_pct_series],
        "opened_pct_series": [round(v, 1) for v in opened_pct_series],
        "pdf_pct_series": [round(v, 1) for v in pdf_pct_series],
        "video_pct_series": [round(v, 1) for v in video_pct_series],
        "week_options": week_options,
        "selected_week": week_filter,
        **context_metrics,
    }


def campaign_overview(request: HttpRequest, brand_campaign_id: str | None = None):
    if not brand_campaign_id:
        return redirect("menu")

    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    access = build_report_access("inclinic", normalized_campaign_id)
    if not request.session.get(access.session_key) and not request.session.get(f"auth_{normalized_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None

    context = _build_report_context(normalized_campaign_id, week_filter)
    return render(request, "dashboard/overview.html", context)


def export_report(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    access = build_report_access("inclinic", normalized_campaign_id)
    if not request.session.get(access.session_key) and not request.session.get(f"auth_{normalized_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    context = _build_report_context(normalized_campaign_id, week_filter)
    context["export_mode"] = True
    return render(request, "dashboard/overview.html", context)
