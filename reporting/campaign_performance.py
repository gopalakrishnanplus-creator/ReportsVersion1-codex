from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from django.db import connection


PE_GLOBAL_SCHEMA = "gold_pe_global"
PE_SILVER_SCHEMA = "silver_pe"
RFA_GOLD_SCHEMA = "gold_sapa"

SYSTEM_LABELS = {
    "rfa": "RFA (Screening / Monitoring)",
    "in_clinic": "InClinic (In-Clinic Sharing)",
    "patient_education": "PE (Patient Education)",
    "entry_point_navigation": "Entry Point / Navigation",
}

SYSTEM_ORDER = ["rfa", "in_clinic", "patient_education", "entry_point_navigation"]
RFA_ALIAS_KEYS = {
    "growthclinic",
    "growthclinicprogram",
    "rfagrowthclinic",
    "sapagrowth",
    "sapagrowthclinic",
}


class CampaignPerformanceNotFound(Exception):
    """Raised when a campaign cannot be resolved from known campaign sources."""


@dataclass(frozen=True)
class CampaignConfig:
    campaign_id: str
    campaign_name: str
    system_rfa: bool
    system_ic: bool
    system_pe: bool
    has_entry_navigation: bool
    banner_target_url: str = ""
    doctor_recruitment_link: str = ""
    add_to_campaign_message: str = ""
    brand_manager_login_link: str = ""
    brand_manager_email: str = ""


@dataclass(frozen=True)
class CampaignReference:
    requested_id: str
    lookup_key: str
    brand_campaign_id: str | None = None
    brand_campaign_name: str = ""
    brand_name: str = ""
    in_clinic_schema: str | None = None
    resolved_campaign_id: str | None = None
    pe_campaign_id: str | None = None
    pe_campaign_normalized: str | None = None
    pe_campaign_name: str = ""
    pe_schema: str | None = None
    pe_dim_campaign: dict[str, Any] | None = None
    campaign_config: CampaignConfig | None = None


def _normalized_sql(column: str) -> str:
    return f"lower(regexp_replace(COALESCE(btrim({column}), ''), '[^a-zA-Z0-9]', '', 'g'))"


def _normalize_lookup(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(value or "").strip().lower())


def _safe_identifier(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _fetch_rows(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _fetch_one(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> dict[str, Any]:
    rows = _fetch_rows(sql, params)
    return rows[0] if rows else {}


def _table_exists(schema: str, table: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", [f"{schema}.{table}"])
        return cursor.fetchone()[0] is not None


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes"}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return default


def _safe_pct(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return round((numerator / denominator) * 100.0, 1)


def _format_number(value: Any) -> str:
    number = _to_float(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.1f}"


def _format_pct(value: Any) -> str:
    return f"{_to_float(value):.1f}%"


def _format_score(value: Any) -> str:
    return f"{_to_float(value):.1f}"


def _pretty_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(text[:19], fmt).strftime("%b %d, %Y")
        except ValueError:
            continue
    return text


def _reporting_window(start: Any, end: Any) -> str:
    start_label = _pretty_date(start)
    end_label = _pretty_date(end)
    if start_label and end_label:
        return f"{start_label} to {end_label}"
    return start_label or end_label or "No dated activity yet"


def _metric(
    key: str,
    label: str,
    value: Any,
    *,
    display_value: str | None = None,
    helper_text: str | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "value": value,
        "display_value": display_value if display_value is not None else _format_number(value),
        "helper_text": helper_text or "",
    }


def _series(key: str, label: str, values: list[Any], color: str) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "color": color,
        "values": [_to_float(value) for value in values],
    }


def _trend(label: str, categories: list[str], series: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not categories or not series:
        return None
    return {"label": label, "categories": categories, "series": series}


def _meta(label: str, value: Any) -> dict[str, str] | None:
    text = str(value or "").strip()
    if not text:
        return None
    return {"label": label, "value": text}


def _query_schema_rows(
    schema: str,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: list[Any] | tuple[Any, ...] | None = None,
    order_by: str = "",
) -> list[dict[str, Any]]:
    safe_schema = _safe_identifier(schema)
    safe_table = _safe_identifier(table)
    sql = f"SELECT {', '.join(columns)} FROM {safe_schema}.{safe_table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    return _fetch_rows(sql, params)


def _query_schema_one(
    schema: str,
    table: str,
    columns: list[str],
    where_sql: str = "",
    params: list[Any] | tuple[Any, ...] | None = None,
) -> dict[str, Any]:
    rows = _query_schema_rows(schema, table, columns, where_sql, params)
    return rows[0] if rows else {}


def _resolve_in_clinic_campaign(lookup_key: str) -> dict[str, Any]:
    if not _table_exists("gold_global", "campaign_registry"):
        return {}

    brand_norm = _normalized_sql("r.brand_campaign_id")
    sql = f"""
        SELECT
            r.brand_campaign_id,
            r.gold_schema_name,
            NULLIF(btrim(m.campaign_id_resolved), '') AS resolved_campaign_id,
            COALESCE(
                MIN(CASE WHEN cm.name IS NULL OR btrim(cm.name) = '' OR lower(btrim(cm.name)) = 'null' THEN NULL ELSE cm.name END),
                MIN(CASE WHEN cc.name IS NULL OR btrim(cc.name) = '' OR lower(btrim(cc.name)) = 'null' THEN NULL ELSE cc.name END),
                r.brand_campaign_id
            ) AS campaign_name,
            COALESCE(
                MIN(CASE WHEN cm.brand_name IS NULL OR btrim(cm.brand_name) = '' OR lower(btrim(cm.brand_name)) = 'null' THEN NULL ELSE cm.brand_name END),
                MIN(CASE WHEN cm.company_name IS NULL OR btrim(cm.company_name) = '' OR lower(btrim(cm.company_name)) = 'null' THEN NULL ELSE cm.company_name END),
                ''
            ) AS brand_name
        FROM gold_global.campaign_registry r
        LEFT JOIN silver.map_brand_campaign_to_campaign m
          ON {brand_norm} = {_normalized_sql("m.brand_campaign_id")}
        LEFT JOIN bronze.campaign_campaign cc
          ON cc.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
        LEFT JOIN bronze.campaign_management_campaign cm
          ON {_normalized_sql("cm.brand_campaign_id")} = {brand_norm}
          OR cm.id::text = btrim(r.brand_campaign_id)
          OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
        WHERE {brand_norm} = %s
        GROUP BY r.brand_campaign_id, r.gold_schema_name, m.campaign_id_resolved
        LIMIT 1
    """
    return _fetch_one(sql, [lookup_key])


def _resolve_pe_campaign(candidate_keys: list[str]) -> dict[str, Any]:
    if not candidate_keys or not _table_exists(PE_GLOBAL_SCHEMA, "campaign_registry"):
        return {}
    distinct_keys = [key for key in dict.fromkeys(candidate_keys) if key]
    placeholders = ", ".join(["%s"] * len(distinct_keys))
    primary = distinct_keys[0]
    sql = f"""
        SELECT
            campaign_id_original,
            campaign_id_normalized,
            campaign_name,
            brand_name,
            gold_schema_name
        FROM {PE_GLOBAL_SCHEMA}.campaign_registry
        WHERE {_normalized_sql("campaign_id_normalized")} IN ({placeholders})
           OR {_normalized_sql("campaign_id_original")} IN ({placeholders})
        ORDER BY
            CASE
                WHEN {_normalized_sql("campaign_id_normalized")} = %s THEN 0
                WHEN {_normalized_sql("campaign_id_original")} = %s THEN 1
                ELSE 2
            END,
            campaign_name
        LIMIT 1
    """
    return _fetch_one(sql, [*distinct_keys, *distinct_keys, primary, primary])


def _resolve_pe_dim_campaign(candidate_keys: list[str]) -> dict[str, Any]:
    if not candidate_keys or not _table_exists(PE_SILVER_SCHEMA, "dim_campaign"):
        return {}
    distinct_keys = [key for key in dict.fromkeys(candidate_keys) if key]
    placeholders = ", ".join(["%s"] * len(distinct_keys))
    primary = distinct_keys[0]
    sql = f"""
        SELECT
            campaign_id_original,
            campaign_id_normalized,
            campaign_name,
            doctors_supported,
            banner_target_url,
            wa_addition,
            email_registration,
            local_video_cluster_name
        FROM {PE_SILVER_SCHEMA}.dim_campaign
        WHERE {_normalized_sql("campaign_id_normalized")} IN ({placeholders})
           OR {_normalized_sql("campaign_id_original")} IN ({placeholders})
        ORDER BY
            CASE
                WHEN {_normalized_sql("campaign_id_normalized")} = %s THEN 0
                WHEN {_normalized_sql("campaign_id_original")} = %s THEN 1
                ELSE 2
            END,
            campaign_name
        LIMIT 1
    """
    return _fetch_one(sql, [*distinct_keys, *distinct_keys, primary, primary])


def _resolve_campaign_config(candidate_keys: list[str]) -> CampaignConfig | None:
    if not candidate_keys or not _table_exists("bronze", "campaign_campaign"):
        return None
    distinct_keys = [key for key in dict.fromkeys(candidate_keys) if key]
    placeholders = ", ".join(["%s"] * len(distinct_keys))
    primary = distinct_keys[0]
    sql = f"""
        SELECT
            id::text AS campaign_id,
            name,
            system_rfa,
            system_pe,
            system_ic,
            banner_target_url,
            doctor_recruitment_link,
            add_to_campaign_message,
            brand_manager_login_link,
            brand_manager_email
        FROM bronze.campaign_campaign
        WHERE {_normalized_sql("id::text")} IN ({placeholders})
        ORDER BY CASE WHEN {_normalized_sql("id::text")} = %s THEN 0 ELSE 1 END, name
        LIMIT 1
    """
    row = _fetch_one(sql, [*distinct_keys, primary])
    if not row:
        return None
    banner_target_url = str(row.get("banner_target_url") or "").strip()
    doctor_recruitment_link = str(row.get("doctor_recruitment_link") or "").strip()
    add_to_campaign_message = str(row.get("add_to_campaign_message") or "").strip()
    return CampaignConfig(
        campaign_id=str(row.get("campaign_id") or ""),
        campaign_name=str(row.get("name") or row.get("campaign_id") or ""),
        system_rfa=_is_truthy(row.get("system_rfa")),
        system_ic=_is_truthy(row.get("system_ic")),
        system_pe=_is_truthy(row.get("system_pe")),
        has_entry_navigation=bool(banner_target_url or doctor_recruitment_link or add_to_campaign_message),
        banner_target_url=banner_target_url,
        doctor_recruitment_link=doctor_recruitment_link,
        add_to_campaign_message=add_to_campaign_message,
        brand_manager_login_link=str(row.get("brand_manager_login_link") or "").strip(),
        brand_manager_email=str(row.get("brand_manager_email") or "").strip(),
    )


def _resolve_campaign_reference(campaign_id: str) -> CampaignReference:
    requested = str(campaign_id or "").strip()
    lookup_key = _normalize_lookup(requested)
    if not lookup_key:
        raise CampaignPerformanceNotFound("A campaign identifier is required.")

    in_clinic = _resolve_in_clinic_campaign(lookup_key)
    candidate_keys = [lookup_key]
    if in_clinic:
        candidate_keys.extend(
            [
                _normalize_lookup(in_clinic.get("brand_campaign_id")),
                _normalize_lookup(in_clinic.get("resolved_campaign_id")),
            ]
        )
    candidate_keys = [key for key in dict.fromkeys(candidate_keys) if key]

    campaign_config = _resolve_campaign_config(candidate_keys)
    if campaign_config:
        candidate_keys.insert(0, _normalize_lookup(campaign_config.campaign_id))
    candidate_keys = [key for key in dict.fromkeys(candidate_keys) if key]

    pe_registry = _resolve_pe_campaign(candidate_keys)
    if pe_registry:
        candidate_keys.extend(
            [
                _normalize_lookup(pe_registry.get("campaign_id_original")),
                _normalize_lookup(pe_registry.get("campaign_id_normalized")),
            ]
        )
    candidate_keys = [key for key in dict.fromkeys(candidate_keys) if key]
    pe_dim_campaign = _resolve_pe_dim_campaign(candidate_keys)

    if not in_clinic and not pe_registry and not campaign_config and lookup_key not in RFA_ALIAS_KEYS:
        raise CampaignPerformanceNotFound(f"Campaign '{requested}' could not be resolved.")

    brand_name = str(in_clinic.get("brand_name") or "") or str((pe_registry or {}).get("brand_name") or "")
    return CampaignReference(
        requested_id=requested,
        lookup_key=lookup_key,
        brand_campaign_id=str(in_clinic.get("brand_campaign_id") or "") or None,
        brand_campaign_name=str(in_clinic.get("campaign_name") or ""),
        brand_name=brand_name,
        in_clinic_schema=str(in_clinic.get("gold_schema_name") or "") or None,
        resolved_campaign_id=str(in_clinic.get("resolved_campaign_id") or "") or (campaign_config.campaign_id if campaign_config else None),
        pe_campaign_id=str((pe_registry or {}).get("campaign_id_original") or "") or None,
        pe_campaign_normalized=str((pe_registry or {}).get("campaign_id_normalized") or "") or None,
        pe_campaign_name=str((pe_registry or {}).get("campaign_name") or ""),
        pe_schema=str((pe_registry or {}).get("gold_schema_name") or "") or None,
        pe_dim_campaign=pe_dim_campaign or None,
        campaign_config=campaign_config,
    )


def _configured_system_keys(reference: CampaignReference) -> list[str]:
    config = reference.campaign_config
    if config is not None:
        keys = []
        if config.system_rfa:
            keys.append("rfa")
        if config.system_ic:
            keys.append("in_clinic")
        if config.system_pe:
            keys.append("patient_education")
        if config.has_entry_navigation:
            keys.append("entry_point_navigation")
        return keys

    inferred = []
    if reference.lookup_key in RFA_ALIAS_KEYS:
        inferred.append("rfa")
    if reference.in_clinic_schema:
        inferred.append("in_clinic")
    if reference.pe_schema:
        inferred.append("patient_education")
    if reference.pe_dim_campaign and (
        str((reference.pe_dim_campaign or {}).get("banner_target_url") or "").strip()
        or str((reference.pe_dim_campaign or {}).get("wa_addition") or "").strip()
        or str((reference.pe_dim_campaign or {}).get("email_registration") or "").strip()
    ):
        inferred.append("entry_point_navigation")
    return inferred


def _base_meta(reference: CampaignReference) -> list[dict[str, str]]:
    config = reference.campaign_config
    return [
        item
        for item in [
            _meta("Requested campaign ID", reference.requested_id),
            _meta("Resolved campaign ID", reference.resolved_campaign_id or (config.campaign_id if config else "")),
            _meta("Brand campaign ID", reference.brand_campaign_id),
        ]
        if item
    ]


def _empty_section(
    *,
    key: str,
    subtitle: str,
    reference: CampaignReference,
    metrics: list[dict[str, Any]],
    extra_meta: list[dict[str, str] | None] | None = None,
    data_status: str = "no_data",
) -> dict[str, Any]:
    meta_items = _base_meta(reference)
    meta_items.extend(item for item in (extra_meta or []) if item)
    return {
        "key": key,
        "type": "system",
        "label": SYSTEM_LABELS[key],
        "subtitle": subtitle,
        "metrics": metrics,
        "trend": None,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": 0,
            "participating_clinics": 0,
            "adoption_rate": 0.0,
        },
        "data_status": data_status,
    }


def _build_in_clinic_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Shares, opens, and content consumption from the campaign's in-clinic collateral journey."
    zero_metrics = [
        _metric("targeted_clinics", "Targeted Clinics", 0),
        _metric("participating_clinics", "Participating Clinics", 0),
        _metric("adoption_rate", "Adoption Rate", 0, display_value="0.0%"),
        _metric("total_shares", "Total Shares", 0),
        _metric("opened_clinics", "Opened Clinics", 0),
        _metric("content_engagement", "Content Engagement", 0, display_value="0 video / 0 pdf"),
        _metric("health_score", "Latest Health Score", 0, display_value="0.0"),
    ]
    if not reference.brand_campaign_id or not reference.in_clinic_schema or not _table_exists(reference.in_clinic_schema, "kpi_weekly_summary"):
        return _empty_section(
            key="in_clinic",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[_meta("Data status", "Configured in campaign DB; in-clinic reporting rows are not published yet.")],
        )

    latest_summary = _fetch_one(
        """
        SELECT
            COUNT(DISTINCT base.doctor_identity_key) AS targeted_clinics,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE COALESCE(NULLIF(action.reached_first_ts, ''), NULLIF(action.opened_first_ts, ''), NULLIF(action.video_gt_50_first_ts, ''), NULLIF(action.pdf_download_first_ts, '')) IS NOT NULL
            ) AS participating_clinics,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.opened_first_ts, '') IS NOT NULL
            ) AS opened_clinics,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.video_gt_50_first_ts, '') IS NOT NULL
            ) AS video_engaged_clinics,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.pdf_download_first_ts, '') IS NOT NULL
            ) AS pdf_download_clinics,
            (
                SELECT COUNT(*)
                FROM silver.fact_collateral_transaction tx
                WHERE tx.brand_campaign_id = %s
            ) AS total_shares
        FROM silver.bridge_brand_campaign_doctor_base base
        LEFT JOIN silver.doctor_action_first_seen action
          ON action.brand_campaign_id = base.brand_campaign_id
         AND action.doctor_identity_key = base.doctor_identity_key
        WHERE base.brand_campaign_id = %s
    """,
        [reference.brand_campaign_id, reference.brand_campaign_id],
    )
    weekly_rows = _query_schema_rows(
        reference.in_clinic_schema,
        "kpi_weekly_summary",
        [
            "week_index",
            "week_start_date",
            "week_end_date",
            "doctors_reached_unique",
            "doctors_opened_unique",
            "video_viewed_50_unique",
            "pdf_download_unique",
            "weekly_health_score",
        ],
        order_by="week_index",
    )
    latest_week = weekly_rows[-1] if weekly_rows else {}
    targeted = _to_int(latest_summary.get("targeted_clinics"))
    participating = _to_int(latest_summary.get("participating_clinics"))
    opened = _to_int(latest_summary.get("opened_clinics"))
    video = _to_int(latest_summary.get("video_engaged_clinics"))
    pdf = _to_int(latest_summary.get("pdf_download_clinics"))
    total_shares = _to_int(latest_summary.get("total_shares"))
    adoption_rate = _safe_pct(participating, targeted)
    health_score = _to_float(latest_week.get("weekly_health_score"))
    metrics = [
        _metric("targeted_clinics", "Targeted Clinics", targeted),
        _metric("participating_clinics", "Participating Clinics", participating),
        _metric("adoption_rate", "Adoption Rate", adoption_rate, display_value=_format_pct(adoption_rate)),
        _metric("total_shares", "Total Shares", total_shares),
        _metric("opened_clinics", "Opened Clinics", opened),
        _metric("content_engagement", "Content Engagement", max(video, pdf), display_value=f"{video} video / {pdf} pdf"),
        _metric("health_score", "Latest Health Score", health_score, display_value=_format_score(health_score)),
    ]
    trend = _trend(
        "Weekly engagement trend",
        [f"Week {row.get('week_index')}" for row in weekly_rows],
        [
            _series("reached", "Reached", [row.get("doctors_reached_unique") for row in weekly_rows], "#0f766e"),
            _series("opened", "Opened", [row.get("doctors_opened_unique") for row in weekly_rows], "#0ea5e9"),
            _series("video", "Viewed >50%", [row.get("video_viewed_50_unique") for row in weekly_rows], "#f59e0b"),
            _series("pdf", "PDF Downloads", [row.get("pdf_download_unique") for row in weekly_rows], "#ef4444"),
        ],
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("Reporting window", _reporting_window(weekly_rows[0].get("week_start_date") if weekly_rows else "", weekly_rows[-1].get("week_end_date") if weekly_rows else "")),
            _meta("Data status", "Ready" if any([targeted, participating, opened, video, pdf, total_shares]) else "Configured in campaign DB; no in-clinic activity yet."),
        ]
        if item
    )
    return {
        "key": "in_clinic",
        "type": "system",
        "label": SYSTEM_LABELS["in_clinic"],
        "subtitle": subtitle,
        "metrics": metrics,
        "trend": trend,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": targeted,
            "participating_clinics": participating,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([targeted, participating, opened, video, pdf, total_shares]) else "no_data",
    }


def _build_patient_education_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Doctor activation, caregiver reach, and playback quality from the Patient Education system."
    zero_metrics = [
        _metric("enrolled_clinics", "Enrolled Clinics", 0),
        _metric("sharing_clinics", "Sharing Clinics", 0),
        _metric("adoption_rate", "Adoption Rate", 0, display_value="0.0%"),
        _metric("total_shares", "Total Shares", 0),
        _metric("caregivers_reached", "Caregivers Reached", 0),
        _metric("playback_funnel", "Playback Funnel", 0, display_value="0 played / 0 >50% / 0 complete"),
        _metric("health_score", "Campaign Health", 0, display_value="0.0"),
    ]
    if not reference.pe_schema or not _table_exists(reference.pe_schema, "kpi_campaign_health_summary"):
        return _empty_section(
            key="patient_education",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[_meta("Data status", "Configured in campaign DB; PE reporting rows are not published yet.")],
        )

    summary = _query_schema_one(
        reference.pe_schema,
        "kpi_campaign_health_summary",
        [
            "as_of_date",
            "enrolled_doctors_current",
            "doctors_sharing_unique_cumulative",
            "shares_total_cumulative",
            "unique_recipient_references_cumulative",
            "shares_played_cumulative",
            "shares_viewed_50_cumulative",
            "shares_viewed_100_cumulative",
            "activation_pct",
            "play_rate_pct",
            "engagement_50_pct",
            "completion_pct",
            "campaign_health_score",
        ],
    )
    weekly_rows = (
        _query_schema_rows(
            reference.pe_schema,
            "kpi_weekly_summary",
            [
                "week_index",
                "week_start_date",
                "week_end_date",
                "activation_pct",
                "play_rate_pct",
                "engagement_50_pct",
                "completion_pct",
                "weekly_health_score",
            ],
            order_by="week_index",
        )
        if _table_exists(reference.pe_schema, "kpi_weekly_summary")
        else []
    )
    enrolled = _to_int(summary.get("enrolled_doctors_current"))
    sharing = _to_int(summary.get("doctors_sharing_unique_cumulative"))
    adoption_rate = _to_float(summary.get("activation_pct"))
    total_shares = _to_int(summary.get("shares_total_cumulative"))
    recipients = _to_int(summary.get("unique_recipient_references_cumulative"))
    played = _to_int(summary.get("shares_played_cumulative"))
    viewed_50 = _to_int(summary.get("shares_viewed_50_cumulative"))
    completed = _to_int(summary.get("shares_viewed_100_cumulative"))
    health_score = _to_float(summary.get("campaign_health_score"))
    metrics = [
        _metric("enrolled_clinics", "Enrolled Clinics", enrolled),
        _metric("sharing_clinics", "Sharing Clinics", sharing),
        _metric("adoption_rate", "Adoption Rate", adoption_rate, display_value=_format_pct(adoption_rate)),
        _metric("total_shares", "Total Shares", total_shares),
        _metric("caregivers_reached", "Caregivers Reached", recipients),
        _metric("playback_funnel", "Playback Funnel", viewed_50, display_value=f"{played} played / {viewed_50} >50% / {completed} complete"),
        _metric("health_score", "Campaign Health", health_score, display_value=_format_score(health_score)),
    ]
    trend = _trend(
        "Monthly/weekly PE health trend",
        [f"Week {row.get('week_index')}" for row in weekly_rows],
        [
            _series("activation_pct", "Activation %", [row.get("activation_pct") for row in weekly_rows], "#0f766e"),
            _series("play_rate_pct", "Play Rate %", [row.get("play_rate_pct") for row in weekly_rows], "#0ea5e9"),
            _series("engagement_50_pct", "Viewed >50% %", [row.get("engagement_50_pct") for row in weekly_rows], "#f59e0b"),
            _series("completion_pct", "Completion %", [row.get("completion_pct") for row in weekly_rows], "#ef4444"),
        ],
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("PE campaign", reference.pe_campaign_id or reference.pe_campaign_normalized),
            _meta("Bundle", (reference.pe_dim_campaign or {}).get("local_video_cluster_name")),
            _meta("As of", _pretty_date(summary.get("as_of_date"))),
            _meta("Data status", "Ready" if any([enrolled, sharing, total_shares, recipients, played, viewed_50, completed]) else "Configured in campaign DB; no PE activity yet."),
        ]
        if item
    )
    return {
        "key": "patient_education",
        "type": "system",
        "label": SYSTEM_LABELS["patient_education"],
        "subtitle": subtitle,
        "metrics": metrics,
        "trend": trend,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": enrolled,
            "participating_clinics": sharing,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([enrolled, sharing, total_shares, recipients, played, viewed_50, completed]) else "no_data",
    }


def _rfa_campaign_match_keys(reference: CampaignReference) -> set[str]:
    config = reference.campaign_config
    return {
        key
        for key in {
            reference.lookup_key,
            _normalize_lookup(reference.brand_campaign_id),
            _normalize_lookup(reference.resolved_campaign_id),
            _normalize_lookup(reference.pe_campaign_id),
            _normalize_lookup(reference.pe_campaign_name),
            _normalize_lookup((config.campaign_id if config else "")),
            _normalize_lookup((config.campaign_name if config else "")),
        }
        if key
    }


def _build_rfa_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Screening throughput, follow-up operations, and risk-tag monitoring from the RFA workflow."
    zero_metrics = [
        _metric("onboarded_clinics", "Onboarded Clinics", 0),
        _metric("active_clinics", "Active Clinics", 0),
        _metric("adoption_rate", "Adoption Rate", 0, display_value="0.0%"),
        _metric("certified_clinics", "Certified Clinics", 0),
        _metric("screenings", "Total Screenings", 0),
        _metric("red_tags", "Red Tags", 0),
        _metric("care_followup", "Follow-ups / Reminders", 0, display_value="0 follow-ups / 0 reminders"),
    ]
    if not (_rfa_campaign_match_keys(reference) & RFA_ALIAS_KEYS):
        return _empty_section(
            key="rfa",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[
                _meta(
                    "Data status",
                    "Configured in campaign DB, but current SAPA/RFA source tables do not expose a campaign-level join for this campaign yet.",
                )
            ],
            data_status="not_attributed",
        )
    if not _table_exists(RFA_GOLD_SCHEMA, "dashboard_summary_snapshot"):
        return _empty_section(
            key="rfa",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[_meta("Data status", "Configured in campaign DB; RFA reporting rows are not published yet.")],
        )

    summary = _fetch_one(
        f"""
        SELECT
            as_of_date,
            onboarded_doctors_cumulative,
            active_clinics_current,
            certified_clinics_current,
            total_screenings_cumulative,
            red_tags_cumulative,
            followups_scheduled_cumulative,
            reminders_sent_cumulative
        FROM {RFA_GOLD_SCHEMA}.dashboard_summary_snapshot
        LIMIT 1
    """
    )
    trend_rows = (
        _fetch_rows(
            f"""
            SELECT
                to_char(date_trunc('week', submitted_at::timestamp), 'YYYY-MM-DD') AS week_bucket,
                COUNT(*) AS screenings,
                COUNT(*) FILTER (WHERE lower(COALESCE(overall_flag_code, '')) = 'red') AS red_tags,
                COUNT(*) FILTER (WHERE lower(COALESCE(overall_flag_code, '')) = 'yellow') AS yellow_tags
            FROM {RFA_GOLD_SCHEMA}.rpt_screening_detail
            WHERE submitted_at IS NOT NULL
              AND btrim(submitted_at) <> ''
            GROUP BY 1
            ORDER BY 1
        """
        )
        if _table_exists(RFA_GOLD_SCHEMA, "rpt_screening_detail")
        else []
    )
    onboarded = _to_int(summary.get("onboarded_doctors_cumulative"))
    active = _to_int(summary.get("active_clinics_current"))
    certified = _to_int(summary.get("certified_clinics_current"))
    screenings = _to_int(summary.get("total_screenings_cumulative"))
    red_tags = _to_int(summary.get("red_tags_cumulative"))
    followups = _to_int(summary.get("followups_scheduled_cumulative"))
    reminders = _to_int(summary.get("reminders_sent_cumulative"))
    adoption_rate = _safe_pct(active, onboarded)
    metrics = [
        _metric("onboarded_clinics", "Onboarded Clinics", onboarded),
        _metric("active_clinics", "Active Clinics", active),
        _metric("adoption_rate", "Adoption Rate", adoption_rate, display_value=_format_pct(adoption_rate)),
        _metric("certified_clinics", "Certified Clinics", certified),
        _metric("screenings", "Total Screenings", screenings),
        _metric("red_tags", "Red Tags", red_tags),
        _metric("care_followup", "Follow-ups / Reminders", followups, display_value=f"{followups} follow-ups / {reminders} reminders"),
    ]
    trend = _trend(
        "Weekly screening trend",
        [_pretty_date(row.get("week_bucket")) for row in trend_rows],
        [
            _series("screenings", "Screenings", [row.get("screenings") for row in trend_rows], "#0f766e"),
            _series("red_tags", "Red Tags", [row.get("red_tags") for row in trend_rows], "#ef4444"),
            _series("yellow_tags", "Yellow Tags", [row.get("yellow_tags") for row in trend_rows], "#f59e0b"),
        ],
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("As of", _pretty_date(summary.get("as_of_date"))),
            _meta("Attribution", "Resolved to the currently published Growth Clinic RFA dataset."),
        ]
        if item
    )
    return {
        "key": "rfa",
        "type": "system",
        "label": SYSTEM_LABELS["rfa"],
        "subtitle": subtitle,
        "metrics": metrics,
        "trend": trend,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": onboarded,
            "participating_clinics": active,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([onboarded, active, certified, screenings, red_tags, followups, reminders]) else "no_data",
    }


def _build_entry_point_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Entry-channel and navigation engagement attributed to campaign links, banner journeys, and navigation pathways."
    dim_campaign = reference.pe_dim_campaign or {}
    config = reference.campaign_config
    banner_target_url = str(dim_campaign.get("banner_target_url") or (config.banner_target_url if config else "")).strip()
    has_whatsapp = _is_truthy(dim_campaign.get("wa_addition"))
    has_email = _is_truthy(dim_campaign.get("email_registration"))
    entry_channels = [label for enabled, label in ((bool(banner_target_url), "Banner"), (has_whatsapp, "WhatsApp"), (has_email, "Email")) if enabled]
    zero_metrics = [
        _metric("entry_channels", "Entry Channels", len(entry_channels), display_value=" / ".join(entry_channels) if entry_channels else "Configured"),
        _metric("banner_clicks", "Banner Clicks", 0),
        _metric("participating_clinics", "Participating Clinics", 0),
        _metric("adoption_rate", "Navigation Adoption", 0, display_value="0.0%"),
        _metric("states_reached", "States Reached", 0),
        _metric("target_url", "Target URL", 1 if banner_target_url else 0, display_value="Configured" if banner_target_url else "Not configured"),
    ]

    campaign_norm = reference.pe_campaign_normalized or _normalize_lookup(reference.pe_campaign_id or reference.resolved_campaign_id)
    if not campaign_norm or not _table_exists(PE_SILVER_SCHEMA, "fact_share_banner_click"):
        return _empty_section(
            key="entry_point_navigation",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[
                _meta("Navigation URL", banner_target_url),
                _meta("Data status", "Configured in campaign DB; navigation click activity is not attributed yet."),
            ],
        )

    click_summary = _fetch_one(
        f"""
        SELECT
            COUNT(*) AS total_clicks,
            COUNT(DISTINCT doctor_key) FILTER (WHERE COALESCE(btrim(doctor_key), '') <> '') AS participating_clinics,
            COUNT(DISTINCT state) FILTER (WHERE COALESCE(btrim(state), '') <> '') AS states_reached,
            MIN(clicked_at_ts) AS period_start,
            MAX(clicked_at_ts) AS period_end
        FROM {PE_SILVER_SCHEMA}.fact_share_banner_click
        WHERE {_normalized_sql('campaign_id_normalized')} = %s
    """,
        [campaign_norm],
    )
    click_trend_rows = _fetch_rows(
        f"""
        SELECT
            to_char(date_trunc('week', clicked_at_ts::timestamp), 'YYYY-MM-DD') AS week_bucket,
            COUNT(*) AS total_clicks,
            COUNT(DISTINCT doctor_key) FILTER (WHERE COALESCE(btrim(doctor_key), '') <> '') AS participating_clinics
        FROM {PE_SILVER_SCHEMA}.fact_share_banner_click
        WHERE {_normalized_sql('campaign_id_normalized')} = %s
          AND clicked_at_ts IS NOT NULL
          AND btrim(clicked_at_ts) <> ''
        GROUP BY 1
        ORDER BY 1
    """,
        [campaign_norm],
    )
    enrolled = _to_int(dim_campaign.get("doctors_supported"))
    if not enrolled and reference.pe_schema and _table_exists(reference.pe_schema, "kpi_campaign_health_summary"):
        enrolled = _to_int(_query_schema_one(reference.pe_schema, "kpi_campaign_health_summary", ["enrolled_doctors_current"]).get("enrolled_doctors_current"))
    participating = _to_int(click_summary.get("participating_clinics"))
    adoption_rate = _safe_pct(participating, enrolled)
    total_clicks = _to_int(click_summary.get("total_clicks"))
    states_reached = _to_int(click_summary.get("states_reached"))
    metrics = [
        _metric("entry_channels", "Entry Channels", len(entry_channels), display_value=" / ".join(entry_channels) if entry_channels else "Configured"),
        _metric("banner_clicks", "Banner Clicks", total_clicks),
        _metric("participating_clinics", "Participating Clinics", participating),
        _metric("adoption_rate", "Navigation Adoption", adoption_rate, display_value=_format_pct(adoption_rate)),
        _metric("states_reached", "States Reached", states_reached),
        _metric("target_url", "Target URL", 1 if banner_target_url else 0, display_value="Configured" if banner_target_url else "Not configured"),
    ]
    trend = _trend(
        "Weekly entry-point activity",
        [_pretty_date(row.get("week_bucket")) for row in click_trend_rows],
        [
            _series("clicks", "Clicks", [row.get("total_clicks") for row in click_trend_rows], "#0ea5e9"),
            _series("clinics", "Participating Clinics", [row.get("participating_clinics") for row in click_trend_rows], "#0f766e"),
        ],
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("Navigation window", _reporting_window(click_summary.get("period_start"), click_summary.get("period_end"))),
            _meta("Navigation URL", banner_target_url),
            _meta("Data status", "Ready" if any([total_clicks, participating, states_reached]) else "Configured in campaign DB; no navigation activity yet."),
        ]
        if item
    )
    return {
        "key": "entry_point_navigation",
        "type": "system",
        "label": SYSTEM_LABELS["entry_point_navigation"],
        "subtitle": subtitle,
        "metrics": metrics,
        "trend": trend,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": enrolled,
            "participating_clinics": participating,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([total_clicks, participating, states_reached]) else "no_data",
    }


def _build_adoption_section(system_sections: list[dict[str, Any]]) -> dict[str, Any]:
    breakdown = [
        {
            "system_key": section.get("key"),
            "label": section.get("label"),
            "eligible_clinics": _to_int((section.get("adoption") or {}).get("eligible_clinics")),
            "participating_clinics": _to_int((section.get("adoption") or {}).get("participating_clinics")),
            "adoption_rate": _to_float((section.get("adoption") or {}).get("adoption_rate")),
        }
        for section in system_sections
    ]
    top_system = max(breakdown, key=lambda item: item.get("adoption_rate", 0), default=None)
    total_eligible = sum(item["eligible_clinics"] for item in breakdown)
    total_participating = sum(item["participating_clinics"] for item in breakdown)
    average_rate = round(sum(item["adoption_rate"] for item in breakdown) / len(breakdown), 1) if breakdown else 0.0
    return {
        "key": "adoption_by_clinics",
        "type": "adoption",
        "label": "Adoption by Clinics",
        "subtitle": "Cross-system participation and clinic adoption rates for the systems selected on this campaign.",
        "metrics": [
            _metric("systems_live", "Systems Selected", len(breakdown)),
            _metric("tracked_clinics", "Tracked Clinics", total_eligible, helper_text="Counted within each selected system's eligible clinic base."),
            _metric("participating_clinics", "Participating Clinics", total_participating, helper_text="Summed across selected systems."),
            _metric("average_adoption_rate", "Average Adoption", average_rate, display_value=_format_pct(average_rate)),
            _metric(
                "best_system",
                "Highest Adoption",
                _to_float((top_system or {}).get("adoption_rate")),
                display_value=f"{(top_system or {}).get('label', 'N/A')} · {_format_pct((top_system or {}).get('adoption_rate', 0))}",
            ),
        ],
        "trend": None,
        "meta": [],
        "breakdown": breakdown,
    }


def build_campaign_performance_payload(campaign_id: str) -> dict[str, Any]:
    reference = _resolve_campaign_reference(campaign_id)
    configured_keys = _configured_system_keys(reference)
    builders = {
        "rfa": _build_rfa_section,
        "in_clinic": _build_in_clinic_section,
        "patient_education": _build_patient_education_section,
        "entry_point_navigation": _build_entry_point_section,
    }
    system_sections = [builders[key](reference) for key in SYSTEM_ORDER if key in configured_keys]
    adoption_section = _build_adoption_section(system_sections)
    configured_systems = [{"key": section["key"], "label": section["label"]} for section in system_sections]
    config = reference.campaign_config
    campaign_name = (
        (config.campaign_name if config else "")
        or reference.brand_campaign_name
        or reference.pe_campaign_name
        or reference.requested_id
    )
    return {
        "campaign": {
            "requested_id": reference.requested_id,
            "campaign_id": (config.campaign_id if config else "") or reference.resolved_campaign_id or reference.brand_campaign_id or reference.pe_campaign_id or reference.requested_id,
            "campaign_name": campaign_name,
            "brand_name": reference.brand_name,
            "identifiers": {
                "brand_campaign_id": reference.brand_campaign_id,
                "resolved_campaign_id": reference.resolved_campaign_id or (config.campaign_id if config else None),
                "pe_campaign_id": reference.pe_campaign_id,
            },
        },
        "system_count": len(configured_systems),
        "configured_systems": configured_systems,
        "available_systems": configured_systems,
        "sections": [*system_sections, adoption_section],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
