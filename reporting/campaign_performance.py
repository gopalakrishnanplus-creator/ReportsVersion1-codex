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
}

SYSTEM_ORDER = ["rfa", "in_clinic", "patient_education"]
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


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "null":
            return text
    return default


def _slugify_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return slug or "unknown"


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


def _campaign_identity(reference: CampaignReference) -> dict[str, Any]:
    config = reference.campaign_config
    campaign_name = (
        (config.campaign_name if config else "")
        or reference.brand_campaign_name
        or reference.pe_campaign_name
        or reference.requested_id
    )
    return {
        "requested_id": reference.requested_id,
        "campaign_id": (config.campaign_id if config else "") or reference.resolved_campaign_id or reference.brand_campaign_id or reference.pe_campaign_id or reference.requested_id,
        "campaign_name": campaign_name,
        "brand_name": reference.brand_name,
        "identifiers": {
            "brand_campaign_id": reference.brand_campaign_id,
            "resolved_campaign_id": reference.resolved_campaign_id or (config.campaign_id if config else None),
            "pe_campaign_id": reference.pe_campaign_id,
        },
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


def _bar(key: str, label: str, value: Any, color: str) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "value": _to_float(value),
        "display_value": _format_number(value),
        "color": color,
    }


def _bar_chart(label: str, bars: list[dict[str, Any]], description: str) -> dict[str, Any] | None:
    clean_bars = [bar for bar in bars if bar]
    if not clean_bars:
        return None
    return {
        "label": label,
        "description": description,
        "bars": clean_bars,
    }


def _table_column(key: str, label: str, align: str = "left") -> dict[str, str]:
    return {"key": key, "label": label, "align": align}


def _table_panel(
    label: str,
    description: str,
    columns: list[dict[str, str]],
    rows: list[dict[str, Any]],
    *,
    empty_message: str,
) -> dict[str, Any]:
    return {
        "label": label,
        "description": description,
        "columns": columns,
        "rows": rows,
        "empty_message": empty_message,
    }


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
        return keys

    inferred = []
    if reference.lookup_key in RFA_ALIAS_KEYS:
        inferred.append("rfa")
    if reference.in_clinic_schema:
        inferred.append("in_clinic")
    if reference.pe_schema:
        inferred.append("patient_education")
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
        "bar_chart": None,
        "table": None,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": 0,
            "active_records": 0,
            "participating_clinics": 0,
            "adoption_rate": 0.0,
        },
        "data_status": data_status,
    }


def _navigation_context(reference: CampaignReference) -> dict[str, Any]:
    dim_campaign = reference.pe_dim_campaign or {}
    config = reference.campaign_config
    banner_target_url = str(dim_campaign.get("banner_target_url") or (config.banner_target_url if config else "")).strip()
    has_whatsapp = _is_truthy(dim_campaign.get("wa_addition"))
    has_email = _is_truthy(dim_campaign.get("email_registration"))
    entry_channels = [
        label
        for enabled, label in (
            (bool(banner_target_url), "Banner"),
            (has_whatsapp, "WhatsApp"),
            (has_email, "Email"),
        )
        if enabled
    ]
    campaign_norm = reference.pe_campaign_normalized or _normalize_lookup(reference.pe_campaign_id or reference.resolved_campaign_id)
    click_summary: dict[str, Any] = {}
    click_trend_rows: list[dict[str, Any]] = []
    if campaign_norm and _table_exists(PE_SILVER_SCHEMA, "fact_share_banner_click"):
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
                COUNT(*) AS total_clicks
            FROM {PE_SILVER_SCHEMA}.fact_share_banner_click
            WHERE {_normalized_sql('campaign_id_normalized')} = %s
              AND clicked_at_ts IS NOT NULL
              AND btrim(clicked_at_ts) <> ''
            GROUP BY 1
            ORDER BY 1
        """,
            [campaign_norm],
        )
    return {
        "entry_channels": entry_channels,
        "banner_target_url": banner_target_url,
        "total_clicks": _to_int(click_summary.get("total_clicks")),
        "participating_clinics": _to_int(click_summary.get("participating_clinics")),
        "states_reached": _to_int(click_summary.get("states_reached")),
        "period_start": click_summary.get("period_start"),
        "period_end": click_summary.get("period_end"),
        "click_trend_rows": click_trend_rows,
    }


def _aggregate_pe_clinic_rows(doctor_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in doctor_rows:
        clinic_group = _first_text(
            source_row.get("city"),
            source_row.get("district"),
            source_row.get("state"),
            default="Unknown",
        )
        clinic = _first_text(
            source_row.get("clinic_name"),
            source_row.get("doctor_display_name"),
            source_row.get("doctor_id"),
            default="Unknown",
        )
        key = (clinic_group, clinic)
        row = grouped.setdefault(
            key,
            {
                "clinic_group": clinic_group,
                "clinic": clinic,
                "video_views": 0,
                "video_completions": 0,
                "cluster_shares": 0,
                "patient_scans": 0,
                "banner_clicks": 0,
            },
        )
        row["video_views"] += _to_int(source_row.get("shares_played_cumulative"))
        row["video_completions"] += _to_int(source_row.get("shares_viewed_100_cumulative"))
        row["cluster_shares"] += _to_int(source_row.get("bundle_shares_cumulative"))
        row["patient_scans"] += _to_int(source_row.get("unique_recipient_references_cumulative"))
        row["banner_clicks"] += _to_int(source_row.get("banner_clicks_cumulative") or source_row.get("banner_clicks"))
    return sorted(
        grouped.values(),
        key=lambda item: (
            -_to_int(item.get("cluster_shares")),
            -_to_int(item.get("video_views")),
            item.get("clinic_group") or "",
            item.get("clinic") or "",
        ),
    )


def _fetch_in_clinic_detail_rows(reference: CampaignReference) -> list[dict[str, Any]]:
    if not reference.brand_campaign_id:
        return []
    rows = _fetch_rows(
        """
        SELECT
            t.doctor_unique_id,
            t.doctor_master_id_resolved,
            t.doctor_identity_key,
            t.doctor_name,
            t.field_rep_email,
            t.opened_event_ts,
            t.video_gt_50_at_ts,
            t.video_100_at_ts,
            t.pdf_download_event_ts,
            t.downloaded_pdf,
            t.pdf_completed,
            t.video_completed,
            t.pdf_last_page_num,
            t.pdf_total_pages_num,
            t.last_video_percentage_num,
            t.video_watch_percentage_num,
            COALESCE(
                NULLIF(btrim(base.state_normalized), ''),
                NULLIF(btrim(d.state_normalized), ''),
                'Unknown'
            ) AS clinic_group
        FROM silver.fact_collateral_transaction t
        LEFT JOIN silver.bridge_brand_campaign_doctor_base base
          ON base.brand_campaign_id = t.brand_campaign_id
         AND base.doctor_identity_key = t.doctor_identity_key
        LEFT JOIN silver.dim_doctor d
          ON d.doctor_identity_key = t.doctor_identity_key
        WHERE t.brand_campaign_id = %s
    """,
        [reference.brand_campaign_id],
    )
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for source_row in rows:
        clinic = _first_text(
            source_row.get("doctor_name"),
            source_row.get("doctor_unique_id"),
            source_row.get("doctor_master_id_resolved"),
            source_row.get("doctor_identity_key"),
            default="Unknown",
        )
        field_rep = _first_text(source_row.get("field_rep_email"), default="Unknown")
        clinic_group = _first_text(source_row.get("clinic_group"), default="Unknown")
        key = (field_rep, clinic_group, clinic)
        row = grouped.setdefault(
            key,
            {
                "field_rep": field_rep,
                "clinic_group": clinic_group,
                "clinic": clinic,
                "shares": 0,
                "link_opens": 0,
                "pdf_reads_completed": 0,
                "video_views": 0,
                "video_completions": 0,
                "pdf_downloads": 0,
            },
        )
        row["shares"] += 1
        if str(source_row.get("opened_event_ts") or "").strip():
            row["link_opens"] += 1
        if str(source_row.get("pdf_completed") or "").strip() == "1" or (
            source_row.get("pdf_last_page_num") is not None
            and source_row.get("pdf_total_pages_num") is not None
            and _to_float(source_row.get("pdf_total_pages_num")) > 0
            and _to_float(source_row.get("pdf_last_page_num")) >= _to_float(source_row.get("pdf_total_pages_num"))
        ):
            row["pdf_reads_completed"] += 1
        if (
            str(source_row.get("video_gt_50_at_ts") or "").strip()
            or str(source_row.get("video_100_at_ts") or "").strip()
            or _to_float(source_row.get("last_video_percentage_num")) >= 50
            or _to_float(source_row.get("video_watch_percentage_num")) >= 50
        ):
            row["video_views"] += 1
        if (
            str(source_row.get("video_completed") or "").strip() == "1"
            or str(source_row.get("video_100_at_ts") or "").strip()
            or _to_float(source_row.get("last_video_percentage_num")) >= 100
            or _to_float(source_row.get("video_watch_percentage_num")) >= 100
        ):
            row["video_completions"] += 1
        if str(source_row.get("downloaded_pdf") or "").strip() == "1" or str(source_row.get("pdf_download_event_ts") or "").strip():
            row["pdf_downloads"] += 1
    return sorted(
        grouped.values(),
        key=lambda item: (
            -_to_int(item.get("shares")),
            -_to_int(item.get("link_opens")),
            item.get("field_rep") or "",
            item.get("clinic") or "",
        ),
    )


def _rfa_detail_rows(reference: CampaignReference) -> list[dict[str, Any]]:
    if not (_rfa_campaign_match_keys(reference) & RFA_ALIAS_KEYS):
        return []
    from reporting.api_services import build_red_flag_alert_rows

    rows = build_red_flag_alert_rows()
    return sorted(
        rows,
        key=lambda item: (
            -_to_int(item.get("form_fills")),
            -_to_int(item.get("red_flags_total")),
            item.get("clinic_group") or "",
            item.get("clinic") or "",
        ),
    )


def _summary_card_section(
    *,
    key: str,
    subtitle: str,
    reference: CampaignReference,
    metrics: list[dict[str, Any]],
    extra_meta: list[dict[str, str] | None] | None = None,
    data_status: str = "ready",
) -> dict[str, Any]:
    meta_items = _base_meta(reference)
    meta_items.extend(item for item in (extra_meta or []) if item)
    return {
        "key": key,
        "type": "system",
        "label": SYSTEM_LABELS[key],
        "subtitle": subtitle,
        "metrics": metrics,
        "meta": meta_items,
        "data_status": data_status,
    }


def _in_clinic_summary_counts(reference: CampaignReference) -> dict[str, int]:
    if not reference.brand_campaign_id:
        return {
            "shares": 0,
            "link_opens": 0,
            "pdf_reads": 0,
            "video_views": 0,
            "video_completions": 0,
            "pdf_downloads": 0,
            "clinics_added": 0,
            "active_records": 0,
            "clinics_with_activity": 0,
        }

    base_counts = _fetch_one(
        """
        SELECT
            COUNT(DISTINCT base.doctor_identity_key) AS clinics_added,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE COALESCE(NULLIF(action.reached_first_ts, ''), NULLIF(action.opened_first_ts, ''), NULLIF(action.video_gt_50_first_ts, ''), NULLIF(action.pdf_download_first_ts, '')) IS NOT NULL
            ) AS active_records,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE COALESCE(NULLIF(action.reached_first_ts, ''), NULLIF(action.opened_first_ts, ''), NULLIF(action.video_gt_50_first_ts, ''), NULLIF(action.pdf_download_first_ts, '')) IS NOT NULL
            ) AS clinics_with_activity,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.reached_first_ts, '') IS NOT NULL
            ) AS shares,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.opened_first_ts, '') IS NOT NULL
            ) AS link_opens,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.video_gt_50_first_ts, '') IS NOT NULL
            ) AS video_views,
            COUNT(DISTINCT action.doctor_identity_key) FILTER (
                WHERE NULLIF(action.pdf_download_first_ts, '') IS NOT NULL
            ) AS pdf_downloads
        FROM silver.bridge_brand_campaign_doctor_base base
        LEFT JOIN silver.doctor_action_first_seen action
          ON action.brand_campaign_id = base.brand_campaign_id
         AND action.doctor_identity_key = base.doctor_identity_key
        WHERE base.brand_campaign_id = %s
    """,
        [reference.brand_campaign_id],
    )
    completion_counts = _fetch_one(
        """
        SELECT
            COUNT(DISTINCT tx.doctor_identity_key) FILTER (
                WHERE (
                    COALESCE(NULLIF(btrim(tx.pdf_completed), ''), '0') = '1'
                    OR (
                        tx.pdf_last_page_num IS NOT NULL
                        AND tx.pdf_total_pages_num IS NOT NULL
                        AND tx.pdf_total_pages_num::numeric > 0
                        AND tx.pdf_last_page_num::numeric >= tx.pdf_total_pages_num::numeric
                    )
                )
            ) AS pdf_reads,
            COUNT(DISTINCT tx.doctor_identity_key) FILTER (
                WHERE (
                    COALESCE(NULLIF(btrim(tx.video_completed), ''), '0') = '1'
                    OR COALESCE(NULLIF(btrim(tx.video_100_at_ts), ''), '') <> ''
                    OR COALESCE(tx.last_video_percentage_num::numeric, 0) >= 100
                    OR COALESCE(tx.video_watch_percentage_num::numeric, 0) >= 100
                )
            ) AS video_completions
        FROM silver.fact_collateral_transaction tx
        WHERE tx.brand_campaign_id = %s
    """,
        [reference.brand_campaign_id],
    )
    return {
        "shares": _to_int(base_counts.get("shares")),
        "link_opens": _to_int(base_counts.get("link_opens")),
        "pdf_reads": _to_int(completion_counts.get("pdf_reads")),
        "video_views": _to_int(base_counts.get("video_views")),
        "video_completions": _to_int(completion_counts.get("video_completions")),
        "pdf_downloads": _to_int(base_counts.get("pdf_downloads")),
        "clinics_added": _to_int(base_counts.get("clinics_added")),
        "active_records": _to_int(base_counts.get("active_records")),
        "clinics_with_activity": _to_int(base_counts.get("clinics_with_activity")),
    }


def _build_in_clinic_summary_section(reference: CampaignReference) -> tuple[dict[str, Any], dict[str, Any]]:
    counts = _in_clinic_summary_counts(reference)
    metrics = [
        _metric("shares", "Shares", counts["shares"]),
        _metric("link_opens", "Link Opens", counts["link_opens"]),
        _metric("pdf_reads", "PDF Reads", counts["pdf_reads"]),
        _metric("video_views", "Video Views", counts["video_views"]),
        _metric("video_completions", "Video Completions", counts["video_completions"]),
        _metric("pdf_downloads", "PDF Downloads", counts["pdf_downloads"]),
    ]
    section = _summary_card_section(
        key="in_clinic",
        subtitle="Cumulative unique-doctor summary for the campaign's In-Clinic sharing journey.",
        reference=reference,
        metrics=metrics,
        extra_meta=[
            _meta("Counting method", "Cumulative unique doctor records, aligned to the InClinic reporting model."),
            _meta("Data status", "Ready" if any(counts.values()) else "Configured in campaign DB; no in-clinic activity yet."),
        ],
        data_status="ready" if any(counts.values()) else "no_data",
    )
    adoption_row = {
        "system_key": "in_clinic",
        "label": "In Clinic",
        "clinics_added": counts["clinics_added"],
        "active_records": counts["active_records"] or counts["clinics_added"],
        "clinics_with_activity": counts["clinics_with_activity"],
    }
    return section, adoption_row


def _build_patient_education_summary_section(reference: CampaignReference) -> tuple[dict[str, Any], dict[str, Any]]:
    doctor_rows = (
        _query_schema_rows(
            reference.pe_schema,
            "rpt_doctor_activity_current",
            [
                "shares_played_cumulative",
                "shares_viewed_100_cumulative",
                "bundle_shares_cumulative",
                "unique_recipient_references_cumulative",
                "banner_clicks_cumulative",
                "banner_clicks",
                "clinic_name",
                "doctor_display_name",
                "doctor_id",
                "city",
                "district",
                "state",
            ],
        )
        if reference.pe_schema and _table_exists(reference.pe_schema, "rpt_doctor_activity_current")
        else []
    )
    clinic_rows = _aggregate_pe_clinic_rows(doctor_rows)
    metrics = [
        _metric("video_views", "Video Views", sum(_to_int(row.get("video_views")) for row in clinic_rows)),
        _metric("video_completions", "Video Completions", sum(_to_int(row.get("video_completions")) for row in clinic_rows)),
        _metric("cluster_shares", "Cluster Shares", sum(_to_int(row.get("cluster_shares")) for row in clinic_rows)),
        _metric("patient_scans", "Patient Scans", sum(_to_int(row.get("patient_scans")) for row in clinic_rows)),
        _metric("banner_clicks", "Banner Clicks", sum(_to_int(row.get("banner_clicks")) for row in clinic_rows)),
    ]
    section = _summary_card_section(
        key="patient_education",
        subtitle="Cumulative Patient Education summary for brand-manager overview cards.",
        reference=reference,
        metrics=metrics,
        extra_meta=[
            _meta("Counting method", "Cumulative PE activity totals from the campaign doctor activity feed."),
            _meta("Data status", "Ready" if any(_to_int(metric.get("value")) for metric in metrics) else "Configured in campaign DB; no PE activity yet."),
        ],
        data_status="ready" if any(_to_int(metric.get("value")) for metric in metrics) else "no_data",
    )
    clinics_with_activity = sum(
        1
        for row in clinic_rows
        if any(_to_int(row.get(key)) > 0 for key in ("video_views", "video_completions", "cluster_shares", "patient_scans", "banner_clicks"))
    )
    adoption_row = {
        "system_key": "patient_education",
        "label": "Patient Education",
        "clinics_added": len(clinic_rows),
        "active_records": len(clinic_rows),
        "clinics_with_activity": clinics_with_activity,
    }
    return section, adoption_row


def _build_rfa_summary_section(reference: CampaignReference) -> tuple[dict[str, Any], dict[str, Any]]:
    detail_rows = _rfa_detail_rows(reference)
    metrics = [
        _metric("form_fills", "Form fills", sum(_to_int(row.get("form_fills")) for row in detail_rows)),
        _metric("red_flags_total", "Red flags", sum(_to_int(row.get("red_flags_total")) for row in detail_rows)),
        _metric("patient_video_views", "Patient video views", sum(_to_int(row.get("patient_video_views")) for row in detail_rows)),
        _metric("reports_emailed_to_doctors", "Reports emailed", sum(_to_int(row.get("reports_emailed_to_doctors")) for row in detail_rows)),
        _metric("form_shares", "Form shares", sum(_to_int(row.get("form_shares")) for row in detail_rows)),
        _metric("patient_scans", "Patient scans", sum(_to_int(row.get("patient_scans")) for row in detail_rows)),
        _metric("follow_ups_scheduled", "Follow-ups", sum(_to_int(row.get("follow_ups_scheduled")) for row in detail_rows)),
        _metric("reminders_sent", "Reminders sent", sum(_to_int(row.get("reminders_sent")) for row in detail_rows)),
    ]
    section = _summary_card_section(
        key="rfa",
        subtitle="Cumulative RFA summary for brand-manager overview cards.",
        reference=reference,
        metrics=metrics,
        extra_meta=[
            _meta(
                "Counting method",
                "Cumulative RFA activity totals from screening, red-flag, scan, and follow-up feeds.",
            ),
            _meta(
                "Data status",
                "Ready" if any(_to_int(metric.get("value")) for metric in metrics) else "Configured in campaign DB; no attributable RFA activity yet.",
            ),
        ],
        data_status="ready" if any(_to_int(metric.get("value")) for metric in metrics) else "no_data",
    )
    clinics_with_activity = sum(
        1
        for row in detail_rows
        if any(
            _to_int(row.get(key)) > 0
            for key in (
                "form_fills",
                "red_flags_total",
                "patient_video_views",
                "reports_emailed_to_doctors",
                "form_shares",
                "patient_scans",
                "follow_ups_scheduled",
                "reminders_sent",
            )
        )
    )
    adoption_row = {
        "system_key": "rfa",
        "label": "Red Flag Alert",
        "clinics_added": len(detail_rows),
        "active_records": len(detail_rows),
        "clinics_with_activity": clinics_with_activity,
    }
    return section, adoption_row


def _build_adoption_summary_section(adoption_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "key": "adoption_by_clinics",
        "type": "adoption",
        "label": "Adoption by Clinics",
        "subtitle": "RFA and patient education doctor counts are approximated from unique clinic activity because those live feeds do not currently expose doctor-level identifiers.",
        "columns": [
            _table_column("label", "System"),
            _table_column("clinics_added", "Clinics added", "right"),
            _table_column("active_records", "Doctors / active records", "right"),
            _table_column("clinics_with_activity", "Clinics with activity", "right"),
        ],
        "rows": adoption_rows,
    }


def _build_in_clinic_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Shares, opens, and content consumption from the campaign's in-clinic collateral journey."
    zero_metrics = [
        _metric("shares", "Shares", 0),
        _metric("link_opens", "Link Opens", 0),
        _metric("pdf_reads_completed", "PDF Reads", 0),
        _metric("video_views", "Video Views", 0),
        _metric("video_completions", "Video Completions", 0),
        _metric("pdf_downloads", "PDF Downloads", 0),
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
    detail_rows = _fetch_in_clinic_detail_rows(reference)
    targeted = _to_int(latest_summary.get("targeted_clinics"))
    participating = _to_int(latest_summary.get("participating_clinics"))
    if not targeted and detail_rows:
        targeted = len(detail_rows)
    if not participating and detail_rows:
        participating = sum(1 for row in detail_rows if _to_int(row.get("shares")) > 0)
    total_shares = sum(_to_int(row.get("shares")) for row in detail_rows)
    link_opens = sum(_to_int(row.get("link_opens")) for row in detail_rows)
    pdf_reads = sum(_to_int(row.get("pdf_reads_completed")) for row in detail_rows)
    video_views = sum(_to_int(row.get("video_views")) for row in detail_rows)
    video_completions = sum(_to_int(row.get("video_completions")) for row in detail_rows)
    pdf_downloads = sum(_to_int(row.get("pdf_downloads")) for row in detail_rows)
    adoption_rate = _safe_pct(participating, targeted)
    metrics = [
        _metric("shares", "Shares", total_shares),
        _metric("link_opens", "Link Opens", link_opens),
        _metric("pdf_reads_completed", "PDF Reads", pdf_reads),
        _metric("video_views", "Video Views", video_views),
        _metric("video_completions", "Video Completions", video_completions),
        _metric("pdf_downloads", "PDF Downloads", pdf_downloads),
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
    bar_chart = _bar_chart(
        "Engagement mix",
        [
            _bar("shares", "Shares", total_shares, "#0f766e"),
            _bar("opens", "Link Opens", link_opens, "#0891b2"),
            _bar("pdf_reads", "PDF Reads", pdf_reads, "#7c3aed"),
            _bar("video_views", "Video Views", video_views, "#f59e0b"),
            _bar("video_completions", "Video Completions", video_completions, "#ef4444"),
            _bar("pdf_downloads", "PDF Downloads", pdf_downloads, "#f97316"),
        ],
        "Side-by-side view of the campaign's in-clinic content actions.",
    )
    table = _table_panel(
        "Top clinic activity",
        "Highest-volume clinic rows from the in-clinic sharing feed.",
        [
            _table_column("field_rep", "Field Rep"),
            _table_column("clinic", "Clinic"),
            _table_column("shares", "Shares", "right"),
            _table_column("link_opens", "Opens", "right"),
            _table_column("pdf_reads_completed", "PDF Reads", "right"),
        ],
        detail_rows[:10],
        empty_message="No in-clinic detail rows are available yet for this campaign.",
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("Reporting window", _reporting_window(weekly_rows[0].get("week_start_date") if weekly_rows else "", weekly_rows[-1].get("week_end_date") if weekly_rows else "")),
            _meta("Tracked base", f"{targeted} targeted / {participating} participating"),
            _meta("Adoption", _format_pct(adoption_rate)),
            _meta("Data status", "Ready" if any([targeted, participating, total_shares, link_opens, pdf_reads, video_views, video_completions, pdf_downloads]) else "Configured in campaign DB; no in-clinic activity yet."),
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
        "bar_chart": bar_chart,
        "table": table,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": targeted,
            "active_records": targeted,
            "participating_clinics": participating,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([targeted, participating, total_shares, link_opens, pdf_reads, video_views, video_completions, pdf_downloads]) else "no_data",
    }


def _build_patient_education_section(reference: CampaignReference) -> dict[str, Any]:
    subtitle = "Video reach, patient scanning, and entry-channel engagement from the Patient Education system."
    zero_metrics = [
        _metric("video_views", "Video Views", 0),
        _metric("video_completions", "Video Completions", 0),
        _metric("cluster_shares", "Cluster Shares", 0),
        _metric("patient_scans", "Patient Scans", 0),
        _metric("banner_clicks", "Banner Clicks", 0),
        _metric("sharing_clinics", "Sharing Clinics", 0),
    ]
    if not reference.pe_schema or not _table_exists(reference.pe_schema, "rpt_doctor_activity_current"):
        return _empty_section(
            key="patient_education",
            subtitle=subtitle,
            reference=reference,
            metrics=zero_metrics,
            extra_meta=[_meta("Data status", "Configured in campaign DB; PE reporting rows are not published yet.")],
        )

    summary = (
        _query_schema_one(
            reference.pe_schema,
            "kpi_campaign_health_summary",
            [
                "as_of_date",
                "enrolled_doctors_current",
                "doctors_sharing_unique_cumulative",
                "activation_pct",
                "play_rate_pct",
                "engagement_50_pct",
                "completion_pct",
                "campaign_health_score",
            ],
        )
        if _table_exists(reference.pe_schema, "kpi_campaign_health_summary")
        else {}
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
    doctor_rows = _query_schema_rows(
        reference.pe_schema,
        "rpt_doctor_activity_current",
        [
            "shares_played_cumulative",
            "shares_viewed_100_cumulative",
            "bundle_shares_cumulative",
            "unique_recipient_references_cumulative",
            "banner_clicks_cumulative",
            "banner_clicks",
            "clinic_name",
            "doctor_display_name",
            "doctor_id",
            "city",
            "district",
            "state",
        ],
    )
    clinic_rows = _aggregate_pe_clinic_rows(doctor_rows)
    navigation = _navigation_context(reference)
    enrolled = _to_int(summary.get("enrolled_doctors_current"))
    if not enrolled and clinic_rows:
        enrolled = len(clinic_rows)
    sharing = _to_int(summary.get("doctors_sharing_unique_cumulative")) or sum(
        1
        for row in clinic_rows
        if any(_to_int(row.get(key)) > 0 for key in ("cluster_shares", "video_views", "patient_scans", "banner_clicks"))
    )
    adoption_rate = _to_float(summary.get("activation_pct")) or _safe_pct(sharing, enrolled)
    video_views = sum(_to_int(row.get("video_views")) for row in clinic_rows)
    video_completions = sum(_to_int(row.get("video_completions")) for row in clinic_rows)
    cluster_shares = sum(_to_int(row.get("cluster_shares")) for row in clinic_rows)
    patient_scans = sum(_to_int(row.get("patient_scans")) for row in clinic_rows)
    banner_clicks = max(sum(_to_int(row.get("banner_clicks")) for row in clinic_rows), _to_int(navigation.get("total_clicks")))
    metrics = [
        _metric("video_views", "Video Views", video_views),
        _metric("video_completions", "Video Completions", video_completions),
        _metric("cluster_shares", "Cluster Shares", cluster_shares),
        _metric("patient_scans", "Patient Scans", patient_scans),
        _metric("banner_clicks", "Banner Clicks", banner_clicks),
        _metric("sharing_clinics", "Sharing Clinics", sharing),
    ]
    trend_series = [
        _series("activation_pct", "Activation %", [row.get("activation_pct") for row in weekly_rows], "#0f766e"),
        _series("play_rate_pct", "Play Rate %", [row.get("play_rate_pct") for row in weekly_rows], "#0ea5e9"),
        _series("completion_pct", "Completion %", [row.get("completion_pct") for row in weekly_rows], "#ef4444"),
    ]
    if navigation.get("click_trend_rows"):
        trend_series.append(
            _series(
                "banner_clicks",
                "Banner Clicks",
                [row.get("total_clicks") for row in navigation["click_trend_rows"]],
                "#f59e0b",
            )
        )
    trend = _trend(
        "Monthly/weekly PE health trend",
        [f"Week {row.get('week_index')}" for row in weekly_rows] if weekly_rows else [_pretty_date(row.get("week_bucket")) for row in navigation.get("click_trend_rows", [])],
        trend_series,
    )
    bar_chart = _bar_chart(
        "Engagement mix",
        [
            _bar("views", "Video Views", video_views, "#0ea5e9"),
            _bar("completions", "Video Completions", video_completions, "#ef4444"),
            _bar("shares", "Cluster Shares", cluster_shares, "#0f766e"),
            _bar("scans", "Patient Scans", patient_scans, "#7c3aed"),
            _bar("clicks", "Banner Clicks", banner_clicks, "#f59e0b"),
        ],
        "Comparison of the main PE engagement actions for this campaign.",
    )
    table = _table_panel(
        "Clinic engagement detail",
        "Top clinic/group rows from the PE doctor activity feed.",
        [
            _table_column("clinic", "Clinic"),
            _table_column("clinic_group", "Group"),
            _table_column("video_views", "Views", "right"),
            _table_column("cluster_shares", "Shares", "right"),
            _table_column("patient_scans", "Scans", "right"),
        ],
        clinic_rows[:10],
        empty_message="No PE detail rows are available yet for this campaign.",
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("PE campaign", reference.pe_campaign_id or reference.pe_campaign_normalized),
            _meta("Bundle", (reference.pe_dim_campaign or {}).get("local_video_cluster_name")),
            _meta("As of", _pretty_date(summary.get("as_of_date"))),
            _meta("Entry channels", " / ".join(navigation.get("entry_channels") or []) or "Not configured"),
            _meta("Navigation URL", navigation.get("banner_target_url")),
            _meta("Navigation window", _reporting_window(navigation.get("period_start"), navigation.get("period_end"))),
            _meta("Adoption", _format_pct(adoption_rate)),
            _meta("Data status", "Ready" if any([enrolled, sharing, video_views, video_completions, cluster_shares, patient_scans, banner_clicks]) else "Configured in campaign DB; no PE activity yet."),
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
        "bar_chart": bar_chart,
        "table": table,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": enrolled,
            "active_records": enrolled,
            "participating_clinics": sharing,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([enrolled, sharing, video_views, video_completions, cluster_shares, patient_scans, banner_clicks]) else "no_data",
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
        _metric("form_fills", "Form Fills", 0),
        _metric("red_flags_total", "Red Flags", 0),
        _metric("patient_video_views", "Patient Video Views", 0),
        _metric("reports_emailed_to_doctors", "Reports Emailed", 0),
        _metric("form_shares", "Form Shares", 0),
        _metric("patient_scans", "Patient Scans", 0),
        _metric("follow_ups_scheduled", "Follow-Ups", 0),
        _metric("reminders_sent", "Reminders Sent", 0),
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

    detail_rows = _rfa_detail_rows(reference)
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
    if not onboarded and detail_rows:
        onboarded = len(detail_rows)
    if not active and detail_rows:
        active = sum(
            1
            for row in detail_rows
            if any(
                _to_int(row.get(key)) > 0
                for key in ("form_fills", "red_flags_total", "patient_video_views", "patient_scans", "follow_ups_scheduled", "reminders_sent")
            )
        )
    form_fills = sum(_to_int(row.get("form_fills")) for row in detail_rows)
    red_flags = sum(_to_int(row.get("red_flags_total")) for row in detail_rows)
    patient_video_views = sum(_to_int(row.get("patient_video_views")) for row in detail_rows)
    reports_emailed = sum(_to_int(row.get("reports_emailed_to_doctors")) for row in detail_rows)
    form_shares = sum(_to_int(row.get("form_shares")) for row in detail_rows)
    patient_scans = sum(_to_int(row.get("patient_scans")) for row in detail_rows)
    followups = sum(_to_int(row.get("follow_ups_scheduled")) for row in detail_rows)
    reminders = sum(_to_int(row.get("reminders_sent")) for row in detail_rows)
    adoption_rate = _safe_pct(active, onboarded)
    metrics = [
        _metric("form_fills", "Form Fills", form_fills),
        _metric("red_flags_total", "Red Flags", red_flags),
        _metric("patient_video_views", "Patient Video Views", patient_video_views),
        _metric("reports_emailed_to_doctors", "Reports Emailed", reports_emailed),
        _metric("form_shares", "Form Shares", form_shares),
        _metric("patient_scans", "Patient Scans", patient_scans),
        _metric("follow_ups_scheduled", "Follow-Ups", followups),
        _metric("reminders_sent", "Reminders Sent", reminders),
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
    bar_chart = _bar_chart(
        "Operational mix",
        [
            _bar("form_fills", "Form Fills", form_fills, "#0f766e"),
            _bar("red_flags", "Red Flags", red_flags, "#ef4444"),
            _bar("video_views", "Patient Video Views", patient_video_views, "#0ea5e9"),
            _bar("scans", "Patient Scans", patient_scans, "#7c3aed"),
            _bar("followups", "Follow-Ups", followups, "#f59e0b"),
            _bar("reminders", "Reminders", reminders, "#f97316"),
        ],
        "Comparison of the main screening and follow-up actions recorded for the campaign.",
    )
    table = _table_panel(
        "Clinic screening detail",
        "Top clinic/group rows from the RFA reporting feed.",
        [
            _table_column("clinic", "Clinic"),
            _table_column("clinic_group", "Group"),
            _table_column("form_fills", "Form Fills", "right"),
            _table_column("red_flags_total", "Red Flags", "right"),
        ],
        detail_rows[:10],
        empty_message="No clinic-level RFA rows are available yet for this campaign.",
    )
    meta_items = _base_meta(reference)
    meta_items.extend(
        item
        for item in [
            _meta("As of", _pretty_date(summary.get("as_of_date"))),
            _meta("Attribution", "Resolved to the currently published Growth Clinic RFA dataset."),
            _meta("Tracked base", f"{onboarded} onboarded / {active} active"),
            _meta("Certified clinics", certified),
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
        "bar_chart": bar_chart,
        "table": table,
        "meta": meta_items,
        "adoption": {
            "eligible_clinics": onboarded,
            "active_records": onboarded,
            "participating_clinics": active,
            "adoption_rate": adoption_rate,
        },
        "data_status": "ready" if any([onboarded, active, certified, form_fills, red_flags, patient_video_views, reports_emailed, form_shares, patient_scans, followups, reminders]) else "no_data",
    }


def _build_adoption_section(system_sections: list[dict[str, Any]]) -> dict[str, Any]:
    breakdown = [
        {
            "system_key": section.get("key"),
            "label": section.get("label"),
            "eligible_clinics": _to_int((section.get("adoption") or {}).get("eligible_clinics")),
            "active_records": _to_int((section.get("adoption") or {}).get("active_records")),
            "participating_clinics": _to_int((section.get("adoption") or {}).get("participating_clinics")),
            "adoption_rate": _to_float((section.get("adoption") or {}).get("adoption_rate")),
        }
        for section in system_sections
    ]
    top_system = max(breakdown, key=lambda item: item.get("adoption_rate", 0), default=None)
    total_eligible = sum(item["eligible_clinics"] for item in breakdown)
    total_active_records = sum(item["active_records"] for item in breakdown)
    total_participating = sum(item["participating_clinics"] for item in breakdown)
    average_rate = round(sum(item["adoption_rate"] for item in breakdown) / len(breakdown), 1) if breakdown else 0.0
    breakdown_table = _table_panel(
        "Clinic adoption detail",
        "System-level view of configured base size and campaign activity.",
        [
            _table_column("label", "System"),
            _table_column("eligible_clinics", "Clinics Added", "right"),
            _table_column("active_records", "Active Records", "right"),
            _table_column("participating_clinics", "Clinics with Activity", "right"),
        ],
        breakdown,
        empty_message="No adoption rows are available yet.",
    )
    return {
        "key": "adoption_by_clinics",
        "type": "adoption",
        "label": "Adoption by Clinics",
        "subtitle": "Cross-system participation and clinic adoption rates for the systems selected on this campaign.",
        "metrics": [
            _metric("systems_live", "Systems Selected", len(breakdown)),
            _metric("tracked_clinics", "Clinics Added", total_eligible, helper_text="Summed from the configured base across selected systems."),
            _metric("active_records", "Active Records", total_active_records, helper_text="Reported base records available for campaign measurement."),
            _metric("participating_clinics", "Clinics with Activity", total_participating, helper_text="Summed across selected systems."),
            _metric("average_adoption_rate", "Average Adoption", average_rate, display_value=_format_pct(average_rate)),
            _metric(
                "best_system",
                "Highest Adoption",
                _to_float((top_system or {}).get("adoption_rate")),
                display_value=f"{(top_system or {}).get('label', 'N/A')} · {_format_pct((top_system or {}).get('adoption_rate', 0))}",
            ),
        ],
        "trend": None,
        "bar_chart": None,
        "table": breakdown_table,
        "meta": [],
        "breakdown": breakdown,
    }


def build_campaign_performance_page_payload(campaign_id: str) -> dict[str, Any]:
    reference = _resolve_campaign_reference(campaign_id)
    configured_keys = _configured_system_keys(reference)
    builders = {
        "rfa": _build_rfa_section,
        "in_clinic": _build_in_clinic_section,
        "patient_education": _build_patient_education_section,
    }
    system_sections = [builders[key](reference) for key in SYSTEM_ORDER if key in configured_keys]
    adoption_section = _build_adoption_section(system_sections)
    configured_systems = [{"key": section["key"], "label": section["label"]} for section in system_sections]
    return {
        "campaign": _campaign_identity(reference),
        "system_count": len(configured_systems),
        "configured_systems": configured_systems,
        "available_systems": configured_systems,
        "sections": [*system_sections, adoption_section],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_campaign_performance_payload(campaign_id: str) -> dict[str, Any]:
    reference = _resolve_campaign_reference(campaign_id)
    configured_keys = _configured_system_keys(reference)
    summary_builders = {
        "rfa": _build_rfa_summary_section,
        "in_clinic": _build_in_clinic_summary_section,
        "patient_education": _build_patient_education_summary_section,
    }
    system_sections: list[dict[str, Any]] = []
    adoption_rows: list[dict[str, Any]] = []
    for key in SYSTEM_ORDER:
        if key not in configured_keys:
            continue
        section, adoption_row = summary_builders[key](reference)
        system_sections.append(section)
        adoption_rows.append(adoption_row)
    adoption_section = _build_adoption_summary_section(adoption_rows)
    configured_systems = [{"key": section["key"], "label": section["label"]} for section in system_sections]
    return {
        "campaign": _campaign_identity(reference),
        "system_count": len(configured_systems),
        "configured_systems": configured_systems,
        "available_systems": configured_systems,
        "sections": [*system_sections, adoption_section],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
