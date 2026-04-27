from __future__ import annotations

import re
from typing import Any

from django.db import connection

from etl.pe_reports.storage import fetch_table as fetch_pe_table
from etl.pe_reports.storage import table_exists as pe_table_exists
from etl.pe_reports.utils import as_int, clean_text, first_non_empty, parse_date, slugify
from etl.sapa_growth.storage import fetch_table as fetch_sapa_table
from etl.sapa_growth.storage import table_exists as sapa_table_exists


SAPA_GOLD_SCHEMA = "gold_sapa"
SAPA_SILVER_SCHEMA = "silver_sapa"
PE_GLOBAL_SCHEMA = "gold_pe_global"


def _normalized_identifier(value: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", clean_text(value)).lower()


def _location_bucket(*values: Any) -> str:
    return first_non_empty(*values, "Unknown") or "Unknown"


def _clinic_label(*values: Any) -> str:
    return first_non_empty(*values, "Unknown") or "Unknown"


def _campaign_slug(value: Any) -> str:
    return slugify(value)


def _update_period(bounds: dict[str, Any], value: Any) -> None:
    parsed = parse_date(value)
    if parsed is None:
        return
    current_start = bounds.get("_period_start")
    current_end = bounds.get("_period_end")
    if current_start is None or parsed < current_start:
        bounds["_period_start"] = parsed
    if current_end is None or parsed > current_end:
        bounds["_period_end"] = parsed


def _finalize_rows(grouped_rows: dict[Any, dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for row in grouped_rows.values():
        start = row.pop("_period_start", None)
        end = row.pop("_period_end", None)
        if start is None or end is None:
            continue
        row["period_start"] = start.isoformat()
        row["period_end"] = end.isoformat()
        results.append(row)
    results.sort(key=lambda item: (item.get("campaign") or "", item.get("clinic_group") or "", item.get("clinic") or ""))
    return results


def _seed_row(row: dict[str, Any], *, campaign: str, clinic_group: str, clinic: str, doctor: str | None = None, field_rep: str | None = None) -> None:
    row.setdefault("campaign", campaign)
    row.setdefault("clinic_group", clinic_group)
    row.setdefault("clinic", clinic)
    if doctor is not None:
        row.setdefault("doctor", doctor)
    if field_rep is not None:
        row.setdefault("field_rep", field_rep)


def _sapa_rows(table: str) -> list[dict[str, Any]]:
    if not sapa_table_exists(SAPA_GOLD_SCHEMA, table):
        return []
    return fetch_sapa_table(SAPA_GOLD_SCHEMA, table)


def _sapa_silver_rows(table: str) -> list[dict[str, Any]]:
    if not sapa_table_exists(SAPA_SILVER_SCHEMA, table):
        return []
    return fetch_sapa_table(SAPA_SILVER_SCHEMA, table)


def _pe_rows(schema: str, table: str) -> list[dict[str, Any]]:
    if not schema or not pe_table_exists(schema, table):
        return []
    return fetch_pe_table(schema, table)


def _pe_campaign_registry_rows() -> list[dict[str, Any]]:
    if not pe_table_exists(PE_GLOBAL_SCHEMA, "campaign_registry"):
        return []
    return fetch_pe_table(PE_GLOBAL_SCHEMA, "campaign_registry")


def _filter_rows_by_field_rep_keys(rows: list[dict[str, Any]], field_rep_keys: set[str] | None) -> list[dict[str, Any]]:
    normalized_keys = {_normalized_identifier(key) for key in (field_rep_keys or set()) if _normalized_identifier(key)}
    if not normalized_keys:
        return rows
    return [
        row
        for row in rows
        if _normalized_identifier(row.get("field_rep_id")) in normalized_keys
    ]


def _aggregate_red_flag_alert_rows(
    *,
    screening_rows: list[dict[str, Any]],
    red_flag_rows: list[dict[str, Any]],
    video_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
    reminder_rows: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    clinic_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    campaign = _campaign_slug("growth-clinic")

    def bucket_for(doctor_key: Any, *rows: dict[str, Any]) -> dict[str, Any]:
        lookup_row = clinic_lookup.get(clean_text(doctor_key) or "") or {}
        clinic_key = clean_text(doctor_key) or _clinic_label(
            lookup_row.get("clinic_name"),
            lookup_row.get("canonical_display_name"),
            *(row.get("doctor_display_name") for row in rows if row),
            "unknown-clinic",
        )
        row = grouped.setdefault(
            clinic_key,
            {
                "campaign": campaign,
                "clinic_group": _location_bucket(
                    lookup_row.get("city"),
                    lookup_row.get("district"),
                    lookup_row.get("state"),
                ),
                "clinic": _clinic_label(
                    lookup_row.get("clinic_name"),
                    lookup_row.get("canonical_display_name"),
                    clinic_key,
                ),
                "form_fills": 0,
                "red_flags_total": 0,
                "patient_video_views": 0,
                "reports_emailed_to_doctors": 0,
                "form_shares": 0,
                "patient_scans": 0,
                "follow_ups_scheduled": 0,
                "reminders_sent": 0,
                "_form_share_codes": set(),
            },
        )
        if row.get("clinic_group") in {"", "Unknown"}:
            row["clinic_group"] = _location_bucket(
                lookup_row.get("city"),
                lookup_row.get("district"),
                lookup_row.get("state"),
                *(item.get("city") for item in rows if item),
                *(item.get("district") for item in rows if item),
                *(item.get("state") for item in rows if item),
            )
        if row.get("clinic") in {"", "Unknown"}:
            row["clinic"] = _clinic_label(
                lookup_row.get("clinic_name"),
                lookup_row.get("canonical_display_name"),
                *(item.get("doctor_display_name") for item in rows if item),
                clinic_key,
            )
        return row

    for source_row in screening_rows:
        row = bucket_for(source_row.get("doctor_key"), source_row)
        row["form_fills"] += 1
        _update_period(row, source_row.get("submitted_at"))

    for source_row in red_flag_rows:
        row = bucket_for(source_row.get("doctor_key"), source_row)
        row["red_flags_total"] += 1
        _update_period(row, source_row.get("submitted_at"))

    for source_row in video_rows:
        if clean_text(source_row.get("audience")) != "patient":
            continue
        row = bucket_for(source_row.get("doctor_key"), source_row)
        row["patient_video_views"] += 1
        _update_period(row, source_row.get("ts"))

    for source_row in followup_rows:
        row = bucket_for(source_row.get("doctor_key"), source_row)
        row["follow_ups_scheduled"] += 1
        _update_period(row, source_row.get("scheduled_followup_date"))

    for source_row in reminder_rows:
        row = bucket_for(source_row.get("doctor_key"), source_row)
        row["reminders_sent"] += 1
        _update_period(row, source_row.get("ts"))

    for source_row in metric_rows:
        if clean_text(source_row.get("is_patient_education")) != "true":
            continue
        row = bucket_for(source_row.get("doctor_key"), source_row)
        share_code = clean_text(source_row.get("share_code"))
        if share_code:
            row["_form_share_codes"].add(share_code)
        row["patient_scans"] += 1
        _update_period(row, source_row.get("ts"))

    for row in grouped.values():
        row["form_shares"] = len(row.pop("_form_share_codes", set()))
        row["reports_emailed_to_doctors"] = row["form_fills"]

    return _finalize_rows(grouped)


def build_red_flag_alert_rows(field_rep_keys: set[str] | None = None) -> list[dict[str, Any]]:
    clinic_lookup = {
        clean_text(row.get("doctor_key")) or "": row
        for row in _sapa_silver_rows("dim_doctor_clinic")
        if clean_text(row.get("doctor_key"))
    }
    filtered_screening_rows = _filter_rows_by_field_rep_keys(_sapa_rows("rpt_screening_detail"), field_rep_keys)
    filtered_red_flag_rows = _filter_rows_by_field_rep_keys(_sapa_rows("rpt_submission_redflag_detail"), field_rep_keys)
    filtered_video_rows = _filter_rows_by_field_rep_keys(_sapa_rows("rpt_video_view_detail"), field_rep_keys)
    filtered_followup_rows = _filter_rows_by_field_rep_keys(_sapa_rows("rpt_followup_schedule_detail"), field_rep_keys)
    filtered_reminder_rows = _filter_rows_by_field_rep_keys(_sapa_rows("rpt_reminder_sent_detail"), field_rep_keys)
    filtered_metric_rows = _filter_rows_by_field_rep_keys(_sapa_silver_rows("fact_metric_event"), field_rep_keys)
    return _aggregate_red_flag_alert_rows(
        screening_rows=filtered_screening_rows,
        red_flag_rows=filtered_red_flag_rows,
        video_rows=filtered_video_rows,
        followup_rows=filtered_followup_rows,
        reminder_rows=filtered_reminder_rows,
        metric_rows=filtered_metric_rows,
        clinic_lookup=clinic_lookup,
    )


def _aggregate_patient_education_campaign_rows(
    campaign_row: dict[str, Any],
    *,
    doctor_rows: list[dict[str, Any]],
    share_rows: list[dict[str, Any]],
    playback_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    campaign = _campaign_slug(campaign_row.get("campaign_id_original") or campaign_row.get("campaign_id_normalized"))
    grouped: dict[str, dict[str, Any]] = {}
    doctor_lookup = {
        clean_text(row.get("doctor_key")) or clean_text(row.get("doctor_id")) or "": row
        for row in doctor_rows
        if clean_text(row.get("doctor_key")) or clean_text(row.get("doctor_id"))
    }

    for source_row in doctor_rows:
        doctor_key = clean_text(source_row.get("doctor_key")) or clean_text(source_row.get("doctor_id"))
        if not doctor_key:
            continue
        grouped[doctor_key] = {
            "campaign": campaign,
            "clinic_group": _location_bucket(source_row.get("city"), source_row.get("district"), source_row.get("state")),
            "clinic": _clinic_label(source_row.get("clinic_name"), source_row.get("doctor_display_name"), source_row.get("doctor_id"), doctor_key),
            "video_views": as_int(source_row.get("shares_played_cumulative")),
            "video_completions": as_int(source_row.get("shares_viewed_100_cumulative")),
            "cluster_shares": as_int(source_row.get("bundle_shares_cumulative")),
            "patient_scans": as_int(source_row.get("unique_recipient_references_cumulative")),
            "banner_clicks": as_int(source_row.get("banner_clicks_cumulative") or source_row.get("banner_clicks")),
        }
        _update_period(grouped[doctor_key], source_row.get("enrolled_at_ts"))
        _update_period(grouped[doctor_key], source_row.get("last_shared_at_ts"))
        _update_period(grouped[doctor_key], source_row.get("first_banner_click_at_ts"))
        _update_period(grouped[doctor_key], source_row.get("last_banner_click_at_ts"))

    for source_row in share_rows:
        doctor_key = clean_text(source_row.get("doctor_key")) or clean_text(source_row.get("doctor_id"))
        if not doctor_key:
            continue
        row = grouped.setdefault(
            doctor_key,
            {
                "campaign": campaign,
                "clinic_group": _location_bucket(source_row.get("city"), source_row.get("district"), source_row.get("state")),
                "clinic": _clinic_label(source_row.get("clinic_name"), source_row.get("doctor_display_name"), source_row.get("doctor_id"), doctor_key),
                "video_views": 0,
                "video_completions": 0,
                "cluster_shares": 0,
                "patient_scans": 0,
                "banner_clicks": as_int((doctor_lookup.get(doctor_key) or {}).get("banner_clicks_cumulative")),
            },
        )
        doctor_meta = doctor_lookup.get(doctor_key) or {}
        row["clinic_group"] = _location_bucket(
            source_row.get("city"),
            source_row.get("district"),
            source_row.get("state"),
            doctor_meta.get("city"),
            doctor_meta.get("district"),
            doctor_meta.get("state"),
        )
        row["clinic"] = _clinic_label(
            doctor_meta.get("clinic_name"),
            source_row.get("clinic_name"),
            doctor_meta.get("doctor_display_name"),
            source_row.get("doctor_display_name"),
            source_row.get("doctor_id"),
            doctor_key,
        )
        _update_period(row, source_row.get("shared_at_ts"))

    for source_row in playback_rows:
        doctor_key = clean_text(source_row.get("doctor_key")) or clean_text(source_row.get("doctor_id"))
        if not doctor_key or doctor_key not in grouped:
            continue
        _update_period(grouped[doctor_key], source_row.get("occurred_at_ts"))

    return _finalize_rows(grouped)


def build_patient_education_rows() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for campaign_row in _pe_campaign_registry_rows():
        schema = clean_text(campaign_row.get("gold_schema_name"))
        if not schema:
            continue
        doctor_rows = _pe_rows(schema, "rpt_doctor_activity_current")
        if not doctor_rows:
            continue
        results.extend(
            _aggregate_patient_education_campaign_rows(
                campaign_row,
                doctor_rows=doctor_rows,
                share_rows=_pe_rows(schema, "rpt_share_detail"),
                playback_rows=_pe_rows(schema, "rpt_playback_detail"),
            )
        )
    results.sort(key=lambda item: (item.get("campaign") or "", item.get("clinic_group") or "", item.get("clinic") or ""))
    return results


def _fetch_in_clinic_activity_rows() -> list[dict[str, Any]]:
    sql = """
        SELECT
            t.brand_campaign_id,
            t.doctor_unique_id,
            t.doctor_master_id_resolved,
            t.doctor_identity_key,
            t.doctor_name,
            t.field_rep_email,
            t.sent_at_ts,
            t.reached_event_ts,
            t.transaction_date_ts,
            t.opened_event_ts,
            t.viewed_last_page_at_ts,
            t.video_gt_50_at_ts,
            t.video_100_at_ts,
            t.pdf_download_event_ts,
            t.last_viewed_at_ts,
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
        INNER JOIN gold_global.campaign_registry registry
            ON registry.brand_campaign_id = t.brand_campaign_id
        LEFT JOIN silver.bridge_brand_campaign_doctor_base base
            ON base.brand_campaign_id = t.brand_campaign_id
           AND base.doctor_identity_key = t.doctor_identity_key
        LEFT JOIN silver.dim_doctor d
            ON d.doctor_identity_key = t.doctor_identity_key
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _aggregate_in_clinic_rows(activity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for source_row in activity_rows:
        campaign = _campaign_slug(source_row.get("brand_campaign_id"))
        doctor_label = _clinic_label(
            source_row.get("doctor_name"),
            source_row.get("doctor_unique_id"),
            source_row.get("doctor_master_id_resolved"),
            source_row.get("doctor_identity_key"),
        )
        key = (campaign, doctor_label)
        row = grouped.setdefault(
            key,
            {
                "campaign": campaign,
                "clinic_group": _clinic_label(source_row.get("clinic_group"), "Unknown"),
                "clinic": doctor_label,
                "field_rep": clean_text(source_row.get("field_rep_email")) or "",
                "doctor": doctor_label,
                "shares": 0,
                "link_opens": 0,
                "pdf_reads_completed": 0,
                "video_views": 0,
                "video_completions": 0,
                "pdf_downloads": 0,
            },
        )
        _seed_row(
            row,
            campaign=campaign,
            clinic_group=_clinic_label(source_row.get("clinic_group"), "Unknown"),
            clinic=doctor_label,
            doctor=doctor_label,
            field_rep=clean_text(source_row.get("field_rep_email")) or "",
        )
        row["shares"] += 1
        if clean_text(source_row.get("opened_event_ts")):
            row["link_opens"] += 1
        if clean_text(source_row.get("pdf_completed")) == "1" or (
            source_row.get("pdf_last_page_num") is not None
            and source_row.get("pdf_total_pages_num") is not None
            and float(source_row["pdf_total_pages_num"] or 0) > 0
            and float(source_row["pdf_last_page_num"] or 0) >= float(source_row["pdf_total_pages_num"] or 0)
        ):
            row["pdf_reads_completed"] += 1
        if (
            clean_text(source_row.get("video_gt_50_at_ts"))
            or clean_text(source_row.get("video_100_at_ts"))
            or float(source_row.get("last_video_percentage_num") or 0) >= 50
            or float(source_row.get("video_watch_percentage_num") or 0) >= 50
        ):
            row["video_views"] += 1
        if (
            clean_text(source_row.get("video_completed")) == "1"
            or clean_text(source_row.get("video_100_at_ts"))
            or float(source_row.get("last_video_percentage_num") or 0) >= 100
            or float(source_row.get("video_watch_percentage_num") or 0) >= 100
        ):
            row["video_completions"] += 1
        if clean_text(source_row.get("downloaded_pdf")) == "1" or clean_text(source_row.get("pdf_download_event_ts")):
            row["pdf_downloads"] += 1

        for date_field in (
            "sent_at_ts",
            "reached_event_ts",
            "transaction_date_ts",
            "opened_event_ts",
            "viewed_last_page_at_ts",
            "video_gt_50_at_ts",
            "video_100_at_ts",
            "pdf_download_event_ts",
            "last_viewed_at_ts",
        ):
            _update_period(row, source_row.get(date_field))

    return _finalize_rows(grouped)


def build_in_clinic_rows() -> list[dict[str, Any]]:
    return _aggregate_in_clinic_rows(_fetch_in_clinic_activity_rows())
