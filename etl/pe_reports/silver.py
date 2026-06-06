from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import timezone
from typing import Any

from etl.pe_reports.specs import BRONZE_SCHEMA, SILVER_SCHEMA
from etl.pe_reports.storage import fetch_table, replace_table
from etl.reporting_privacy import (
    active_campaign_privacy_allowlist,
    active_person_privacy_rules,
    campaign_allowed_by_allowlist,
    person_privacy_matching_rules,
    row_visible_by_person_privacy,
)
from etl.pe_reports.utils import (
    as_int,
    clean_text,
    first_non_empty,
    iso_date,
    iso_datetime,
    normalize_campaign_id,
    normalize_email,
    normalize_identifier,
    normalize_phone,
    parse_date,
    parse_datetime,
    safe_pct,
    unique_preserving_order,
    week_end_saturday,
    week_start_sunday,
)


SILVER_DEFAULT_COLUMNS: dict[str, list[str]] = {
    "dim_doctor": [
        "doctor_key",
        "doctor_id",
        "campaign_doctor_id",
        "full_name",
        "first_name",
        "last_name",
        "email",
        "phone_normalized",
        "whatsapp_normalized",
        "clinic_name",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "recruited_via",
        "created_at_ts",
        "has_master_source",
        "has_campaign_source",
        "identity_match_method",
        "_dq_status",
        "_dq_errors",
    ],
    "dim_field_rep": [
        "field_rep_key",
        "field_rep_id",
        "brand_supplied_field_rep_id",
        "full_name",
        "phone_number_normalized",
        "is_active_flag",
        "state",
        "first_seen_ts",
        "last_seen_ts",
    ],
    "dim_campaign": [
        "campaign_key",
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_name",
        "brand_id",
        "brand_name",
        "system_pe_flag",
        "doctors_supported",
        "wa_addition",
        "email_registration",
        "banner_small_url",
        "banner_large_url",
        "banner_target_url",
        "publisher_campaign_present_flag",
        "local_video_cluster_id",
        "local_video_cluster_code",
        "local_video_cluster_name",
        "local_selection_json",
        "start_date",
        "end_date",
        "created_at_ts",
    ],
    "dim_video": [
        "video_id",
        "video_code",
        "default_display_label",
        "primary_therapy_id",
        "primary_therapy_code",
        "primary_therapy_name",
        "primary_trigger_id",
        "primary_trigger_code",
        "primary_trigger_name",
        "is_published",
        "is_active",
        "created_at_ts",
        "updated_at_ts",
    ],
    "dim_bundle": [
        "video_cluster_id",
        "video_cluster_code",
        "display_name",
        "trigger_id",
        "trigger_code",
        "trigger_name",
        "primary_therapy_id",
        "primary_therapy_code",
        "primary_therapy_name",
        "is_published",
        "is_active",
        "created_at_ts",
        "updated_at_ts",
    ],
    "bridge_bundle_video": ["video_cluster_id", "video_cluster_code", "video_id", "video_code", "sort_order"],
    "bridge_campaign_content": [
        "campaign_id_normalized",
        "campaign_id_original",
        "video_cluster_id",
        "video_cluster_code",
        "video_cluster_name",
        "video_id",
        "video_code",
        "linked_via_bundle_flag",
        "active_campaign_content_flag",
    ],
    "map_campaign_doctor_to_doctor": [
        "campaign_doctor_id",
        "doctor_key",
        "logical_doctor_id",
        "email_match_flag",
        "phone_match_flag",
        "match_method",
        "_dq_status",
        "_dq_errors",
    ],
    "fact_campaign_enrollment": [
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_doctor_id",
        "doctor_key",
        "registered_at_ts",
        "registered_by_field_rep_id",
        "registered_by_field_rep_external_id",
        "whitelabel_enabled",
        "whitelabel_subdomain",
        "doctor_id",
        "full_name",
        "clinic_name",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "campaign_name",
        "brand_name",
        "enrollment_unresolved_flag",
    ],
    "bridge_campaign_doctor_base": [
        "campaign_id_original",
        "campaign_id_normalized",
        "doctor_key",
        "doctor_id",
        "full_name",
        "clinic_name",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "enrolled_at_ts",
        "is_enrolled_flag",
    ],
    "fact_share_activity": [
        "share_public_id",
        "source_share_id",
        "source_share_uuid",
        "doctor_summary_id",
        "doctor_id",
        "doctor_key",
        "doctor_name_snapshot",
        "clinic_name_snapshot",
        "share_channel",
        "shared_by_role",
        "shared_item_type",
        "shared_item_code",
        "shared_item_name",
        "language_code",
        "recipient_reference",
        "recipient_reference_version",
        "shared_at_ts",
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_attribution_method",
        "is_campaign_attributed_flag",
        "video_id",
        "video_code",
        "video_display_label",
        "video_cluster_id",
        "video_cluster_code",
        "video_cluster_display_label",
        "therapy_area_name",
        "trigger_name",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "_dq_status",
        "_dq_errors",
    ],
    "fact_share_playback_event": [
        "source_playback_id",
        "source_share_uuid",
        "share_id",
        "share_public_id",
        "doctor_summary_id",
        "doctor_id",
        "doctor_key",
        "page_item_type",
        "event_type",
        "video_code",
        "video_id",
        "video_name",
        "milestone_percent_num",
        "occurred_at_ts",
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_attribution_method",
        "is_campaign_attributed_flag",
        "shared_item_type",
        "video_cluster_code",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "orphan_playback_flag",
        "_dq_status",
        "_dq_errors",
    ],
    "fact_share_banner_click": [
        "source_banner_click_id",
        "source_banner_click_uuid",
        "doctor_summary_id",
        "doctor_id",
        "doctor_key",
        "page_type",
        "banner_id",
        "banner_name",
        "banner_target_url",
        "clicked_at_ts",
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_attribution_method",
        "is_campaign_attributed_flag",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "_dq_status",
        "_dq_errors",
    ],
    "fact_share_funnel_first_seen": [
        "share_public_id",
        "campaign_id_original",
        "campaign_id_normalized",
        "doctor_key",
        "doctor_id",
        "shared_item_type",
        "shared_item_code",
        "shared_item_name",
        "language_code",
        "recipient_reference",
        "shared_at_ts",
        "play_first_ts",
        "view_25_first_ts",
        "view_50_first_ts",
        "view_75_first_ts",
        "view_100_first_ts",
        "is_played",
        "is_viewed_50",
        "is_viewed_100",
        "video_code",
        "video_cluster_code",
        "therapy_area_name",
        "trigger_name",
        "city",
        "district",
        "state",
        "field_rep_id_resolved",
        "field_rep_external_id",
    ],
    "fact_video_view": [
        "share_public_id",
        "doctor_key",
        "doctor_id",
        "campaign_id_original",
        "campaign_id_normalized",
        "shared_item_type",
        "shared_at_ts",
        "language_code",
        "video_code",
        "video_id",
        "preferred_display_label",
        "video_cluster_code",
        "video_cluster_display_label",
        "therapy_area_name",
        "trigger_name",
        "state",
        "district",
        "city",
        "field_rep_id_resolved",
        "field_rep_external_id",
        "is_played",
        "is_viewed_50",
        "is_viewed_100",
        "play_first_ts",
        "view_50_first_ts",
        "view_100_first_ts",
    ],
    "recon_doctor_share_summary": [
        "doctor_id",
        "doctor_summary_total_shares",
        "fact_share_count",
        "discrepancy_flag",
    ],
}


def _stringify_row(row: dict[str, Any]) -> dict[str, str]:
    return {key: "" if value is None else str(value) for key, value in row.items()}


def _replace_silver_table(table: str, rows: list[dict[str, Any]]) -> None:
    columns = list(rows[0].keys()) if rows else SILVER_DEFAULT_COLUMNS[table]
    replace_table(SILVER_SCHEMA, table, columns, [_stringify_row(row) for row in rows])


def _pe_campaign_allowed(row: dict[str, Any], allowlist: set[str]) -> bool:
    if not allowlist:
        return True
    for field in ("campaign_id_normalized", "campaign_id_original", "campaign_key", "campaign_id"):
        value = clean_text(row.get(field))
        if value and campaign_allowed_by_allowlist(value, allowlist):
            return True
    return False


def _filter_pe_campaign_rows(rows: list[dict[str, Any]], allowlist: set[str]) -> list[dict[str, Any]]:
    if not allowlist:
        return rows
    return [row for row in rows if _pe_campaign_allowed(row, allowlist)]


def _pe_person_visible(row: dict[str, Any], person_rules: list[dict[str, Any]]) -> bool:
    return row_visible_by_person_privacy(
        row,
        person_rules,
        campaign_fields=("campaign_id_normalized", "campaign_id_original", "campaign_key", "campaign_id"),
        email_fields=("email", "recipient_email", "user_email"),
        phone_fields=("phone_normalized", "whatsapp_normalized", "phone", "phone_number_normalized", "recipient_reference"),
    )


def _pe_person_matches(row: dict[str, Any], person_rules: list[dict[str, Any]]) -> bool:
    return bool(
        person_privacy_matching_rules(
            row,
            person_rules,
            email_fields=("email", "recipient_email", "user_email"),
            phone_fields=("phone_normalized", "whatsapp_normalized", "phone", "phone_number_normalized", "recipient_reference"),
        )
    )


def _preferred_localized_label(rows: list[dict[str, Any]], value_field: str, fallback: str | None = None) -> str | None:
    if not rows:
        return clean_text(fallback)
    english_rows = [
        row
        for row in rows
        if (clean_text(row.get("language_code")) or "").lower() in {"en", "eng", "english"}
        and clean_text(row.get(value_field))
    ]
    if english_rows:
        return clean_text(english_rows[0].get(value_field))
    for row in rows:
        value = clean_text(row.get(value_field))
        if value:
            return value
    return clean_text(fallback)


def _campaign_active_for_date(campaign_row: dict[str, Any] | None, shared_date: Any) -> bool:
    if not campaign_row:
        return False
    event_date = parse_date(shared_date)
    if event_date is None:
        return True
    start_date = parse_date(campaign_row.get("start_date"))
    end_date = parse_date(campaign_row.get("end_date"))
    if start_date and event_date < start_date:
        return False
    if end_date and event_date > end_date:
        return False
    return True


def _content_lookup_key(value: Any) -> str | None:
    return normalize_identifier(value)


def _append_content_reference(mapping: dict[str, list[str]], value: Any, campaign_id_normalized: str | None) -> None:
    key = _content_lookup_key(value)
    if not key or not campaign_id_normalized:
        return
    mapping[key].append(campaign_id_normalized)


def _campaigns_for_references(mapping: dict[str, list[str]] | None, *values: Any) -> list[str]:
    if not mapping:
        return []
    candidates: list[str] = []
    for value in values:
        key = _content_lookup_key(value)
        if key:
            candidates.extend(mapping.get(key, []))
    return unique_preserving_order(candidates)


def _single_active_campaign(
    candidate_campaigns: list[str],
    campaign_by_id: dict[str, dict[str, Any]],
    event_date: Any,
) -> tuple[dict[str, Any] | None, str]:
    active_campaigns = unique_preserving_order(
        [
            campaign_id_normalized
            for campaign_id_normalized in candidate_campaigns
            if _campaign_active_for_date(campaign_by_id.get(campaign_id_normalized), event_date)
        ]
    )
    if len(active_campaigns) == 1:
        return campaign_by_id.get(active_campaigns[0], {}), ""
    if len(active_campaigns) > 1:
        return None, "ambiguous"
    return None, "none"


def _comparable_datetime(value: Any):
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _campaign_from_exact_banner_id(
    banner_id: Any,
    *,
    campaign_by_id: dict[str, dict[str, Any]],
    campaigns_by_banner_id: dict[str, list[str]] | None = None,
) -> tuple[dict[str, Any] | None, str]:
    banner_key = normalize_campaign_id(banner_id)
    if not banner_key:
        return None, "none"
    candidate_campaigns = unique_preserving_order(
        [
            campaign_id_normalized
            for campaign_id_normalized in [
                *(campaigns_by_banner_id or {}).get(banner_key, []),
                *([banner_key] if banner_key in campaign_by_id else []),
            ]
            if campaign_id_normalized in campaign_by_id
        ]
    )
    if len(candidate_campaigns) == 1:
        return campaign_by_id.get(candidate_campaigns[0], {}), ""
    if len(candidate_campaigns) > 1:
        return None, "ambiguous"
    return None, "none"


def _campaign_from_source_uuid(
    row: dict[str, Any],
    *,
    campaign_by_id: dict[str, dict[str, Any]],
    campaign_id_by_source_uuid: dict[str, str] | None = None,
) -> tuple[dict[str, Any] | None, str]:
    candidate_campaigns: list[str] = []
    for field in ("campaign_uuid", "pe_campaign_uuid", "campaign_id", "campaign_id_normalized"):
        value = clean_text(row.get(field))
        normalized_value = normalize_campaign_id(value)
        if not normalized_value:
            continue
        if normalized_value in campaign_by_id:
            candidate_campaigns.append(normalized_value)
        mapped_campaign_id = (campaign_id_by_source_uuid or {}).get(normalized_value)
        if mapped_campaign_id:
            candidate_campaigns.append(mapped_campaign_id)
    candidate_campaigns = unique_preserving_order(
        [campaign_id_normalized for campaign_id_normalized in candidate_campaigns if campaign_id_normalized in campaign_by_id]
    )
    if len(candidate_campaigns) == 1:
        return campaign_by_id.get(candidate_campaigns[0], {}), ""
    if len(candidate_campaigns) > 1:
        return None, "ambiguous"
    return None, "none"


def _direct_campaign_banner_events(
    banner_click_rows: list[dict[str, Any]],
    *,
    campaign_by_id: dict[str, dict[str, Any]],
    campaigns_by_banner_id: dict[str, list[str]] | None = None,
    campaign_id_by_source_uuid: dict[str, str] | None = None,
) -> list[dict[str, str | None]]:
    events: list[dict[str, str | None]] = []
    for row in banner_click_rows:
        campaign, status = _campaign_from_source_uuid(
            row,
            campaign_by_id=campaign_by_id,
            campaign_id_by_source_uuid=campaign_id_by_source_uuid,
        )
        if not campaign and status != "ambiguous":
            campaign, status = _campaign_from_exact_banner_id(
                row.get("banner_id"),
                campaign_by_id=campaign_by_id,
                campaigns_by_banner_id=campaigns_by_banner_id,
            )
        if not campaign or status:
            continue
        clicked_at = iso_datetime(row.get("clicked_at_ts") or row.get("clicked_at"))
        if not clicked_at:
            continue
        events.append(
            {
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(campaign.get("campaign_id_normalized")),
                "clicked_at_ts": clicked_at,
            }
        )
    return events


def _single_campaign_from_banner_click_window(
    campaign_banner_click_events: list[dict[str, Any]] | None,
    campaign_by_id: dict[str, dict[str, Any]],
    event_date: Any,
    *,
    window_minutes: int = 120,
) -> tuple[dict[str, Any] | None, str]:
    event_dt = _comparable_datetime(event_date)
    if event_dt is None:
        return None, "none"
    candidate_campaigns: list[str] = []
    for event in campaign_banner_click_events or []:
        campaign_id_normalized = clean_text(event.get("campaign_id_normalized"))
        clicked_dt = _comparable_datetime(event.get("clicked_at_ts"))
        if not campaign_id_normalized or campaign_id_normalized not in campaign_by_id or clicked_dt is None:
            continue
        seconds_after_click = (event_dt - clicked_dt).total_seconds()
        if 0 <= seconds_after_click <= window_minutes * 60:
            candidate_campaigns.append(campaign_id_normalized)
    candidate_campaigns = unique_preserving_order(candidate_campaigns)
    if len(candidate_campaigns) == 1:
        return campaign_by_id.get(candidate_campaigns[0], {}), ""
    if len(candidate_campaigns) > 1:
        return None, "ambiguous"
    return None, "none"


def _campaign_attribution_payload(
    campaign: dict[str, Any],
    method: str,
) -> dict[str, str | None]:
    return {
        "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
        "campaign_id_normalized": clean_text(campaign.get("campaign_id_normalized")),
        "campaign_attribution_method": method,
        "is_campaign_attributed_flag": "true",
        "dq_error": "",
    }


def _selection_json_references(value: Any) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        parsed = text

    output: list[str] = []
    ignored = {
        "false",
        "true",
        "null",
        "none",
        "video",
        "videos",
        "cluster",
        "clusters",
        "selected",
        "items",
        "en",
        "hi",
    }

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                key_text = clean_text(key)
                if key_text and len(key_text) >= 4 and key_text.lower() not in ignored:
                    output.append(key_text)
                visit(child)
            return
        if isinstance(node, list):
            for child in node:
                visit(child)
            return
        item = clean_text(node)
        if item and len(item) >= 4 and item.lower() not in ignored:
            output.append(item)

    visit(parsed)
    return unique_preserving_order(output)


def resolve_field_rep_identity(
    raw_value: Any,
    field_rep_by_id: dict[str, dict[str, Any]],
    field_rep_by_external: dict[str, dict[str, Any]],
    campaign_link_by_id: dict[str, str],
) -> dict[str, str | None]:
    raw = clean_text(raw_value)
    if raw is None:
        return {"field_rep_id": None, "field_rep_external_id": None, "match_method": None}

    external_key = normalize_identifier(raw)
    if raw in field_rep_by_id:
        row = field_rep_by_id[raw]
        return {
            "field_rep_id": clean_text(row.get("field_rep_id")),
            "field_rep_external_id": clean_text(row.get("brand_supplied_field_rep_id")),
            "match_method": "direct_id",
        }
    if external_key and external_key in field_rep_by_external:
        row = field_rep_by_external[external_key]
        return {
            "field_rep_id": clean_text(row.get("field_rep_id")),
            "field_rep_external_id": clean_text(row.get("brand_supplied_field_rep_id")),
            "match_method": "external_id",
        }

    numeric_tail = None
    match = re.search(r"(\d+)$", raw)
    if match:
        numeric_tail = match.group(1)
        if numeric_tail in field_rep_by_id:
            row = field_rep_by_id[numeric_tail]
            return {
                "field_rep_id": clean_text(row.get("field_rep_id")),
                "field_rep_external_id": clean_text(row.get("brand_supplied_field_rep_id")),
                "match_method": "token_numeric_id",
            }

    candidate_link = raw if raw in campaign_link_by_id else numeric_tail if numeric_tail and numeric_tail in campaign_link_by_id else None
    if candidate_link:
        rep_id = campaign_link_by_id[candidate_link]
        row = field_rep_by_id.get(rep_id)
        return {
            "field_rep_id": clean_text(rep_id),
            "field_rep_external_id": clean_text((row or {}).get("brand_supplied_field_rep_id")),
            "match_method": "campaign_link",
        }

    return {"field_rep_id": None, "field_rep_external_id": None, "match_method": None}


def match_campaign_doctors(
    campaign_doctors: list[dict[str, Any]],
    master_doctors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    doctor_by_id = {clean_text(row.get("doctor_id")): row for row in master_doctors if clean_text(row.get("doctor_id"))}
    doctors_by_email: dict[str, list[dict[str, Any]]] = defaultdict(list)
    doctors_by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in master_doctors:
        email = normalize_email(row.get("email"))
        phone = normalize_phone(row.get("whatsapp_no"))
        if email:
            doctors_by_email[email].append(row)
        if phone:
            doctors_by_phone[phone].append(row)

    output: list[dict[str, Any]] = []
    for row in campaign_doctors:
        campaign_doctor_id = clean_text(row.get("id"))
        logical_doctor_id = clean_text(row.get("doctor_id"))
        email = normalize_email(row.get("email"))
        phone = normalize_phone(row.get("phone"))
        matched = None
        match_method = "unmapped"

        if logical_doctor_id and logical_doctor_id in doctor_by_id:
            matched = doctor_by_id[logical_doctor_id]
            match_method = "logical_doctor_id"
        elif email and len(doctors_by_email.get(email, [])) == 1:
            matched = doctors_by_email[email][0]
            match_method = "email"
        elif phone and len(doctors_by_phone.get(phone, [])) == 1:
            matched = doctors_by_phone[phone][0]
            match_method = "phone"

        output.append(
            {
                "campaign_doctor_id": campaign_doctor_id,
                "doctor_key": clean_text((matched or {}).get("doctor_id")),
                "logical_doctor_id": logical_doctor_id,
                "email_match_flag": "true" if match_method == "email" else "false",
                "phone_match_flag": "true" if match_method == "phone" else "false",
                "match_method": match_method,
                "_dq_status": "PASS" if matched else "WARN",
                "_dq_errors": "" if matched else "campaign_doctor_unmapped",
            }
        )
    return output


def attribute_share_row(
    share_row: dict[str, Any],
    *,
    campaigns_by_doctor: dict[str, list[str]],
    campaign_by_id: dict[str, dict[str, Any]],
    campaign_by_cluster_code: dict[str, list[str]],
    campaign_videos_by_campaign: dict[str, set[str]],
    campaign_by_cluster_reference: dict[str, list[str]] | None = None,
    campaign_by_video_reference: dict[str, list[str]] | None = None,
    single_active_fallback_campaigns: list[str] | None = None,
    campaign_banner_click_events: list[dict[str, Any]] | None = None,
    campaign_id_by_source_uuid: dict[str, str] | None = None,
) -> dict[str, str | None]:
    shared_item_type = clean_text(share_row.get("shared_item_type"))
    shared_item_code = clean_text(share_row.get("shared_item_code"))
    shared_item_name = clean_text(share_row.get("shared_item_name"))
    doctor_key = clean_text(share_row.get("doctor_key")) or clean_text(share_row.get("doctor_id"))
    shared_at = share_row.get("shared_at_ts") or share_row.get("shared_at")

    if not shared_item_type or not shared_item_code:
        return {
            "campaign_id_original": None,
            "campaign_id_normalized": None,
            "campaign_attribution_method": "missing_content",
            "is_campaign_attributed_flag": "false",
            "dq_error": "missing_share_content",
        }

    source_campaign, source_status = _campaign_from_source_uuid(
        share_row,
        campaign_by_id=campaign_by_id,
        campaign_id_by_source_uuid=campaign_id_by_source_uuid,
    )
    if source_campaign:
        return _campaign_attribution_payload(source_campaign, "source_campaign_uuid")

    banner_campaign, banner_status = _single_campaign_from_banner_click_window(campaign_banner_click_events, campaign_by_id, shared_at)
    if banner_campaign:
        return _campaign_attribution_payload(banner_campaign, "campaign_banner_click_window")
    if source_status == "ambiguous":
        banner_status = "ambiguous"

    if shared_item_type == "cluster":
        candidate_campaigns = unique_preserving_order(
            [
                *campaign_by_cluster_code.get(shared_item_code, []),
                *_campaigns_for_references(campaign_by_cluster_reference, shared_item_code, shared_item_name),
            ]
        )
        campaign, active_status = _single_active_campaign(candidate_campaigns, campaign_by_id, shared_at)
        if campaign:
            method = "direct_cluster" if len(candidate_campaigns) == 1 else "direct_cluster_active_window"
            return _campaign_attribution_payload(campaign, method)
        if banner_status == "ambiguous":
            active_status = "ambiguous"
        if active_status != "ambiguous":
            fallback_campaign, fallback_status = _single_active_campaign(single_active_fallback_campaigns or [], campaign_by_id, shared_at)
            if fallback_campaign:
                return _campaign_attribution_payload(fallback_campaign, "single_active_pe_campaign")
            if fallback_status == "ambiguous":
                active_status = "ambiguous"
        return {
            "campaign_id_original": None,
            "campaign_id_normalized": None,
            "campaign_attribution_method": "ambiguous_cluster" if active_status == "ambiguous" else "unattributed_cluster",
            "is_campaign_attributed_flag": "false",
            "dq_error": "ambiguous_cluster_attribution" if active_status == "ambiguous" else "cluster_not_campaign_content",
        }

    if shared_item_type == "video":
        eligible_campaigns: list[str] = []
        for campaign_id_normalized in campaigns_by_doctor.get(doctor_key or "", []):
            campaign = campaign_by_id.get(campaign_id_normalized)
            if not _campaign_active_for_date(campaign, share_row.get("shared_at_ts") or share_row.get("shared_at")):
                continue
            if shared_item_code in campaign_videos_by_campaign.get(campaign_id_normalized, set()):
                eligible_campaigns.append(campaign_id_normalized)
        candidate_campaigns = unique_preserving_order(eligible_campaigns)
        if len(candidate_campaigns) == 1:
            campaign = campaign_by_id.get(candidate_campaigns[0], {})
            return _campaign_attribution_payload(campaign, "conservative_video")
        if len(candidate_campaigns) > 1:
            return {
                "campaign_id_original": None,
                "campaign_id_normalized": None,
                "campaign_attribution_method": "ambiguous_video",
                "is_campaign_attributed_flag": "false",
                "dq_error": "ambiguous_video_attribution",
            }

        content_campaigns = unique_preserving_order(
            [
                campaign_id_normalized
                for campaign_id_normalized, video_codes in campaign_videos_by_campaign.items()
                if shared_item_code in video_codes
            ]
            + _campaigns_for_references(campaign_by_video_reference, shared_item_code, shared_item_name)
        )
        campaign, active_status = _single_active_campaign(content_campaigns, campaign_by_id, shared_at)
        if campaign:
            return _campaign_attribution_payload(campaign, "direct_video_active_content")
        if banner_status == "ambiguous":
            active_status = "ambiguous"
        if active_status != "ambiguous":
            fallback_campaign, fallback_status = _single_active_campaign(single_active_fallback_campaigns or [], campaign_by_id, shared_at)
            if fallback_campaign:
                return _campaign_attribution_payload(fallback_campaign, "single_active_pe_campaign")
            if fallback_status == "ambiguous":
                active_status = "ambiguous"
        return {
            "campaign_id_original": None,
            "campaign_id_normalized": None,
            "campaign_attribution_method": "ambiguous_video" if active_status == "ambiguous" else "unattributed_video",
            "is_campaign_attributed_flag": "false",
            "dq_error": "ambiguous_video_attribution" if active_status == "ambiguous" else "video_not_uniquely_attributed",
        }

    return {
        "campaign_id_original": None,
        "campaign_id_normalized": None,
        "campaign_attribution_method": "unsupported_share_type",
        "is_campaign_attributed_flag": "false",
        "dq_error": "unsupported_share_type",
    }


def _normalized_banner_url(value: Any) -> str | None:
    url = clean_text(value)
    if not url:
        return None
    return url.rstrip("/") or url


def _valid_doctor_identifier(value: Any) -> str | None:
    text = clean_text(value)
    if text in {None, "", "0", "0.0"}:
        return None
    return text


def attribute_banner_click_row(
    banner_click_row: dict[str, Any],
    *,
    campaigns_by_doctor: dict[str, list[str]],
    campaign_by_id: dict[str, dict[str, Any]],
    campaigns_by_banner_target_url: dict[str, list[str]],
    campaigns_by_banner_id: dict[str, list[str]] | None = None,
    campaign_id_by_source_uuid: dict[str, str] | None = None,
) -> dict[str, str | None]:
    doctor_key = clean_text(banner_click_row.get("doctor_key")) or clean_text(banner_click_row.get("doctor_id"))
    clicked_at = clean_text(banner_click_row.get("clicked_at_ts")) or clean_text(banner_click_row.get("clicked_at"))
    banner_id = normalize_campaign_id(banner_click_row.get("banner_id"))
    banner_target_url = _normalized_banner_url(banner_click_row.get("banner_target_url"))

    source_campaign, source_status = _campaign_from_source_uuid(
        banner_click_row,
        campaign_by_id=campaign_by_id,
        campaign_id_by_source_uuid=campaign_id_by_source_uuid,
    )
    if source_campaign:
        return _campaign_attribution_payload(source_campaign, "source_campaign_uuid")
    if source_status == "ambiguous":
        return {
            "campaign_id_original": None,
            "campaign_id_normalized": None,
            "campaign_attribution_method": "ambiguous_source_campaign_uuid",
            "is_campaign_attributed_flag": "false",
            "dq_error": "ambiguous_source_campaign_uuid",
        }

    if banner_id:
        campaign, active_status = _campaign_from_exact_banner_id(
            banner_id,
            campaign_by_id=campaign_by_id,
            campaigns_by_banner_id=campaigns_by_banner_id,
        )
        if campaign:
            return _campaign_attribution_payload(campaign, "banner_id")
        if active_status == "ambiguous":
            return {
                "campaign_id_original": None,
                "campaign_id_normalized": None,
                "campaign_attribution_method": "ambiguous_banner_id",
                "is_campaign_attributed_flag": "false",
                "dq_error": "ambiguous_banner_id",
            }

    if banner_target_url:
        eligible_campaigns = []
        for campaign_id_normalized in campaigns_by_banner_target_url.get(banner_target_url, []):
            campaign = campaign_by_id.get(campaign_id_normalized)
            if doctor_key and campaign_id_normalized not in campaigns_by_doctor.get(doctor_key or "", []):
                continue
            if not _campaign_active_for_date(campaign, clicked_at):
                continue
            eligible_campaigns.append(campaign_id_normalized)
        candidate_campaigns = unique_preserving_order(eligible_campaigns)
        if len(candidate_campaigns) == 1:
            campaign = campaign_by_id.get(candidate_campaigns[0], {})
            return {
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(campaign.get("campaign_id_normalized")),
                "campaign_attribution_method": "banner_target_url",
                "is_campaign_attributed_flag": "true",
                "dq_error": "",
            }
        if len(candidate_campaigns) > 1:
            return {
                "campaign_id_original": None,
                "campaign_id_normalized": None,
                "campaign_attribution_method": "ambiguous_banner_target_url",
                "is_campaign_attributed_flag": "false",
                "dq_error": "ambiguous_banner_target_url",
            }

    doctor_campaigns = unique_preserving_order(
        [
            campaign_id_normalized
            for campaign_id_normalized in campaigns_by_doctor.get(doctor_key or "", [])
            if _campaign_active_for_date(campaign_by_id.get(campaign_id_normalized), clicked_at)
        ]
    )
    if len(doctor_campaigns) == 1:
        campaign = campaign_by_id.get(doctor_campaigns[0], {})
        return {
            "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
            "campaign_id_normalized": clean_text(campaign.get("campaign_id_normalized")),
            "campaign_attribution_method": "doctor_active_campaign",
            "is_campaign_attributed_flag": "true",
            "dq_error": "",
        }
    if len(doctor_campaigns) > 1:
        return {
            "campaign_id_original": None,
            "campaign_id_normalized": None,
            "campaign_attribution_method": "ambiguous_doctor_campaign",
            "is_campaign_attributed_flag": "false",
            "dq_error": "ambiguous_doctor_campaign",
        }

    return {
        "campaign_id_original": None,
        "campaign_id_normalized": None,
        "campaign_attribution_method": "unattributed_banner_click",
        "is_campaign_attributed_flag": "false",
        "dq_error": "banner_click_not_campaign_attributed",
    }


def _share_supports_playback_video(
    share: dict[str, Any],
    video_code: str | None,
    bundle_videos_by_code: dict[str, list[str]],
) -> bool:
    if not video_code:
        return False
    if clean_text(share.get("shared_item_type")) == "video":
        return video_code in {
            clean_text(share.get("video_code")),
            clean_text(share.get("shared_item_code")),
        }
    if clean_text(share.get("shared_item_type")) == "cluster":
        cluster_code = clean_text(share.get("video_cluster_code")) or clean_text(share.get("shared_item_code"))
        return bool(cluster_code and video_code in bundle_videos_by_code.get(cluster_code, []))
    return False


def resolve_playback_share(
    playback_row: dict[str, Any],
    *,
    share_by_public_id: dict[str, dict[str, Any]],
    share_by_source_id: dict[str, dict[str, Any]],
    share_by_source_uuid: dict[str, dict[str, Any]] | None = None,
    share_rows: list[dict[str, Any]],
    bundle_videos_by_code: dict[str, list[str]],
) -> dict[str, Any] | None:
    share_event_uuid = normalize_campaign_id(playback_row.get("share_event_uuid"))
    if share_event_uuid and share_by_source_uuid and share_event_uuid in share_by_source_uuid:
        return share_by_source_uuid[share_event_uuid]

    share_public_id = clean_text(playback_row.get("share_public_id"))
    if share_public_id and share_public_id in share_by_public_id:
        return share_by_public_id[share_public_id]

    share_id = clean_text(playback_row.get("share_id"))
    if share_id:
        if share_id in share_by_public_id:
            return share_by_public_id[share_id]
        if share_id in share_by_source_id:
            return share_by_source_id[share_id]

    doctor_id = clean_text(playback_row.get("doctor_id"))
    video_code = clean_text(playback_row.get("video_code"))
    occurred_at = parse_datetime(playback_row.get("occurred_at") or playback_row.get("occurred_at_ts"))
    if not doctor_id or not video_code or occurred_at is None:
        return None

    candidates: list[dict[str, Any]] = []
    for share in share_rows:
        if clean_text(share.get("doctor_id")) != doctor_id:
            continue
        if not _share_supports_playback_video(share, video_code, bundle_videos_by_code):
            continue
        shared_at = parse_datetime(share.get("shared_at_ts") or share.get("shared_at"))
        if shared_at is None:
            continue
        day_delta = (occurred_at.date() - shared_at.date()).days
        if day_delta < 0 or day_delta > 7:
            continue
        candidates.append(share)

    if not candidates:
        return None
    return max(candidates, key=lambda item: clean_text(item.get("shared_at_ts")) or "")


def rollup_share_funnel(
    share_rows: list[dict[str, Any]],
    playback_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    playback_by_share: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in playback_rows:
        share_public_id = clean_text(row.get("share_public_id"))
        if share_public_id:
            playback_by_share[share_public_id].append(row)

    rolled_rows: list[dict[str, Any]] = []
    for share in share_rows:
        share_public_id = clean_text(share.get("share_public_id"))
        events = playback_by_share.get(share_public_id or "", [])
        play_first = None
        view_25 = None
        view_50 = None
        view_75 = None
        view_100 = None
        for event in events:
            occurred_at = clean_text(event.get("occurred_at_ts")) or clean_text(event.get("occurred_at"))
            event_type = clean_text(event.get("event_type"))
            milestone = as_int(event.get("milestone_percent_num") or event.get("milestone_percent"), default=-1)
            if event_type == "play":
                play_first = min([value for value in [play_first, occurred_at] if value], default=occurred_at)
            if event_type == "progress":
                if milestone >= 25:
                    view_25 = min([value for value in [view_25, occurred_at] if value], default=occurred_at)
                if milestone >= 50:
                    view_50 = min([value for value in [view_50, occurred_at] if value], default=occurred_at)
                if milestone >= 75:
                    view_75 = min([value for value in [view_75, occurred_at] if value], default=occurred_at)
                if milestone >= 100:
                    view_100 = min([value for value in [view_100, occurred_at] if value], default=occurred_at)

        rolled_rows.append(
            {
                "share_public_id": share_public_id,
                "campaign_id_original": clean_text(share.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(share.get("campaign_id_normalized")),
                "doctor_key": clean_text(share.get("doctor_key")),
                "doctor_id": clean_text(share.get("doctor_id")),
                "shared_item_type": clean_text(share.get("shared_item_type")),
                "shared_item_code": clean_text(share.get("shared_item_code")),
                "shared_item_name": clean_text(share.get("shared_item_name")),
                "language_code": clean_text(share.get("language_code")),
                "recipient_reference": clean_text(share.get("recipient_reference")),
                "shared_at_ts": clean_text(share.get("shared_at_ts")),
                "play_first_ts": play_first,
                "view_25_first_ts": view_25,
                "view_50_first_ts": view_50,
                "view_75_first_ts": view_75,
                "view_100_first_ts": view_100,
                "is_played": "true" if play_first else "false",
                "is_viewed_50": "true" if view_50 else "false",
                "is_viewed_100": "true" if view_100 else "false",
                "video_code": clean_text(share.get("video_code")),
                "video_cluster_code": clean_text(share.get("video_cluster_code")),
                "therapy_area_name": clean_text(share.get("therapy_area_name")),
                "trigger_name": clean_text(share.get("trigger_name")),
                "city": clean_text(share.get("city")),
                "district": clean_text(share.get("district")),
                "state": clean_text(share.get("state")),
                "field_rep_id_resolved": clean_text(share.get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text(share.get("field_rep_external_id")),
            }
        )
    return rolled_rows


def _best_registered_at(row: dict[str, Any]) -> str | None:
    return first_non_empty(iso_datetime(row.get("registered_at")), iso_datetime(row.get("created_at")), iso_datetime(row.get("updated_at")))


def build_silver(run_id: str) -> dict[str, Any]:
    issues: dict[str, int] = defaultdict(int)
    counts: dict[str, int] = {}
    privacy_allowlist = active_campaign_privacy_allowlist()
    person_privacy_rules = active_person_privacy_rules()

    doctor_rows = fetch_table(BRONZE_SCHEMA, "redflags_doctor")
    campaign_doctor_rows = fetch_table(BRONZE_SCHEMA, "campaign_doctor")
    enrollment_rows = fetch_table(BRONZE_SCHEMA, "campaign_doctorcampaignenrollment")
    campaign_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaign")
    brand_rows = fetch_table(BRONZE_SCHEMA, "campaign_brand")
    field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_fieldrep")
    campaign_field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaignfieldrep")
    publisher_campaign_rows = fetch_table(BRONZE_SCHEMA, "publisher_campaign")
    therapy_rows = fetch_table(BRONZE_SCHEMA, "catalog_therapyarea")
    trigger_cluster_rows = fetch_table(BRONZE_SCHEMA, "catalog_triggercluster")
    trigger_rows = fetch_table(BRONZE_SCHEMA, "catalog_trigger")
    video_rows = fetch_table(BRONZE_SCHEMA, "catalog_video")
    video_language_rows = fetch_table(BRONZE_SCHEMA, "catalog_videolanguage")
    bundle_rows = fetch_table(BRONZE_SCHEMA, "catalog_videocluster")
    bundle_language_rows = fetch_table(BRONZE_SCHEMA, "catalog_videoclusterlanguage")
    bundle_video_rows = fetch_table(BRONZE_SCHEMA, "catalog_videoclustervideo")
    _ = fetch_table(BRONZE_SCHEMA, "catalog_videotriggermap")
    share_summary_rows = fetch_table(BRONZE_SCHEMA, "sharing_doctorsharesummary")
    share_rows = fetch_table(BRONZE_SCHEMA, "sharing_shareactivity")
    playback_rows = fetch_table(BRONZE_SCHEMA, "sharing_shareplaybackevent")
    banner_click_rows = fetch_table(BRONZE_SCHEMA, "sharing_sharebannerclickevent")
    rep_assignment_credit_rows = fetch_table(BRONZE_SCHEMA, "pe_rep_assignment_credit")

    brands_by_id = {clean_text(row.get("id")): row for row in brand_rows if clean_text(row.get("id"))}
    therapy_by_id = {clean_text(row.get("id")): row for row in therapy_rows if clean_text(row.get("id"))}
    trigger_cluster_by_id = {clean_text(row.get("id")): row for row in trigger_cluster_rows if clean_text(row.get("id"))}
    trigger_by_id = {clean_text(row.get("id")): row for row in trigger_rows if clean_text(row.get("id"))}

    dim_field_rep_rows: list[dict[str, Any]] = []
    for row in field_rep_rows:
        field_rep_id = clean_text(row.get("id"))
        if not field_rep_id:
            continue
        dim_field_rep_rows.append(
            {
                "field_rep_key": field_rep_id,
                "field_rep_id": field_rep_id,
                "brand_supplied_field_rep_id": clean_text(row.get("brand_supplied_field_rep_id")),
                "full_name": clean_text(row.get("full_name")),
                "phone_number_normalized": normalize_phone(row.get("phone_number")),
                "is_active_flag": clean_text(row.get("is_active")) or "true",
                "state": clean_text(row.get("state")),
                "first_seen_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
                "last_seen_ts": first_non_empty(iso_datetime(row.get("updated_at")), iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
            }
        )

    field_rep_by_id = {clean_text(row.get("field_rep_id")): row for row in dim_field_rep_rows if clean_text(row.get("field_rep_id"))}
    field_rep_by_external: dict[str, dict[str, Any]] = {}
    for row in dim_field_rep_rows:
        external_id = clean_text(row.get("brand_supplied_field_rep_id"))
        if external_id:
            field_rep_by_external[normalize_identifier(external_id) or external_id] = row
    source_field_rep_by_uuid = {
        clean_text(row.get("field_rep_uuid")): row
        for row in field_rep_rows
        if clean_text(row.get("field_rep_uuid"))
    }
    campaign_link_by_id = {
        clean_text(row.get("id")): clean_text(row.get("field_rep_id"))
        for row in campaign_field_rep_rows
        if clean_text(row.get("id")) and clean_text(row.get("field_rep_id"))
    }

    map_rows = match_campaign_doctors(campaign_doctor_rows, doctor_rows)
    map_by_campaign_doctor_id = {clean_text(row.get("campaign_doctor_id")): row for row in map_rows if clean_text(row.get("campaign_doctor_id"))}

    dim_doctor_rows: list[dict[str, Any]] = []
    master_doctor_keys: set[str] = set()
    for row in doctor_rows:
        doctor_id = clean_text(row.get("doctor_id"))
        if not doctor_id:
            continue
        field_rep_resolution = resolve_field_rep_identity(row.get("field_rep_id"), field_rep_by_id, field_rep_by_external, campaign_link_by_id)
        dim_doctor_rows.append(
            {
                "doctor_key": doctor_id,
                "doctor_id": doctor_id,
                "campaign_doctor_id": None,
                "full_name": " ".join(part for part in [clean_text(row.get("first_name")) or "", clean_text(row.get("last_name")) or ""] if part).strip() or doctor_id,
                "first_name": clean_text(row.get("first_name")),
                "last_name": clean_text(row.get("last_name")),
                "email": normalize_email(row.get("email")),
                "phone_normalized": normalize_phone(row.get("clinic_phone")),
                "whatsapp_normalized": normalize_phone(row.get("whatsapp_no")),
                "clinic_name": clean_text(row.get("clinic_name")),
                "city": None,
                "district": clean_text(row.get("district")),
                "state": clean_text(row.get("state")),
                "field_rep_id_resolved": field_rep_resolution["field_rep_id"],
                "field_rep_external_id": field_rep_resolution["field_rep_external_id"],
                "recruited_via": clean_text(row.get("recruited_via")),
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
                "has_master_source": "true",
                "has_campaign_source": "false",
                "identity_match_method": "master_doctor_id",
                "_dq_status": "PASS",
                "_dq_errors": "",
            }
        )
        master_doctor_keys.add(doctor_id)

    for row in campaign_doctor_rows:
        campaign_doctor_id = clean_text(row.get("id"))
        if not campaign_doctor_id:
            continue
        mapped = map_by_campaign_doctor_id.get(campaign_doctor_id)
        if mapped and clean_text(mapped.get("doctor_key")):
            continue
        dim_doctor_rows.append(
            {
                "doctor_key": f"campaign-doctor:{campaign_doctor_id}",
                "doctor_id": None,
                "campaign_doctor_id": campaign_doctor_id,
                "full_name": clean_text(row.get("full_name")) or clean_text(row.get("email")) or clean_text(row.get("phone")) or campaign_doctor_id,
                "first_name": None,
                "last_name": None,
                "email": normalize_email(row.get("email")),
                "phone_normalized": normalize_phone(row.get("phone")),
                "whatsapp_normalized": None,
                "clinic_name": None,
                "city": clean_text(row.get("city")),
                "district": None,
                "state": clean_text(row.get("state")),
                "field_rep_id_resolved": None,
                "field_rep_external_id": None,
                "recruited_via": None,
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
                "has_master_source": "false",
                "has_campaign_source": "true",
                "identity_match_method": "campaign_doctor_only",
                "_dq_status": "WARN",
                "_dq_errors": "campaign_doctor_only",
            }
        )

    doctor_by_key = {clean_text(row.get("doctor_key")): row for row in dim_doctor_rows if clean_text(row.get("doctor_key"))}
    campaigns_local_by_normalized = {
        normalize_campaign_id(row.get("campaign_id")): row for row in publisher_campaign_rows if normalize_campaign_id(row.get("campaign_id"))
    }
    bundle_by_id = {clean_text(row.get("id")): row for row in bundle_rows if clean_text(row.get("id"))}
    bundle_lang_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bundle_language_rows:
        bundle_id = clean_text(row.get("video_cluster_id"))
        if bundle_id:
            bundle_lang_by_id[bundle_id].append(row)
    video_lang_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in video_language_rows:
        video_id = clean_text(row.get("video_id"))
        if video_id:
            video_lang_by_id[video_id].append(row)

    dim_video_rows: list[dict[str, Any]] = []
    for row in video_rows:
        video_id = clean_text(row.get("id"))
        video_code = clean_text(row.get("code"))
        if not video_id or not video_code:
            continue
        primary_therapy = therapy_by_id.get(clean_text(row.get("primary_therapy_id")))
        trigger = trigger_by_id.get(clean_text(row.get("primary_trigger_id")))
        dim_video_rows.append(
            {
                "video_id": video_id,
                "video_code": video_code,
                "default_display_label": _preferred_localized_label(video_lang_by_id.get(video_id, []), "title", fallback=video_code),
                "primary_therapy_id": clean_text(row.get("primary_therapy_id")),
                "primary_therapy_code": clean_text((primary_therapy or {}).get("code")),
                "primary_therapy_name": clean_text((primary_therapy or {}).get("display_name")),
                "primary_trigger_id": clean_text(row.get("primary_trigger_id")),
                "primary_trigger_code": clean_text((trigger or {}).get("code")),
                "primary_trigger_name": clean_text((trigger or {}).get("display_name")),
                "is_published": clean_text(row.get("is_published")) or "false",
                "is_active": clean_text(row.get("is_active")) or "false",
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
                "updated_at_ts": first_non_empty(iso_datetime(row.get("updated_at")), iso_datetime(row.get("created_at"))),
            }
        )
    video_by_code = {clean_text(row.get("video_code")): row for row in dim_video_rows if clean_text(row.get("video_code"))}
    video_by_id = {clean_text(row.get("video_id")): row for row in dim_video_rows if clean_text(row.get("video_id"))}

    dim_bundle_rows: list[dict[str, Any]] = []
    for row in bundle_rows:
        bundle_id = clean_text(row.get("id"))
        bundle_code = clean_text(row.get("code"))
        if not bundle_id or not bundle_code:
            continue
        trigger = trigger_by_id.get(clean_text(row.get("trigger_id")))
        therapy = therapy_by_id.get(clean_text((trigger or {}).get("primary_therapy_id")))
        dim_bundle_rows.append(
            {
                "video_cluster_id": bundle_id,
                "video_cluster_code": bundle_code,
                "display_name": _preferred_localized_label(bundle_lang_by_id.get(bundle_id, []), "name", fallback=clean_text(row.get("display_name")) or bundle_code),
                "trigger_id": clean_text(row.get("trigger_id")),
                "trigger_code": clean_text((trigger or {}).get("code")),
                "trigger_name": clean_text((trigger or {}).get("display_name")),
                "primary_therapy_id": clean_text((trigger or {}).get("primary_therapy_id")),
                "primary_therapy_code": clean_text((therapy or {}).get("code")),
                "primary_therapy_name": clean_text((therapy or {}).get("display_name")),
                "is_published": clean_text(row.get("is_published")) or "false",
                "is_active": clean_text(row.get("is_active")) or "false",
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
                "updated_at_ts": first_non_empty(iso_datetime(row.get("updated_at")), iso_datetime(row.get("created_at"))),
            }
        )
    bundle_by_code = {clean_text(row.get("video_cluster_code")): row for row in dim_bundle_rows if clean_text(row.get("video_cluster_code"))}

    bridge_bundle_video_rows: list[dict[str, Any]] = []
    bundle_videos_by_code: dict[str, list[str]] = defaultdict(list)
    for row in bundle_video_rows:
        bundle_id = clean_text(row.get("video_cluster_id"))
        video_id = clean_text(row.get("video_id"))
        bundle = bundle_by_id.get(bundle_id or "")
        video = video_by_id.get(video_id or "")
        if not bundle or not video:
            continue
        bridge_bundle_video_rows.append(
            {
                "video_cluster_id": bundle_id,
                "video_cluster_code": clean_text(bundle.get("code")) or clean_text(bundle.get("video_cluster_code")),
                "video_id": video_id,
                "video_code": clean_text(video.get("video_code")),
                "sort_order": clean_text(row.get("sort_order")) or "0",
            }
        )
        if clean_text(video.get("video_code")):
            bundle_videos_by_code[clean_text(bundle.get("code")) or clean_text(bundle.get("video_cluster_code"))].append(clean_text(video.get("video_code")) or "")

    dim_campaign_rows: list[dict[str, Any]] = []
    seen_campaign_ids: set[str] = set()
    for row in campaign_rows:
        campaign_id_normalized = normalize_campaign_id(row.get("id"))
        if not campaign_id_normalized:
            continue
        local = campaigns_local_by_normalized.get(campaign_id_normalized)
        system_pe_flag = clean_text(row.get("system_pe")) in {"1", "true", "True"}
        if not system_pe_flag and not local:
            continue
        if local and not system_pe_flag:
            issues["publisher_campaign_without_system_pe"] += 1
        bundle = bundle_by_id.get(clean_text((local or {}).get("video_cluster_id")) or "")
        brand = brands_by_id.get(clean_text(row.get("brand_id")) or "")
        dim_campaign_rows.append(
            {
                "campaign_key": campaign_id_normalized,
                "campaign_id_original": clean_text(row.get("id")) or clean_text((local or {}).get("campaign_id")),
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_name": first_non_empty(clean_text(row.get("name")), clean_text((local or {}).get("new_video_cluster_name")), campaign_id_normalized),
                "brand_id": clean_text(row.get("brand_id")),
                "brand_name": clean_text((brand or {}).get("name")),
                "system_pe_flag": "true" if system_pe_flag else "false",
                "doctors_supported": first_non_empty(clean_text((local or {}).get("doctors_supported")), clean_text(row.get("num_doctors_supported"))),
                "wa_addition": clean_text((local or {}).get("wa_addition")),
                "email_registration": clean_text((local or {}).get("email_registration")),
                "banner_small_url": first_non_empty(clean_text((local or {}).get("banner_small")), clean_text(row.get("banner_small_url"))),
                "banner_large_url": first_non_empty(clean_text((local or {}).get("banner_large")), clean_text(row.get("banner_large_url"))),
                "banner_target_url": first_non_empty(clean_text((local or {}).get("banner_target_url")), clean_text(row.get("banner_target_url"))),
                "publisher_campaign_present_flag": "true" if local else "false",
                "local_video_cluster_id": clean_text((local or {}).get("video_cluster_id")),
                "local_video_cluster_code": clean_text((bundle or {}).get("video_cluster_code")) or clean_text((bundle or {}).get("code")),
                "local_video_cluster_name": clean_text((bundle or {}).get("display_name")) or _preferred_localized_label(bundle_lang_by_id.get(clean_text((local or {}).get("video_cluster_id")) or "", []), "name"),
                "local_selection_json": clean_text((local or {}).get("selection_json")),
                "start_date": first_non_empty(iso_date((local or {}).get("start_date")), iso_date(row.get("start_date"))),
                "end_date": first_non_empty(iso_date((local or {}).get("end_date")), iso_date(row.get("end_date"))),
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime((local or {}).get("created_at"))),
            }
        )
        seen_campaign_ids.add(campaign_id_normalized)

    for row in publisher_campaign_rows:
        campaign_id_normalized = normalize_campaign_id(row.get("campaign_id"))
        if not campaign_id_normalized or campaign_id_normalized in seen_campaign_ids:
            continue
        bundle = bundle_by_id.get(clean_text(row.get("video_cluster_id")) or "")
        dim_campaign_rows.append(
            {
                "campaign_key": campaign_id_normalized,
                "campaign_id_original": clean_text(row.get("campaign_id")),
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_name": clean_text(row.get("new_video_cluster_name")) or campaign_id_normalized,
                "brand_id": None,
                "brand_name": None,
                "system_pe_flag": "false",
                "doctors_supported": clean_text(row.get("doctors_supported")),
                "wa_addition": clean_text(row.get("wa_addition")),
                "email_registration": clean_text(row.get("email_registration")),
                "banner_small_url": clean_text(row.get("banner_small")),
                "banner_large_url": clean_text(row.get("banner_large")),
                "banner_target_url": clean_text(row.get("banner_target_url")),
                "publisher_campaign_present_flag": "true",
                "local_video_cluster_id": clean_text(row.get("video_cluster_id")),
                "local_video_cluster_code": clean_text((bundle or {}).get("video_cluster_code")) or clean_text((bundle or {}).get("code")),
                "local_video_cluster_name": clean_text((bundle or {}).get("display_name")) or clean_text(row.get("new_video_cluster_name")),
                "local_selection_json": clean_text(row.get("selection_json")),
                "start_date": iso_date(row.get("start_date")),
                "end_date": iso_date(row.get("end_date")),
                "created_at_ts": first_non_empty(iso_datetime(row.get("created_at")), iso_datetime(row.get("_ingested_at"))),
            }
        )
        issues["publisher_campaign_without_system_pe"] += 1

    dim_campaign_rows = _filter_pe_campaign_rows(dim_campaign_rows, privacy_allowlist)
    campaign_by_id = {clean_text(row.get("campaign_id_normalized")): row for row in dim_campaign_rows if clean_text(row.get("campaign_id_normalized"))}
    campaign_id_by_source_uuid: dict[str, str] = {}
    for row in campaign_rows:
        campaign_id_normalized = normalize_campaign_id(row.get("id"))
        if not campaign_id_normalized:
            continue
        for value in (row.get("campaign_uuid"), row.get("id")):
            source_key = normalize_campaign_id(value)
            if source_key:
                campaign_id_by_source_uuid[source_key] = campaign_id_normalized
    for row in publisher_campaign_rows:
        campaign_id_normalized = normalize_campaign_id(row.get("campaign_id"))
        if not campaign_id_normalized:
            continue
        for value in (
            row.get("campaign_id"),
            row.get("campaign_uuid"),
            row.get("pe_campaign_uuid"),
            row.get("pe_campaign_id_normalized"),
            row.get("master_campaign_id_normalized"),
        ):
            source_key = normalize_campaign_id(value)
            if source_key:
                campaign_id_by_source_uuid[source_key] = campaign_id_normalized
    campaigns_by_banner_id: dict[str, list[str]] = defaultdict(list)
    campaigns_by_banner_target_url: dict[str, list[str]] = defaultdict(list)
    for campaign in dim_campaign_rows:
        banner_target_url = _normalized_banner_url(campaign.get("banner_target_url"))
        campaign_id_normalized = clean_text(campaign.get("campaign_id_normalized"))
        for campaign_identifier in (
            campaign.get("campaign_id_normalized"),
            campaign.get("campaign_id_original"),
            campaign.get("campaign_key"),
        ):
            banner_id_key = normalize_campaign_id(campaign_identifier)
            if banner_id_key and campaign_id_normalized:
                campaigns_by_banner_id[banner_id_key].append(campaign_id_normalized)
        if banner_target_url and campaign_id_normalized:
            campaigns_by_banner_target_url[banner_target_url].append(campaign_id_normalized)
    campaign_banner_click_events = _direct_campaign_banner_events(
        banner_click_rows,
        campaign_by_id=campaign_by_id,
        campaigns_by_banner_id=campaigns_by_banner_id,
        campaign_id_by_source_uuid=campaign_id_by_source_uuid,
    )
    campaign_by_cluster_code: dict[str, list[str]] = defaultdict(list)
    campaign_by_cluster_reference: dict[str, list[str]] = defaultdict(list)
    campaign_by_video_reference: dict[str, list[str]] = defaultdict(list)
    single_active_fallback_campaigns: list[str] = []
    bridge_campaign_content_rows: list[dict[str, Any]] = []
    campaign_videos_by_campaign: dict[str, set[str]] = defaultdict(set)
    for campaign in dim_campaign_rows:
        campaign_id_normalized = clean_text(campaign.get("campaign_id_normalized"))
        if campaign_id_normalized and clean_text(campaign.get("publisher_campaign_present_flag")) == "true":
            single_active_fallback_campaigns.append(campaign_id_normalized)
        cluster_code = clean_text(campaign.get("local_video_cluster_code"))
        if not campaign_id_normalized:
            continue
        selection_refs = _selection_json_references(campaign.get("local_selection_json"))
        for reference in selection_refs:
            _append_content_reference(campaign_by_cluster_reference, reference, campaign_id_normalized)
            _append_content_reference(campaign_by_video_reference, reference, campaign_id_normalized)
        _append_content_reference(campaign_by_cluster_reference, campaign.get("local_video_cluster_name"), campaign_id_normalized)
        _append_content_reference(campaign_by_cluster_reference, campaign.get("campaign_name"), campaign_id_normalized)
        if not cluster_code:
            continue
        campaign_by_cluster_code[cluster_code].append(campaign_id_normalized)
        _append_content_reference(campaign_by_cluster_reference, cluster_code, campaign_id_normalized)
        video_codes = bundle_videos_by_code.get(cluster_code, [])
        if not video_codes:
            bridge_campaign_content_rows.append(
                {
                    "campaign_id_normalized": campaign_id_normalized,
                    "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
                    "video_cluster_id": clean_text(campaign.get("local_video_cluster_id")),
                    "video_cluster_code": cluster_code,
                    "video_cluster_name": clean_text(campaign.get("local_video_cluster_name")),
                    "video_id": None,
                    "video_code": None,
                    "linked_via_bundle_flag": "true",
                    "active_campaign_content_flag": "true",
                }
            )
        for video_code in video_codes:
            video = video_by_code.get(video_code)
            _append_content_reference(campaign_by_video_reference, video_code, campaign_id_normalized)
            _append_content_reference(campaign_by_video_reference, (video or {}).get("default_display_label"), campaign_id_normalized)
            bridge_campaign_content_rows.append(
                {
                    "campaign_id_normalized": campaign_id_normalized,
                    "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
                    "video_cluster_id": clean_text(campaign.get("local_video_cluster_id")),
                    "video_cluster_code": cluster_code,
                    "video_cluster_name": clean_text(campaign.get("local_video_cluster_name")),
                    "video_id": clean_text((video or {}).get("video_id")),
                    "video_code": video_code,
                    "linked_via_bundle_flag": "true",
                    "active_campaign_content_flag": "true",
                }
            )
            campaign_videos_by_campaign[campaign_id_normalized].add(video_code)

    fact_campaign_enrollment_rows: list[dict[str, Any]] = []
    bridge_campaign_doctor_base_rows: list[dict[str, Any]] = []
    base_seen: set[tuple[str, str]] = set()
    campaigns_by_doctor: dict[str, list[str]] = defaultdict(list)

    campaign_doctor_by_id = {clean_text(row.get("id")): row for row in campaign_doctor_rows if clean_text(row.get("id"))}
    campaign_id_by_uuid = {
        clean_text(row.get("campaign_uuid")): normalize_campaign_id(row.get("id"))
        for row in campaign_rows
        if clean_text(row.get("campaign_uuid")) and normalize_campaign_id(row.get("id"))
    }
    campaign_doctor_id_by_uuid = {
        clean_text(row.get("doctor_uuid")): clean_text(row.get("id"))
        for row in campaign_doctor_rows
        if clean_text(row.get("doctor_uuid")) and clean_text(row.get("id"))
    }

    def resolve_credit_field_rep(row: dict[str, Any]) -> dict[str, str | None]:
        field_rep_uuid = clean_text(row.get("field_rep_uuid"))
        source_rep = source_field_rep_by_uuid.get(field_rep_uuid or "")
        if source_rep:
            return {
                "field_rep_id": clean_text(source_rep.get("id")),
                "field_rep_external_id": clean_text(source_rep.get("brand_supplied_field_rep_id")),
                "match_method": "field_rep_uuid",
            }
        return resolve_field_rep_identity(field_rep_uuid, field_rep_by_id, field_rep_by_external, campaign_link_by_id)

    rep_credit_by_campaign_doctor: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rep_assignment_credit_rows:
        campaign_id_normalized = (
            campaign_id_by_uuid.get(clean_text(row.get("campaign_uuid")) or "")
            or normalize_campaign_id(row.get("campaign_uuid"))
        )
        if privacy_allowlist and not campaign_allowed_by_allowlist(campaign_id_normalized, privacy_allowlist):
            continue
        campaign_doctor_id = (
            campaign_doctor_id_by_uuid.get(clean_text(row.get("doctor_uuid")) or "")
            or clean_text(row.get("doctor_uuid"))
        )
        if not campaign_id_normalized or not campaign_doctor_id:
            issues["rep_credit_unresolved_identity"] += 1
            continue
        rep_resolution = resolve_credit_field_rep(row)
        if not rep_resolution.get("field_rep_id"):
            issues["rep_credit_field_rep_resolution_failures"] += 1
            continue
        key = (campaign_id_normalized, campaign_doctor_id)
        if key in rep_credit_by_campaign_doctor:
            continue
        rep_credit_by_campaign_doctor[key] = {
            "row": row,
            "rep_resolution": rep_resolution,
        }

    for row in enrollment_rows:
        campaign_id_normalized = normalize_campaign_id(row.get("campaign_id"))
        campaign_doctor_id = clean_text(row.get("doctor_id"))
        if not campaign_id_normalized or not campaign_doctor_id:
            continue
        if privacy_allowlist and not campaign_allowed_by_allowlist(campaign_id_normalized, privacy_allowlist):
            continue
        mapped = map_by_campaign_doctor_id.get(campaign_doctor_id, {})
        mapped_doctor_key = clean_text(mapped.get("doctor_key"))
        campaign_doctor = campaign_doctor_by_id.get(campaign_doctor_id, {})
        doctor_dim = doctor_by_key.get(mapped_doctor_key or "")
        credit = rep_credit_by_campaign_doctor.get((campaign_id_normalized, campaign_doctor_id))
        rep_resolution = (
            credit["rep_resolution"]
            if credit
            else resolve_field_rep_identity(row.get("registered_by_id"), field_rep_by_id, field_rep_by_external, campaign_link_by_id)
        )
        campaign = campaign_by_id.get(campaign_id_normalized, {})
        resolved_doctor_id = clean_text((doctor_dim or {}).get("doctor_id"))
        state = first_non_empty(clean_text((doctor_dim or {}).get("state")), clean_text(campaign_doctor.get("state")))
        city = first_non_empty(clean_text((doctor_dim or {}).get("city")), clean_text(campaign_doctor.get("city")))
        full_name = first_non_empty(clean_text((doctor_dim or {}).get("full_name")), clean_text(campaign_doctor.get("full_name")))
        if person_privacy_rules and not _pe_person_visible(
            {
                **(doctor_dim or {}),
                **campaign_doctor,
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_id_original": clean_text((campaign or {}).get("campaign_id_original")) or clean_text(row.get("campaign_id")),
            },
            person_privacy_rules,
        ):
            continue

        fact_campaign_enrollment_rows.append(
            {
                "campaign_id_original": clean_text((campaign or {}).get("campaign_id_original")) or clean_text(row.get("campaign_id")),
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_doctor_id": campaign_doctor_id,
                "doctor_key": mapped_doctor_key,
                "registered_at_ts": first_non_empty(
                    iso_datetime((credit or {}).get("row", {}).get("credit_effective_from") if credit else None),
                    _best_registered_at(row),
                ),
                "registered_by_field_rep_id": rep_resolution["field_rep_id"],
                "registered_by_field_rep_external_id": rep_resolution["field_rep_external_id"],
                "whitelabel_enabled": clean_text(row.get("whitelabel_enabled")) or "false",
                "whitelabel_subdomain": clean_text(row.get("whitelabel_subdomain")),
                "doctor_id": resolved_doctor_id,
                "full_name": full_name,
                "clinic_name": clean_text((doctor_dim or {}).get("clinic_name")),
                "city": city,
                "district": clean_text((doctor_dim or {}).get("district")),
                "state": state,
                "field_rep_id_resolved": clean_text((doctor_dim or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((doctor_dim or {}).get("field_rep_external_id")),
                "campaign_name": clean_text((campaign or {}).get("campaign_name")),
                "brand_name": clean_text((campaign or {}).get("brand_name")),
                "enrollment_unresolved_flag": "false" if mapped_doctor_key and resolved_doctor_id else "true",
            }
        )

        if rep_resolution["field_rep_id"] is None and clean_text(row.get("registered_by_id")):
            issues["field_rep_resolution_failures"] += 1
        if not mapped_doctor_key or not resolved_doctor_id:
            issues["unmapped_enrollments"] += 1
            continue
        key = (campaign_id_normalized, mapped_doctor_key)
        if key in base_seen:
            continue
        base_seen.add(key)
        bridge_campaign_doctor_base_rows.append(
            {
                "campaign_id_original": clean_text((campaign or {}).get("campaign_id_original")) or clean_text(row.get("campaign_id")),
                "campaign_id_normalized": campaign_id_normalized,
                "doctor_key": mapped_doctor_key,
                "doctor_id": resolved_doctor_id,
                "full_name": full_name,
                "clinic_name": clean_text((doctor_dim or {}).get("clinic_name")),
                "city": city,
                "district": clean_text((doctor_dim or {}).get("district")),
                "state": state,
                "field_rep_id_resolved": clean_text((doctor_dim or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((doctor_dim or {}).get("field_rep_external_id")),
                "enrolled_at_ts": first_non_empty(
                    iso_datetime((credit or {}).get("row", {}).get("credit_effective_from") if credit else None),
                    _best_registered_at(row),
                ),
                "is_enrolled_flag": "true",
            }
        )
        campaigns_by_doctor[mapped_doctor_key].append(campaign_id_normalized)

    for (campaign_id_normalized, campaign_doctor_id), credit in rep_credit_by_campaign_doctor.items():
        if privacy_allowlist and not campaign_allowed_by_allowlist(campaign_id_normalized, privacy_allowlist):
            continue
        mapped = map_by_campaign_doctor_id.get(campaign_doctor_id, {})
        mapped_doctor_key = clean_text(mapped.get("doctor_key"))
        if not mapped_doctor_key or not clean_text((doctor_by_key.get(mapped_doctor_key or "") or {}).get("doctor_id")):
            issues["rep_credit_unmapped_doctor"] += 1
            continue
        key = (campaign_id_normalized, mapped_doctor_key)
        if key in base_seen:
            continue
        campaign = campaign_by_id.get(campaign_id_normalized, {})
        if not campaign:
            issues["rep_credit_unresolved_campaign"] += 1
            continue
        campaign_doctor = campaign_doctor_by_id.get(campaign_doctor_id, {})
        doctor_dim = doctor_by_key.get(mapped_doctor_key or "")
        rep_resolution = credit["rep_resolution"]
        resolved_doctor_id = clean_text((doctor_dim or {}).get("doctor_id"))
        state = first_non_empty(clean_text((doctor_dim or {}).get("state")), clean_text(campaign_doctor.get("state")))
        city = first_non_empty(clean_text((doctor_dim or {}).get("city")), clean_text(campaign_doctor.get("city")))
        full_name = first_non_empty(clean_text((doctor_dim or {}).get("full_name")), clean_text(campaign_doctor.get("full_name")))
        enrolled_at = iso_datetime(credit["row"].get("credit_effective_from"))
        if person_privacy_rules and not _pe_person_visible(
            {
                **(doctor_dim or {}),
                **campaign_doctor,
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")) or campaign_id_normalized,
            },
            person_privacy_rules,
        ):
            continue

        fact_campaign_enrollment_rows.append(
            {
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")) or campaign_id_normalized,
                "campaign_id_normalized": campaign_id_normalized,
                "campaign_doctor_id": campaign_doctor_id,
                "doctor_key": mapped_doctor_key,
                "registered_at_ts": enrolled_at,
                "registered_by_field_rep_id": rep_resolution["field_rep_id"],
                "registered_by_field_rep_external_id": rep_resolution["field_rep_external_id"],
                "whitelabel_enabled": "false",
                "whitelabel_subdomain": None,
                "doctor_id": resolved_doctor_id,
                "full_name": full_name,
                "clinic_name": clean_text((doctor_dim or {}).get("clinic_name")),
                "city": city,
                "district": clean_text((doctor_dim or {}).get("district")),
                "state": state,
                "field_rep_id_resolved": clean_text((doctor_dim or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((doctor_dim or {}).get("field_rep_external_id")),
                "campaign_name": clean_text(campaign.get("campaign_name")),
                "brand_name": clean_text(campaign.get("brand_name")),
                "enrollment_unresolved_flag": "false",
            }
        )
        base_seen.add(key)
        bridge_campaign_doctor_base_rows.append(
            {
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")) or campaign_id_normalized,
                "campaign_id_normalized": campaign_id_normalized,
                "doctor_key": mapped_doctor_key,
                "doctor_id": resolved_doctor_id,
                "full_name": full_name,
                "clinic_name": clean_text((doctor_dim or {}).get("clinic_name")),
                "city": city,
                "district": clean_text((doctor_dim or {}).get("district")),
                "state": state,
                "field_rep_id_resolved": clean_text((doctor_dim or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((doctor_dim or {}).get("field_rep_external_id")),
                "enrolled_at_ts": enrolled_at,
                "is_enrolled_flag": "true",
            }
        )
        campaigns_by_doctor[mapped_doctor_key].append(campaign_id_normalized)

    share_rows_enriched: list[dict[str, Any]] = []
    share_by_public_id: dict[str, dict[str, Any]] = {}
    share_by_source_id: dict[str, dict[str, Any]] = {}
    share_by_source_uuid: dict[str, dict[str, Any]] = {}
    for row in share_rows:
        share_public_id = clean_text(row.get("public_id"))
        shared_item_type = clean_text(row.get("shared_item_type"))
        shared_item_code = clean_text(row.get("shared_item_code"))
        doctor_id = clean_text(row.get("doctor_id"))
        doctor = doctor_by_key.get(doctor_id or "")
        video = video_by_code.get(shared_item_code or "") if shared_item_type == "video" else None
        bundle = bundle_by_code.get(shared_item_code or "") if shared_item_type == "cluster" else None
        attribution_input = dict(row)
        attribution_input["doctor_key"] = clean_text((doctor or {}).get("doctor_key")) or doctor_id
        attribution_input["shared_at_ts"] = iso_datetime(row.get("shared_at"))
        attribution_input["campaign_uuid"] = row.get("campaign_uuid")
        attribution_input["pe_campaign_uuid"] = row.get("pe_campaign_uuid")
        attribution = attribute_share_row(
            attribution_input,
            campaigns_by_doctor=campaigns_by_doctor,
            campaign_by_id=campaign_by_id,
            campaign_by_cluster_code=campaign_by_cluster_code,
            campaign_videos_by_campaign=campaign_videos_by_campaign,
            campaign_by_cluster_reference=campaign_by_cluster_reference,
            campaign_by_video_reference=campaign_by_video_reference,
            single_active_fallback_campaigns=single_active_fallback_campaigns,
            campaign_banner_click_events=campaign_banner_click_events,
            campaign_id_by_source_uuid=campaign_id_by_source_uuid,
        )
        if privacy_allowlist and not _pe_campaign_allowed(attribution, privacy_allowlist):
            continue
        if person_privacy_rules and not _pe_person_visible(
            {
                **(doctor or {}),
                **attribution,
                "recipient_reference": row.get("recipient_reference"),
            },
            person_privacy_rules,
        ):
            continue
        dq_errors = []
        if not doctor_id:
            dq_errors.append("missing_doctor_id")
            issues["share_missing_doctor_id"] += 1
        if shared_item_type == "video" and video is None:
            dq_errors.append("video_not_found")
            issues["share_item_missing"] += 1
        if shared_item_type == "cluster" and bundle is None:
            dq_errors.append("bundle_not_found")
            issues["share_item_missing"] += 1
        if clean_text(attribution.get("dq_error")):
            dq_errors.append(clean_text(attribution.get("dq_error")))
            if attribution.get("campaign_attribution_method") in {"unattributed_cluster", "unattributed_video"}:
                issues["unattributed_shares"] += 1
            if attribution.get("campaign_attribution_method") == "ambiguous_video":
                issues["ambiguous_video_attribution"] += 1

        share_payload = {
            "share_public_id": share_public_id,
            "source_share_id": clean_text(row.get("id")),
            "source_share_uuid": clean_text(row.get("share_event_uuid")),
            "doctor_summary_id": clean_text(row.get("doctor_summary_id")),
            "doctor_id": doctor_id,
            "doctor_key": clean_text((doctor or {}).get("doctor_key")) or doctor_id,
            "doctor_name_snapshot": clean_text(row.get("doctor_name_snapshot")),
            "clinic_name_snapshot": clean_text(row.get("clinic_name_snapshot")),
            "share_channel": clean_text(row.get("share_channel")),
            "shared_by_role": clean_text(row.get("shared_by_role")),
            "shared_item_type": shared_item_type,
            "shared_item_code": shared_item_code,
            "shared_item_name": first_non_empty(
                clean_text(row.get("shared_item_name")),
                clean_text((video or {}).get("default_display_label")),
                clean_text((bundle or {}).get("display_name")),
            ),
            "language_code": clean_text(row.get("language_code")),
            "recipient_reference": clean_text(row.get("recipient_reference")),
            "recipient_reference_version": clean_text(row.get("recipient_reference_version")) or "1",
            "shared_at_ts": iso_datetime(row.get("shared_at")),
            "campaign_id_original": clean_text(attribution.get("campaign_id_original")),
            "campaign_id_normalized": clean_text(attribution.get("campaign_id_normalized")),
            "campaign_attribution_method": clean_text(attribution.get("campaign_attribution_method")),
            "is_campaign_attributed_flag": clean_text(attribution.get("is_campaign_attributed_flag")) or "false",
            "video_id": clean_text((video or {}).get("video_id")),
            "video_code": clean_text((video or {}).get("video_code")) if video else (shared_item_code if shared_item_type == "video" else None),
            "video_display_label": clean_text((video or {}).get("default_display_label")),
            "video_cluster_id": clean_text((bundle or {}).get("video_cluster_id")),
            "video_cluster_code": clean_text((bundle or {}).get("video_cluster_code")) if bundle else (shared_item_code if shared_item_type == "cluster" else None),
            "video_cluster_display_label": clean_text((bundle or {}).get("display_name")),
            "therapy_area_name": clean_text((video or {}).get("primary_therapy_name")) or clean_text((bundle or {}).get("primary_therapy_name")),
            "trigger_name": clean_text((video or {}).get("primary_trigger_name")) or clean_text((bundle or {}).get("trigger_name")),
            "city": clean_text((doctor or {}).get("city")),
            "district": clean_text((doctor or {}).get("district")),
            "state": clean_text((doctor or {}).get("state")),
            "field_rep_id_resolved": clean_text((doctor or {}).get("field_rep_id_resolved")),
            "field_rep_external_id": clean_text((doctor or {}).get("field_rep_external_id")),
            "_dq_status": "PASS" if not dq_errors else "WARN",
            "_dq_errors": ",".join(error for error in dq_errors if error),
        }
        share_rows_enriched.append(share_payload)
        if share_public_id:
            share_by_public_id[share_public_id] = share_payload
        source_share_id = clean_text(row.get("id"))
        if source_share_id:
            share_by_source_id[source_share_id] = share_payload
        source_share_uuid = normalize_campaign_id(row.get("share_event_uuid"))
        if source_share_uuid:
            share_by_source_uuid[source_share_uuid] = share_payload

    playback_rows_enriched: list[dict[str, Any]] = []
    for row in playback_rows:
        share = resolve_playback_share(
            row,
            share_by_public_id=share_by_public_id,
            share_by_source_id=share_by_source_id,
            share_by_source_uuid=share_by_source_uuid,
            share_rows=share_rows_enriched,
            bundle_videos_by_code=bundle_videos_by_code,
        )
        share_public_id = clean_text((share or {}).get("share_public_id")) or clean_text(row.get("share_public_id"))
        if privacy_allowlist and not share:
            continue
        doctor_id = clean_text((share or {}).get("doctor_id")) or clean_text(row.get("doctor_id"))
        doctor = doctor_by_key.get(clean_text((share or {}).get("doctor_key")) or doctor_id or "")
        if person_privacy_rules and not _pe_person_visible({**(doctor or {}), **(share or {})}, person_privacy_rules):
            continue
        milestone = as_int(row.get("milestone_percent"), default=-1)
        dq_errors = []
        if clean_text(row.get("event_type")) == "progress" and milestone not in {25, 50, 75, 100}:
            dq_errors.append("invalid_playback_milestone")
            issues["invalid_playback_milestone"] += 1
        if share is None:
            dq_errors.append("orphan_playback")
            issues["orphan_playback"] += 1
        video = video_by_code.get(clean_text(row.get("video_code")) or "")
        playback_rows_enriched.append(
            {
                "source_playback_id": clean_text(row.get("id")),
                "source_share_uuid": clean_text(row.get("share_event_uuid")),
                "share_id": clean_text(row.get("share_id")),
                "share_public_id": share_public_id,
                "doctor_summary_id": clean_text(row.get("doctor_summary_id")),
                "doctor_id": doctor_id,
                "doctor_key": clean_text((share or {}).get("doctor_key")) or clean_text((doctor or {}).get("doctor_key")) or doctor_id,
                "page_item_type": clean_text(row.get("page_item_type")),
                "event_type": clean_text(row.get("event_type")),
                "video_code": clean_text(row.get("video_code")),
                "video_id": clean_text((video or {}).get("video_id")),
                "video_name": first_non_empty(clean_text(row.get("video_name")), clean_text((video or {}).get("default_display_label"))),
                "milestone_percent_num": str(milestone) if milestone >= 0 else None,
                "occurred_at_ts": iso_datetime(row.get("occurred_at")),
                "campaign_id_original": clean_text((share or {}).get("campaign_id_original")),
                "campaign_id_normalized": clean_text((share or {}).get("campaign_id_normalized")),
                "campaign_attribution_method": clean_text((share or {}).get("campaign_attribution_method")),
                "is_campaign_attributed_flag": clean_text((share or {}).get("is_campaign_attributed_flag")) or "false",
                "shared_item_type": clean_text((share or {}).get("shared_item_type")),
                "video_cluster_code": clean_text((share or {}).get("video_cluster_code")),
                "city": clean_text((share or {}).get("city")) or clean_text((doctor or {}).get("city")),
                "district": clean_text((share or {}).get("district")) or clean_text((doctor or {}).get("district")),
                "state": clean_text((share or {}).get("state")) or clean_text((doctor or {}).get("state")),
                "field_rep_id_resolved": clean_text((share or {}).get("field_rep_id_resolved")) or clean_text((doctor or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((share or {}).get("field_rep_external_id")) or clean_text((doctor or {}).get("field_rep_external_id")),
                "orphan_playback_flag": "true" if share is None else "false",
                "_dq_status": "PASS" if not dq_errors else "WARN",
                "_dq_errors": ",".join(dq_errors),
            }
        )

    share_summary_by_id = {
        clean_text(row.get("id")): row
        for row in share_summary_rows
        if clean_text(row.get("id"))
    }

    banner_click_rows_enriched: list[dict[str, Any]] = []
    for row in banner_click_rows:
        doctor_summary_id = clean_text(row.get("doctor_summary_id"))
        share_summary = share_summary_by_id.get(doctor_summary_id or "", {})
        doctor_id = _valid_doctor_identifier(row.get("doctor_id")) or clean_text((share_summary or {}).get("doctor_id"))
        doctor = doctor_by_key.get(doctor_id or "")
        attribution = attribute_banner_click_row(
            {
                "doctor_key": clean_text((doctor or {}).get("doctor_key")) or doctor_id,
                "doctor_id": doctor_id,
                "banner_target_url": row.get("banner_target_url"),
                "banner_id": row.get("banner_id"),
                "campaign_uuid": row.get("campaign_uuid"),
                "clicked_at_ts": iso_datetime(row.get("clicked_at")),
            },
            campaigns_by_doctor=campaigns_by_doctor,
            campaign_by_id=campaign_by_id,
            campaigns_by_banner_target_url=campaigns_by_banner_target_url,
            campaigns_by_banner_id=campaigns_by_banner_id,
            campaign_id_by_source_uuid=campaign_id_by_source_uuid,
        )
        if privacy_allowlist and not _pe_campaign_allowed(attribution, privacy_allowlist):
            continue
        if person_privacy_rules and not _pe_person_visible({**(doctor or {}), **attribution}, person_privacy_rules):
            continue
        dq_errors = []
        if not doctor_id:
            dq_errors.append("missing_doctor_id")
            issues["banner_click_missing_doctor_id"] += 1
        if clean_text(attribution.get("dq_error")):
            dq_errors.append(clean_text(attribution.get("dq_error")))
            if attribution.get("campaign_attribution_method") == "ambiguous_banner_target_url":
                issues["ambiguous_banner_click_attribution"] += 1
            elif attribution.get("campaign_attribution_method") == "ambiguous_doctor_campaign":
                issues["ambiguous_banner_click_attribution"] += 1
            else:
                issues["unattributed_banner_clicks"] += 1

        banner_click_rows_enriched.append(
            {
                "source_banner_click_id": clean_text(row.get("id")),
                "source_banner_click_uuid": clean_text(row.get("source_banner_click_id")),
                "doctor_summary_id": doctor_summary_id,
                "doctor_id": doctor_id,
                "doctor_key": clean_text((doctor or {}).get("doctor_key")) or doctor_id,
                "page_type": clean_text(row.get("page_type")),
                "banner_id": clean_text(row.get("banner_id")),
                "banner_name": clean_text(row.get("banner_name")),
                "banner_target_url": clean_text(row.get("banner_target_url")),
                "clicked_at_ts": iso_datetime(row.get("clicked_at")),
                "campaign_id_original": clean_text(attribution.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(attribution.get("campaign_id_normalized")),
                "campaign_attribution_method": clean_text(attribution.get("campaign_attribution_method")),
                "is_campaign_attributed_flag": clean_text(attribution.get("is_campaign_attributed_flag")) or "false",
                "city": clean_text((doctor or {}).get("city")),
                "district": clean_text((doctor or {}).get("district")),
                "state": clean_text((doctor or {}).get("state")),
                "field_rep_id_resolved": clean_text((doctor or {}).get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text((doctor or {}).get("field_rep_external_id")),
                "_dq_status": "PASS" if not dq_errors else "WARN",
                "_dq_errors": ",".join(error for error in dq_errors if error),
            }
        )

    funnel_rows = rollup_share_funnel(share_rows_enriched, playback_rows_enriched)

    playback_by_share_video: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in playback_rows_enriched:
        share_public_id = clean_text(row.get("share_public_id"))
        video_code = clean_text(row.get("video_code"))
        if share_public_id and video_code:
            playback_by_share_video[(share_public_id, video_code)].append(row)

    fact_video_view_rows: list[dict[str, Any]] = []
    fact_video_view_index: dict[tuple[str, str], dict[str, Any]] = {}
    for share in share_rows_enriched:
        share_public_id = clean_text(share.get("share_public_id"))
        if not share_public_id:
            continue
        candidate_video_codes: list[str] = []
        if clean_text(share.get("shared_item_type")) == "video" and clean_text(share.get("video_code")):
            candidate_video_codes = [clean_text(share.get("video_code")) or ""]
        elif clean_text(share.get("shared_item_type")) == "cluster" and clean_text(share.get("video_cluster_code")):
            candidate_video_codes = [code for code in bundle_videos_by_code.get(clean_text(share.get("video_cluster_code")) or "", []) if code]
        for video_code in candidate_video_codes:
            video = video_by_code.get(video_code)
            payload = {
                "share_public_id": share_public_id,
                "doctor_key": clean_text(share.get("doctor_key")),
                "doctor_id": clean_text(share.get("doctor_id")),
                "campaign_id_original": clean_text(share.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(share.get("campaign_id_normalized")),
                "shared_item_type": clean_text(share.get("shared_item_type")),
                "shared_at_ts": clean_text(share.get("shared_at_ts")),
                "language_code": clean_text(share.get("language_code")),
                "video_code": video_code,
                "video_id": clean_text((video or {}).get("video_id")),
                "preferred_display_label": clean_text((video or {}).get("default_display_label")) or video_code,
                "video_cluster_code": clean_text(share.get("video_cluster_code")),
                "video_cluster_display_label": clean_text(share.get("video_cluster_display_label")),
                "therapy_area_name": clean_text((video or {}).get("primary_therapy_name")) or clean_text(share.get("therapy_area_name")),
                "trigger_name": clean_text((video or {}).get("primary_trigger_name")) or clean_text(share.get("trigger_name")),
                "state": clean_text(share.get("state")),
                "district": clean_text(share.get("district")),
                "city": clean_text(share.get("city")),
                "field_rep_id_resolved": clean_text(share.get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text(share.get("field_rep_external_id")),
                "is_played": "false",
                "is_viewed_50": "false",
                "is_viewed_100": "false",
                "play_first_ts": None,
                "view_50_first_ts": None,
                "view_100_first_ts": None,
            }
            fact_video_view_rows.append(payload)
            fact_video_view_index[(share_public_id, video_code)] = payload

    for key, events in playback_by_share_video.items():
        share_public_id, video_code = key
        payload = fact_video_view_index.get(key)
        if payload is None:
            video = video_by_code.get(video_code)
            payload = {
                "share_public_id": share_public_id,
                "doctor_key": None,
                "doctor_id": None,
                "campaign_id_original": None,
                "campaign_id_normalized": None,
                "shared_item_type": None,
                "shared_at_ts": None,
                "language_code": None,
                "video_code": video_code,
                "video_id": clean_text((video or {}).get("video_id")),
                "preferred_display_label": clean_text((video or {}).get("default_display_label")) or video_code,
                "video_cluster_code": None,
                "video_cluster_display_label": None,
                "therapy_area_name": clean_text((video or {}).get("primary_therapy_name")),
                "trigger_name": clean_text((video or {}).get("primary_trigger_name")),
                "state": None,
                "district": None,
                "city": None,
                "field_rep_id_resolved": None,
                "field_rep_external_id": None,
                "is_played": "false",
                "is_viewed_50": "false",
                "is_viewed_100": "false",
                "play_first_ts": None,
                "view_50_first_ts": None,
                "view_100_first_ts": None,
            }
            fact_video_view_rows.append(payload)
            fact_video_view_index[key] = payload
        for event in events:
            occurred_at = clean_text(event.get("occurred_at_ts"))
            if clean_text(event.get("event_type")) == "play":
                payload["is_played"] = "true"
                payload["play_first_ts"] = min([value for value in [clean_text(payload.get("play_first_ts")), occurred_at] if value], default=occurred_at)
            milestone = as_int(event.get("milestone_percent_num"), default=-1)
            if clean_text(event.get("event_type")) == "progress" and milestone >= 50:
                payload["is_viewed_50"] = "true"
                payload["view_50_first_ts"] = min([value for value in [clean_text(payload.get("view_50_first_ts")), occurred_at] if value], default=occurred_at)
            if clean_text(event.get("event_type")) == "progress" and milestone >= 100:
                payload["is_viewed_100"] = "true"
                payload["view_100_first_ts"] = min([value for value in [clean_text(payload.get("view_100_first_ts")), occurred_at] if value], default=occurred_at)

    actual_share_counts: dict[str, int] = defaultdict(int)
    for row in share_rows_enriched:
        doctor_id = clean_text(row.get("doctor_id"))
        if doctor_id:
            actual_share_counts[doctor_id] += 1
    recon_rows: list[dict[str, Any]] = []
    for row in share_summary_rows:
        doctor_id = clean_text(row.get("doctor_id"))
        if not doctor_id:
            continue
        summary_total = as_int(row.get("total_shares"))
        actual_total = actual_share_counts.get(doctor_id, 0)
        discrepancy = summary_total != actual_total
        if discrepancy:
            issues["share_summary_mismatch"] += 1
        recon_rows.append(
            {
                "doctor_id": doctor_id,
                "doctor_summary_total_shares": summary_total,
                "fact_share_count": actual_total,
                "discrepancy_flag": "true" if discrepancy else "false",
            }
        )

    if privacy_allowlist:
        allowed_doctor_keys = {
            clean_text(row.get("doctor_key"))
            for rows in (
                fact_campaign_enrollment_rows,
                bridge_campaign_doctor_base_rows,
                share_rows_enriched,
                playback_rows_enriched,
                banner_click_rows_enriched,
                funnel_rows,
                fact_video_view_rows,
            )
            for row in rows
            if clean_text(row.get("doctor_key"))
        }
        allowed_campaign_doctor_ids = {
            clean_text(row.get("campaign_doctor_id"))
            for row in fact_campaign_enrollment_rows + bridge_campaign_doctor_base_rows
            if clean_text(row.get("campaign_doctor_id"))
        }
        allowed_field_rep_ids = {
            clean_text(row.get(field))
            for rows in (
                fact_campaign_enrollment_rows,
                bridge_campaign_doctor_base_rows,
                share_rows_enriched,
                playback_rows_enriched,
                banner_click_rows_enriched,
                funnel_rows,
                fact_video_view_rows,
            )
            for row in rows
            for field in ("registered_by_field_rep_id", "field_rep_id_resolved")
            if clean_text(row.get(field))
        }
        dim_doctor_rows = [row for row in dim_doctor_rows if clean_text(row.get("doctor_key")) in allowed_doctor_keys]
        dim_field_rep_rows = [row for row in dim_field_rep_rows if clean_text(row.get("field_rep_id")) in allowed_field_rep_ids]
        map_rows = [row for row in map_rows if clean_text(row.get("campaign_doctor_id")) in allowed_campaign_doctor_ids]
        recon_rows = [row for row in recon_rows if clean_text(row.get("doctor_id")) in allowed_doctor_keys]

    if person_privacy_rules:
        visible_doctor_keys = {
            clean_text(row.get("doctor_key"))
            for rows in (
                fact_campaign_enrollment_rows,
                bridge_campaign_doctor_base_rows,
                share_rows_enriched,
                playback_rows_enriched,
                banner_click_rows_enriched,
                funnel_rows,
                fact_video_view_rows,
            )
            for row in rows
            if clean_text(row.get("doctor_key"))
        }
        visible_field_rep_ids = {
            clean_text(row.get(field))
            for rows in (
                fact_campaign_enrollment_rows,
                bridge_campaign_doctor_base_rows,
                share_rows_enriched,
                playback_rows_enriched,
                banner_click_rows_enriched,
                funnel_rows,
                fact_video_view_rows,
            )
            for row in rows
            for field in ("registered_by_field_rep_id", "field_rep_id_resolved")
            if clean_text(row.get(field))
        }
        person_matched_doctor_ids = {
            clean_text(row.get("doctor_id"))
            for row in dim_doctor_rows
            if clean_text(row.get("doctor_id")) and _pe_person_matches(row, person_privacy_rules)
        }
        dim_doctor_rows = [
            row
            for row in dim_doctor_rows
            if not _pe_person_matches(row, person_privacy_rules)
            or clean_text(row.get("doctor_key")) in visible_doctor_keys
        ]
        dim_field_rep_rows = [
            row
            for row in dim_field_rep_rows
            if not _pe_person_matches(row, person_privacy_rules)
            or clean_text(row.get("field_rep_id")) in visible_field_rep_ids
        ]
        visible_dim_doctor_ids = {clean_text(dim.get("doctor_id")) for dim in dim_doctor_rows if clean_text(dim.get("doctor_id"))}
        recon_rows = [
            row
            for row in recon_rows
            if clean_text(row.get("doctor_id")) not in person_matched_doctor_ids
            or clean_text(row.get("doctor_id")) in visible_dim_doctor_ids
        ]

    _replace_silver_table("dim_field_rep", dim_field_rep_rows)
    _replace_silver_table("map_campaign_doctor_to_doctor", map_rows)
    _replace_silver_table("dim_doctor", dim_doctor_rows)
    _replace_silver_table("dim_video", dim_video_rows)
    _replace_silver_table("dim_bundle", dim_bundle_rows)
    _replace_silver_table("bridge_bundle_video", bridge_bundle_video_rows)
    _replace_silver_table("dim_campaign", dim_campaign_rows)
    _replace_silver_table("bridge_campaign_content", bridge_campaign_content_rows)
    _replace_silver_table("fact_campaign_enrollment", fact_campaign_enrollment_rows)
    _replace_silver_table("bridge_campaign_doctor_base", bridge_campaign_doctor_base_rows)
    _replace_silver_table("fact_share_activity", share_rows_enriched)
    _replace_silver_table("fact_share_playback_event", playback_rows_enriched)
    _replace_silver_table("fact_share_banner_click", banner_click_rows_enriched)
    _replace_silver_table("fact_share_funnel_first_seen", funnel_rows)
    _replace_silver_table("fact_video_view", fact_video_view_rows)
    _replace_silver_table("recon_doctor_share_summary", recon_rows)

    counts.update(
        {
            "dim_field_rep": len(dim_field_rep_rows),
            "map_campaign_doctor_to_doctor": len(map_rows),
            "dim_doctor": len(dim_doctor_rows),
            "dim_video": len(dim_video_rows),
            "dim_bundle": len(dim_bundle_rows),
            "bridge_bundle_video": len(bridge_bundle_video_rows),
            "dim_campaign": len(dim_campaign_rows),
            "bridge_campaign_content": len(bridge_campaign_content_rows),
            "fact_campaign_enrollment": len(fact_campaign_enrollment_rows),
            "bridge_campaign_doctor_base": len(bridge_campaign_doctor_base_rows),
            "fact_share_activity": len(share_rows_enriched),
            "fact_share_playback_event": len(playback_rows_enriched),
            "fact_share_banner_click": len(banner_click_rows_enriched),
            "fact_share_funnel_first_seen": len(funnel_rows),
            "fact_video_view": len(fact_video_view_rows),
            "recon_doctor_share_summary": len(recon_rows),
            "ops.reporting_campaign_privacy_allowlist_active": len(privacy_allowlist),
            "ops.reporting_person_privacy_rule_active": len(person_privacy_rules),
        }
    )

    return {"counts": counts, "issues": dict(issues)}
