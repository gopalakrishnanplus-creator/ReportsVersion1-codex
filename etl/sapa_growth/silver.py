from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from django.conf import settings

from etl.sapa_growth.specs import BRONZE_SCHEMA, SILVER_SCHEMA
from etl.sapa_growth.storage import fetch_table, replace_table
from etl.reporting_privacy import (
    active_campaign_privacy_allowlist,
    active_person_privacy_rules,
    active_raw_visibility_rules,
    campaign_allowed_by_allowlist,
    person_privacy_matching_rules,
    raw_visibility_entity_ids,
    row_matches_raw_visibility_ids,
    row_visible_by_person_privacy,
)
from sapa_growth.logic import (
    as_int,
    canonical_doctor_key,
    classify_metric_event,
    clean_text,
    display_name_from_sources,
    explode_followup_schedule,
    hash_fields,
    iso_date,
    iso_datetime,
    location_label,
    map_course_status,
    normalize_phone,
    parse_date,
    parse_datetime,
    split_full_name,
    webinar_effective_date,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_text(value: Any, fallback: str = "") -> str:
    return clean_text(value) or fallback


def _doctor_indexes(dim_rows: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_doctor_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_email: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in dim_rows:
        doctor_id = clean_text(row.get("source_doctor_id"))
        email_values = [
            clean_text(row.get("canonical_email")),
            clean_text(row.get("clinic_user1_email")),
            clean_text(row.get("clinic_user2_email")),
        ]
        phone_values = [
            normalize_phone(row.get("canonical_phone")),
            normalize_phone(row.get("canonical_whatsapp_no")),
            normalize_phone(row.get("receptionist_whatsapp_number")),
        ]
        if doctor_id:
            by_doctor_id[doctor_id].append(row)
        for email in email_values:
            if email:
                by_email[email.lower()].append(row)
        for phone in phone_values:
            if phone:
                by_phone[phone].append(row)
    return by_doctor_id, by_email, by_phone


def _doctor_matches_for_api(
    row: dict[str, Any],
    by_email: dict[str, list[dict[str, Any]]],
    by_phone: dict[str, list[dict[str, Any]]],
    event_date: Any = None,
) -> list[tuple[dict[str, Any] | None, str]]:
    email = clean_text(row.get("email") or row.get("user_email"))
    if email and email.lower() in by_email:
        return [(_best_dim_for_event(by_email[email.lower()], event_date), "email")]
    phone = normalize_phone(row.get("phone"))
    if phone and phone in by_phone:
        return [(_best_dim_for_event(by_phone[phone], event_date), "phone")]
    return [(None, "unmapped")]


def _doctor_filters(dim_row: dict[str, Any] | None) -> dict[str, str]:
    if not dim_row:
        return {
            "doctor_display_name": "Unmapped",
            "city": "",
            "district": "",
            "state": "",
            "field_rep_id": "Unassigned",
            "field_rep_name": "Unassigned",
            "campaign_key": "",
            "campaign_label": "",
        }
    return {
        "doctor_display_name": _empty_text(dim_row.get("canonical_display_name"), "Unknown Doctor"),
        "city": _empty_text(dim_row.get("city")),
        "district": _empty_text(dim_row.get("district")),
        "state": _empty_text(dim_row.get("state")),
        "field_rep_id": _empty_text(dim_row.get("field_rep_id"), "Unassigned"),
        "field_rep_name": _empty_text(dim_row.get("field_rep_name"), _empty_text(dim_row.get("field_rep_id"), "Unassigned")),
        "campaign_key": _empty_text(dim_row.get("campaign_key")),
        "campaign_label": _empty_text(dim_row.get("campaign_label")),
    }


def _campaign_key_label(*rows: dict[str, Any] | None) -> tuple[str, str]:
    default_key = clean_text(settings.SAPA_DASHBOARD.get("DEFAULT_CAMPAIGN_KEY")) or "growth-clinic"
    default_label = clean_text(settings.SAPA_DASHBOARD.get("DEFAULT_CAMPAIGN_LABEL")) or "SAPA Growth Clinic Program"
    key = ""
    label = ""
    key_fields = ("brand_campaign_id", "campaign_id", "campaign_key", "campaign", "program_id")
    label_fields = ("campaign_name", "program_name", "brand_name", "campaign_label")
    for row in rows:
        if not row:
            continue
        if not key:
            for field in key_fields:
                key = clean_text(row.get(field))
                if key:
                    break
        if not label:
            for field in label_fields:
                label = clean_text(row.get(field))
                if label:
                    break
        if key and label:
            break
    if not key:
        key = default_key
    if not label:
        label = default_label if key == default_key else key.replace("-", " ").replace("_", " ").title()
    return key, label


def _sapa_campaign_allowed(value: Any, allowlist: set[str]) -> bool:
    return campaign_allowed_by_allowlist(value, allowlist)


def _sapa_person_visible(row: dict[str, Any], person_rules: list[dict[str, Any]]) -> bool:
    return row_visible_by_person_privacy(
        row,
        person_rules,
        campaign_fields=("campaign_id", "campaign_key", "brand_campaign_id"),
        email_fields=("canonical_email", "clinic_user1_email", "clinic_user2_email", "email", "user_email"),
        phone_fields=("canonical_phone", "canonical_whatsapp_no", "receptionist_whatsapp_number", "phone", "patient_whatsapp"),
    )


def _sapa_person_matches(row: dict[str, Any], person_rules: list[dict[str, Any]]) -> bool:
    return bool(
        person_privacy_matching_rules(
            row,
            person_rules,
            email_fields=("canonical_email", "clinic_user1_email", "clinic_user2_email", "email", "user_email"),
            phone_fields=("canonical_phone", "canonical_whatsapp_no", "receptionist_whatsapp_number", "phone", "patient_whatsapp"),
        )
    )


def _apply_raw_visibility_to_sapa_inputs(
    tables: dict[str, list[dict[str, Any]]],
    raw_visibility_rules: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    if not raw_visibility_rules:
        return tables

    output = {key: list(rows) for key, rows in tables.items()}

    def remove_hidden(table_key: str, hidden_ids: set[str], fields: tuple[str, ...]) -> None:
        if not hidden_ids:
            return
        output[table_key] = [
            row for row in output.get(table_key, []) if not row_matches_raw_visibility_ids(row, hidden_ids, fields)
        ]

    campaign_ids = raw_visibility_entity_ids(raw_visibility_rules, "campaign", system_key="sapa")
    if campaign_ids:
        for table_key, fields in {
            "campaign_rows": ("id", "campaign_id", "brand_campaign_id"),
            "campaign_enrollments": ("campaign_id",),
            "campaign_field_rep_rows": ("campaign_id",),
            "campaign_doctors": ("campaign_id", "brand_campaign_id"),
            "rfa_activity_events": ("campaign_uuid", "campaign_id", "brand_campaign_id"),
            "redflag_submissions": ("campaign_id",),
            "gnd_submissions": ("campaign_id",),
            "followup_rows": ("campaign_id",),
            "metric_rows": ("campaign_id",),
        }.items():
            remove_hidden(table_key, campaign_ids, fields)

    field_rep_ids = raw_visibility_entity_ids(raw_visibility_rules, "field_rep", system_key="sapa")
    if field_rep_ids:
        for table_key, fields in {
            "source_field_rep_rows": ("id", "brand_supplied_field_rep_id", "user_id", "phone_number"),
            "campaign_field_rep_rows": ("id", "field_rep_id"),
            "campaign_doctors": ("field_rep_id",),
            "campaign_enrollments": ("registered_by_id",),
            "rfa_activity_events": ("field_rep_uuid_at_event_time",),
            "metric_rows": ("field_rep_id", "action_key"),
        }.items():
            remove_hidden(table_key, field_rep_ids, fields)

    doctor_ids = raw_visibility_entity_ids(raw_visibility_rules, "doctor", system_key="sapa")
    doctor_ids |= raw_visibility_entity_ids(raw_visibility_rules, "patient", system_key="sapa")
    if doctor_ids:
        for table_key, fields in {
            "campaign_doctors": ("id", "doctor_id", "email", "phone"),
            "redflag_doctors": ("doctor_id", "email", "whatsapp_no", "clinic_phone"),
            "campaign_enrollments": ("doctor_id",),
            "redflag_submissions": ("doctor_id", "patient_id", "record_id"),
            "gnd_submissions": ("doctor_id", "patient_id", "id"),
            "followup_rows": ("doctor_id", "patient_id", "patient_whatsapp", "id"),
            "metric_rows": ("doctor_id", "patient_id", "id"),
            "clinic_outcomes": ("doctor_id",),
            "rfa_activity_events": ("doctor_uuid", "patient_id_raw"),
            "webinar_rows": ("email", "phone", "registration_id"),
            "course_summary_rows": ("user_id", "user_email", "phone"),
            "course_breakdown_rows": ("user_id", "user_email", "phone"),
        }.items():
            remove_hidden(table_key, doctor_ids, fields)

    content_ids = raw_visibility_entity_ids(raw_visibility_rules, "content", system_key="sapa")
    content_ids |= raw_visibility_entity_ids(raw_visibility_rules, "collateral", system_key="sapa")
    if content_ids:
        for table_key, fields in {
            "redflag_catalog_rows": ("red_flag_id", "doctor_video_url"),
            "gnd_redflag_catalog_rows": ("red_flag_id", "doctor_video_url"),
            "redflags_patientvideo_rows": ("id", "red_flag_id", "patient_video_url"),
            "gnd_patientvideo_rows": ("id", "red_flag_id", "patient_video_url"),
            "redflag_occurrences": ("red_flag_id", "red_flag", "id"),
            "gnd_occurrences": ("red_flag_id", "red_flag", "id"),
            "metric_rows": ("red_flag_id", "video_url", "form_id", "action_key"),
            "rfa_activity_events": ("red_flag_id_raw", "form_id_raw", "overall_flag_code"),
        }.items():
            remove_hidden(table_key, content_ids, fields)

    activity_ids = raw_visibility_entity_ids(raw_visibility_rules, "activity", system_key="sapa")
    if activity_ids:
        for table_key, fields in {
            "rfa_activity_events": ("activity_event_uuid", "source_event_id", "source_pk_value"),
            "redflag_submissions": ("record_id",),
            "gnd_submissions": ("id",),
            "redflag_occurrences": ("id", "submission_id"),
            "gnd_occurrences": ("id", "submission_id"),
            "followup_rows": ("id",),
            "metric_rows": ("id",),
        }.items():
            remove_hidden(table_key, activity_ids, fields)

    return output


def _truthy(value: Any) -> bool:
    return (clean_text(value) or "").lower() in {"1", "true", "t", "yes", "y", "on"}


def _norm_id(value: Any) -> str:
    raw = clean_text(value) or ""
    return "".join(ch.lower() for ch in raw if ch.isalnum())


def _campaign_specific_doctor_key(base_key: str, campaign_key: str) -> str:
    if not campaign_key:
        return base_key
    return f"{base_key}::campaign:{campaign_key}"


def _campaign_ids_for_field_rep_login_event(
    *,
    rep: dict[str, Any] | None,
    row: dict[str, Any],
    rfa_campaigns: dict[str, dict[str, Any]],
    rep_campaign_ids: dict[str, set[str]],
    campaign_rep_ids: dict[str, set[str]] | None = None,
    campaign_rep_membership_ids: dict[str, set[str]] | None = None,
    rep_campaign_assignments: dict[str, list[dict[str, Any]]] | None = None,
) -> list[str]:
    meta = _json_object(row.get("meta"))
    campaign_hint = clean_text(row.get("campaign_id")) or clean_text(meta.get("campaign_id"))
    rep_identity_values = {
        clean_text((rep or {}).get("id")),
        clean_text((rep or {}).get("brand_supplied_field_rep_id")),
        clean_text(row.get("action_key")),
        clean_text(row.get("field_rep_id")),
        clean_text(meta.get("field_rep_id")),
        clean_text(meta.get("brand_supplied_field_rep_id")),
    }
    rep_identity_values = {value for value in rep_identity_values if value}
    source_rep_id = clean_text((rep or {}).get("id")) or clean_text(row.get("action_key")) or clean_text(row.get("field_rep_id")) or clean_text(meta.get("field_rep_id"))

    def rep_is_assigned(campaign_id: str) -> bool:
        if not rep_identity_values:
            return False
        if campaign_rep_membership_ids is not None:
            campaign_values = campaign_rep_membership_ids.get(campaign_id, set())
            normalized_campaign_values = {_norm_id(value) for value in campaign_values}
            return any(value in campaign_values or _norm_id(value) in normalized_campaign_values for value in rep_identity_values)
        if campaign_rep_ids is not None:
            campaign_values = campaign_rep_ids.get(campaign_id, set())
            return any(value in campaign_values for value in rep_identity_values)
        return bool(source_rep_id and campaign_id in rep_campaign_ids.get(source_rep_id, set()))

    if campaign_hint:
        hint_norm = _norm_id(campaign_hint)
        matched_campaigns = [
            campaign_id
            for campaign_id, campaign in rfa_campaigns.items()
            if _norm_id(campaign_id) == hint_norm or _norm_id(campaign.get("name")) == hint_norm
        ]
        output = []
        for campaign_id in matched_campaigns:
            if rep_is_assigned(campaign_id):
                output.append(campaign_id)
                continue
            if campaign_rep_membership_ids is not None and not campaign_rep_membership_ids.get(campaign_id):
                output.append(campaign_id)
        return sorted(output)

    if rep_campaign_assignments is not None:
        login_date = parse_date(row.get("ts"))
        candidates = []
        for identity in sorted(rep_identity_values):
            for assignment in rep_campaign_assignments.get(identity, []):
                campaign_id = clean_text(assignment.get("campaign_id"))
                if campaign_id not in rfa_campaigns:
                    continue
                assigned_at = parse_date(assignment.get("assigned_at"))
                campaign = rfa_campaigns.get(campaign_id) or {}
                campaign_start = parse_date(campaign.get("start_date"))
                campaign_end = parse_date(campaign.get("end_date"))
                if login_date and assigned_at and login_date < assigned_at:
                    continue
                if login_date and campaign_start and login_date < campaign_start:
                    continue
                if login_date and campaign_end and login_date > campaign_end:
                    continue
                candidates.append(campaign_id)
        candidate_campaigns = sorted(set(candidates))
        return candidate_campaigns

    candidate_campaigns = sorted(set().union(*(rep_campaign_ids.get(identity, set()) for identity in rep_identity_values)) & set(rfa_campaigns.keys()))
    return candidate_campaigns if len(candidate_campaigns) == 1 else []


def _campaign_start_for_match(row: dict[str, Any]) -> date | None:
    return (
        parse_date(row.get("campaign_registered_at"))
        or parse_date(row.get("campaign_start_date"))
        or parse_date(row.get("first_seen_at"))
    )


def _campaign_end_for_match(row: dict[str, Any]) -> date | None:
    return parse_date(row.get("campaign_end_date"))


def _best_dim_for_event(dim_rows: list[dict[str, Any]], event_date: Any = None) -> dict[str, Any] | None:
    if not dim_rows:
        return None
    if len(dim_rows) == 1:
        return dim_rows[0]

    event = parse_date(event_date) or date.today()

    def sort_key(row: dict[str, Any]) -> tuple[date, str]:
        return (_campaign_start_for_match(row) or date.min, clean_text(row.get("doctor_key")) or "")

    active_rows = []
    for row in dim_rows:
        start = _campaign_start_for_match(row)
        end = _campaign_end_for_match(row)
        if (start is None or start <= event) and (end is None or event <= end):
            active_rows.append(row)
    if active_rows:
        return max(active_rows, key=sort_key)

    prior_rows = [row for row in dim_rows if (_campaign_start_for_match(row) or date.min) <= event]
    if prior_rows:
        return max(prior_rows, key=sort_key)

    return min(dim_rows, key=sort_key)


def _activity_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw_payload = clean_text(row.get("event_payload_json"))
    if not raw_payload:
        return {}
    try:
        parsed = json.loads(raw_payload)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _activity_value(row: dict[str, Any], payload: dict[str, Any], *fields: str) -> Any:
    for field in fields:
        if field in payload and clean_text(payload.get(field)) is not None:
            return payload.get(field)
        if clean_text(row.get(field)) is not None:
            return row.get(field)
    return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = clean_text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _activity_events_as_legacy_rows(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    legacy_rows: dict[str, list[dict[str, Any]]] = {
        "redflags_patientsubmission": [],
        "gnd_gndpatientsubmission": [],
        "redflags_submissionredflag": [],
        "gnd_gndsubmissionredflag": [],
        "redflags_followupreminder": [],
        "redflags_metricevent": [],
    }
    for row in events:
        source_table = clean_text(row.get("source_table"))
        activity_type = (clean_text(row.get("activity_type")) or "").lower().replace("-", "_")
        activity_slug = "".join(ch for ch in activity_type if ch.isalnum())
        if source_table not in legacy_rows:
            if activity_slug in {"fieldreplogin", "fieldreploggedin"}:
                source_table = "redflags_metricevent"
            elif activity_slug in {
                "patientsubmission",
                "patientsubmitted",
                "screeningsubmission",
                "screeningsubmitted",
                "patientscreening",
                "formsubmission",
                "formsubmitted",
                "patientformsubmission",
                "patientformsubmitted",
                "formfilled",
                "formfill",
                "redflagspatientsubmission",
                "gndgndpatientsubmission",
            }:
                source_table = "redflags_patientsubmission"
        if source_table not in legacy_rows:
            continue
        payload = _activity_payload(row)
        if source_table == "redflags_patientsubmission":
            legacy_rows[source_table].append(
                {
                    "record_id": _activity_value(row, payload, "record_id", "source_event_id", "source_pk_value"),
                    "language_code": _activity_value(row, payload, "language_code"),
                    "submitted_at": _activity_value(row, payload, "submitted_at", "event_at", "source_created_at"),
                    "patient_id": _activity_value(row, payload, "patient_id", "patient_id_raw"),
                    "doctor_id": _activity_value(row, payload, "doctor_id", "doctor_uuid"),
                    "campaign_id": _activity_value(row, payload, "campaign_id", "campaign_uuid"),
                    "field_rep_id": _activity_value(row, payload, "field_rep_id", "field_rep_uuid_at_event_time"),
                    "form_id": _activity_value(row, payload, "form_id", "form_id_raw"),
                    "overall_flag_code": _activity_value(row, payload, "overall_flag_code"),
                }
            )
        elif source_table == "gnd_gndpatientsubmission":
            legacy_rows[source_table].append(
                {
                    "id": _activity_value(row, payload, "id", "source_event_id", "source_pk_value"),
                    "patient_id": _activity_value(row, payload, "patient_id", "patient_id_raw"),
                    "language_code": _activity_value(row, payload, "language_code"),
                    "submitted_at": _activity_value(row, payload, "submitted_at", "event_at", "source_created_at"),
                    "doctor_id": _activity_value(row, payload, "doctor_id", "doctor_uuid"),
                    "campaign_id": _activity_value(row, payload, "campaign_id", "campaign_uuid"),
                    "field_rep_id": _activity_value(row, payload, "field_rep_id", "field_rep_uuid_at_event_time"),
                    "form_id": _activity_value(row, payload, "form_id", "form_id_raw"),
                    "overall_flag_code": _activity_value(row, payload, "overall_flag_code"),
                }
            )
        elif source_table in {"redflags_submissionredflag", "gnd_gndsubmissionredflag"}:
            legacy_rows[source_table].append(
                {
                    "id": _activity_value(row, payload, "id", "source_event_id", "source_pk_value"),
                    "red_flag_id": _activity_value(row, payload, "red_flag_id", "red_flag_id_raw"),
                    "submission_id": _activity_value(row, payload, "submission_id"),
                }
            )
        elif source_table == "redflags_followupreminder":
            legacy_rows[source_table].append(
                {
                    "id": _activity_value(row, payload, "id", "source_event_id", "source_pk_value"),
                    "created_at": _activity_value(row, payload, "created_at", "source_created_at"),
                    "updated_at": _activity_value(row, payload, "updated_at", "source_updated_at"),
                    "patient_id": _activity_value(row, payload, "patient_id", "patient_id_raw"),
                    "patient_name": _activity_value(row, payload, "patient_name"),
                    "patient_whatsapp": _activity_value(row, payload, "patient_whatsapp"),
                    "followup_date1": _activity_value(row, payload, "followup_date1"),
                    "followup_date2": _activity_value(row, payload, "followup_date2"),
                    "followup_date3": _activity_value(row, payload, "followup_date3"),
                    "frequency_unit": _activity_value(row, payload, "frequency_unit"),
                    "frequency": _activity_value(row, payload, "frequency"),
                    "first_followup_date": _activity_value(row, payload, "first_followup_date"),
                    "num_followups": _activity_value(row, payload, "num_followups"),
                    "doctor_id": _activity_value(row, payload, "doctor_id", "doctor_uuid"),
                    "campaign_id": _activity_value(row, payload, "campaign_id", "campaign_uuid"),
                    "field_rep_id": _activity_value(row, payload, "field_rep_id", "field_rep_uuid_at_event_time"),
                }
            )
        elif source_table == "redflags_metricevent":
            event_type = _activity_value(row, payload, "event_type") or activity_type
            legacy_rows[source_table].append(
                {
                    "id": _activity_value(row, payload, "id", "source_event_id", "source_pk_value"),
                    "event_type": event_type,
                    "action_key": _activity_value(row, payload, "action_key")
                    or (_activity_value(row, payload, "field_rep_uuid_at_event_time") if (clean_text(event_type) or "").lower() == "field_rep_login" else None),
                    "share_code": _activity_value(row, payload, "share_code"),
                    "form_id": _activity_value(row, payload, "form_id", "form_id_raw"),
                    "language_code": _activity_value(row, payload, "language_code"),
                    "video_url": _activity_value(row, payload, "video_url"),
                    "meta": _activity_value(row, payload, "meta"),
                    "ts": _activity_value(row, payload, "ts", "event_at", "source_created_at"),
                    "doctor_id": _activity_value(row, payload, "doctor_id", "doctor_uuid"),
                    "campaign_id": _activity_value(row, payload, "campaign_id", "campaign_uuid"),
                    "field_rep_id": _activity_value(row, payload, "field_rep_id", "field_rep_uuid_at_event_time"),
                    "patient_id": _activity_value(row, payload, "patient_id", "patient_id_raw"),
                    "red_flag_id": _activity_value(row, payload, "red_flag_id", "red_flag_id_raw"),
                    "overall_flag_code": _activity_value(row, payload, "overall_flag_code"),
                }
            )
    return legacy_rows


def _legacy_row_key(row: dict[str, Any], fields: tuple[str, ...]) -> str:
    for field in fields:
        value = clean_text(row.get(field))
        if value:
            return f"{field}:{value}"
    return f"hash:{hash_fields(*sorted((key, clean_text(value) or '') for key, value in row.items()))}"


def _merge_legacy_rows(source_rows: list[dict[str, Any]], activity_rows: list[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in source_rows + activity_rows:
        key = _legacy_row_key(row, key_fields)
        existing = merged.get(key, {})
        combined = dict(existing)
        for field, value in row.items():
            if clean_text(value) is not None:
                combined[field] = value
            elif field not in combined:
                combined[field] = value
        merged[key] = combined
    return list(merged.values())


def build_silver(run_id: str) -> dict[str, Any]:
    now_iso = _now_iso()
    privacy_allowlist = active_campaign_privacy_allowlist()
    person_privacy_rules = active_person_privacy_rules()
    raw_visibility_rules = active_raw_visibility_rules(system_key="sapa")

    campaign_doctors = fetch_table(BRONZE_SCHEMA, "campaign_doctor")
    campaign_enrollments = fetch_table(BRONZE_SCHEMA, "campaign_doctorcampaignenrollment")
    campaign_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaign")
    brand_rows = fetch_table(BRONZE_SCHEMA, "campaign_brand")
    source_field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_fieldrep")
    campaign_field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaignfieldrep")
    rfa_activity_events = fetch_table(BRONZE_SCHEMA, "rfa_activity_event")
    redflag_doctors = fetch_table(BRONZE_SCHEMA, "redflags_doctor")
    redflag_submissions = fetch_table(BRONZE_SCHEMA, "redflags_patientsubmission")
    gnd_submissions = fetch_table(BRONZE_SCHEMA, "gnd_gndpatientsubmission")
    redflag_occurrences = fetch_table(BRONZE_SCHEMA, "redflags_submissionredflag")
    gnd_occurrences = fetch_table(BRONZE_SCHEMA, "gnd_gndsubmissionredflag")
    redflag_catalog_rows = fetch_table(BRONZE_SCHEMA, "redflags_redflag")
    gnd_redflag_catalog_rows = fetch_table(BRONZE_SCHEMA, "gnd_gndredflag")
    redflags_patientvideo_rows = fetch_table(BRONZE_SCHEMA, "redflags_patientvideo")
    gnd_patientvideo_rows = fetch_table(BRONZE_SCHEMA, "gnd_gndpatientvideo")
    followup_rows = fetch_table(BRONZE_SCHEMA, "redflags_followupreminder")
    metric_rows = fetch_table(BRONZE_SCHEMA, "redflags_metricevent")
    clinic_outcomes = fetch_table(BRONZE_SCHEMA, "campaign_clinic_outcome_master")
    webinar_rows = fetch_table(BRONZE_SCHEMA, "wp_webinar_registrations")
    course_summary_rows = fetch_table(BRONZE_SCHEMA, "wp_course_summary")
    course_breakdown_rows = fetch_table(BRONZE_SCHEMA, "wp_course_breakdown")

    filtered_inputs = _apply_raw_visibility_to_sapa_inputs(
        {
            "campaign_doctors": campaign_doctors,
            "campaign_enrollments": campaign_enrollments,
            "campaign_rows": campaign_rows,
            "brand_rows": brand_rows,
            "source_field_rep_rows": source_field_rep_rows,
            "campaign_field_rep_rows": campaign_field_rep_rows,
            "rfa_activity_events": rfa_activity_events,
            "redflag_doctors": redflag_doctors,
            "redflag_submissions": redflag_submissions,
            "gnd_submissions": gnd_submissions,
            "redflag_occurrences": redflag_occurrences,
            "gnd_occurrences": gnd_occurrences,
            "redflag_catalog_rows": redflag_catalog_rows,
            "gnd_redflag_catalog_rows": gnd_redflag_catalog_rows,
            "redflags_patientvideo_rows": redflags_patientvideo_rows,
            "gnd_patientvideo_rows": gnd_patientvideo_rows,
            "followup_rows": followup_rows,
            "metric_rows": metric_rows,
            "clinic_outcomes": clinic_outcomes,
            "webinar_rows": webinar_rows,
            "course_summary_rows": course_summary_rows,
            "course_breakdown_rows": course_breakdown_rows,
        },
        raw_visibility_rules,
    )
    campaign_doctors = filtered_inputs["campaign_doctors"]
    campaign_enrollments = filtered_inputs["campaign_enrollments"]
    campaign_rows = filtered_inputs["campaign_rows"]
    brand_rows = filtered_inputs["brand_rows"]
    source_field_rep_rows = filtered_inputs["source_field_rep_rows"]
    campaign_field_rep_rows = filtered_inputs["campaign_field_rep_rows"]
    rfa_activity_events = filtered_inputs["rfa_activity_events"]
    redflag_doctors = filtered_inputs["redflag_doctors"]
    redflag_submissions = filtered_inputs["redflag_submissions"]
    gnd_submissions = filtered_inputs["gnd_submissions"]
    redflag_occurrences = filtered_inputs["redflag_occurrences"]
    gnd_occurrences = filtered_inputs["gnd_occurrences"]
    redflag_catalog_rows = filtered_inputs["redflag_catalog_rows"]
    gnd_redflag_catalog_rows = filtered_inputs["gnd_redflag_catalog_rows"]
    redflags_patientvideo_rows = filtered_inputs["redflags_patientvideo_rows"]
    gnd_patientvideo_rows = filtered_inputs["gnd_patientvideo_rows"]
    followup_rows = filtered_inputs["followup_rows"]
    metric_rows = filtered_inputs["metric_rows"]
    clinic_outcomes = filtered_inputs["clinic_outcomes"]
    webinar_rows = filtered_inputs["webinar_rows"]
    course_summary_rows = filtered_inputs["course_summary_rows"]
    course_breakdown_rows = filtered_inputs["course_breakdown_rows"]

    if rfa_activity_events:
        activity_legacy_rows = _activity_events_as_legacy_rows(rfa_activity_events)
        redflag_submissions = _merge_legacy_rows(redflag_submissions, activity_legacy_rows["redflags_patientsubmission"], ("record_id", "id"))
        gnd_submissions = _merge_legacy_rows(gnd_submissions, activity_legacy_rows["gnd_gndpatientsubmission"], ("id", "record_id"))
        redflag_occurrences = _merge_legacy_rows(redflag_occurrences, activity_legacy_rows["redflags_submissionredflag"], ("id",))
        gnd_occurrences = _merge_legacy_rows(gnd_occurrences, activity_legacy_rows["gnd_gndsubmissionredflag"], ("id",))
        followup_rows = _merge_legacy_rows(followup_rows, activity_legacy_rows["redflags_followupreminder"], ("id",))
        metric_rows = _merge_legacy_rows(metric_rows, activity_legacy_rows["redflags_metricevent"], ("id",))

    redflag_doctor_by_id = {clean_text(row.get("doctor_id")): row for row in redflag_doctors if clean_text(row.get("doctor_id"))}
    clinic_outcome_by_doctor = {clean_text(row.get("doctor_id")): row for row in clinic_outcomes if clean_text(row.get("doctor_id"))}
    campaign_doctor_by_row_id = {clean_text(row.get("id")): row for row in campaign_doctors if clean_text(row.get("id"))}
    brand_by_id = {clean_text(row.get("id")): row for row in brand_rows if clean_text(row.get("id"))}
    has_rfa_flag_values = any(clean_text(row.get("system_rfa")) is not None for row in campaign_rows)
    rfa_campaigns = {
        clean_text(row.get("id")): row
        for row in campaign_rows
        if clean_text(row.get("id")) and (not has_rfa_flag_values or _truthy(row.get("system_rfa")))
    }
    if privacy_allowlist:
        rfa_campaigns = {
            campaign_id: row
            for campaign_id, row in rfa_campaigns.items()
            if _sapa_campaign_allowed(campaign_id, privacy_allowlist)
        }
    field_rep_by_id = {clean_text(row.get("id")): row for row in source_field_rep_rows if clean_text(row.get("id"))}
    field_rep_by_external = {
        _norm_id(row.get("brand_supplied_field_rep_id")): row
        for row in source_field_rep_rows
        if _norm_id(row.get("brand_supplied_field_rep_id"))
    }
    campaign_rep_ids: dict[str, set[str]] = defaultdict(set)
    campaign_rep_membership_ids: dict[str, set[str]] = defaultdict(set)
    rep_campaign_ids: dict[str, set[str]] = defaultdict(set)
    rep_campaign_assignments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in campaign_field_rep_rows:
        campaign_id = clean_text(row.get("campaign_id"))
        field_rep_id = clean_text(row.get("field_rep_id"))
        if not campaign_id or not field_rep_id:
            continue
        if privacy_allowlist and campaign_id not in rfa_campaigns:
            continue
        campaign_rep_ids[campaign_id].add(field_rep_id)
        campaign_rep_membership_ids[campaign_id].add(field_rep_id)
        assigned_rep = field_rep_by_id.get(field_rep_id)
        assigned_rep_brand_id = clean_text((assigned_rep or {}).get("brand_supplied_field_rep_id"))
        if assigned_rep_brand_id:
            campaign_rep_membership_ids[campaign_id].add(assigned_rep_brand_id)
        rep_campaign_ids[field_rep_id].add(campaign_id)
        assignment_payload = {
            "campaign_id": campaign_id,
            "assigned_at": row.get("created_at"),
        }
        rep_campaign_assignments[field_rep_id].append(assignment_payload)
        if assigned_rep_brand_id:
            rep_campaign_ids[assigned_rep_brand_id].add(campaign_id)
            rep_campaign_assignments[assigned_rep_brand_id].append(assignment_payload)

    doctor_activity_campaign_ids: dict[str, set[str]] = defaultdict(set)

    def add_doctor_campaign_evidence(doctor_id: Any, campaign_id: Any) -> None:
        doctor_key = clean_text(doctor_id)
        campaign_key = clean_text(campaign_id)
        if doctor_key and campaign_key in rfa_campaigns:
            doctor_activity_campaign_ids[doctor_key].add(campaign_key)

    for row in rfa_activity_events:
        add_doctor_campaign_evidence(row.get("doctor_uuid"), row.get("campaign_uuid"))
    for source_rows in (redflag_submissions, gnd_submissions, followup_rows, metric_rows):
        for row in source_rows:
            add_doctor_campaign_evidence(row.get("doctor_id"), row.get("campaign_id"))

    def campaign_meta(campaign_id: Any) -> tuple[str, str]:
        campaign = rfa_campaigns.get(clean_text(campaign_id)) or {}
        brand = brand_by_id.get(clean_text(campaign.get("brand_id"))) or {}
        key = clean_text(campaign.get("id"))
        label = clean_text(campaign.get("name")) or clean_text(brand.get("name"))
        return _campaign_key_label({"campaign_id": key, "campaign_name": label})

    def campaign_dates(campaign_id: Any) -> dict[str, str]:
        campaign = rfa_campaigns.get(clean_text(campaign_id)) or {}
        return {
            "campaign_start_date": _empty_text(iso_date(campaign.get("start_date"))),
            "campaign_end_date": _empty_text(iso_date(campaign.get("end_date"))),
        }

    def campaign_matches(row: dict[str, Any], campaign_hint: Any) -> bool:
        hint = clean_text(campaign_hint)
        if not hint:
            return True
        hint_norm = _norm_id(hint)
        candidate_values = (
            row.get("campaign_id"),
            row.get("campaign_key"),
            row.get("campaign_label"),
        )
        return any(clean_text(value) == hint or _norm_id(value) == hint_norm for value in candidate_values)

    def resolve_field_rep(field_rep_value: Any, campaign_id: Any = None) -> dict[str, str]:
        raw = clean_text(field_rep_value)
        rep = field_rep_by_id.get(raw or "") or field_rep_by_external.get(_norm_id(raw))
        campaign_rep_set = campaign_rep_ids.get(clean_text(campaign_id) or "", set())
        if not rep and campaign_rep_set:
            first_rep_id = sorted(campaign_rep_set)[0]
            rep = field_rep_by_id.get(first_rep_id)
        display_id = clean_text((rep or {}).get("brand_supplied_field_rep_id")) or clean_text((rep or {}).get("id")) or raw or "Unassigned"
        return {
            "field_rep_id": display_id,
            "field_rep_name": clean_text((rep or {}).get("full_name")) or display_id,
            "field_rep_state": clean_text((rep or {}).get("state")) or "",
        }

    enrollments_by_campaign_doctor_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in campaign_enrollments:
        campaign_id = clean_text(row.get("campaign_id"))
        campaign_doctor_id = clean_text(row.get("doctor_id"))
        if not campaign_id or campaign_id not in rfa_campaigns or not campaign_doctor_id:
            continue
        enrollments_by_campaign_doctor_id[campaign_doctor_id].append(row)

    def enrollment_campaigns_for_doctor(campaign_row: dict[str, Any], doctor_row: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        row_id = clean_text(campaign_row.get("id"))
        enrollments = enrollments_by_campaign_doctor_id.get(row_id or "", [])
        output = []
        for enrollment in enrollments:
            campaign_id = clean_text(enrollment.get("campaign_id"))
            if not campaign_id:
                continue
            campaign_key, campaign_label = campaign_meta(campaign_id)
            date_info = campaign_dates(campaign_id)
            rep_info = resolve_field_rep(enrollment.get("registered_by_id") or (doctor_row or {}).get("field_rep_id"), campaign_id)
            output.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_key": campaign_key,
                    "campaign_label": campaign_label,
                    "campaign_start_date": date_info["campaign_start_date"],
                    "campaign_end_date": date_info["campaign_end_date"],
                    "field_rep_id": rep_info["field_rep_id"],
                    "field_rep_name": rep_info["field_rep_name"],
                    "field_rep_state": rep_info["field_rep_state"],
                    "registered_at": clean_text(enrollment.get("registered_at") or enrollment.get("created_at")) or "",
                }
            )
        if output:
            return output
        if privacy_allowlist:
            return []
        campaign_key, campaign_label = _campaign_key_label(campaign_row, doctor_row)
        rep_info = resolve_field_rep((doctor_row or {}).get("field_rep_id"))
        return [
            {
                "campaign_id": "",
                "campaign_key": campaign_key,
                "campaign_label": campaign_label,
                "campaign_start_date": "",
                "campaign_end_date": "",
                "field_rep_id": rep_info["field_rep_id"],
                "field_rep_name": rep_info["field_rep_name"],
                "field_rep_state": rep_info["field_rep_state"],
                "registered_at": "",
            }
        ]

    def campaigns_for_redflags_doctor(doctor_row: dict[str, Any]) -> list[dict[str, Any]]:
        doctor_id = clean_text(doctor_row.get("doctor_id"))
        rep = field_rep_by_id.get(clean_text(doctor_row.get("field_rep_id")) or "") or field_rep_by_external.get(_norm_id(doctor_row.get("field_rep_id")))
        campaign_ids = sorted(doctor_activity_campaign_ids.get(doctor_id, set()) & set(rfa_campaigns.keys()))
        output = []
        for campaign_id in campaign_ids:
            campaign_key, campaign_label = campaign_meta(campaign_id)
            date_info = campaign_dates(campaign_id)
            rep_info = resolve_field_rep((rep or {}).get("id") or doctor_row.get("field_rep_id"), campaign_id)
            output.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_key": campaign_key,
                    "campaign_label": campaign_label,
                    "campaign_start_date": date_info["campaign_start_date"],
                    "campaign_end_date": date_info["campaign_end_date"],
                    "field_rep_id": rep_info["field_rep_id"],
                    "field_rep_name": rep_info["field_rep_name"],
                    "field_rep_state": rep_info["field_rep_state"],
                    "registered_at": "",
                }
            )
        if output:
            return [_best_dim_for_event(output, doctor_row.get("created_at")) or output[0]]
        if privacy_allowlist or rfa_campaigns:
            return []
        campaign_key, campaign_label = _campaign_key_label(doctor_row)
        rep_info = resolve_field_rep(doctor_row.get("field_rep_id"))
        return [{"campaign_id": "", "campaign_key": campaign_key, "campaign_label": campaign_label, "campaign_start_date": "", "campaign_end_date": "", "field_rep_id": rep_info["field_rep_id"], "field_rep_name": rep_info["field_rep_name"], "field_rep_state": rep_info["field_rep_state"], "registered_at": ""}]

    dim_rows: list[dict[str, Any]] = []
    included_doctor_ids: set[str] = set()

    for campaign_row in campaign_doctors:
        doctor_id = clean_text(campaign_row.get("doctor_id"))
        doctor_row = redflag_doctor_by_id.get(doctor_id)
        clinic_outcome_row = clinic_outcome_by_doctor.get(doctor_id)
        base_doctor_key = canonical_doctor_key(doctor_id, campaign_row.get("id"))
        first_name, last_name = split_full_name(campaign_row.get("full_name"))
        if doctor_row:
            first_name = clean_text(doctor_row.get("first_name")) or first_name
            last_name = clean_text(doctor_row.get("last_name")) or last_name
        for campaign_info in enrollment_campaigns_for_doctor(campaign_row, doctor_row):
            campaign_key = campaign_info["campaign_key"]
            dim_rows.append(
                {
                    "doctor_key": _campaign_specific_doctor_key(base_doctor_key, campaign_key),
                    "source_doctor_id": doctor_id or "",
                    "base_doctor_key": base_doctor_key,
                    "campaign_doctor_row_id": _empty_text(campaign_row.get("id")),
                    "campaign_id": campaign_info["campaign_id"],
                    "campaign_key": campaign_key,
                    "campaign_label": campaign_info["campaign_label"],
                    "campaign_registered_at": _empty_text(iso_datetime(campaign_info.get("registered_at"))),
                    "campaign_start_date": campaign_info["campaign_start_date"],
                    "campaign_end_date": campaign_info["campaign_end_date"],
                    "canonical_display_name": display_name_from_sources(campaign_row, doctor_row),
                    "first_name": first_name or "",
                    "last_name": last_name or "",
                    "canonical_email": _empty_text(campaign_row.get("email") or (doctor_row or {}).get("email")),
                    "canonical_phone": _empty_text(campaign_row.get("phone") or (doctor_row or {}).get("clinic_phone")),
                    "canonical_whatsapp_no": _empty_text((doctor_row or {}).get("whatsapp_no")),
                    "receptionist_whatsapp_number": _empty_text((doctor_row or {}).get("receptionist_whatsapp_number")),
                    "clinic_name": _empty_text((doctor_row or {}).get("clinic_name")),
                    "clinic_password_set_at": _empty_text(iso_datetime((doctor_row or {}).get("clinic_password_set_at"))),
                    "special_instructions_uploaded_at": _empty_text(iso_datetime((doctor_row or {}).get("special_instructions_uploaded_at"))),
                    "special_instructions_removed_at": _empty_text(iso_datetime((doctor_row or {}).get("special_instructions_removed_at"))),
                    "clinic_user1_email": _empty_text((doctor_row or {}).get("clinic_user1_email")),
                    "clinic_user2_email": _empty_text((doctor_row or {}).get("clinic_user2_email")),
                    "city": _empty_text(campaign_row.get("city")),
                    "district": _empty_text((doctor_row or {}).get("district")),
                    "state": _empty_text(campaign_row.get("state") or campaign_info["field_rep_state"] or (doctor_row or {}).get("state")),
                    "field_rep_id": campaign_info["field_rep_id"],
                    "field_rep_name": campaign_info["field_rep_name"],
                    "recruited_via": _empty_text((doctor_row or {}).get("recruited_via")),
                    "first_seen_at": _empty_text(min(filter(None, [iso_datetime(campaign_info.get("registered_at")), iso_datetime(campaign_row.get("created_at")), iso_datetime((doctor_row or {}).get("created_at"))]), default="")),
                    "latest_seen_at": _empty_text(max(filter(None, [iso_datetime(campaign_info.get("registered_at")), iso_datetime(campaign_row.get("created_at")), iso_datetime((doctor_row or {}).get("created_at"))]), default="")),
                    "is_user_created_doctor": "true",
                    "has_campaign_source": "true",
                    "has_redflags_source": "true" if doctor_row else "false",
                    "identity_quality_status": "logical_doctor_id" if doctor_id else "campaign_row_only",
                    "_silver_updated_at": now_iso,
                    "_dq_status": "PASS",
                    "_dq_errors": "",
                }
            )
        if doctor_id:
            included_doctor_ids.add(doctor_id)

    for doctor_row in redflag_doctors:
        doctor_id = clean_text(doctor_row.get("doctor_id"))
        if doctor_id in included_doctor_ids:
            continue
        clinic_outcome_row = clinic_outcome_by_doctor.get(doctor_id)
        base_doctor_key = canonical_doctor_key(doctor_id, None)
        for campaign_info in campaigns_for_redflags_doctor(doctor_row):
            dim_rows.append(
                {
                    "doctor_key": _campaign_specific_doctor_key(base_doctor_key, campaign_info["campaign_key"]),
                    "source_doctor_id": doctor_id or "",
                    "base_doctor_key": base_doctor_key,
                    "campaign_doctor_row_id": "",
                    "campaign_id": campaign_info["campaign_id"],
                    "campaign_key": campaign_info["campaign_key"],
                    "campaign_label": campaign_info["campaign_label"],
                    "campaign_registered_at": "",
                    "campaign_start_date": campaign_info["campaign_start_date"],
                    "campaign_end_date": campaign_info["campaign_end_date"],
                    "canonical_display_name": display_name_from_sources(None, doctor_row),
                    "first_name": _empty_text(doctor_row.get("first_name")),
                    "last_name": _empty_text(doctor_row.get("last_name")),
                    "canonical_email": _empty_text(doctor_row.get("email")),
                    "canonical_phone": _empty_text(doctor_row.get("clinic_phone")),
                    "canonical_whatsapp_no": _empty_text(doctor_row.get("whatsapp_no")),
                    "receptionist_whatsapp_number": _empty_text(doctor_row.get("receptionist_whatsapp_number")),
                    "clinic_name": _empty_text(doctor_row.get("clinic_name")),
                    "clinic_password_set_at": _empty_text(iso_datetime(doctor_row.get("clinic_password_set_at"))),
                    "special_instructions_uploaded_at": _empty_text(iso_datetime(doctor_row.get("special_instructions_uploaded_at"))),
                    "special_instructions_removed_at": _empty_text(iso_datetime(doctor_row.get("special_instructions_removed_at"))),
                    "clinic_user1_email": _empty_text(doctor_row.get("clinic_user1_email")),
                    "clinic_user2_email": _empty_text(doctor_row.get("clinic_user2_email")),
                    "city": "",
                    "district": _empty_text(doctor_row.get("district")),
                    "state": _empty_text(campaign_info["field_rep_state"] or doctor_row.get("state")),
                    "field_rep_id": campaign_info["field_rep_id"],
                    "field_rep_name": campaign_info["field_rep_name"],
                    "recruited_via": _empty_text(doctor_row.get("recruited_via")),
                    "first_seen_at": _empty_text(iso_datetime(doctor_row.get("created_at"))),
                    "latest_seen_at": _empty_text(iso_datetime(doctor_row.get("created_at"))),
                    # campaign_doctor = field-rep recruitment; redflags_doctor = self recruitment.
                    # Both are enrolled doctors for SAPA dashboard reporting.
                    "is_user_created_doctor": "true",
                    "has_campaign_source": "false",
                    "has_redflags_source": "true",
                    "identity_quality_status": "logical_doctor_id" if doctor_id else "redflags_missing_doctor_id",
                    "_silver_updated_at": now_iso,
                    "_dq_status": "FAIL" if not doctor_id else "PASS",
                    "_dq_errors": "Missing doctor_id in redflags_doctor" if not doctor_id else "",
                }
            )

    person_restricted_source_doctor_ids = {
        clean_text(row.get("source_doctor_id"))
        for row in dim_rows
        if clean_text(row.get("source_doctor_id")) and _sapa_person_matches(row, person_privacy_rules)
    }
    if person_privacy_rules:
        dim_rows = [row for row in dim_rows if _sapa_person_visible(row, person_privacy_rules)]

    dim_columns = [
        "doctor_key",
        "source_doctor_id",
        "base_doctor_key",
        "campaign_doctor_row_id",
        "campaign_id",
        "campaign_key",
        "campaign_label",
        "campaign_registered_at",
        "campaign_start_date",
        "campaign_end_date",
        "canonical_display_name",
        "first_name",
        "last_name",
        "canonical_email",
        "canonical_phone",
        "canonical_whatsapp_no",
        "receptionist_whatsapp_number",
        "clinic_name",
        "clinic_password_set_at",
        "special_instructions_uploaded_at",
        "special_instructions_removed_at",
        "clinic_user1_email",
        "clinic_user2_email",
        "city",
        "district",
        "state",
        "field_rep_id",
        "field_rep_name",
        "recruited_via",
        "first_seen_at",
        "latest_seen_at",
        "is_user_created_doctor",
        "has_campaign_source",
        "has_redflags_source",
        "identity_quality_status",
        "_silver_updated_at",
        "_dq_status",
        "_dq_errors",
    ]
    replace_table(SILVER_SCHEMA, "dim_doctor_clinic", dim_columns, dim_rows)

    field_rep_rows = []
    for field_rep_id in sorted({row.get("field_rep_id") or "Unassigned" for row in dim_rows}):
        related = [row for row in dim_rows if (row.get("field_rep_id") or "Unassigned") == field_rep_id]
        field_rep_name = next((row.get("field_rep_name") for row in related if row.get("field_rep_name")), field_rep_id)
        state = next((row.get("state") for row in related if row.get("state")), "")
        field_rep_rows.append(
            {
                "field_rep_id": field_rep_id,
                "field_rep_name": field_rep_name,
                "state": state,
                "is_unassigned": "true" if field_rep_id == "Unassigned" else "false",
                "first_seen_at": min((row.get("first_seen_at") or now_iso for row in related), default=now_iso),
                "last_seen_at": max((row.get("latest_seen_at") or now_iso for row in related), default=now_iso),
            }
        )
    replace_table(
        SILVER_SCHEMA,
        "dim_field_rep",
        ["field_rep_id", "field_rep_name", "state", "is_unassigned", "first_seen_at", "last_seen_at"],
        field_rep_rows,
    )

    geography_rows = []
    geography_seen: set[tuple[str, str, str]] = set()
    for row in dim_rows:
        key = (row.get("city") or "", row.get("district") or "", row.get("state") or "")
        if key in geography_seen:
            continue
        geography_seen.add(key)
        geography_rows.append(
            {
                "geography_key": hash_fields(*key),
                "city": key[0],
                "district": key[1],
                "state": key[2],
                "location_completeness_flag": "complete" if key[0] and key[2] else "partial",
            }
        )
    replace_table(
        SILVER_SCHEMA,
        "dim_geography",
        ["geography_key", "city", "district", "state", "location_completeness_flag"],
        geography_rows,
    )

    dim_by_doctor_id, dim_by_email, dim_by_phone = _doctor_indexes(dim_rows)

    red_flag_catalog: dict[str, dict[str, str]] = {}
    for row in redflag_catalog_rows + gnd_redflag_catalog_rows:
        red_flag_id = clean_text(row.get("red_flag_id"))
        if not red_flag_id:
            continue
        current = red_flag_catalog.setdefault(
            red_flag_id,
            {
                "red_flag_name": "",
                "doctor_video_url": "",
            },
        )
        current["red_flag_name"] = current["red_flag_name"] or _empty_text(row.get("default_patient_response"))
        current["doctor_video_url"] = current["doctor_video_url"] or _empty_text(row.get("doctor_video_url"))

    patient_video_by_flag_and_language: dict[tuple[str, str], str] = {}
    patient_video_by_flag: dict[str, str] = {}
    for row in redflags_patientvideo_rows + gnd_patientvideo_rows:
        red_flag_id = clean_text(row.get("red_flag_id"))
        language_code = clean_text(row.get("language_code")) or ""
        patient_video_url = _empty_text(row.get("patient_video_url"))
        if not red_flag_id or not patient_video_url:
            continue
        patient_video_by_flag_and_language.setdefault((red_flag_id, language_code), patient_video_url)
        patient_video_by_flag.setdefault(red_flag_id, patient_video_url)

    def resolve_doctor_matches_from_source_id(
        source_doctor_id: Any,
        source_hint: str,
        event_date: Any = None,
        campaign_hint: Any = None,
    ) -> list[tuple[str, dict[str, Any] | None]]:
        doctor_id = clean_text(source_doctor_id)
        dim_matches = dim_by_doctor_id.get(doctor_id) or []
        if campaign_hint:
            campaign_matches_for_event = [row for row in dim_matches if campaign_matches(row, campaign_hint)]
            if campaign_matches_for_event:
                dim_matches = campaign_matches_for_event
        if dim_matches:
            match = _best_dim_for_event(dim_matches, event_date)
            return [(match["doctor_key"], match)] if match else []
        if doctor_id and doctor_id in person_restricted_source_doctor_ids:
            return []
        if doctor_id:
            return [(f"unmatched:{doctor_id}", None)]
        return [(f"unmatched:{source_hint}", None)]

    def field_rep_from_event(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        event_type = (clean_text(row.get("event_type")) or "").lower()
        meta = _json_object(row.get("meta"))
        raw_value = clean_text(row.get("field_rep_id"))
        if event_type == "field_rep_login":
            raw_value = (
                clean_text(row.get("action_key"))
                or raw_value
                or clean_text(meta.get("field_rep_id"))
                or clean_text(meta.get("brand_supplied_field_rep_id"))
            )
        rep = field_rep_by_id.get(raw_value or "") or field_rep_by_external.get(_norm_id(raw_value))
        return rep, raw_value or ""

    def campaign_ids_for_field_rep_event(rep: dict[str, Any] | None, row: dict[str, Any]) -> list[str]:
        return _campaign_ids_for_field_rep_login_event(
            rep=rep,
            row=row,
            rfa_campaigns=rfa_campaigns,
            rep_campaign_ids=rep_campaign_ids,
            campaign_rep_ids=campaign_rep_ids,
            campaign_rep_membership_ids=campaign_rep_membership_ids,
            rep_campaign_assignments=rep_campaign_assignments,
        )

    def field_rep_display(rep: dict[str, Any] | None, raw_value: str, meta: dict[str, Any] | None = None) -> dict[str, str]:
        meta = meta or {}
        source_id = clean_text((rep or {}).get("id")) or raw_value
        display_id = clean_text((rep or {}).get("brand_supplied_field_rep_id")) or source_id or "Unassigned"
        return {
            "source_field_rep_id": source_id or display_id,
            "field_rep_id": display_id,
            "field_rep_name": clean_text((rep or {}).get("full_name")) or clean_text(meta.get("field_rep_name")) or display_id,
            "state": clean_text((rep or {}).get("state")) or "",
            "field_rep_email": clean_text(meta.get("email")) or "",
            "login_method": clean_text(meta.get("login_method")) or "",
            "user_agent": clean_text(meta.get("user_agent")) or "",
        }

    def device_type_from_metric_meta(row: dict[str, Any]) -> str:
        meta = _json_object(row.get("meta"))
        value = clean_text(meta.get("device_type") or meta.get("device") or meta.get("platform"))
        return value or ""

    field_rep_login_rows = []
    for row in metric_rows:
        if (clean_text(row.get("event_type")) or "").lower() != "field_rep_login":
            continue
        login_ts = _empty_text(iso_datetime(row.get("ts")))
        meta = _json_object(row.get("meta"))
        rep, raw_field_rep_id = field_rep_from_event(row)
        display = field_rep_display(rep, raw_field_rep_id, meta)
        for campaign_id in campaign_ids_for_field_rep_event(rep, row):
            if privacy_allowlist and campaign_id not in rfa_campaigns:
                continue
            campaign_key, campaign_label = campaign_meta(campaign_id)
            field_rep_login_id = _empty_text(row.get("id")) or hash_fields("field_rep_login", raw_field_rep_id, login_ts)
            if campaign_key:
                field_rep_login_id = f"{field_rep_login_id}:campaign:{campaign_key}"
            field_rep_login_rows.append(
                {
                    "field_rep_login_id": field_rep_login_id,
                    "source_metric_event_id": _empty_text(row.get("id")),
                    "source_field_rep_id": display["source_field_rep_id"],
                    "campaign_key": campaign_key,
                    "campaign_label": campaign_label,
                    "field_rep_id": display["field_rep_id"],
                    "field_rep_name": display["field_rep_name"],
                    "field_rep_email": display["field_rep_email"],
                    "state": display["state"],
                    "login_ts": login_ts,
                    "login_method": display["login_method"],
                    "device_type": device_type_from_metric_meta(row),
                    "user_agent": display["user_agent"],
                }
            )
    replace_table(
        SILVER_SCHEMA,
        "fact_field_rep_login",
        [
            "field_rep_login_id",
            "source_metric_event_id",
            "source_field_rep_id",
            "campaign_key",
            "campaign_label",
            "field_rep_id",
            "field_rep_name",
            "field_rep_email",
            "state",
            "login_ts",
            "login_method",
            "device_type",
            "user_agent",
        ],
        field_rep_login_rows,
    )

    screening_rows = []
    screening_source_index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for source_table, source_rows in (("redflags_patientsubmission", redflag_submissions), ("gnd_gndpatientsubmission", gnd_submissions)):
        for row in source_rows:
            source_submission_id = clean_text(row.get("record_id") or row.get("id")) or hash_fields(source_table, row)
            submitted_at = iso_datetime(row.get("submitted_at"))
            for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(
                row.get("doctor_id"),
                f"{source_table}:{source_submission_id}",
                submitted_at,
                campaign_hint=row.get("campaign_id"),
            ):
                filters = _doctor_filters(doctor_dim)
                if privacy_allowlist and not _sapa_campaign_allowed(filters["campaign_key"], privacy_allowlist):
                    continue
                if person_privacy_rules and not _sapa_person_visible({**(doctor_dim or {}), **filters}, person_privacy_rules):
                    continue
                overall_flag = (_empty_text(row.get("overall_flag_code"))).lower()
                submission_key = f"{source_table}:{source_submission_id}"
                if filters["campaign_key"]:
                    submission_key = f"{submission_key}:campaign:{filters['campaign_key']}"
                screening_row = {
                    "submission_key": submission_key,
                    "source_table": source_table,
                    "source_submission_id": source_submission_id,
                    "doctor_key": doctor_key,
                    "campaign_key": filters["campaign_key"],
                    "campaign_label": filters["campaign_label"],
                    "patient_id": _empty_text(row.get("patient_id")),
                    "form_identifier": _empty_text(row.get("form_id") or row.get("form")),
                    "language_code": _empty_text(row.get("language_code")),
                    "submitted_at": submitted_at or "",
                    "overall_flag_code": overall_flag,
                    "doctor_display_name": filters["doctor_display_name"],
                    "city": filters["city"],
                    "district": filters["district"],
                    "state": filters["state"],
                    "field_rep_id": filters["field_rep_id"],
                    "field_rep_name": filters["field_rep_name"],
                    "is_red_tag": "true" if overall_flag == "red" else "false",
                    "is_yellow_tag": "true" if overall_flag == "yellow" else "false",
                    "is_green_tag": "true" if overall_flag == "green" else "false",
                    "unresolved_doctor_flag": "true" if doctor_dim is None else "false",
                }
                screening_rows.append(screening_row)
                screening_source_index[(source_table, source_submission_id)].append(screening_row)
    replace_table(
        SILVER_SCHEMA,
        "fact_screening_submission",
        [
            "submission_key",
            "source_table",
            "source_submission_id",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "patient_id",
            "form_identifier",
            "language_code",
            "submitted_at",
            "overall_flag_code",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
            "is_red_tag",
            "is_yellow_tag",
            "is_green_tag",
            "unresolved_doctor_flag",
        ],
        screening_rows,
    )

    redflag_fact_rows = []
    for source_table, occurrence_rows, submission_table in (
        ("redflags_submissionredflag", redflag_occurrences, "redflags_patientsubmission"),
        ("gnd_gndsubmissionredflag", gnd_occurrences, "gnd_gndpatientsubmission"),
    ):
        for row in occurrence_rows:
            source_submission_id = clean_text(row.get("submission_id") or row.get("submission"))
            if not source_submission_id:
                continue
            submissions = screening_source_index.get((submission_table, source_submission_id), [])
            if not submissions:
                continue
            for submission in submissions:
                redflag_fact_rows.append(
                    {
                        "source_row_id": _empty_text(row.get("id")),
                        "submission_key": submission["submission_key"],
                        "source_submission_id": source_submission_id,
                        "doctor_key": submission["doctor_key"],
                        "campaign_key": submission["campaign_key"],
                        "campaign_label": submission["campaign_label"],
                        "red_flag": _empty_text(row.get("red_flag_id") or row.get("red_flag")),
                        "red_flag_name": _empty_text((red_flag_catalog.get(_empty_text(row.get("red_flag_id") or row.get("red_flag"))) or {}).get("red_flag_name")),
                        "patient_video_url": _empty_text(
                            patient_video_by_flag.get(_empty_text(row.get("red_flag_id") or row.get("red_flag")))
                        ),
                        "doctor_video_url": _empty_text(
                            (red_flag_catalog.get(_empty_text(row.get("red_flag_id") or row.get("red_flag"))) or {}).get("doctor_video_url")
                        ),
                        "submitted_at": submission["submitted_at"],
                        "doctor_display_name": submission["doctor_display_name"],
                        "city": submission["city"],
                        "district": submission["district"],
                        "state": submission["state"],
                        "field_rep_id": submission["field_rep_id"],
                        "field_rep_name": submission["field_rep_name"],
                    }
                )
    replace_table(
        SILVER_SCHEMA,
        "fact_submission_redflag",
        [
            "source_row_id",
            "submission_key",
            "source_submission_id",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "red_flag",
            "red_flag_name",
            "patient_video_url",
            "doctor_video_url",
            "submitted_at",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
        ],
        redflag_fact_rows,
    )

    metric_fact_rows = []
    for row in metric_rows:
        event_ts = _empty_text(iso_datetime(row.get("ts")))
        for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(
            row.get("doctor_id"),
            f"metric:{row.get('id')}",
            event_ts,
            campaign_hint=row.get("campaign_id"),
        ):
            filters = _doctor_filters(doctor_dim)
            if privacy_allowlist and not _sapa_campaign_allowed(filters["campaign_key"], privacy_allowlist):
                continue
            if person_privacy_rules and not _sapa_person_visible({**(doctor_dim or {}), **filters}, person_privacy_rules):
                continue
            classifications = classify_metric_event(row.get("event_type"), row.get("action_key"))
            metric_event_id = _empty_text(row.get("id"))
            if filters["campaign_key"]:
                metric_event_id = f"{metric_event_id}:campaign:{filters['campaign_key']}"
            metric_fact_rows.append(
                {
                    "metric_event_id": metric_event_id,
                    "source_metric_event_id": _empty_text(row.get("id")),
                    "event_type": _empty_text(row.get("event_type")),
                    "action_key": _empty_text(row.get("action_key")),
                    "doctor_key": doctor_key,
                    "campaign_key": filters["campaign_key"],
                    "campaign_label": filters["campaign_label"],
                    "patient_id": _empty_text(row.get("patient_id")),
                    "share_code": _empty_text(row.get("share_code")),
                    "form_id": _empty_text(row.get("form_id")),
                    "language_code": _empty_text(row.get("language_code")),
                    "video_url": _empty_text(row.get("video_url")),
                    "meta_raw": _empty_text(row.get("meta")),
                    "ts": event_ts,
                    "red_flag_id": _empty_text(row.get("red_flag_id")),
                    "overall_flag_code": _empty_text(row.get("overall_flag_code")),
                    "doctor_display_name": filters["doctor_display_name"],
                    "city": filters["city"],
                    "district": filters["district"],
                    "state": filters["state"],
                    "field_rep_id": filters["field_rep_id"],
                    "field_rep_name": filters["field_rep_name"],
                    "is_reminder_sent": "true" if classifications["is_reminder_sent"] else "false",
                    "is_patient_education": "true" if classifications["is_patient_education"] else "false",
                    "is_doctor_education": "true" if classifications["is_doctor_education"] else "false",
                }
            )
    replace_table(
        SILVER_SCHEMA,
        "fact_metric_event",
        [
            "metric_event_id",
            "source_metric_event_id",
            "event_type",
            "action_key",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "patient_id",
            "share_code",
            "form_id",
            "language_code",
            "video_url",
            "meta_raw",
            "ts",
            "red_flag_id",
            "overall_flag_code",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
            "is_reminder_sent",
            "is_patient_education",
            "is_doctor_education",
        ],
        metric_fact_rows,
    )

    followup_fact_rows = []
    for row in followup_rows:
        for item in explode_followup_schedule(row):
            for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(
                row.get("doctor_id"),
                f"followup:{row.get('id')}",
                item["scheduled_followup_date"],
                campaign_hint=row.get("campaign_id"),
            ):
                filters = _doctor_filters(doctor_dim)
                if privacy_allowlist and not _sapa_campaign_allowed(filters["campaign_key"], privacy_allowlist):
                    continue
                if person_privacy_rules and not _sapa_person_visible({**(doctor_dim or {}), **filters, **row}, person_privacy_rules):
                    continue
                reminder_id = _empty_text(row.get("id"))
                if filters["campaign_key"]:
                    reminder_id = f"{reminder_id}:campaign:{filters['campaign_key']}"
                followup_fact_rows.append(
                    {
                        "reminder_id": reminder_id,
                        "source_reminder_id": _empty_text(row.get("id")),
                        "doctor_key": doctor_key,
                        "campaign_key": filters["campaign_key"],
                        "campaign_label": filters["campaign_label"],
                        "patient_id": _empty_text(row.get("patient_id")),
                        "patient_name": _empty_text(row.get("patient_name")),
                        "patient_whatsapp": _empty_text(normalize_phone(row.get("patient_whatsapp"))),
                        "scheduled_followup_date": item["scheduled_followup_date"],
                        "schedule_sequence": item["schedule_sequence"],
                        "generation_method": item["generation_method"],
                        "source_date_field": item["source_date_field"],
                        "frequency_unit": _empty_text(row.get("frequency_unit")),
                        "frequency": _empty_text(row.get("frequency")),
                        "num_followups": _empty_text(row.get("num_followups")),
                        "first_followup_date": _empty_text(iso_date(row.get("first_followup_date"))),
                        "created_at": _empty_text(iso_datetime(row.get("created_at"))),
                        "updated_at": _empty_text(iso_datetime(row.get("updated_at"))),
                        "doctor_display_name": filters["doctor_display_name"],
                        "city": filters["city"],
                        "district": filters["district"],
                        "state": filters["state"],
                        "field_rep_id": filters["field_rep_id"],
                        "field_rep_name": filters["field_rep_name"],
                    }
                )
    replace_table(
        SILVER_SCHEMA,
        "fact_followup_schedule_instance",
        [
            "reminder_id",
            "source_reminder_id",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "patient_id",
            "patient_name",
            "patient_whatsapp",
            "scheduled_followup_date",
            "schedule_sequence",
            "generation_method",
            "source_date_field",
            "frequency_unit",
            "frequency",
            "num_followups",
            "first_followup_date",
            "created_at",
            "updated_at",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
        ],
        followup_fact_rows,
    )

    reminder_sent_rows = [
        {
            "metric_event_id": row["metric_event_id"],
            "source_metric_event_id": row["source_metric_event_id"],
            "doctor_key": row["doctor_key"],
            "campaign_key": row["campaign_key"],
            "campaign_label": row["campaign_label"],
            "patient_id": row["patient_id"],
            "ts": row["ts"],
            "action_key": row["action_key"],
            "doctor_display_name": row["doctor_display_name"],
            "city": row["city"],
            "district": row["district"],
            "state": row["state"],
            "field_rep_id": row["field_rep_id"],
            "field_rep_name": row["field_rep_name"],
        }
        for row in metric_fact_rows
        if row["is_reminder_sent"] == "true"
    ]
    replace_table(
        SILVER_SCHEMA,
        "fact_reminder_sent",
        [
            "metric_event_id",
            "source_metric_event_id",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "patient_id",
            "ts",
            "action_key",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
        ],
        reminder_sent_rows,
    )

    webinar_fact_rows = []
    title_filter = (settings.SAPA_WORDPRESS["WEBINAR_TITLE_FILTER"] or "").lower()
    for row in webinar_rows:
        if title_filter and title_filter not in (_empty_text(row.get("event_title"))).lower():
            continue
        effective_date = webinar_effective_date(row)
        base_registration_key = clean_text(row.get("registration_id")) or hash_fields(
            row.get("event_id"),
            row.get("email"),
            normalize_phone(row.get("phone")),
            row.get("start_date"),
        )
        for matched_dim, match_method in _doctor_matches_for_api(row, dim_by_email, dim_by_phone, effective_date):
            filters = _doctor_filters(matched_dim)
            if privacy_allowlist and not _sapa_campaign_allowed(filters["campaign_key"], privacy_allowlist):
                continue
            if person_privacy_rules and not _sapa_person_visible({**(matched_dim or {}), **filters, **row}, person_privacy_rules):
                continue
            registration_key = base_registration_key
            if filters["campaign_key"]:
                registration_key = f"{registration_key}:campaign:{filters['campaign_key']}"
            webinar_fact_rows.append(
                {
                    "registration_key": registration_key,
                    "source_registration_key": base_registration_key,
                    "event_id": _empty_text(row.get("event_id")),
                    "event_title": _empty_text(row.get("event_title")),
                    "start_date": _empty_text(iso_datetime(row.get("start_date")) or iso_date(row.get("start_date"))),
                    "end_date": _empty_text(iso_datetime(row.get("end_date")) or iso_date(row.get("end_date"))),
                    "timezone": _empty_text(row.get("timezone")),
                    "email": _empty_text(row.get("email")),
                    "first_name": _empty_text(row.get("first_name")),
                    "last_name": _empty_text(row.get("last_name")),
                    "phone": _empty_text(normalize_phone(row.get("phone"))),
                    "registration_effective_date": effective_date.isoformat() if effective_date else "",
                    "doctor_key": _empty_text((matched_dim or {}).get("doctor_key")),
                    "campaign_key": filters["campaign_key"],
                    "campaign_label": filters["campaign_label"],
                    "doctor_display_name": filters["doctor_display_name"],
                    "state": filters["state"],
                    "city": filters["city"],
                    "field_rep_id": filters["field_rep_id"],
                    "field_rep_name": filters["field_rep_name"],
                    "match_method": match_method,
                    "unmapped_flag": "true" if match_method == "unmapped" else "false",
                }
            )
    replace_table(
        SILVER_SCHEMA,
        "fact_webinar_registration",
        [
            "registration_key",
            "source_registration_key",
            "event_id",
            "event_title",
            "start_date",
            "end_date",
            "timezone",
            "email",
            "first_name",
            "last_name",
            "phone",
            "registration_effective_date",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "doctor_display_name",
            "state",
            "city",
            "field_rep_id",
            "field_rep_name",
            "match_method",
            "unmapped_flag",
        ],
        webinar_fact_rows,
    )

    course_progress_rows = []
    invalid_course_status_counter = Counter()
    for row in course_breakdown_rows:
        dashboard_status = map_course_status(row.get("progress_status"))
        if dashboard_status is None:
            invalid_course_status_counter[_empty_text(row.get("progress_status"), "BLANK")] += 1
        course_match_date = row.get("completed_at") or row.get("started_at") or row.get("enrolled_at") or date.today().isoformat()
        for matched_dim, match_method in _doctor_matches_for_api(row, dim_by_email, dim_by_phone, course_match_date):
            filters = _doctor_filters(matched_dim)
            if privacy_allowlist and not _sapa_campaign_allowed(filters["campaign_key"], privacy_allowlist):
                continue
            if person_privacy_rules and not _sapa_person_visible({**(matched_dim or {}), **filters, **row}, person_privacy_rules):
                continue
            course_progress_rows.append(
                {
                    "extract_snapshot_date": date.today().isoformat(),
                    "course_id": _empty_text(row.get("course_id")),
                    "course_audience": _empty_text(row.get("course_audience")),
                    "user_id": _empty_text(row.get("user_id")),
                    "display_name": _empty_text(row.get("display_name")),
                    "user_email": _empty_text(row.get("user_email")),
                    "first_name": _empty_text(row.get("first_name")),
                    "last_name": _empty_text(row.get("last_name")),
                    "phone": _empty_text(normalize_phone(row.get("phone"))),
                    "progress_status": _empty_text(row.get("progress_status")),
                    "enrolled_at": _empty_text(iso_datetime(row.get("enrolled_at"))),
                    "started_at": _empty_text(iso_datetime(row.get("started_at"))),
                    "completed_at": _empty_text(iso_datetime(row.get("completed_at"))),
                    "dashboard_status": dashboard_status or "",
                    "doctor_key": _empty_text((matched_dim or {}).get("doctor_key")),
                    "campaign_key": filters["campaign_key"],
                    "campaign_label": filters["campaign_label"],
                    "doctor_display_name": filters["doctor_display_name"],
                    "state": filters["state"],
                    "city": filters["city"],
                    "field_rep_id": filters["field_rep_id"],
                    "field_rep_name": filters["field_rep_name"],
                    "match_method": match_method,
                    "unmapped_flag": "true" if match_method == "unmapped" else "false",
                }
            )
    replace_table(
        SILVER_SCHEMA,
        "fact_course_user_progress",
        [
            "extract_snapshot_date",
            "course_id",
            "course_audience",
            "user_id",
            "display_name",
            "user_email",
            "first_name",
            "last_name",
            "phone",
            "progress_status",
            "enrolled_at",
            "started_at",
            "completed_at",
            "dashboard_status",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "doctor_display_name",
            "state",
            "city",
            "field_rep_id",
            "field_rep_name",
            "match_method",
            "unmapped_flag",
        ],
        course_progress_rows,
    )

    as_of_date = date.today()
    relevant_dates = [parse_date(row.get("submitted_at")) for row in screening_rows if parse_date(row.get("submitted_at"))]
    start_date = min(relevant_dates) if relevant_dates else as_of_date
    doctor_status_rows = []
    screening_by_doctor: dict[str, list[date]] = defaultdict(list)
    for row in screening_rows:
        submitted = parse_date(row.get("submitted_at"))
        if submitted:
            screening_by_doctor[row["doctor_key"]].append(submitted)

    current_date = start_date
    while current_date <= as_of_date:
        window_start = current_date - timedelta(days=14)
        for doctor in dim_rows:
            doctor_key = doctor["doctor_key"]
            doctor_dates = screening_by_doctor.get(doctor_key, [])
            count_last_15d = sum(1 for submitted in doctor_dates if window_start <= submitted <= current_date)
            last_screening = max((submitted for submitted in doctor_dates if submitted <= current_date), default=None)
            doctor_status_rows.append(
                {
                    "as_of_date": current_date.isoformat(),
                    "doctor_key": doctor_key,
                    "campaign_key": doctor["campaign_key"],
                    "campaign_label": doctor["campaign_label"],
                    "doctor_display_name": doctor["canonical_display_name"],
                    "screenings_last_15d": str(count_last_15d),
                    "is_active": "true" if count_last_15d >= 3 else "false",
                    "is_inactive": "true" if count_last_15d == 0 else "false",
                    "is_other": "true" if 0 < count_last_15d < 3 else "false",
                    "last_screening_at": last_screening.isoformat() if last_screening else "",
                    "city": doctor["city"],
                    "district": doctor["district"],
                    "state": doctor["state"],
                    "field_rep_id": doctor["field_rep_id"],
                    "field_rep_name": doctor["field_rep_name"],
                }
            )
        current_date += timedelta(days=1)
    replace_table(
        SILVER_SCHEMA,
        "fact_doctor_status_daily",
        [
            "as_of_date",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "doctor_display_name",
            "screenings_last_15d",
            "is_active",
            "is_inactive",
            "is_other",
            "last_screening_at",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
        ],
        doctor_status_rows,
    )

    video_rows = []
    for row in metric_fact_rows:
        audience = ""
        if row["is_patient_education"] == "true":
            audience = "patient"
        elif row["is_doctor_education"] == "true":
            audience = "doctor"
        if not audience:
            continue
        red_flag_id = _empty_text(row.get("red_flag_id"))
        language_code = _empty_text(row.get("language_code"))
        mapped_patient_video_url = patient_video_by_flag_and_language.get((red_flag_id, language_code)) or patient_video_by_flag.get(red_flag_id)
        mapped_doctor_video_url = _empty_text((red_flag_catalog.get(red_flag_id) or {}).get("doctor_video_url"))
        resolved_video_url = (
            mapped_patient_video_url
            if audience == "patient" and mapped_patient_video_url
            else mapped_doctor_video_url
            if audience == "doctor" and mapped_doctor_video_url
            else _empty_text(row.get("video_url"))
        )
        content_identifier = resolved_video_url or _empty_text(row.get("video_url") or row.get("red_flag_id") or row.get("action_key"))
        video_rows.append(
            {
                "metric_event_id": row["metric_event_id"],
                "source_metric_event_id": row["source_metric_event_id"],
                "doctor_key": row["doctor_key"],
                "campaign_key": row["campaign_key"],
                "campaign_label": row["campaign_label"],
                "patient_id": row["patient_id"],
                "audience": audience,
                "content_identifier": content_identifier,
                "video_url": resolved_video_url or row["video_url"],
                "action_key": row["action_key"],
                "event_type": row["event_type"],
                "ts": row["ts"],
                "doctor_display_name": row["doctor_display_name"],
                "city": row["city"],
                "district": row["district"],
                "state": row["state"],
                "field_rep_id": row["field_rep_id"],
                "field_rep_name": row["field_rep_name"],
            }
        )
    replace_table(
        SILVER_SCHEMA,
        "fact_video_view",
        [
            "metric_event_id",
            "source_metric_event_id",
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "patient_id",
            "audience",
            "content_identifier",
            "video_url",
            "action_key",
            "event_type",
            "ts",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
        ],
        video_rows,
    )

    screening_count_by_doctor = Counter(row["doctor_key"] for row in screening_rows)
    red_tag_count_by_doctor = Counter(row["doctor_key"] for row in screening_rows if row["is_red_tag"] == "true")
    allowed_recon_doctor_keys = {
        key
        for row in dim_rows
        for key in (clean_text(row.get("doctor_key")), clean_text(row.get("base_doctor_key")))
        if key
    }
    recon_rows = []
    for outcome in clinic_outcomes:
        doctor_key = canonical_doctor_key(outcome.get("doctor_id"))
        if privacy_allowlist and doctor_key not in allowed_recon_doctor_keys:
            continue
        recon_rows.append(
            {
                "doctor_key": doctor_key,
                "source_total_form_fills": _empty_text(outcome.get("total_form_fills")),
                "source_total_red_flags": _empty_text(outcome.get("total_red_flags")),
                "event_driven_screening_count": str(screening_count_by_doctor.get(doctor_key, 0)),
                "event_driven_red_tag_count": str(red_tag_count_by_doctor.get(doctor_key, 0)),
                "discrepancy_flag": "true"
                if as_int(outcome.get("total_form_fills")) < screening_count_by_doctor.get(doctor_key, 0)
                or as_int(outcome.get("total_red_flags")) < red_tag_count_by_doctor.get(doctor_key, 0)
                else "false",
            }
        )
    replace_table(
        SILVER_SCHEMA,
        "recon_clinic_outcome",
        [
            "doctor_key",
            "source_total_form_fills",
            "source_total_red_flags",
            "event_driven_screening_count",
            "event_driven_red_tag_count",
            "discrepancy_flag",
        ],
        recon_rows,
    )

    certification_rows = []
    doctor_course_enrollments: dict[str, dict[str, Any]] = {}
    for row in course_progress_rows:
        if clean_text(row.get("course_audience")) != "doctor":
            continue
        doctor_key = clean_text(row.get("doctor_key"))
        if not doctor_key:
            continue
        existing = doctor_course_enrollments.get(doctor_key)
        enrolled_at = row.get("enrolled_at") or ""
        if existing is None or enrolled_at < existing.get("certification_date", ""):
            doctor_course_enrollments[doctor_key] = {
                "doctor_key": doctor_key,
                "campaign_key": row.get("campaign_key", ""),
                "campaign_label": row.get("campaign_label", ""),
                "certification_status": "enrolled",
                "certification_date": enrolled_at,
                "certification_source": "doctor_course_enrollment",
                "derivation_note": "Derived from doctor course enrollment",
                "support_flag": "true",
            }
    certification_rows.extend(doctor_course_enrollments.values())
    replace_table(
        SILVER_SCHEMA,
        "certification_status_prepared",
        [
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "certification_status",
            "certification_date",
            "certification_source",
            "derivation_note",
            "support_flag",
        ],
        certification_rows,
    )

    return {
        "counts": {
            "dim_doctor_clinic": len(dim_rows),
            "fact_screening_submission": len(screening_rows),
            "fact_metric_event": len(metric_fact_rows),
            "fact_field_rep_login": len(field_rep_login_rows),
            "fact_followup_schedule_instance": len(followup_fact_rows),
            "fact_webinar_registration": len(webinar_fact_rows),
            "fact_course_user_progress": len(course_progress_rows),
            "fact_doctor_status_daily": len(doctor_status_rows),
            "ops.reporting_campaign_privacy_allowlist_active": len(privacy_allowlist),
            "ops.reporting_person_privacy_rule_active": len(person_privacy_rules),
            "ops.reporting_raw_visibility_rule_active": len(raw_visibility_rules),
        },
        "issues": {
            "invalid_course_status": dict(invalid_course_status_counter),
        },
        "as_of_date": as_of_date.isoformat(),
    }
