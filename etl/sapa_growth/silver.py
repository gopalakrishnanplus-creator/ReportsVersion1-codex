from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from django.conf import settings

from etl.sapa_growth.specs import BRONZE_SCHEMA, SILVER_SCHEMA
from etl.sapa_growth.storage import fetch_table, replace_table
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
        email = clean_text(row.get("canonical_email"))
        phone = normalize_phone(row.get("canonical_phone") or row.get("canonical_whatsapp_no"))
        if doctor_id:
            by_doctor_id[doctor_id].append(row)
        if email:
            by_email[email.lower()].append(row)
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


def _truthy(value: Any) -> bool:
    return (clean_text(value) or "").lower() in {"1", "true", "t", "yes", "y", "on"}


def _norm_id(value: Any) -> str:
    raw = clean_text(value) or ""
    return "".join(ch.lower() for ch in raw if ch.isalnum())


def _campaign_specific_doctor_key(base_key: str, campaign_key: str) -> str:
    if not campaign_key:
        return base_key
    return f"{base_key}::campaign:{campaign_key}"


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


def build_silver(run_id: str) -> dict[str, Any]:
    now_iso = _now_iso()

    campaign_doctors = fetch_table(BRONZE_SCHEMA, "campaign_doctor")
    campaign_enrollments = fetch_table(BRONZE_SCHEMA, "campaign_doctorcampaignenrollment")
    campaign_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaign")
    brand_rows = fetch_table(BRONZE_SCHEMA, "campaign_brand")
    source_field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_fieldrep")
    campaign_field_rep_rows = fetch_table(BRONZE_SCHEMA, "campaign_campaignfieldrep")
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
    field_rep_by_id = {clean_text(row.get("id")): row for row in source_field_rep_rows if clean_text(row.get("id"))}
    field_rep_by_external = {
        _norm_id(row.get("brand_supplied_field_rep_id")): row
        for row in source_field_rep_rows
        if _norm_id(row.get("brand_supplied_field_rep_id"))
    }
    campaign_rep_ids: dict[str, set[str]] = defaultdict(set)
    rep_campaign_ids: dict[str, set[str]] = defaultdict(set)
    for row in campaign_field_rep_rows:
        campaign_id = clean_text(row.get("campaign_id"))
        field_rep_id = clean_text(row.get("field_rep_id"))
        if not campaign_id or not field_rep_id:
            continue
        campaign_rep_ids[campaign_id].add(field_rep_id)
        rep_campaign_ids[field_rep_id].add(campaign_id)

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
        rep = field_rep_by_id.get(clean_text(doctor_row.get("field_rep_id")) or "") or field_rep_by_external.get(_norm_id(doctor_row.get("field_rep_id")))
        campaign_ids = sorted(rep_campaign_ids.get(clean_text((rep or {}).get("id")) or "", set()) & set(rfa_campaigns.keys()))
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
                    "clinic_name": _empty_text((doctor_row or {}).get("clinic_name")),
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
                    "clinic_name": _empty_text(doctor_row.get("clinic_name")),
                    "city": "",
                    "district": _empty_text(doctor_row.get("district")),
                    "state": _empty_text(campaign_info["field_rep_state"] or doctor_row.get("state")),
                    "field_rep_id": campaign_info["field_rep_id"],
                    "field_rep_name": campaign_info["field_rep_name"],
                    "recruited_via": _empty_text(doctor_row.get("recruited_via")),
                    "first_seen_at": _empty_text(iso_datetime(doctor_row.get("created_at"))),
                    "latest_seen_at": _empty_text(iso_datetime(doctor_row.get("created_at"))),
                    "is_user_created_doctor": "false",
                    "has_campaign_source": "false",
                    "has_redflags_source": "true",
                    "identity_quality_status": "logical_doctor_id" if doctor_id else "redflags_missing_doctor_id",
                    "_silver_updated_at": now_iso,
                    "_dq_status": "FAIL" if not doctor_id else "PASS",
                    "_dq_errors": "Missing doctor_id in redflags_doctor" if not doctor_id else "",
                }
            )

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
        "clinic_name",
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

    def resolve_doctor_matches_from_source_id(source_doctor_id: Any, source_hint: str, event_date: Any = None) -> list[tuple[str, dict[str, Any] | None]]:
        doctor_id = clean_text(source_doctor_id)
        dim_matches = dim_by_doctor_id.get(doctor_id) or []
        if dim_matches:
            match = _best_dim_for_event(dim_matches, event_date)
            return [(match["doctor_key"], match)] if match else []
        if doctor_id:
            return [(f"unmatched:{doctor_id}", None)]
        return [(f"unmatched:{source_hint}", None)]

    screening_rows = []
    screening_source_index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for source_table, source_rows in (("redflags_patientsubmission", redflag_submissions), ("gnd_gndpatientsubmission", gnd_submissions)):
        for row in source_rows:
            source_submission_id = clean_text(row.get("record_id") or row.get("id")) or hash_fields(source_table, row)
            submitted_at = iso_datetime(row.get("submitted_at"))
            for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(row.get("doctor_id"), f"{source_table}:{source_submission_id}", submitted_at):
                filters = _doctor_filters(doctor_dim)
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
        for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(row.get("doctor_id"), f"metric:{row.get('id')}", event_ts):
            filters = _doctor_filters(doctor_dim)
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
            for doctor_key, doctor_dim in resolve_doctor_matches_from_source_id(row.get("doctor_id"), f"followup:{row.get('id')}", item["scheduled_followup_date"]):
                filters = _doctor_filters(doctor_dim)
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
    recon_rows = []
    for outcome in clinic_outcomes:
        doctor_key = canonical_doctor_key(outcome.get("doctor_id"))
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
            "fact_followup_schedule_instance": len(followup_fact_rows),
            "fact_webinar_registration": len(webinar_fact_rows),
            "fact_course_user_progress": len(course_progress_rows),
            "fact_doctor_status_daily": len(doctor_status_rows),
        },
        "issues": {
            "invalid_course_status": dict(invalid_course_status_counter),
        },
        "as_of_date": as_of_date.isoformat(),
    }
