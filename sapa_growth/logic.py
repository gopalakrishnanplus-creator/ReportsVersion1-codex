from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from typing import Any

NULL_LIKE = {"", "null", "none", "nil", "n/a"}


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in NULL_LIKE:
        return None
    return text or None


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def normalize_phone(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    if len(digits) > 10 and digits.startswith("91"):
        digits = digits[-10:]
    return digits


def parse_datetime(value: Any) -> datetime | None:
    text = clean_text(value)
    if text is None:
        return None
    normalized = text.replace("T", " ")
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized[:26], fmt)
        except ValueError:
            continue
    return None


def parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = parse_datetime(value)
    return parsed.date() if parsed else None


def iso_datetime(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return parsed.isoformat(sep=" ") if parsed else None


def iso_date(value: Any) -> str | None:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else None


def hash_fields(*values: Any) -> str:
    payload = json.dumps([str(v) if v is not None else None for v in values], ensure_ascii=True)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def canonical_doctor_key(doctor_id: Any, campaign_row_id: Any | None = None) -> str:
    clean_doctor_id = clean_text(doctor_id)
    if clean_doctor_id:
        return clean_doctor_id
    campaign_id = clean_text(campaign_row_id) or "unknown"
    return f"campaign-doctor:{campaign_id}"


def split_full_name(full_name: Any) -> tuple[str | None, str | None]:
    text = clean_text(full_name)
    if not text:
        return None, None
    parts = text.split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def display_name_from_sources(campaign_row: dict[str, Any] | None, doctor_row: dict[str, Any] | None) -> str:
    if campaign_row:
        value = clean_text(campaign_row.get("full_name"))
        if value:
            return value
    if doctor_row:
        first = clean_text(doctor_row.get("first_name")) or ""
        last = clean_text(doctor_row.get("last_name")) or ""
        name = " ".join(part for part in (first, last) if part).strip()
        if name:
            return name
    if campaign_row:
        value = clean_text(campaign_row.get("email")) or clean_text(campaign_row.get("phone"))
        if value:
            return value
    if doctor_row:
        value = clean_text(doctor_row.get("email")) or clean_text(doctor_row.get("whatsapp_no"))
        if value:
            return value
    return "Unknown Doctor"


def map_course_status(progress_status: Any) -> str | None:
    status = clean_text(progress_status)
    mapping = {
        "in progress": "Started",
        "completed": "Completed",
        "not started": "Pending",
    }
    if status is None:
        return None
    return mapping.get(status.lower())


def webinar_effective_date(row: dict[str, Any]) -> date | None:
    return (
        parse_date(row.get("registration_created_at"))
        or parse_date(row.get("registered_at"))
        or parse_date(row.get("created_at"))
        or parse_date(row.get("start_date"))
    )


def classify_metric_event(event_type: Any, action_key: Any) -> dict[str, bool]:
    event = (clean_text(event_type) or "").lower()
    action = (clean_text(action_key) or "").lower()
    is_reminder_sent = action == "reminder_sent"
    is_patient_education = action == "patient_edu"
    is_doctor_education = event == "doctor_edu_click" or action == "doctor_video_click"
    return {
        "is_reminder_sent": is_reminder_sent,
        "is_patient_education": is_patient_education,
        "is_doctor_education": is_doctor_education,
    }


def explode_followup_schedule(row: dict[str, Any]) -> list[dict[str, Any]]:
    explicit_dates: list[date] = []
    output: list[dict[str, Any]] = []
    for idx, field_name in enumerate(("followup_date1", "followup_date2", "followup_date3"), start=1):
        parsed = parse_date(row.get(field_name))
        if parsed is None:
            continue
        explicit_dates.append(parsed)
        output.append(
            {
                "scheduled_followup_date": parsed.isoformat(),
                "schedule_sequence": str(idx),
                "generation_method": "explicit",
                "source_date_field": field_name,
            }
        )

    frequency = as_int(row.get("frequency"), default=0)
    num_followups = as_int(row.get("num_followups"), default=0)
    first_followup = parse_date(row.get("first_followup_date"))
    frequency_unit = (clean_text(row.get("frequency_unit")) or "").lower()
    if first_followup and num_followups > 0 and frequency > 0 and frequency_unit in {"d", "day", "days"}:
        explicit_set = {value.isoformat() for value in explicit_dates}
        for idx in range(1, num_followups + 1):
            derived = first_followup + timedelta(days=frequency * (idx - 1))
            derived_iso = derived.isoformat()
            if derived_iso in explicit_set:
                continue
            output.append(
                {
                    "scheduled_followup_date": derived_iso,
                    "schedule_sequence": str(idx),
                    "generation_method": "derived",
                    "source_date_field": "first_followup_date",
                }
            )

    output.sort(key=lambda item: (item["scheduled_followup_date"], item["schedule_sequence"]))
    deduped: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for item in output:
        followup_date = item["scheduled_followup_date"]
        if followup_date in seen_dates:
            continue
        seen_dates.add(followup_date)
        deduped.append(item)
    return deduped


def location_label(city: Any, district: Any, state: Any) -> str | None:
    return clean_text(city) or clean_text(district) or clean_text(state)
