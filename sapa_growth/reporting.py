from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Any

from django.conf import settings

from sapa_growth.logic import clean_text, map_course_status, parse_date


def filter_rows(rows: list[dict[str, Any]], filters: dict[str, str | None], city_field: str = "city") -> list[dict[str, Any]]:
    state = clean_text(filters.get("state"))
    field_rep_id = clean_text(filters.get("field_rep_id"))
    doctor_key = clean_text(filters.get("doctor_key"))
    city = clean_text(filters.get("city"))
    filtered = rows
    if state:
        filtered = [row for row in filtered if clean_text(row.get("state")) == state]
    if field_rep_id:
        filtered = [row for row in filtered if clean_text(row.get("field_rep_id")) == field_rep_id]
    if doctor_key:
        filtered = [row for row in filtered if clean_text(row.get("doctor_key")) == doctor_key]
    if city:
        filtered = [row for row in filtered if clean_text(row.get(city_field)) == city]
    return filtered


def course_status_counts(course_rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: {"Started": 0, "Completed": 0, "Pending": 0, "Total": 0})
    for row in course_rows:
        audience = clean_text(row.get("course_audience")) or "unknown"
        status = clean_text(row.get("dashboard_status")) or map_course_status(row.get("progress_status")) or ""
        if status:
            counts[audience][status] += 1
            counts[audience]["Total"] += 1
    return counts


def compute_dashboard_metrics(
    *,
    as_of_date: date,
    doctor_rows: list[dict[str, Any]],
    doctor_status_current_rows: list[dict[str, Any]],
    doctor_status_history_rows: list[dict[str, Any]],
    certification_rows: list[dict[str, Any]],
    webinar_rows: list[dict[str, Any]],
    screening_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
    reminder_rows: list[dict[str, Any]],
    course_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    weekly_start = as_of_date - timedelta(days=6)
    previous_start = as_of_date - timedelta(days=13)
    previous_end = as_of_date - timedelta(days=7)
    activity_start = as_of_date - timedelta(days=14)

    def event_count(rows: list[dict[str, Any]], field_name: str, predicate=None, start: date | None = None, end: date | None = None) -> int:
        total = 0
        for row in rows:
            if predicate and not predicate(row):
                continue
            row_date = parse_date(row.get(field_name))
            if row_date is None:
                continue
            if start and row_date < start:
                continue
            if end and row_date > end:
                continue
            total += 1
        return total

    def distinct_doctor_count(rows: list[dict[str, Any]], field_name: str, predicate=None, start: date | None = None, end: date | None = None) -> int:
        keys: set[str] = set()
        for row in rows:
            if predicate and not predicate(row):
                continue
            row_date = parse_date(row.get(field_name))
            if start and (row_date is None or row_date < start):
                continue
            if end and (row_date is None or row_date > end):
                continue
            doctor_key = clean_text(row.get("doctor_key"))
            if doctor_key:
                keys.add(doctor_key)
        return len(keys)

    current_active = len({row["doctor_key"] for row in doctor_status_current_rows if row.get("is_active") == "true"})
    current_inactive = len({row["doctor_key"] for row in doctor_status_current_rows if row.get("is_inactive") == "true"})
    previous_snapshot_date = (as_of_date - timedelta(days=1)).isoformat()
    previous_snapshot_rows = [row for row in doctor_status_history_rows if row.get("as_of_date") == previous_snapshot_date]
    previous_active = len({row["doctor_key"] for row in previous_snapshot_rows if row.get("is_active") == "true"})
    previous_inactive = len({row["doctor_key"] for row in previous_snapshot_rows if row.get("is_inactive") == "true"})

    cert_enabled = True
    current_certified = None
    previous_certified = None
    cumulative_certified = None
    if cert_enabled:
        enrolled_keys = {
            row["doctor_key"]
            for row in certification_rows
            if clean_text(row.get("doctor_key")) and row.get("support_flag") == "true"
        }
        current_active_keys = {row["doctor_key"] for row in doctor_status_current_rows if row.get("is_active") == "true"}
        previous_active_keys = {row["doctor_key"] for row in previous_snapshot_rows if row.get("is_active") == "true"}
        historical_active_keys = {row["doctor_key"] for row in doctor_status_history_rows if row.get("is_active") == "true"}
        current_certified = len(current_active_keys & enrolled_keys)
        previous_certified = len(previous_active_keys & enrolled_keys)
        cumulative_certified = len(historical_active_keys & enrolled_keys)

    course_counts = course_status_counts(course_rows)
    doctor_course = course_counts.get("doctor", {"Started": 0, "Completed": 0, "Pending": 0, "Total": 0})
    paramedic_course = course_counts.get("paramedic", {"Started": 0, "Completed": 0, "Pending": 0, "Total": 0})

    return {
        "as_of_date": as_of_date.isoformat(),
        "weekly_window_start": weekly_start.isoformat(),
        "weekly_window_end": as_of_date.isoformat(),
        "activity_window_start": activity_start.isoformat(),
        "activity_window_end": as_of_date.isoformat(),
        "webinar_registrations_weekly": event_count(webinar_rows, "registration_effective_date", start=weekly_start, end=as_of_date),
        "webinar_registrations_cumulative": event_count(webinar_rows, "registration_effective_date"),
        "webinar_registrations_previous": event_count(webinar_rows, "registration_effective_date", start=previous_start, end=previous_end),
        "onboarded_doctors_weekly": distinct_doctor_count(
            doctor_rows,
            "first_seen_at",
            predicate=lambda row: row.get("is_user_created_doctor") == "true",
            start=weekly_start,
            end=as_of_date,
        ),
        "onboarded_doctors_cumulative": len({row["doctor_key"] for row in doctor_rows if row.get("is_user_created_doctor") == "true"}),
        "onboarded_doctors_previous": distinct_doctor_count(
            doctor_rows,
            "first_seen_at",
            predicate=lambda row: row.get("is_user_created_doctor") == "true",
            start=previous_start,
            end=previous_end,
        ),
        "active_clinics_current": current_active,
        "active_clinics_cumulative": len({row["doctor_key"] for row in doctor_status_history_rows if row.get("is_active") == "true"}),
        "active_clinics_previous": previous_active,
        "inactive_clinics_current": current_inactive,
        "inactive_clinics_cumulative": len({row["doctor_key"] for row in doctor_status_history_rows if row.get("is_inactive") == "true"}),
        "inactive_clinics_previous": previous_inactive,
        "certified_clinics_current": current_certified,
        "certified_clinics_cumulative": cumulative_certified,
        "certified_clinics_previous": previous_certified,
        "certified_clinics_supported": "true" if cert_enabled else "false",
        "total_screenings_weekly": event_count(screening_rows, "submitted_at", start=weekly_start, end=as_of_date),
        "total_screenings_cumulative": event_count(screening_rows, "submitted_at"),
        "total_screenings_previous": event_count(screening_rows, "submitted_at", start=previous_start, end=previous_end),
        "red_tags_weekly": event_count(
            screening_rows,
            "submitted_at",
            predicate=lambda row: clean_text(row.get("overall_flag_code")) == "red",
            start=weekly_start,
            end=as_of_date,
        ),
        "red_tags_cumulative": event_count(screening_rows, "submitted_at", predicate=lambda row: clean_text(row.get("overall_flag_code")) == "red"),
        "red_tags_previous": event_count(
            screening_rows,
            "submitted_at",
            predicate=lambda row: clean_text(row.get("overall_flag_code")) == "red",
            start=previous_start,
            end=previous_end,
        ),
        "yellow_tags_weekly": event_count(
            screening_rows,
            "submitted_at",
            predicate=lambda row: clean_text(row.get("overall_flag_code")) == "yellow",
            start=weekly_start,
            end=as_of_date,
        ),
        "yellow_tags_cumulative": event_count(screening_rows, "submitted_at", predicate=lambda row: clean_text(row.get("overall_flag_code")) == "yellow"),
        "yellow_tags_previous": event_count(
            screening_rows,
            "submitted_at",
            predicate=lambda row: clean_text(row.get("overall_flag_code")) == "yellow",
            start=previous_start,
            end=previous_end,
        ),
        "followups_scheduled_weekly": event_count(followup_rows, "scheduled_followup_date", start=weekly_start, end=as_of_date),
        "followups_scheduled_cumulative": event_count(followup_rows, "scheduled_followup_date"),
        "followups_scheduled_previous": event_count(followup_rows, "scheduled_followup_date", start=previous_start, end=previous_end),
        "reminders_sent_weekly": event_count(reminder_rows, "ts", start=weekly_start, end=as_of_date),
        "reminders_sent_cumulative": event_count(reminder_rows, "ts"),
        "reminders_sent_previous": event_count(reminder_rows, "ts", start=previous_start, end=previous_end),
        "doctor_course_started": doctor_course["Started"],
        "doctor_course_completed": doctor_course["Completed"],
        "doctor_course_pending": doctor_course["Pending"],
        "doctor_course_total": doctor_course["Total"],
        "paramedic_course_started": paramedic_course["Started"],
        "paramedic_course_completed": paramedic_course["Completed"],
        "paramedic_course_pending": paramedic_course["Pending"],
        "paramedic_course_total": paramedic_course["Total"],
    }


def build_rankings(rows: list[dict[str, Any]], group_field: str, label_field: str | None = None, top_n: int = 5) -> list[dict[str, Any]]:
    counts = Counter()
    latest_seen: dict[str, str] = {}
    labels: dict[str, str] = {}
    for row in rows:
        group_value = clean_text(row.get(group_field))
        if not group_value:
            continue
        counts[group_value] += 1
        timestamp = clean_text(row.get("ts") or row.get("submitted_at") or row.get("latest_seen_at")) or ""
        if timestamp > latest_seen.get(group_value, ""):
            latest_seen[group_value] = timestamp
        labels[group_value] = clean_text(row.get(label_field or group_field)) or group_value
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [
        {
            "content_identifier": key,
            "preferred_display_label": labels.get(key, key),
            "view_count": count,
            "latest_interaction_timestamp": latest_seen.get(key, ""),
            "rank": index + 1,
        }
        for index, (key, count) in enumerate(ranked[:top_n])
    ]


def build_video_rankings(video_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for audience in ("patient", "doctor"):
        audience_rows = [row for row in video_rows if clean_text(row.get("audience")) == audience]
        for row in build_rankings(audience_rows, "content_identifier", label_field="preferred_display_label", top_n=5):
            row["audience"] = audience
            row["video_url"] = row["content_identifier"]
            row["video_title"] = clean_text(row.get("preferred_display_label"))
            output.append(row)
    return output


def build_red_flag_rankings(redflag_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows = []
    for row in redflag_rows:
        normalized = dict(row)
        normalized["red_flag_label"] = clean_text(row.get("red_flag_name")) or clean_text(row.get("red_flag"))
        normalized_rows.append(normalized)
    rankings = build_rankings(normalized_rows, "red_flag_label", label_field="red_flag_label", top_n=3)
    return [
        {
            "red_flag": row["content_identifier"],
            "occurrence_count": row["view_count"],
            "latest_seen_at": row["latest_interaction_timestamp"],
            "rank": row["rank"],
        }
        for row in rankings
    ]
