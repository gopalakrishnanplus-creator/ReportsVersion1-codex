from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO
from math import ceil
from typing import Any
from urllib.parse import urlencode

from django.http import Http404, HttpRequest, HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed
from openpyxl import Workbook
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as pdf_canvas

from etl.sapa_growth.control import log_export
from etl.sapa_growth.specs import GOLD_SCHEMA
from etl.sapa_growth.storage import fetch_table, table_exists
from sapa_growth.logic import clean_text, parse_date
from sapa_growth.reporting import build_red_flag_rankings, build_video_rankings, compute_dashboard_metrics, course_status_counts, filter_rows
from sapa_growth.video_metadata import resolve_video_metadata, supported_video_link

SUMMARY_FIELDS = [
    "webinar_registrations_weekly",
    "webinar_registrations_cumulative",
    "webinar_registrations_previous",
    "onboarded_doctors_weekly",
    "onboarded_doctors_cumulative",
    "onboarded_doctors_previous",
    "active_clinics_current",
    "active_clinics_cumulative",
    "active_clinics_previous",
    "inactive_clinics_current",
    "inactive_clinics_cumulative",
    "inactive_clinics_previous",
    "certified_clinics_current",
    "certified_clinics_cumulative",
    "certified_clinics_previous",
    "certified_clinics_supported",
    "total_screenings_weekly",
    "total_screenings_cumulative",
    "total_screenings_previous",
    "red_tags_weekly",
    "red_tags_cumulative",
    "red_tags_previous",
    "yellow_tags_weekly",
    "yellow_tags_cumulative",
    "yellow_tags_previous",
    "followups_scheduled_weekly",
    "followups_scheduled_cumulative",
    "followups_scheduled_previous",
    "reminders_sent_weekly",
    "reminders_sent_cumulative",
    "reminders_sent_previous",
    "doctor_course_started",
    "doctor_course_completed",
    "doctor_course_pending",
    "doctor_course_total",
    "paramedic_course_started",
    "paramedic_course_completed",
    "paramedic_course_pending",
    "paramedic_course_total",
]

DETAIL_SPECS = {
    "webinar_registrations": {
        "table": "rpt_webinar_registration_detail",
        "date_field": "registration_effective_date",
        "title": "Webinar Registrations",
        "weekly": True,
        "columns": ["event_title", "start_date", "end_date", "timezone", "email", "first_name", "last_name", "doctor_display_name", "state", "field_rep_id"],
    },
    "onboarded_doctors": {
        "table": "rpt_doctor_status_current",
        "date_field": "first_seen_at",
        "title": "Onboarded Doctors",
        "weekly": True,
        "predicate": lambda row: row.get("onboarding_flag") == "true",
        "columns": ["doctor_display_name", "doctor_key", "city", "state", "field_rep_id", "first_seen_at"],
    },
    "active_clinics": {
        "table": "rpt_doctor_status_current",
        "title": "Active Clinics",
        "weekly": False,
        "predicate": lambda row: row.get("active_flag") == "true",
        "columns": ["doctor_display_name", "city", "state", "field_rep_id", "screenings_last_15d", "last_screening_at"],
    },
    "inactive_clinics": {
        "table": "rpt_doctor_status_current",
        "title": "Inactive Clinics",
        "weekly": False,
        "predicate": lambda row: row.get("inactive_flag") == "true",
        "columns": ["doctor_display_name", "city", "state", "field_rep_id", "screenings_last_15d", "last_screening_at"],
    },
    "certified_clinics": {
        "table": "rpt_certified_clinics",
        "title": "Certified Clinics",
        "weekly": False,
        "columns": ["serial_order", "doctor_display_name", "city", "state", "field_rep_id", "certification_status", "certification_date"],
    },
    "total_screenings": {
        "table": "rpt_screening_detail",
        "date_field": "submitted_at",
        "title": "Total Screenings",
        "weekly": True,
        "columns": ["doctor_display_name", "patient_id", "form_identifier", "language_code", "submitted_at", "overall_flag_code", "state", "field_rep_id"],
    },
    "red_tags": {
        "table": "rpt_tag_detail",
        "date_field": "submitted_at",
        "title": "Red Tags",
        "weekly": True,
        "predicate": lambda row: clean_text(row.get("tag_color")) == "red",
        "columns": ["doctor_display_name", "patient_id", "submitted_at", "tag_color", "individual_red_flag_count", "state", "field_rep_id"],
    },
    "yellow_tags": {
        "table": "rpt_tag_detail",
        "date_field": "submitted_at",
        "title": "Yellow Tags",
        "weekly": True,
        "predicate": lambda row: clean_text(row.get("tag_color")) == "yellow",
        "columns": ["doctor_display_name", "patient_id", "submitted_at", "tag_color", "state", "field_rep_id"],
    },
    "followups_scheduled": {
        "table": "rpt_followup_schedule_detail",
        "date_field": "scheduled_followup_date",
        "title": "Follow-ups Scheduled",
        "weekly": True,
        "columns": ["doctor_display_name", "patient_id", "patient_name", "scheduled_followup_date", "schedule_sequence", "field_rep_id", "state"],
    },
    "reminders_sent": {
        "table": "rpt_reminder_sent_detail",
        "date_field": "ts",
        "title": "Reminders Sent",
        "weekly": True,
        "columns": ["doctor_display_name", "patient_id", "ts", "action_key", "field_rep_id", "state"],
    },
    "doctor_course_started": {
        "table": "rpt_course_detail",
        "title": "Doctor Course Started",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "doctor" and clean_text(row.get("dashboard_status")) == "Started",
        "columns": ["display_name", "user_email", "phone", "progress_status", "enrolled_at", "started_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "doctor_course_completed": {
        "table": "rpt_course_detail",
        "title": "Doctor Course Completed",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "doctor" and clean_text(row.get("dashboard_status")) == "Completed",
        "columns": ["display_name", "user_email", "phone", "progress_status", "completed_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "doctor_course_pending": {
        "table": "rpt_course_detail",
        "title": "Doctor Course Pending",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "doctor" and clean_text(row.get("dashboard_status")) == "Pending",
        "columns": ["display_name", "user_email", "phone", "progress_status", "enrolled_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "paramedic_course_started": {
        "table": "rpt_course_detail",
        "title": "Paramedic Course Started",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "paramedic" and clean_text(row.get("dashboard_status")) == "Started",
        "columns": ["display_name", "user_email", "phone", "progress_status", "enrolled_at", "started_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "paramedic_course_completed": {
        "table": "rpt_course_detail",
        "title": "Paramedic Course Completed",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "paramedic" and clean_text(row.get("dashboard_status")) == "Completed",
        "columns": ["display_name", "user_email", "phone", "progress_status", "completed_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "paramedic_course_pending": {
        "table": "rpt_course_detail",
        "title": "Paramedic Course Pending",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("course_audience")) == "paramedic" and clean_text(row.get("dashboard_status")) == "Pending",
        "columns": ["display_name", "user_email", "phone", "progress_status", "enrolled_at", "doctor_display_name", "state", "field_rep_id"],
    },
    "patient_videos": {
        "table": "rpt_video_view_detail",
        "title": "Top Patient Education Videos Viewed",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("audience")) == "patient",
        "columns": ["rank", "preferred_display_label", "view_count", "latest_interaction_timestamp"],
    },
    "doctor_videos": {
        "table": "rpt_video_view_detail",
        "title": "Top Doctor Education Videos Viewed",
        "weekly": False,
        "predicate": lambda row: clean_text(row.get("audience")) == "doctor",
        "columns": ["rank", "preferred_display_label", "view_count", "latest_interaction_timestamp"],
    },
}


def _gold_rows(table: str) -> list[dict[str, Any]]:
    if not table_exists(GOLD_SCHEMA, table):
        return []
    return fetch_table(GOLD_SCHEMA, table)


def _supported_video_link(value: Any) -> str:
    return supported_video_link(value)


def _resolved_video_link(row: dict[str, Any]) -> str:
    return (
        _supported_video_link(row.get("video_url"))
        or _supported_video_link(row.get("content_identifier"))
        or _supported_video_link(row.get("preferred_display_label"))
        or ""
    )


def _enrich_red_flag_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        item = dict(row)
        item["red_flag_name"] = clean_text(row.get("red_flag_name")) or clean_text(row.get("red_flag")) or ""
        enriched.append(item)
    return enriched


def _enrich_video_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        item = dict(row)
        resolved_link = _resolved_video_link(item)
        if not resolved_link:
            continue
        metadata = resolve_video_metadata(resolved_link)
        video_title = metadata["video_title"] or clean_text(row.get("video_title")) or clean_text(row.get("preferred_display_label"))
        item["content_identifier"] = resolved_link
        item["video_url"] = resolved_link
        item["video_title"] = video_title if video_title and video_title != resolved_link else ""
        item["preferred_display_label"] = metadata["preferred_display_label"] or item["video_title"] or resolved_link
        enriched.append(item)
    return enriched


def _latest_refresh() -> dict[str, Any] | None:
    rows = _gold_rows("refresh_status")
    if not rows:
        return None
    rows.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    return rows[0]


def _to_int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def _summary_from_row(row: dict[str, Any]) -> dict[str, Any]:
    summary = {}
    for field in SUMMARY_FIELDS:
        if field == "certified_clinics_supported":
            summary[field] = "true" if clean_text(row.get(field)) == "true" else "false"
        else:
            summary[field] = _to_int(row.get(field))
    summary["as_of_date"] = clean_text(row.get("as_of_date")) or date.today().isoformat()
    summary["weekly_window_start"] = clean_text(row.get("weekly_window_start")) or summary["as_of_date"]
    summary["weekly_window_end"] = clean_text(row.get("weekly_window_end")) or summary["as_of_date"]
    summary["activity_window_start"] = clean_text(row.get("activity_window_start")) or summary["as_of_date"]
    summary["activity_window_end"] = clean_text(row.get("activity_window_end")) or summary["as_of_date"]
    summary["published_at"] = clean_text(row.get("published_at")) or ""
    return summary


def _metric_ready_doctor_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        item = dict(row)
        item["is_user_created_doctor"] = clean_text(row.get("is_user_created_doctor")) or clean_text(row.get("onboarding_flag")) or "false"
        output.append(item)
    return output


def _metric_ready_status_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        item = dict(row)
        item["is_active"] = clean_text(row.get("is_active")) or clean_text(row.get("active_flag")) or "false"
        item["is_inactive"] = clean_text(row.get("is_inactive")) or clean_text(row.get("inactive_flag")) or "false"
        output.append(item)
    return output


def _doctor_course_enrollments(course_rows: list[dict[str, Any]]) -> dict[str, str]:
    enrollments: dict[str, str] = {}
    for row in course_rows:
        if clean_text(row.get("course_audience")) != "doctor":
            continue
        doctor_key = clean_text(row.get("doctor_key"))
        if not doctor_key:
            continue
        enrolled_at = clean_text(row.get("enrolled_at")) or ""
        if doctor_key not in enrollments or (enrolled_at and enrolled_at < enrollments[doctor_key]):
            enrollments[doctor_key] = enrolled_at
    return enrollments


def _derived_certified_summary(
    filters: dict[str, str | None],
    refresh: dict[str, Any] | None,
    doctor_rows: list[dict[str, Any]],
    doctor_history_rows: list[dict[str, Any]],
    course_rows: list[dict[str, Any]],
) -> dict[str, int | str]:
    enrolled = _doctor_course_enrollments(filter_rows(course_rows, filters))
    current_rows = filter_rows(doctor_rows, filters)
    current = len(
        {
            clean_text(row.get("doctor_key"))
            for row in current_rows
            if row.get("active_flag") == "true" and clean_text(row.get("doctor_key")) in enrolled
        }
        - {None}
    )
    as_of = parse_date((refresh or {}).get("as_of_date")) or date.today()
    previous_date = (as_of - timedelta(days=1)).isoformat()
    history_rows = filter_rows(doctor_history_rows, filters)
    previous = len(
        {
            clean_text(row.get("doctor_key"))
            for row in history_rows
            if row.get("as_of_date") == previous_date and row.get("is_active") == "true" and clean_text(row.get("doctor_key")) in enrolled
        }
        - {None}
    )
    cumulative = len(
        {
            clean_text(row.get("doctor_key"))
            for row in history_rows
            if row.get("is_active") == "true" and clean_text(row.get("doctor_key")) in enrolled
        }
        - {None}
    )
    return {
        "certified_clinics_current": current,
        "certified_clinics_previous": previous,
        "certified_clinics_cumulative": cumulative,
        "certified_clinics_supported": "true",
    }


def _derived_certified_rows(
    filters: dict[str, str | None],
    doctor_rows: list[dict[str, Any]],
    course_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    enrolled = _doctor_course_enrollments(filter_rows(course_rows, filters))
    filtered_doctors = filter_rows(doctor_rows, filters)
    rows: list[dict[str, Any]] = []
    for doctor in filtered_doctors:
        doctor_key = clean_text(doctor.get("doctor_key"))
        if doctor.get("active_flag") != "true" or not doctor_key or doctor_key not in enrolled:
            continue
        rows.append(
            {
                "serial_order": str(len(rows) + 1),
                "doctor_key": doctor_key,
                "doctor_display_name": doctor.get("doctor_display_name", ""),
                "city": doctor.get("city", ""),
                "district": doctor.get("district", ""),
                "state": doctor.get("state", ""),
                "field_rep_id": doctor.get("field_rep_id", ""),
                "certification_status": "enrolled",
                "certification_date": enrolled.get(doctor_key, ""),
                "certification_source": "doctor_course_enrollment",
            }
        )
    return rows


def parse_global_filters(query_params: Any) -> dict[str, str | None]:
    return {
        "state": clean_text(query_params.get("state")),
        "field_rep_id": clean_text(query_params.get("field_rep_id")),
        "doctor_key": clean_text(query_params.get("doctor_key")),
    }


def parse_certified_filters(query_params: Any, global_filters: dict[str, str | None]) -> dict[str, str | None]:
    return {
        "state": global_filters.get("state"),
        "field_rep_id": clean_text(query_params.get("cert_field_rep_id")) or global_filters.get("field_rep_id"),
        "doctor_key": clean_text(query_params.get("cert_doctor_key")) or global_filters.get("doctor_key"),
        "city": clean_text(query_params.get("cert_city")),
    }


def current_filters_query(filters: dict[str, str | None]) -> str:
    return urlencode({key: value for key, value in filters.items() if value})


def _with_delta(current: int | None, previous: int | None) -> dict[str, Any]:
    if current is None or previous is None:
        return {"value": current, "delta": None}
    return {"value": current, "delta": current - previous}


def _dashboard_tiles(summary: dict[str, Any], filters: dict[str, str | None]) -> dict[str, list[dict[str, Any]]]:
    query_string = current_filters_query(filters)
    suffix = f"?{query_string}" if query_string else ""
    return {
        "field_rep": [
            {
                "title": "Webinar Registrations",
                "value": summary["webinar_registrations_weekly"],
                "cumulative": summary["webinar_registrations_cumulative"],
                "delta": summary["webinar_registrations_weekly"] - summary["webinar_registrations_previous"],
                "href": f"/sapa-growth/details/webinar_registrations/{suffix}",
                "theme": "teal",
                "supported": True,
            },
            {
                "title": "Onboarded Doctors (User Created)",
                "value": summary["onboarded_doctors_weekly"],
                "cumulative": summary["onboarded_doctors_cumulative"],
                "delta": summary["onboarded_doctors_weekly"] - summary["onboarded_doctors_previous"],
                "href": f"/sapa-growth/details/onboarded_doctors/{suffix}",
                "theme": "teal",
                "supported": True,
            },
        ],
        "clinic": [
            {
                "title": "Active Clinics",
                "value": summary["active_clinics_current"],
                "cumulative": summary["active_clinics_cumulative"],
                "delta": summary["active_clinics_current"] - summary["active_clinics_previous"],
                "href": f"/sapa-growth/details/active_clinics/{suffix}",
                "theme": "positive",
                "supported": True,
            },
            {
                "title": "Inactive Clinics",
                "value": summary["inactive_clinics_current"],
                "cumulative": summary["inactive_clinics_cumulative"],
                "delta": summary["inactive_clinics_current"] - summary["inactive_clinics_previous"],
                "href": f"/sapa-growth/details/inactive_clinics/{suffix}",
                "theme": "negative",
                "supported": True,
            },
            {
                "title": "Certified Clinics",
                "value": summary["certified_clinics_current"],
                "cumulative": summary["certified_clinics_cumulative"],
                "delta": 0 if summary["certified_clinics_current"] is None or summary["certified_clinics_previous"] is None else summary["certified_clinics_current"] - summary["certified_clinics_previous"],
                "href": f"/sapa-growth/details/certified_clinics/{suffix}",
                "theme": "neutral",
                "supported": True,
            },
            {
                "title": "Total Screenings",
                "value": summary["total_screenings_weekly"],
                "cumulative": summary["total_screenings_cumulative"],
                "delta": summary["total_screenings_weekly"] - summary["total_screenings_previous"],
                "href": f"/sapa-growth/details/total_screenings/{suffix}",
                "theme": "neutral",
                "supported": True,
            },
            {
                "title": "Red Tags",
                "value": summary["red_tags_weekly"],
                "cumulative": summary["red_tags_cumulative"],
                "delta": summary["red_tags_weekly"] - summary["red_tags_previous"],
                "href": f"/sapa-growth/details/red_tags/{suffix}",
                "theme": "warning",
                "supported": True,
            },
            {
                "title": "Yellow Tags",
                "value": summary["yellow_tags_weekly"],
                "cumulative": summary["yellow_tags_cumulative"],
                "delta": summary["yellow_tags_weekly"] - summary["yellow_tags_previous"],
                "href": f"/sapa-growth/details/yellow_tags/{suffix}",
                "theme": "warning",
                "supported": True,
            },
        ],
        "operational": [
            {
                "title": "Follow-ups Scheduled",
                "value": summary["followups_scheduled_weekly"],
                "cumulative": summary["followups_scheduled_cumulative"],
                "delta": summary["followups_scheduled_weekly"] - summary["followups_scheduled_previous"],
                "href": f"/sapa-growth/details/followups_scheduled/{suffix}",
                "theme": "teal",
                "supported": True,
            },
            {
                "title": "Reminders Sent",
                "value": summary["reminders_sent_weekly"],
                "cumulative": summary["reminders_sent_cumulative"],
                "delta": summary["reminders_sent_weekly"] - summary["reminders_sent_previous"],
                "href": f"/sapa-growth/details/reminders_sent/{suffix}",
                "theme": "teal",
                "supported": True,
            },
        ],
    }


def _course_cards(course_rows: list[dict[str, Any]], filters: dict[str, str | None]) -> list[dict[str, Any]]:
    counts = course_status_counts(course_rows)
    query_string = current_filters_query(filters)
    suffix = f"?{query_string}" if query_string else ""
    cards = []
    for audience, title in (("doctor", "Doctor Course"), ("paramedic", "Paramedic Course")):
        summary = counts.get(audience, {"Started": 0, "Completed": 0, "Pending": 0, "Total": 0})
        total = summary["Total"] or 0
        rows = []
        for status in ("Started", "Completed", "Pending"):
            metric_key = f"{audience}_course_{status.lower()}"
            rows.append(
                {
                    "label": status,
                    "count": summary[status],
                    "ratio": round((summary[status] / total) * 100, 2) if total else 0,
                    "href": f"/sapa-growth/details/{metric_key}/{suffix}",
                }
            )
        cards.append({"title": title, "rows": rows, "total": total})
    return cards


def filter_options() -> dict[str, list[dict[str, Any]]]:
    return {
        "states": _gold_rows("dim_filter_state"),
        "field_reps": _gold_rows("dim_filter_field_rep"),
        "doctors": _gold_rows("dim_filter_doctor"),
        "cities": _gold_rows("dim_filter_city"),
    }


def dashboard_context(filters: dict[str, str | None]) -> dict[str, Any]:
    refresh = _latest_refresh()
    options = filter_options()
    if refresh is None:
        return {
            "ready": False,
            "filters": filters,
            "filter_options": options,
            "export_filename": "sapa-growth-dashboard-report.pdf",
        }

    summary = None
    snapshot_rows = _gold_rows("dashboard_summary_snapshot")
    if not any(filters.values()) and snapshot_rows:
        summary = _summary_from_row(snapshot_rows[0])
    elif not filters.get("doctor_key"):
        helper_rows = _gold_rows("dashboard_summary_state_rep")
        for row in helper_rows:
            if clean_text(row.get("state")) == filters.get("state") and clean_text(row.get("field_rep_id")) == filters.get("field_rep_id"):
                summary = _summary_from_row(row)
                break

    doctor_rows = _gold_rows("rpt_doctor_status_current")
    doctor_history_rows = _gold_rows("rpt_doctor_status_history")
    screening_rows = _gold_rows("rpt_screening_detail")
    raw_redflag_rows = _gold_rows("rpt_submission_redflag_detail")
    followup_rows = _gold_rows("rpt_followup_schedule_detail")
    reminder_rows = _gold_rows("rpt_reminder_sent_detail")
    webinar_rows = _gold_rows("rpt_webinar_registration_detail")
    course_rows = _gold_rows("rpt_course_detail")
    video_rows = _gold_rows("rpt_video_view_detail")
    certified_rows = _derived_certified_rows(filters, doctor_rows, course_rows)

    if summary is None:
        as_of = parse_date(refresh.get("as_of_date")) or date.today()
        metric_doctor_rows = _metric_ready_doctor_rows(doctor_rows)
        metric_status_rows = _metric_ready_status_rows(doctor_rows)
        summary = compute_dashboard_metrics(
            as_of_date=as_of,
            doctor_rows=filter_rows(metric_doctor_rows, filters),
            doctor_status_current_rows=filter_rows(metric_status_rows, filters),
            doctor_status_history_rows=filter_rows(doctor_history_rows, filters),
            certification_rows=[],
            webinar_rows=filter_rows(webinar_rows, filters),
            screening_rows=filter_rows(screening_rows, filters),
            followup_rows=filter_rows(followup_rows, filters),
            reminder_rows=filter_rows(reminder_rows, filters),
            course_rows=filter_rows(course_rows, filters),
        )
        summary["published_at"] = refresh.get("published_at") or ""

    summary.update(_derived_certified_summary(filters, refresh, doctor_rows, doctor_history_rows, course_rows))

    filtered_course_rows = filter_rows(course_rows, filters)
    filtered_redflag_rows = _enrich_red_flag_rows(filter_rows(raw_redflag_rows, filters))
    filtered_video_rows = _enrich_video_rows(filter_rows(video_rows, filters))
    patient_videos = [row for row in build_video_rankings(filtered_video_rows) if clean_text(row.get("audience")) == "patient"]
    doctor_videos = [row for row in build_video_rankings(filtered_video_rows) if clean_text(row.get("audience")) == "doctor"]
    certified_supported = True

    return {
        "ready": True,
        "refresh": refresh,
        "summary": summary,
        "export_filename": f"sapa-growth-dashboard-{summary.get('as_of_date') or refresh.get('as_of_date') or 'report'}.pdf",
        "tiles": _dashboard_tiles(summary, filters),
        "course_cards": _course_cards(filtered_course_rows, filters),
        "patient_videos": patient_videos[:5],
        "doctor_videos": doctor_videos[:5],
        "red_flag_rankings": build_red_flag_rankings(filtered_redflag_rows),
        "filters": filters,
        "filters_query": current_filters_query(filters),
        "filter_options": options,
        "certified_supported": certified_supported,
        "certified_toggle_url": f"/sapa-growth/certified-clinics/?{current_filters_query(filters)}" if current_filters_query(filters) else "/sapa-growth/certified-clinics/",
        "certified_rows": certified_rows,
    }


def certified_context(global_filters: dict[str, str | None], local_filters: dict[str, str | None]) -> dict[str, Any]:
    rows = _derived_certified_rows(local_filters, _gold_rows("rpt_doctor_status_current"), _gold_rows("rpt_course_detail"))
    return {
        "supported": True,
        "rows": rows,
        "filters": local_filters,
        "filter_options": filter_options(),
        "export_query": current_filters_query(
            {
                "state": global_filters.get("state"),
                "cert_field_rep_id": local_filters.get("field_rep_id"),
                "cert_doctor_key": local_filters.get("doctor_key"),
                "cert_city": local_filters.get("city"),
            }
        ),
    }


def _rows_for_metric(metric: str, filters: dict[str, str | None]) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    spec = DETAIL_SPECS.get(metric)
    if spec is None:
        raise Http404("Unknown metric")
    rows = filter_rows(_gold_rows(spec["table"]), filters)
    if spec.get("predicate"):
        rows = [row for row in rows if spec["predicate"](row)]

    if metric in {"patient_videos", "doctor_videos"}:
        rows = [
            row
            for row in build_video_rankings(_enrich_video_rows(rows))
            if clean_text(row.get("audience")) == ("patient" if metric == "patient_videos" else "doctor")
        ]

    refresh = _latest_refresh()
    as_of = parse_date((refresh or {}).get("as_of_date")) or date.today()
    if spec.get("weekly"):
        weekly_start = as_of - timedelta(days=6)
        date_field = spec.get("date_field")
        rows = [
            row
            for row in rows
            if date_field and (parse_date(row.get(date_field)) and weekly_start <= parse_date(row.get(date_field)) <= as_of)
        ]
    elif metric == "certified_clinics":
        rows = _derived_certified_rows(filters, _gold_rows("rpt_doctor_status_current"), _gold_rows("rpt_course_detail"))

    date_field = spec.get("date_field")
    if date_field:
        rows.sort(key=lambda row: row.get(date_field) or "", reverse=True)
    elif "rank" in spec["columns"]:
        rows.sort(key=lambda row: _to_int(row.get("rank")), reverse=False)
    return spec, rows, refresh or {}


def detail_context(metric: str, filters: dict[str, str | None], page: int = 1, per_page: int = 25) -> dict[str, Any]:
    spec, rows, refresh = _rows_for_metric(metric, filters)
    total_rows = len(rows)
    page = max(page, 1)
    start = (page - 1) * per_page
    end = start + per_page
    page_rows = rows[start:end]
    return {
        "metric": metric,
        "title": spec["title"],
        "columns": spec["columns"],
        "rows": page_rows,
        "row_count": total_rows,
        "page": page,
        "page_count": max(1, ceil(total_rows / per_page)) if total_rows else 1,
        "filters": filters,
        "filters_query": current_filters_query(filters),
        "last_updated": refresh.get("published_at", ""),
        "as_of_date": refresh.get("as_of_date", ""),
    }


def export_detail_csv(metric: str, filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    import csv

    spec, rows, _ = _rows_for_metric(metric, filters)
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="sapa-growth-{metric}.csv"'
    writer = csv.writer(response)
    writer.writerow(spec["columns"])
    for row in rows:
        writer.writerow([row.get(column, "") for column in spec["columns"]])
    log_export(metric, f"/sapa-growth/details/{metric}/export/", current_filters_query(filters), len(rows), request.session.session_key)
    return response


def export_certified_csv(global_filters: dict[str, str | None], local_filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    import csv

    context = certified_context(global_filters, local_filters)
    if not context["supported"]:
        raise Http404("Certified clinics are not configured")
    rows = context["rows"]
    columns = ["serial_order", "doctor_display_name", "city", "state", "field_rep_id", "certification_status", "certification_date"]
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="sapa-growth-certified-clinics.csv"'
    writer = csv.writer(response)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(column, "") for column in columns])
    log_export("certified-clinics", "/sapa-growth/certified-clinics/export/", current_filters_query(local_filters), len(rows), request.session.session_key)
    return response


def export_dashboard_pdf(filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    snapshot = request.FILES.get("snapshot")
    if snapshot is None:
        return HttpResponseBadRequest("Missing dashboard snapshot")

    image_bytes = snapshot.read()
    if not image_bytes:
        return HttpResponseBadRequest("Empty dashboard snapshot")

    image_reader = ImageReader(BytesIO(image_bytes))
    image_width, image_height = image_reader.getSize()
    if not image_width or not image_height:
        return HttpResponseBadRequest("Invalid dashboard snapshot")

    refresh = _latest_refresh() or {}
    as_of_date = clean_text(refresh.get("as_of_date")) or date.today().isoformat()
    filename = f"sapa-growth-dashboard-{as_of_date}.pdf"

    buffer = BytesIO()
    pdf = pdf_canvas.Canvas(buffer, pagesize=(float(image_width), float(image_height)))
    pdf.drawImage(
        image_reader,
        0,
        0,
        width=float(image_width),
        height=float(image_height),
        preserveAspectRatio=True,
        mask="auto",
    )
    pdf.showPage()
    pdf.save()
    buffer.seek(0)

    response = HttpResponse(buffer.getvalue(), content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    log_export("dashboard-pdf", "/sapa-growth/export/dashboard.pdf", current_filters_query(filters), 0, request.session.session_key)
    return response


def export_dashboard_xlsx(filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    context = dashboard_context(filters)
    if not context["ready"]:
        raise Http404("Dashboard data has not been published yet")

    workbook = Workbook()
    summary_sheet = workbook.active
    summary_sheet.title = "Summary"
    summary_sheet.append(["Metric", "Value"])
    summary = context["summary"]
    for label, field in (
        ("Webinar Registrations (Weekly)", "webinar_registrations_weekly"),
        ("Onboarded Doctors (Weekly)", "onboarded_doctors_weekly"),
        ("Active Clinics", "active_clinics_current"),
        ("Inactive Clinics", "inactive_clinics_current"),
        ("Total Screenings (Weekly)", "total_screenings_weekly"),
        ("Red Tags (Weekly)", "red_tags_weekly"),
        ("Yellow Tags (Weekly)", "yellow_tags_weekly"),
        ("Follow-ups Scheduled (Weekly)", "followups_scheduled_weekly"),
        ("Reminders Sent (Weekly)", "reminders_sent_weekly"),
    ):
        summary_sheet.append([label, summary.get(field)])

    sheet_specs = [
        ("Active Clinics", _rows_for_metric("active_clinics", filters)[1]),
        ("Inactive Clinics", _rows_for_metric("inactive_clinics", filters)[1]),
        ("Certified Clinics", _rows_for_metric("certified_clinics", filters)[1] if context["certified_supported"] else []),
        ("Total Screenings", _rows_for_metric("total_screenings", filters)[1]),
        ("Red Tags", _rows_for_metric("red_tags", filters)[1]),
        ("Yellow Tags", _rows_for_metric("yellow_tags", filters)[1]),
        ("Followups", _rows_for_metric("followups_scheduled", filters)[1]),
        ("Reminders", _rows_for_metric("reminders_sent", filters)[1]),
        ("Doctor Course", _rows_for_metric("doctor_course_started", filters)[1] + _rows_for_metric("doctor_course_completed", filters)[1] + _rows_for_metric("doctor_course_pending", filters)[1]),
        ("Paramedic Course", _rows_for_metric("paramedic_course_started", filters)[1] + _rows_for_metric("paramedic_course_completed", filters)[1] + _rows_for_metric("paramedic_course_pending", filters)[1]),
        ("Webinar", _rows_for_metric("webinar_registrations", filters)[1]),
        ("Patient Videos", _rows_for_metric("patient_videos", filters)[1]),
        ("Doctor Videos", _rows_for_metric("doctor_videos", filters)[1]),
    ]

    for title, rows in sheet_specs:
        sheet = workbook.create_sheet(title[:31])
        if rows:
            columns = list(rows[0].keys())
            sheet.append(columns)
            for row in rows:
                sheet.append([row.get(column, "") for column in columns])
        else:
            sheet.append(["No data"])

    filters_sheet = workbook.create_sheet("Filters")
    filters_sheet.append(["Filter", "Value"])
    for key, value in filters.items():
        filters_sheet.append([key, value or ""])
    filters_sheet.append(["As Of Date", context["summary"]["as_of_date"]])
    filters_sheet.append(["Published At", context["summary"]["published_at"]])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    response = HttpResponse(
        buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="sapa-growth-dashboard.xlsx"'
    log_export("dashboard-xlsx", "/sapa-growth/export/dashboard.xlsx", current_filters_query(filters), 0, request.session.session_key)
    return response
