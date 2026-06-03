from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from django.db import connection, transaction

from etl.sapa_growth.specs import GOLD_GLOBAL_SCHEMA, GOLD_SCHEMA, GOLD_STAGE_SCHEMA, SILVER_SCHEMA
from etl.sapa_growth.storage import ensure_schema, fetch_table, qident, replace_table
from sapa_growth.logic import clean_text, location_label, parse_date
from sapa_growth.reporting import build_red_flag_rankings, build_video_rankings, compute_dashboard_metrics, course_status_counts, filter_rows
from sapa_growth.video_metadata import resolve_video_metadata


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stringify_row(row: dict[str, Any]) -> dict[str, str]:
    return {key: "" if value is None else str(value) for key, value in row.items()}


def _campaign_token(value: Any) -> str:
    token = "".join(ch.lower() for ch in clean_text(value) if ch.isalnum())
    return token or "unknown"


def campaign_schema_name(campaign_key: Any) -> str:
    return f"gold_sapa_campaign_{_campaign_token(campaign_key)}"


def _table_columns(schema: str, table: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema, table],
        )
        return [row[0] for row in cursor.fetchall()]


def _copy_campaign_rows(rows: list[dict[str, Any]], source_campaign_key: str, route_campaign_key: str) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for row in rows:
        if clean_text(row.get("campaign_key")) != source_campaign_key:
            continue
        item = dict(row)
        if not clean_text(item.get("campaign_key")):
            item["campaign_key"] = route_campaign_key
        output.append(_stringify_row(item))
    return output


def _metric_doctor_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        item = dict(row)
        item["is_user_created_doctor"] = clean_text(row.get("is_user_created_doctor")) or clean_text(row.get("onboarding_flag")) or "false"
        output.append(item)
    return output


def _metric_status_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        item = dict(row)
        item["is_active"] = clean_text(row.get("is_active")) or clean_text(row.get("active_flag")) or "false"
        item["is_inactive"] = clean_text(row.get("is_inactive")) or clean_text(row.get("inactive_flag")) or "false"
        output.append(item)
    return output


def _doctor_course_enrollments(course_rows: list[dict[str, Any]]) -> set[str]:
    enrolled: set[str] = set()
    for row in course_rows:
        if clean_text(row.get("course_audience")) != "doctor":
            continue
        doctor_key = clean_text(row.get("doctor_key"))
        if doctor_key:
            enrolled.add(doctor_key)
    return enrolled


def _derived_certified_summary(
    *,
    as_of_date: date,
    doctor_rows: list[dict[str, Any]],
    doctor_history_rows: list[dict[str, Any]],
    course_rows: list[dict[str, Any]],
) -> dict[str, int | str]:
    enrolled = _doctor_course_enrollments(course_rows)
    previous_date = (as_of_date - timedelta(days=1)).isoformat()
    current = {
        clean_text(row.get("doctor_key"))
        for row in doctor_rows
        if row.get("active_flag") == "true" and clean_text(row.get("doctor_key")) in enrolled
    }
    previous = {
        clean_text(row.get("doctor_key"))
        for row in doctor_history_rows
        if row.get("as_of_date") == previous_date and row.get("is_active") == "true" and clean_text(row.get("doctor_key")) in enrolled
    }
    cumulative = {
        clean_text(row.get("doctor_key"))
        for row in doctor_history_rows
        if row.get("is_active") == "true" and clean_text(row.get("doctor_key")) in enrolled
    }
    return {
        "certified_clinics_current": len(current - {""}),
        "certified_clinics_previous": len(previous - {""}),
        "certified_clinics_cumulative": len(cumulative - {""}),
        "certified_clinics_supported": "true",
    }


def _summary_snapshot_row(
    *,
    as_of_date: date,
    published_at: str,
    doctor_rows: list[dict[str, Any]],
    doctor_history_rows: list[dict[str, Any]],
    screening_rows: list[dict[str, Any]],
    followup_rows: list[dict[str, Any]],
    reminder_rows: list[dict[str, Any]],
    webinar_rows: list[dict[str, Any]],
    course_rows: list[dict[str, Any]],
) -> dict[str, str]:
    summary = compute_dashboard_metrics(
        as_of_date=as_of_date,
        doctor_rows=_metric_doctor_rows(doctor_rows),
        doctor_status_current_rows=_metric_status_rows(doctor_rows),
        doctor_status_history_rows=doctor_history_rows,
        certification_rows=[],
        webinar_rows=webinar_rows,
        screening_rows=screening_rows,
        followup_rows=followup_rows,
        reminder_rows=reminder_rows,
        course_rows=course_rows,
    )
    summary.update(
        _derived_certified_summary(
            as_of_date=as_of_date,
            doctor_rows=doctor_rows,
            doctor_history_rows=doctor_history_rows,
            course_rows=course_rows,
        )
    )
    summary["published_at"] = published_at
    summary["unsupported_condition_rankings"] = "true"
    return _stringify_row(summary)


def _course_summary_rows(as_of_date: date, course_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for audience, counts in course_status_counts(course_rows).items():
        total = counts["Total"] or 0
        course_id = next((row.get("course_id") for row in course_rows if row.get("course_audience") == audience), "")
        rows.append(
            _stringify_row(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "course_id": course_id,
                    "course_audience": audience,
                    "started_count": counts["Started"],
                    "completed_count": counts["Completed"],
                    "pending_count": counts["Pending"],
                    "total_enrolled": total,
                    "completed_rate": round((counts["Completed"] / total) * 100, 2) if total else 0,
                    "engaged_rate": round(((counts["Started"] + counts["Completed"]) / total) * 100, 2) if total else 0,
                }
            )
        )
    return rows


def _campaign_filter_rows(route_campaign_key: str, campaign_label: str, doctor_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    states = sorted({clean_text(row.get("state")) or "" for row in doctor_rows})
    reps = sorted(
        {
            (
                clean_text(row.get("field_rep_id")) or "Unassigned",
                clean_text(row.get("field_rep_name")) or clean_text(row.get("field_rep_id")) or "Unassigned",
            )
            for row in doctor_rows
        },
        key=lambda item: (item[1].lower(), item[0].lower()),
    )
    doctors = sorted(doctor_rows, key=lambda row: (row.get("doctor_display_name") or "", row.get("doctor_key") or ""))
    cities = sorted({clean_text(row.get("city")) or "" for row in doctor_rows})
    return {
        "dim_filter_campaign": [
            _stringify_row(
                {
                    "display_label": campaign_label or route_campaign_key,
                    "underlying_key": route_campaign_key,
                    "sort_order": 1,
                    "active_flag": "true",
                    "unknown_flag": "true" if route_campaign_key == "unknown" else "false",
                }
            )
        ],
        "dim_filter_state": [
            _stringify_row({"display_label": value or "Unknown", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"})
            for index, value in enumerate(states, start=1)
        ],
        "dim_filter_field_rep": [
            _stringify_row({"display_label": (label if label == value else f"{label} ({value})") or "Unassigned", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"})
            for index, (value, label) in enumerate(reps, start=1)
        ],
        "dim_filter_doctor": [
            _stringify_row({"display_label": row.get("doctor_display_name"), "underlying_key": row.get("doctor_key"), "sort_order": index, "active_flag": "true", "unknown_flag": "false"})
            for index, row in enumerate(doctors, start=1)
        ],
        "dim_filter_city": [
            _stringify_row({"display_label": value or "Unknown", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"})
            for index, value in enumerate(cities, start=1)
        ],
    }


def _publish_campaign_schemas(
    *,
    table_names: list[str],
    run_id: str,
    as_of_date: date,
    published_at: str,
    source_status: str,
    stale_source_flags: str,
    notes: str,
) -> list[str]:
    all_rows = {table: fetch_table(GOLD_SCHEMA, table) for table in table_names}
    all_columns = {table: _table_columns(GOLD_SCHEMA, table) for table in table_names}
    campaigns: dict[str, dict[str, str]] = {}
    for row in all_rows.get("dim_filter_campaign", []):
        source_key = clean_text(row.get("underlying_key") or row.get("campaign_key"))
        route_key = source_key or "unknown"
        campaigns[route_key] = {
            "source_key": source_key,
            "label": clean_text(row.get("display_label") or row.get("campaign_label")) or route_key,
        }
    for table_rows in all_rows.values():
        for row in table_rows:
            if "campaign_key" not in row:
                continue
            source_key = clean_text(row.get("campaign_key"))
            route_key = source_key or "unknown"
            campaigns.setdefault(
                route_key,
                {
                    "source_key": source_key,
                    "label": clean_text(row.get("campaign_label")) or route_key,
                },
            )

    target_schemas = {campaign_schema_name(route_key) for route_key in campaigns}
    ensure_schema(GOLD_GLOBAL_SCHEMA)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'gold_sapa_campaign_%'
            """
        )
        for (schema_name,) in cursor.fetchall():
            if schema_name not in target_schemas:
                cursor.execute(f"DROP SCHEMA IF EXISTS {qident(schema_name)} CASCADE")

    registry_rows: list[dict[str, str]] = []
    refresh_rows = [
        _stringify_row(
            {
                "publish_id": run_id,
                "as_of_date": as_of_date.isoformat(),
                "published_at": published_at,
                "source_completeness_status": source_status,
                "stale_source_flags": stale_source_flags,
                "notes": notes,
            }
        )
    ]
    campaign_schemas: list[str] = []

    for route_key, campaign in sorted(campaigns.items(), key=lambda item: (item[1]["label"].lower(), item[0].lower())):
        source_key = campaign["source_key"]
        label = campaign["label"] or route_key
        schema = campaign_schema_name(route_key)
        campaign_schemas.append(schema)

        campaign_tables: dict[str, list[dict[str, str]]] = {}
        for table in table_names:
            rows = all_rows.get(table, [])
            columns = all_columns.get(table, [])
            if "campaign_key" in columns:
                campaign_tables[table] = _copy_campaign_rows(rows, source_key, route_key)
            else:
                campaign_tables[table] = []

        doctor_rows = campaign_tables.get("rpt_doctor_status_current", [])
        doctor_history_rows = campaign_tables.get("rpt_doctor_status_history", [])
        screening_rows = campaign_tables.get("rpt_screening_detail", [])
        followup_rows = campaign_tables.get("rpt_followup_schedule_detail", [])
        reminder_rows = campaign_tables.get("rpt_reminder_sent_detail", [])
        webinar_rows = campaign_tables.get("rpt_webinar_registration_detail", [])
        course_rows = campaign_tables.get("rpt_course_detail", [])
        video_rows = campaign_tables.get("rpt_video_view_detail", [])
        redflag_rows = campaign_tables.get("rpt_submission_redflag_detail", [])

        campaign_tables["refresh_status"] = refresh_rows
        campaign_tables["dashboard_summary_snapshot"] = [
            _summary_snapshot_row(
                as_of_date=as_of_date,
                published_at=published_at,
                doctor_rows=doctor_rows,
                doctor_history_rows=doctor_history_rows,
                screening_rows=screening_rows,
                followup_rows=followup_rows,
                reminder_rows=reminder_rows,
                webinar_rows=webinar_rows,
                course_rows=course_rows,
            )
        ]
        campaign_tables["dashboard_summary_state_rep"] = _copy_campaign_rows(all_rows.get("dashboard_summary_state_rep", []), source_key, route_key)
        campaign_tables["rpt_course_summary"] = _course_summary_rows(as_of_date, course_rows)
        campaign_tables["rpt_video_rankings"] = [_stringify_row(row) for row in build_video_rankings(video_rows)]
        campaign_tables["rpt_red_flag_rankings"] = [_stringify_row(row) for row in build_red_flag_rankings(redflag_rows)]
        campaign_tables["rpt_condition_rankings"] = []
        campaign_tables.update(_campaign_filter_rows(route_key, label, doctor_rows))

        for table in table_names:
            rows = campaign_tables.get(table, [])
            columns = all_columns.get(table, [])
            if rows:
                columns = list(rows[0].keys())
                replace_table(schema, table, columns, rows)
            else:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(table)}")

        first_seen = min((clean_text(row.get("first_seen_at")) for row in doctor_rows if clean_text(row.get("first_seen_at"))), default=published_at)
        last_seen = max((clean_text(row.get("latest_seen_at")) for row in doctor_rows if clean_text(row.get("latest_seen_at"))), default=published_at)
        registry_rows.append(
            _stringify_row(
                {
                    "campaign_key": route_key,
                    "campaign_id_normalized": _campaign_token(route_key),
                    "campaign_label": label,
                    "gold_schema_name": schema,
                    "first_seen_ts": first_seen,
                    "last_seen_ts": last_seen,
                    "_created_at": published_at,
                    "_updated_at": published_at,
                }
            )
        )

    replace_table(
        GOLD_GLOBAL_SCHEMA,
        "campaign_registry",
        ["campaign_key", "campaign_id_normalized", "campaign_label", "gold_schema_name", "first_seen_ts", "last_seen_ts", "_created_at", "_updated_at"],
        registry_rows,
    )
    replace_table(
        GOLD_GLOBAL_SCHEMA,
        "refresh_status",
        ["publish_id", "as_of_date", "published_at", "source_completeness_status", "stale_source_flags", "notes"],
        refresh_rows,
    )
    return campaign_schemas


def _doctor_campaign_map(doctor_rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for row in doctor_rows:
        doctor_key = clean_text(row.get("doctor_key"))
        if not doctor_key:
            continue
        mapping[doctor_key] = {
            "campaign_key": clean_text(row.get("campaign_key")),
            "campaign_label": clean_text(row.get("campaign_label")),
        }
    return mapping


def _with_campaign_fields(row: dict[str, Any], doctor_campaigns: dict[str, dict[str, str]]) -> dict[str, Any]:
    item = dict(row)
    doctor_key = clean_text(item.get("doctor_key"))
    campaign = doctor_campaigns.get(doctor_key, {})
    item["campaign_key"] = clean_text(item.get("campaign_key")) or campaign.get("campaign_key") or ""
    item["campaign_label"] = clean_text(item.get("campaign_label")) or campaign.get("campaign_label") or ""
    return item


def _enriched_video_rows(video_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in video_rows:
        item = dict(row)
        metadata = resolve_video_metadata(row.get("video_url") or row.get("content_identifier"))
        resolved_url = metadata["video_url"] or clean_text(row.get("video_url")) or clean_text(row.get("content_identifier"))
        item["content_identifier"] = resolved_url or clean_text(row.get("content_identifier"))
        item["video_url"] = resolved_url or clean_text(row.get("video_url"))
        item["video_title"] = metadata["video_title"] or clean_text(row.get("video_title"))
        item["preferred_display_label"] = (
            metadata["preferred_display_label"]
            or clean_text(row.get("preferred_display_label"))
            or item["video_title"]
            or item["video_url"]
            or item["content_identifier"]
        )
        enriched.append(item)
    return enriched


def _publish_stage_tables(table_names: list[str]) -> None:
    ensure_schema(GOLD_SCHEMA)
    ensure_schema(GOLD_STAGE_SCHEMA)
    with transaction.atomic():
        with connection.cursor() as cursor:
            for table in table_names:
                cursor.execute(f"DROP TABLE IF EXISTS {qident(GOLD_SCHEMA)}.{qident(table)}")
            for table in table_names:
                cursor.execute(f"ALTER TABLE {qident(GOLD_STAGE_SCHEMA)}.{qident(table)} SET SCHEMA {qident(GOLD_SCHEMA)}")


def build_gold(run_id: str, source_status: str = "SUCCESS", stale_source_flags: str = "", notes: str = "") -> dict[str, Any]:
    as_of_date = date.today()
    published_at = _now_iso()

    doctor_rows = fetch_table(SILVER_SCHEMA, "dim_doctor_clinic")
    doctor_status_history_rows = fetch_table(SILVER_SCHEMA, "fact_doctor_status_daily")
    doctor_status_current_rows = [row for row in doctor_status_history_rows if row.get("as_of_date") == as_of_date.isoformat()]
    certification_rows = fetch_table(SILVER_SCHEMA, "certification_status_prepared")
    screening_rows = fetch_table(SILVER_SCHEMA, "fact_screening_submission")
    redflag_rows = fetch_table(SILVER_SCHEMA, "fact_submission_redflag")
    followup_rows = fetch_table(SILVER_SCHEMA, "fact_followup_schedule_instance")
    reminder_rows = fetch_table(SILVER_SCHEMA, "fact_reminder_sent")
    webinar_rows = fetch_table(SILVER_SCHEMA, "fact_webinar_registration")
    course_rows = fetch_table(SILVER_SCHEMA, "fact_course_user_progress")
    video_rows = _enriched_video_rows(fetch_table(SILVER_SCHEMA, "fact_video_view"))
    doctor_campaigns = _doctor_campaign_map(doctor_rows)

    certification_by_doctor = {row.get("doctor_key"): row for row in certification_rows}
    redflag_count_by_submission: dict[str, int] = {}
    for row in redflag_rows:
        submission_key = row.get("submission_key") or ""
        redflag_count_by_submission[submission_key] = redflag_count_by_submission.get(submission_key, 0) + 1

    refresh_rows = [
        _stringify_row(
            {
                "publish_id": run_id,
                "as_of_date": as_of_date.isoformat(),
                "published_at": published_at,
                "source_completeness_status": source_status,
                "stale_source_flags": stale_source_flags,
                "notes": notes,
            }
        )
    ]
    replace_table(
        GOLD_STAGE_SCHEMA,
        "refresh_status",
        ["publish_id", "as_of_date", "published_at", "source_completeness_status", "stale_source_flags", "notes"],
        refresh_rows,
    )

    summary = compute_dashboard_metrics(
        as_of_date=as_of_date,
        doctor_rows=doctor_rows,
        doctor_status_current_rows=doctor_status_current_rows,
        doctor_status_history_rows=doctor_status_history_rows,
        certification_rows=certification_rows,
        webinar_rows=webinar_rows,
        screening_rows=screening_rows,
        followup_rows=followup_rows,
        reminder_rows=reminder_rows,
        course_rows=course_rows,
    )
    summary["published_at"] = published_at
    summary["unsupported_condition_rankings"] = "true"
    summary_row = _stringify_row(summary)
    replace_table(GOLD_STAGE_SCHEMA, "dashboard_summary_snapshot", list(summary_row.keys()), [summary_row])

    summary_state_rep_rows = []
    combos = sorted(
        {
            (
                clean_text(row.get("campaign_key")) or "",
                clean_text(row.get("campaign_label")) or "",
                clean_text(row.get("state")) or "",
                clean_text(row.get("field_rep_id")) or "Unassigned",
            )
            for row in doctor_rows
        }
    )
    for campaign_key, campaign_label, state, field_rep_id in combos:
        filters = {"campaign_key": campaign_key or None, "state": state or None, "field_rep_id": field_rep_id or None}
        metrics = compute_dashboard_metrics(
            as_of_date=as_of_date,
            doctor_rows=filter_rows(doctor_rows, filters),
            doctor_status_current_rows=filter_rows(doctor_status_current_rows, filters),
            doctor_status_history_rows=filter_rows(doctor_status_history_rows, filters),
            certification_rows=filter_rows(certification_rows, filters),
            webinar_rows=filter_rows(webinar_rows, filters),
            screening_rows=filter_rows(screening_rows, filters),
            followup_rows=filter_rows(followup_rows, filters),
            reminder_rows=filter_rows(reminder_rows, filters),
            course_rows=filter_rows(course_rows, filters),
        )
        metrics["campaign_key"] = campaign_key
        metrics["campaign_label"] = campaign_label
        metrics["state"] = state
        metrics["field_rep_id"] = field_rep_id
        metrics["published_at"] = published_at
        summary_state_rep_rows.append(_stringify_row(metrics))
    summary_state_rep_columns = list(summary_state_rep_rows[0].keys()) if summary_state_rep_rows else ["campaign_key", "campaign_label", "state", "field_rep_id", "as_of_date", "published_at"]
    replace_table(GOLD_STAGE_SCHEMA, "dashboard_summary_state_rep", summary_state_rep_columns, summary_state_rep_rows)

    current_status_rows = []
    for doctor in doctor_rows:
        status_row = next((row for row in doctor_status_current_rows if row.get("doctor_key") == doctor.get("doctor_key")), None)
        certification = certification_by_doctor.get(doctor.get("doctor_key"), {})
        current_status_rows.append(
            _stringify_row(
                {
                    "doctor_key": doctor.get("doctor_key"),
                    "campaign_key": doctor.get("campaign_key"),
                    "campaign_label": doctor.get("campaign_label"),
                    "doctor_display_name": doctor.get("canonical_display_name"),
                    "city": doctor.get("city"),
                    "district": doctor.get("district"),
                    "state": doctor.get("state"),
                    "field_rep_id": doctor.get("field_rep_id"),
                    "field_rep_name": doctor.get("field_rep_name"),
                    "screenings_last_15d": (status_row or {}).get("screenings_last_15d", "0"),
                    "active_flag": (status_row or {}).get("is_active", "false"),
                    "inactive_flag": (status_row or {}).get("is_inactive", "false"),
                    "last_screening_at": (status_row or {}).get("last_screening_at", ""),
                    "onboarding_flag": doctor.get("is_user_created_doctor"),
                    "first_seen_at": doctor.get("first_seen_at", ""),
                    "latest_seen_at": doctor.get("latest_seen_at", ""),
                    "certification_status": certification.get("certification_status", ""),
                    "certification_date": certification.get("certification_date", ""),
                    "certification_source": certification.get("certification_source", ""),
                }
            )
        )
    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_doctor_status_current",
        [
            "doctor_key",
            "campaign_key",
            "campaign_label",
            "doctor_display_name",
            "city",
            "district",
            "state",
            "field_rep_id",
            "field_rep_name",
            "screenings_last_15d",
            "active_flag",
            "inactive_flag",
            "last_screening_at",
            "onboarding_flag",
            "first_seen_at",
            "latest_seen_at",
            "certification_status",
            "certification_date",
            "certification_source",
        ],
        current_status_rows,
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_doctor_status_history",
        list(doctor_status_history_rows[0].keys()) if doctor_status_history_rows else [
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
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in doctor_status_history_rows],
    )

    screening_detail_rows = [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in screening_rows]
    screening_detail_columns = list(screening_detail_rows[0].keys()) if screening_detail_rows else [
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
    ]
    replace_table(GOLD_STAGE_SCHEMA, "rpt_screening_detail", screening_detail_columns, screening_detail_rows)

    tag_detail_rows = []
    for row in screening_rows:
        overall_flag = clean_text(row.get("overall_flag_code"))
        if overall_flag not in {"red", "yellow"}:
            continue
        tag_row = _with_campaign_fields(row, doctor_campaigns)
        tag_row["tag_color"] = overall_flag
        tag_row["individual_red_flag_count"] = str(redflag_count_by_submission.get(row.get("submission_key") or "", 0))
        tag_detail_rows.append(_stringify_row(tag_row))
    tag_detail_columns = (
        list(tag_detail_rows[0].keys())
        if tag_detail_rows
        else (list(screening_rows[0].keys()) + ["tag_color", "individual_red_flag_count"] if screening_rows else ["tag_color", "individual_red_flag_count"])
    )
    replace_table(GOLD_STAGE_SCHEMA, "rpt_tag_detail", tag_detail_columns, tag_detail_rows)

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_followup_schedule_detail",
        list(followup_rows[0].keys()) if followup_rows else [
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
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in followup_rows],
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_reminder_sent_detail",
        list(reminder_rows[0].keys()) if reminder_rows else ["metric_event_id", "source_metric_event_id", "doctor_key", "campaign_key", "campaign_label", "patient_id", "ts", "action_key", "doctor_display_name", "city", "district", "state", "field_rep_id", "field_rep_name"],
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in reminder_rows],
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_webinar_registration_detail",
        list(webinar_rows[0].keys()) if webinar_rows else ["registration_key", "source_registration_key", "event_id", "event_title", "start_date", "end_date", "timezone", "email", "first_name", "last_name", "phone", "registration_effective_date", "doctor_key", "campaign_key", "campaign_label", "doctor_display_name", "state", "city", "field_rep_id", "field_rep_name", "match_method", "unmapped_flag"],
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in webinar_rows],
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_course_detail",
        list(course_rows[0].keys()) if course_rows else ["extract_snapshot_date", "course_id", "course_audience", "user_id", "display_name", "user_email", "first_name", "last_name", "phone", "progress_status", "enrolled_at", "started_at", "completed_at", "dashboard_status", "doctor_key", "campaign_key", "campaign_label", "doctor_display_name", "state", "city", "field_rep_id", "field_rep_name", "match_method", "unmapped_flag"],
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in course_rows],
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_submission_redflag_detail",
        list(redflag_rows[0].keys()) if redflag_rows else ["source_row_id", "submission_key", "source_submission_id", "doctor_key", "campaign_key", "campaign_label", "red_flag", "red_flag_name", "patient_video_url", "doctor_video_url", "submitted_at", "doctor_display_name", "city", "district", "state", "field_rep_id", "field_rep_name"],
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in redflag_rows],
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_video_view_detail",
        list(video_rows[0].keys()) if video_rows else ["metric_event_id", "source_metric_event_id", "doctor_key", "campaign_key", "campaign_label", "patient_id", "audience", "content_identifier", "video_url", "video_title", "preferred_display_label", "action_key", "event_type", "ts", "doctor_display_name", "city", "district", "state", "field_rep_id", "field_rep_name"],
        [_stringify_row(_with_campaign_fields(row, doctor_campaigns)) for row in video_rows],
    )

    course_summary_counts = course_status_counts(course_rows)
    course_summary_rows = []
    for audience, counts in course_summary_counts.items():
        total = counts["Total"] or 0
        course_id = next((row.get("course_id") for row in course_rows if row.get("course_audience") == audience), "")
        course_summary_rows.append(
            _stringify_row(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "course_id": course_id,
                    "course_audience": audience,
                    "started_count": counts["Started"],
                    "completed_count": counts["Completed"],
                    "pending_count": counts["Pending"],
                    "total_enrolled": total,
                    "completed_rate": round((counts["Completed"] / total) * 100, 2) if total else 0,
                    "engaged_rate": round(((counts["Started"] + counts["Completed"]) / total) * 100, 2) if total else 0,
                }
            )
        )
    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_course_summary",
        ["as_of_date", "course_id", "course_audience", "started_count", "completed_count", "pending_count", "total_enrolled", "completed_rate", "engaged_rate"],
        course_summary_rows,
    )

    video_ranking_rows = [_stringify_row(row) for row in build_video_rankings(video_rows)]
    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_video_rankings",
        ["audience", "content_identifier", "preferred_display_label", "view_count", "latest_interaction_timestamp", "rank"],
        video_ranking_rows,
    )

    red_flag_ranking_rows = [_stringify_row(row) for row in build_red_flag_rankings(redflag_rows)]
    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_red_flag_rankings",
        ["red_flag", "occurrence_count", "latest_seen_at", "rank"],
        red_flag_ranking_rows,
    )

    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_condition_rankings",
        ["condition_name", "occurrence_count", "latest_seen_at", "rank"],
        [],
    )

    certified_rows = []
    for doctor in doctor_rows:
        certification = certification_by_doctor.get(doctor.get("doctor_key"), {})
        status_row = next((row for row in doctor_status_current_rows if row.get("doctor_key") == doctor.get("doctor_key")), None)
        if certification.get("support_flag") != "true" or not status_row or status_row.get("is_active") != "true":
            continue
        certified_rows.append(
            _stringify_row(
                {
                    "serial_order": len(certified_rows) + 1,
                    "doctor_key": doctor.get("doctor_key"),
                    "campaign_key": doctor.get("campaign_key"),
                    "campaign_label": doctor.get("campaign_label"),
                    "doctor_display_name": doctor.get("canonical_display_name"),
                    "city": doctor.get("city"),
                    "district": doctor.get("district"),
                    "state": doctor.get("state"),
                    "field_rep_id": doctor.get("field_rep_id"),
                    "field_rep_name": doctor.get("field_rep_name"),
                    "certification_status": certification.get("certification_status", ""),
                    "certification_date": certification.get("certification_date", ""),
                    "certification_source": certification.get("certification_source", ""),
                }
            )
        )
    replace_table(
        GOLD_STAGE_SCHEMA,
        "rpt_certified_clinics",
        ["serial_order", "doctor_key", "campaign_key", "campaign_label", "doctor_display_name", "city", "district", "state", "field_rep_id", "field_rep_name", "certification_status", "certification_date", "certification_source"],
        certified_rows,
    )

    filter_state_rows = []
    filter_campaign_rows = []
    filter_field_rep_rows = []
    filter_doctor_rows = []
    filter_city_rows = []
    unique_campaigns = sorted(
        {
            (
                clean_text(row.get("campaign_key")) or "",
                clean_text(row.get("campaign_label")) or clean_text(row.get("campaign_key")) or "",
            )
            for row in doctor_rows
        },
        key=lambda item: (item[1], item[0]),
    )
    unique_states = sorted({clean_text(row.get("state")) or "" for row in doctor_rows})
    unique_reps = sorted(
        {
            (
                clean_text(row.get("field_rep_id")) or "Unassigned",
                clean_text(row.get("field_rep_name")) or clean_text(row.get("field_rep_id")) or "Unassigned",
            )
            for row in doctor_rows
        },
        key=lambda item: (item[1].lower(), item[0].lower()),
    )
    unique_doctors = sorted(doctor_rows, key=lambda row: (row.get("canonical_display_name") or "", row.get("doctor_key") or ""))
    unique_cities = sorted({clean_text(row.get("city")) or "" for row in doctor_rows})

    for index, (campaign_key, campaign_label) in enumerate(unique_campaigns, start=1):
        filter_campaign_rows.append(_stringify_row({"display_label": campaign_label or campaign_key or "Unknown", "underlying_key": campaign_key, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not campaign_key else "false"}))
    for index, value in enumerate(unique_states, start=1):
        filter_state_rows.append(_stringify_row({"display_label": value or "Unknown", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"}))
    for index, (value, label) in enumerate(unique_reps, start=1):
        display_label = label if label == value else f"{label} ({value})"
        filter_field_rep_rows.append(_stringify_row({"display_label": display_label or "Unassigned", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"}))
    for index, row in enumerate(unique_doctors, start=1):
        filter_doctor_rows.append(_stringify_row({"display_label": row.get("canonical_display_name"), "underlying_key": row.get("doctor_key"), "sort_order": index, "active_flag": "true", "unknown_flag": "false"}))
    for index, value in enumerate(unique_cities, start=1):
        filter_city_rows.append(_stringify_row({"display_label": value or "Unknown", "underlying_key": value, "sort_order": index, "active_flag": "true", "unknown_flag": "true" if not value else "false"}))

    replace_table(GOLD_STAGE_SCHEMA, "dim_filter_campaign", ["display_label", "underlying_key", "sort_order", "active_flag", "unknown_flag"], filter_campaign_rows)
    replace_table(GOLD_STAGE_SCHEMA, "dim_filter_state", ["display_label", "underlying_key", "sort_order", "active_flag", "unknown_flag"], filter_state_rows)
    replace_table(GOLD_STAGE_SCHEMA, "dim_filter_field_rep", ["display_label", "underlying_key", "sort_order", "active_flag", "unknown_flag"], filter_field_rep_rows)
    replace_table(GOLD_STAGE_SCHEMA, "dim_filter_doctor", ["display_label", "underlying_key", "sort_order", "active_flag", "unknown_flag"], filter_doctor_rows)
    replace_table(GOLD_STAGE_SCHEMA, "dim_filter_city", ["display_label", "underlying_key", "sort_order", "active_flag", "unknown_flag"], filter_city_rows)

    table_names = [
        "refresh_status",
        "dashboard_summary_snapshot",
        "dashboard_summary_state_rep",
        "rpt_doctor_status_current",
        "rpt_doctor_status_history",
        "rpt_screening_detail",
        "rpt_tag_detail",
        "rpt_followup_schedule_detail",
        "rpt_reminder_sent_detail",
        "rpt_webinar_registration_detail",
        "rpt_course_detail",
        "rpt_submission_redflag_detail",
        "rpt_video_view_detail",
        "rpt_course_summary",
        "rpt_video_rankings",
        "rpt_red_flag_rankings",
        "rpt_condition_rankings",
        "rpt_certified_clinics",
        "dim_filter_campaign",
        "dim_filter_state",
        "dim_filter_field_rep",
        "dim_filter_doctor",
        "dim_filter_city",
    ]
    _publish_stage_tables(table_names)
    campaign_schemas = _publish_campaign_schemas(
        table_names=table_names,
        run_id=run_id,
        as_of_date=as_of_date,
        published_at=published_at,
        source_status=source_status,
        stale_source_flags=stale_source_flags,
        notes=notes,
    )
    return {"as_of_date": as_of_date.isoformat(), "published_at": published_at, "tables": table_names, "campaign_schemas": campaign_schemas}
