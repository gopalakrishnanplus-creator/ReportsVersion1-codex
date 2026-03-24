from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from django.conf import settings

from etl.integrations.sapa_growth.learndash import LearnDashClient, LearnDashIntegrationError
from etl.sapa_growth.control import get_watermark, log_step, upsert_watermark
from etl.sapa_growth.mysql import SapaMySQLExtractionError, extract_rows
from etl.sapa_growth.specs import API_TABLE_SPECS, MYSQL_TABLE_SPECS, RAW_API_SCHEMA, RAW_AUDIT_COLUMNS, RAW_MYSQL_SCHEMA
from etl.sapa_growth.storage import append_rows, ensure_text_table, fetch_all, qident, table_exists
from sapa_growth.logic import clean_text, hash_fields, normalize_phone, parse_datetime


def _audit_payload(run_id: str, source_system: str, source_table: str, extracted_at: str, values: list[Any]) -> dict[str, Any]:
    return {
        "_ingestion_run_id": run_id,
        "_ingested_at": extracted_at,
        "_source_system": source_system,
        "_source_table": source_table,
        "_extract_started_at": extracted_at,
        "_extract_ended_at": extracted_at,
        "_record_hash": hash_fields(source_table, *values),
        "_dq_status": "PASS",
        "_dq_errors": "",
    }


def _watermark_start(source_name: str, entity_name: str, watermark_field: str | None, lookback_days: int) -> str | None:
    if watermark_field is None:
        return None
    stored = get_watermark(source_name, entity_name)
    if not stored:
        return None
    value = clean_text(stored.get("last_successful_watermark_value"))
    parsed = parse_datetime(value)
    if parsed is None:
        return value
    return (parsed - timedelta(days=lookback_days)).isoformat(sep=" ")


def ensure_raw_tables() -> None:
    for spec in MYSQL_TABLE_SPECS.values():
        ensure_text_table(RAW_MYSQL_SCHEMA, spec.raw_table, spec.columns + RAW_AUDIT_COLUMNS)
    for spec in API_TABLE_SPECS.values():
        ensure_text_table(RAW_API_SCHEMA, spec["raw_table"], spec["columns"] + RAW_AUDIT_COLUMNS)


def ingest_mysql_sources(run_id: str, extracted_at: str) -> dict[str, Any]:
    ensure_raw_tables()
    counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    max_watermarks: dict[str, str] = {}

    for name, spec in MYSQL_TABLE_SPECS.items():
        watermark_start = _watermark_start("mysql", name, spec.watermark_field, spec.lookback_days)
        try:
            rows = extract_rows(
                spec.source_table,
                spec.columns,
                watermark_field=spec.watermark_field,
                watermark_start=watermark_start,
            )
            prepared_rows: list[dict[str, Any]] = []
            max_watermark_value: str | None = None
            for row in rows:
                values = [row.get(column) for column in spec.columns]
                payload = {column: row.get(column) for column in spec.columns}
                payload.update(_audit_payload(run_id, "sapa_mysql", name, extracted_at, values))
                prepared_rows.append(payload)
                if spec.watermark_field:
                    candidate = clean_text(row.get(spec.watermark_field))
                    if candidate and (max_watermark_value is None or candidate > max_watermark_value):
                        max_watermark_value = candidate

            append_rows(RAW_MYSQL_SCHEMA, spec.raw_table, spec.columns + RAW_AUDIT_COLUMNS, prepared_rows)
            counts[name] = len(prepared_rows)
            if max_watermark_value:
                max_watermarks[name] = max_watermark_value
            current_watermark = get_watermark("mysql", name)
            previous_value = clean_text((current_watermark or {}).get("last_successful_watermark_value"))
            upsert_watermark(
                "mysql",
                name,
                spec.watermark_field,
                max_watermark_value or previous_value,
                spec.lookback_days,
                "incremental" if spec.watermark_field else "full_snapshot",
                run_id,
            )
            log_step(run_id, "extract_mysql", name, "SUCCESS", rows_read=len(rows), rows_written=len(prepared_rows))
        except SapaMySQLExtractionError as exc:
            counts[name] = 0
            errors[name] = str(exc)
            log_step(run_id, "extract_mysql", name, "FAIL", error_message=str(exc))

    return {"counts": counts, "errors": errors, "max_watermarks": max_watermarks}


def _latest_previous_row_count(schema: str, table: str, current_run_id: str) -> int | None:
    if not table_exists(schema, table):
        return None
    rows = fetch_all(
        f"""
        SELECT _ingestion_run_id, COUNT(*)::text AS row_count
        FROM {qident(schema)}.{qident(table)}
        WHERE _ingestion_run_id <> %s
        GROUP BY _ingestion_run_id
        ORDER BY MAX(_ingested_at) DESC
        LIMIT 1
        """,
        [current_run_id],
    )
    if not rows:
        return None
    try:
        return int(rows[0]["row_count"])
    except (KeyError, TypeError, ValueError):
        return None


def _stale_payload(current_count: int, previous_count: int | None) -> bool:
    if previous_count in (None, 0):
        return False
    min_ratio = float(settings.SAPA_WORDPRESS["STALE_MIN_RATIO"])
    return current_count == 0 or current_count < (previous_count * min_ratio)


def ingest_api_sources(run_id: str, extracted_at: str) -> dict[str, Any]:
    ensure_raw_tables()
    client = LearnDashClient()
    counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    stale_sources: list[str] = []

    try:
        webinar_rows = client.get_webinar_registrations()
        previous_count = _latest_previous_row_count(RAW_API_SCHEMA, API_TABLE_SPECS["wp_webinar_registrations"]["raw_table"], run_id)
        if _stale_payload(len(webinar_rows), previous_count):
            raise LearnDashIntegrationError("webinar registrations payload looks stale compared with previous successful snapshot")
        prepared_webinars = []
        for row in webinar_rows:
            registration_id = clean_text(row.get("registration_id")) or clean_text(row.get("id"))
            prepared = {
                "registration_id": registration_id,
                "event_id": clean_text(row.get("event_id")),
                "event_title": clean_text(row.get("event_title")),
                "start_date": clean_text(row.get("start_date")),
                "end_date": clean_text(row.get("end_date")),
                "timezone": clean_text(row.get("timezone")),
                "email": clean_text(row.get("email")),
                "first_name": clean_text(row.get("first_name")),
                "last_name": clean_text(row.get("last_name")),
                "phone": normalize_phone(row.get("phone") or row.get("mobile") or row.get("whatsapp")),
                "registration_created_at": clean_text(row.get("registration_created_at") or row.get("created_at")),
                "payload_json": json.dumps(row, default=str),
            }
            prepared.update(_audit_payload(run_id, "sapa_api", "webinar_registrations", extracted_at, list(prepared.values())))
            prepared_webinars.append(prepared)
        append_rows(
            RAW_API_SCHEMA,
            API_TABLE_SPECS["wp_webinar_registrations"]["raw_table"],
            API_TABLE_SPECS["wp_webinar_registrations"]["columns"] + RAW_AUDIT_COLUMNS,
            prepared_webinars,
        )
        counts["webinar_registrations"] = len(prepared_webinars)
        log_step(run_id, "extract_api", "webinar_registrations", "SUCCESS", rows_read=len(webinar_rows), rows_written=len(prepared_webinars))
    except LearnDashIntegrationError as exc:
        counts["webinar_registrations"] = 0
        errors["webinar_registrations"] = str(exc)
        stale_sources.append("webinar_registrations")
        log_step(run_id, "extract_api", "webinar_registrations", "FAIL", error_message=str(exc))

    course_specs = [
        ("doctor", int(settings.SAPA_WORDPRESS["DOCTOR_COURSE_ID"])),
        ("paramedic", int(settings.SAPA_WORDPRESS["PARAMEDIC_COURSE_ID"])),
    ]
    summary_rows: list[dict[str, Any]] = []
    breakdown_rows: list[dict[str, Any]] = []

    for audience, course_id in course_specs:
        try:
            summary = client.get_course_summary(course_id)
            summary_row = {
                "course_id": str(course_id),
                "course_audience": audience,
                "total_enrolled": str(summary.get("total_enrolled", "")),
                "completed": str(summary.get("completed", "")),
                "in_progress": str(summary.get("in_progress", "")),
                "not_started": str(summary.get("not_started", "")),
                "payload_json": json.dumps(summary, default=str),
            }
            summary_row.update(_audit_payload(run_id, "sapa_api", f"course_summary_{audience}", extracted_at, list(summary_row.values())))
            summary_rows.append(summary_row)

            breakdown = client.get_course_breakdown(course_id)
            previous_count = _latest_previous_row_count(RAW_API_SCHEMA, API_TABLE_SPECS["wp_course_breakdown"]["raw_table"], run_id)
            if _stale_payload(len(breakdown), previous_count):
                raise LearnDashIntegrationError(f"{audience} course breakdown payload looks stale compared with previous successful snapshot")
            for row in breakdown:
                prepared = {
                    "course_id": str(course_id),
                    "course_audience": audience,
                    "user_id": clean_text(row.get("user_id")),
                    "display_name": clean_text(row.get("display_name")),
                    "user_email": clean_text(row.get("user_email")),
                    "first_name": clean_text(row.get("first_name")),
                    "last_name": clean_text(row.get("last_name")),
                    "progress_status": clean_text(row.get("progress_status")),
                    "enrolled_at": clean_text(row.get("enrolled_at")),
                    "started_at": clean_text(row.get("started_at")),
                    "completed_at": clean_text(row.get("completed_at")),
                    "phone": normalize_phone(row.get("phone")),
                    "payload_json": json.dumps(row, default=str),
                }
                prepared.update(_audit_payload(run_id, "sapa_api", f"course_breakdown_{audience}", extracted_at, list(prepared.values())))
                breakdown_rows.append(prepared)

            counts[f"course_summary_{audience}"] = 1
            counts[f"course_breakdown_{audience}"] = len(breakdown)
            log_step(run_id, "extract_api", f"course_{audience}", "SUCCESS", rows_read=len(breakdown) + 1, rows_written=len(breakdown) + 1)
        except LearnDashIntegrationError as exc:
            errors[f"course_{audience}"] = str(exc)
            stale_sources.append(f"course_{audience}")
            log_step(run_id, "extract_api", f"course_{audience}", "FAIL", error_message=str(exc))

    append_rows(
        RAW_API_SCHEMA,
        API_TABLE_SPECS["wp_course_summary"]["raw_table"],
        API_TABLE_SPECS["wp_course_summary"]["columns"] + RAW_AUDIT_COLUMNS,
        summary_rows,
    )
    append_rows(
        RAW_API_SCHEMA,
        API_TABLE_SPECS["wp_course_breakdown"]["raw_table"],
        API_TABLE_SPECS["wp_course_breakdown"]["columns"] + RAW_AUDIT_COLUMNS,
        breakdown_rows,
    )

    return {"counts": counts, "errors": errors, "stale_sources": stale_sources}
