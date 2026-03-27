from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from django.conf import settings
from django.db import connection

from etl.pe_reports.storage import fetch_all


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_control_tables() -> None:
    default_thresholds = {
        "low_activation_pct": settings.PE_REPORTS["LOW_ACTIVATION_THRESHOLD"],
        "low_play_rate_pct": settings.PE_REPORTS["LOW_PLAY_RATE_THRESHOLD"],
        "low_engagement_50_pct": settings.PE_REPORTS["LOW_ENGAGEMENT_50_THRESHOLD"],
        "low_completion_pct": settings.PE_REPORTS["LOW_COMPLETION_THRESHOLD"],
        "multi_state_alert_count": settings.PE_REPORTS["MULTI_STATE_ALERT_COUNT"],
    }

    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS control")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS ops")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_etl_run_log (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                ended_at TEXT,
                status TEXT,
                trigger_type TEXT,
                notes TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_etl_step_log (
                run_id TEXT,
                step_name TEXT,
                source_name TEXT,
                started_at TEXT,
                ended_at TEXT,
                rows_read TEXT,
                rows_written TEXT,
                rows_rejected TEXT,
                status TEXT,
                error_message TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_etl_watermark (
                source_name TEXT,
                entity_name TEXT,
                watermark_field_name TEXT,
                last_successful_watermark_value TEXT,
                lookback_window_days TEXT,
                extraction_strategy TEXT,
                last_successful_run_id TEXT,
                last_successful_started_at TEXT,
                last_successful_completed_at TEXT,
                enabled_flag TEXT,
                notes TEXT,
                PRIMARY KEY (source_name, entity_name)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_dq_issue_log (
                run_id TEXT,
                layer TEXT,
                table_name TEXT,
                issue_type TEXT,
                issue_count TEXT,
                issue_sample TEXT,
                created_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_refresh_registry (
                publish_id TEXT PRIMARY KEY,
                as_of_date TEXT,
                published_at TEXT,
                status TEXT,
                notes TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS control.pe_export_audit_log (
                exported_at TEXT,
                export_name TEXT,
                campaign_id TEXT,
                route_name TEXT,
                filters_json TEXT,
                row_count TEXT,
                session_key TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ops.pe_exclusion_rules (
                entity_name TEXT,
                field_name TEXT,
                match_value TEXT,
                reason TEXT,
                enabled_flag TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ops.pe_thresholds (
                threshold_key TEXT PRIMARY KEY,
                threshold_value TEXT,
                label TEXT,
                description TEXT,
                updated_at TEXT
            )
            """
        )
        for key, value in default_thresholds.items():
            cursor.execute(
                """
                INSERT INTO ops.pe_thresholds (threshold_key, threshold_value, label, description, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (threshold_key)
                DO NOTHING
                """,
                [key, str(value), key.replace("_", " ").title(), "", _utc_now()],
            )


@contextmanager
def pipeline_lock():
    ensure_control_tables()
    lock_key = int(settings.PE_REPORTS["PIPELINE_LOCK_KEY"])
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [lock_key])
        acquired = bool(cursor.fetchone()[0])
    if not acquired:
        raise RuntimeError("PE reports ETL is already running")
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [lock_key])


def log_run(run_id: str, status: str, trigger_type: str = "manual", notes: str = "") -> None:
    now = _utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_etl_run_log (run_id, started_at, ended_at, status, trigger_type, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id)
            DO UPDATE SET ended_at = EXCLUDED.ended_at, status = EXCLUDED.status, notes = EXCLUDED.notes
            """,
            [run_id, now, now, status, trigger_type, notes],
        )


def log_step(
    run_id: str,
    step_name: str,
    source_name: str,
    status: str,
    *,
    rows_read: int = 0,
    rows_written: int = 0,
    rows_rejected: int = 0,
    error_message: str = "",
) -> None:
    now = _utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_etl_step_log
            (run_id, step_name, source_name, started_at, ended_at, rows_read, rows_written, rows_rejected, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [run_id, step_name, source_name, now, now, str(rows_read), str(rows_written), str(rows_rejected), status, error_message],
        )


def log_dq_issue(run_id: str, layer: str, table_name: str, issue_type: str, issue_count: int, issue_sample: str = "") -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_dq_issue_log
            (run_id, layer, table_name, issue_type, issue_count, issue_sample, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [run_id, layer, table_name, issue_type, str(issue_count), issue_sample, _utc_now()],
        )


def get_watermark(source_name: str, entity_name: str) -> dict[str, Any] | None:
    rows = fetch_all(
        """
        SELECT *
        FROM control.pe_etl_watermark
        WHERE source_name = %s AND entity_name = %s
        """,
        [source_name, entity_name],
    )
    return rows[0] if rows else None


def upsert_watermark(
    source_name: str,
    entity_name: str,
    watermark_field_name: str | None,
    watermark_value: str | None,
    lookback_window_days: int,
    extraction_strategy: str,
    run_id: str,
    notes: str = "",
) -> None:
    now = _utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_etl_watermark
            (source_name, entity_name, watermark_field_name, last_successful_watermark_value, lookback_window_days,
             extraction_strategy, last_successful_run_id, last_successful_started_at, last_successful_completed_at, enabled_flag, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'true', %s)
            ON CONFLICT (source_name, entity_name)
            DO UPDATE SET
                watermark_field_name = EXCLUDED.watermark_field_name,
                last_successful_watermark_value = EXCLUDED.last_successful_watermark_value,
                lookback_window_days = EXCLUDED.lookback_window_days,
                extraction_strategy = EXCLUDED.extraction_strategy,
                last_successful_run_id = EXCLUDED.last_successful_run_id,
                last_successful_started_at = EXCLUDED.last_successful_started_at,
                last_successful_completed_at = EXCLUDED.last_successful_completed_at,
                enabled_flag = EXCLUDED.enabled_flag,
                notes = EXCLUDED.notes
            """,
            [source_name, entity_name, watermark_field_name, watermark_value, str(lookback_window_days), extraction_strategy, run_id, now, now, notes],
        )


def record_refresh(publish_id: str, as_of_date: str, status: str, notes: str = "") -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_refresh_registry (publish_id, as_of_date, published_at, status, notes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (publish_id)
            DO UPDATE SET
                as_of_date = EXCLUDED.as_of_date,
                published_at = EXCLUDED.published_at,
                status = EXCLUDED.status,
                notes = EXCLUDED.notes
            """,
            [publish_id, as_of_date, _utc_now(), status, notes],
        )


def log_export(export_name: str, campaign_id: str, route_name: str, filters_json: str, row_count: int, session_key: str | None) -> None:
    ensure_control_tables()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pe_export_audit_log
            (exported_at, export_name, campaign_id, route_name, filters_json, row_count, session_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [_utc_now(), export_name, campaign_id, route_name, filters_json, str(row_count), session_key or ""],
        )


def get_thresholds() -> dict[str, float]:
    ensure_control_tables()
    rows = fetch_all("SELECT threshold_key, threshold_value FROM ops.pe_thresholds")
    values: dict[str, float] = {}
    for row in rows:
        try:
            values[str(row["threshold_key"])] = float(row["threshold_value"])
        except (TypeError, ValueError):
            continue
    return values


def active_exclusion_rules() -> list[dict[str, Any]]:
    ensure_control_tables()
    return fetch_all(
        """
        SELECT entity_name, field_name, match_value, reason
        FROM ops.pe_exclusion_rules
        WHERE COALESCE(enabled_flag, 'true') <> 'false'
        """
    )
