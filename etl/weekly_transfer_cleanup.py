from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import pymysql
from django.conf import settings
from django.db import connection

from etl.connectors.postgres import execute, fetchall
from etl.inclinic_pipeline import run_pipeline as run_inclinic_pipeline
from etl.sapa_growth.pipeline import run_pipeline as run_sapa_pipeline

CLEANUP_LOCK_KEY = 991843


@dataclass(frozen=True)
class CleanupSpec:
    domain: str
    source_table: str
    raw_schema: str
    raw_table: str
    key_column: str
    mysql_settings_name: str
    guard_column: str | None = None


INCLINIC_SPECS: tuple[CleanupSpec, ...] = (
    CleanupSpec(
        domain="inclinic",
        source_table="sharing_management_collateraltransaction",
        raw_schema="raw_server2",
        raw_table="sharing_management_collateraltransaction",
        key_column="id",
        guard_column="updated_at",
        mysql_settings_name="MYSQL_SERVER_2",
    ),
)

RFA_SPECS: tuple[CleanupSpec, ...] = (
    CleanupSpec(
        domain="rfa",
        source_table="redflags_submissionredflag",
        raw_schema="raw_sapa_mysql",
        raw_table="redflags_submissionredflag_raw",
        key_column="id",
        mysql_settings_name="SAPA_MYSQL",
    ),
    CleanupSpec(
        domain="rfa",
        source_table="gnd_gndsubmissionredflag",
        raw_schema="raw_sapa_mysql",
        raw_table="gnd_gndsubmissionredflag_raw",
        key_column="id",
        mysql_settings_name="SAPA_MYSQL",
    ),
    CleanupSpec(
        domain="rfa",
        source_table="redflags_patientsubmission",
        raw_schema="raw_sapa_mysql",
        raw_table="redflags_patientsubmission_raw",
        key_column="record_id",
        mysql_settings_name="SAPA_MYSQL",
    ),
    CleanupSpec(
        domain="rfa",
        source_table="gnd_gndpatientsubmission",
        raw_schema="raw_sapa_mysql",
        raw_table="gnd_gndpatientsubmission_raw",
        key_column="id",
        mysql_settings_name="SAPA_MYSQL",
    ),
)

SPECS_BY_DOMAIN = {
    "inclinic": INCLINIC_SPECS,
    "rfa": RFA_SPECS,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_domains(domains: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    aliases = {
        "inclinic": "inclinic",
        "ic": "inclinic",
        "rfa": "rfa",
        "sapa": "rfa",
    }
    for domain in domains:
        key = aliases.get(str(domain).strip().lower())
        if not key:
            raise ValueError(f"Unsupported cleanup domain: {domain}")
        if key not in normalized:
            normalized.append(key)
    return normalized


def ensure_cleanup_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS control;")
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.transfer_cleanup_run_log (
            cleanup_run_id TEXT PRIMARY KEY,
            started_at TEXT,
            ended_at TEXT,
            status TEXT,
            domains TEXT,
            notes TEXT
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.transfer_cleanup_step_log (
            cleanup_run_id TEXT,
            domain TEXT,
            pipeline_run_id TEXT,
            source_table TEXT,
            started_at TEXT,
            ended_at TEXT,
            rows_copied TEXT,
            rows_manifested TEXT,
            rows_deleted TEXT,
            rows_already_absent TEXT,
            rows_guard_blocked TEXT,
            status TEXT,
            error_message TEXT
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.transfer_cleanup_manifest (
            manifest_id BIGSERIAL PRIMARY KEY,
            cleanup_run_id TEXT,
            pipeline_run_id TEXT,
            domain TEXT,
            source_table TEXT,
            source_pk TEXT,
            guard_column TEXT,
            guard_value TEXT,
            raw_schema TEXT,
            raw_table TEXT,
            copied_at TEXT,
            delete_status TEXT,
            deleted_at TEXT,
            delete_error TEXT
        );
        """
    )


@contextmanager
def cleanup_lock():
    ensure_cleanup_tables()
    acquired = False
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", [CLEANUP_LOCK_KEY])
        acquired = bool(cursor.fetchone()[0])
    if not acquired:
        raise RuntimeError("Weekly transfer cleanup is already running")
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", [CLEANUP_LOCK_KEY])


def log_cleanup_run(cleanup_run_id: str, status: str, domains: list[str], notes: str = "") -> None:
    execute(
        """
        INSERT INTO control.transfer_cleanup_run_log (cleanup_run_id, started_at, ended_at, status, domains, notes)
        VALUES (%s, NOW()::text, NOW()::text, %s, %s, %s)
        ON CONFLICT (cleanup_run_id)
        DO UPDATE SET ended_at = NOW()::text, status = EXCLUDED.status, domains = EXCLUDED.domains, notes = EXCLUDED.notes
        """,
        [cleanup_run_id, status, ",".join(domains), notes],
    )


def log_cleanup_step(
    cleanup_run_id: str,
    domain: str,
    pipeline_run_id: str,
    source_table: str,
    status: str,
    *,
    rows_copied: int = 0,
    rows_manifested: int = 0,
    rows_deleted: int = 0,
    rows_already_absent: int = 0,
    rows_guard_blocked: int = 0,
    error_message: str = "",
) -> None:
    execute(
        """
        INSERT INTO control.transfer_cleanup_step_log
        (cleanup_run_id, domain, pipeline_run_id, source_table, started_at, ended_at, rows_copied, rows_manifested,
         rows_deleted, rows_already_absent, rows_guard_blocked, status, error_message)
        VALUES (%s, %s, %s, %s, NOW()::text, NOW()::text, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            cleanup_run_id,
            domain,
            pipeline_run_id,
            source_table,
            str(rows_copied),
            str(rows_manifested),
            str(rows_deleted),
            str(rows_already_absent),
            str(rows_guard_blocked),
            status,
            error_message,
        ],
    )


def _mysql_connection_params(server_settings: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {
        "host": server_settings["HOST"],
        "port": int(server_settings["PORT"]),
        "user": server_settings["USER"],
        "password": server_settings["PASSWORD"],
        "database": server_settings["DATABASE"],
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
        "charset": "utf8mb4",
        "connect_timeout": int(server_settings.get("CONNECT_TIMEOUT", 10)),
        "read_timeout": int(server_settings.get("READ_TIMEOUT", 60)),
        "write_timeout": int(server_settings.get("WRITE_TIMEOUT", 60)),
    }
    ssl_mode = str(server_settings.get("SSL_MODE", "")).strip().lower()
    if ssl_mode in {"required", "verify_ca", "verify_identity"}:
        ssl_cfg: dict[str, Any] = {}
        ssl_ca = server_settings.get("SSL_CA")
        if ssl_ca:
            ssl_cfg["ca"] = ssl_ca
        params["ssl"] = ssl_cfg or {"check_hostname": ssl_mode == "verify_identity"}
    return params


@contextmanager
def source_mysql_cursor(settings_name: str):
    server_settings = getattr(settings, settings_name)
    with pymysql.connect(**_mysql_connection_params(server_settings)) as conn:
        with conn.cursor() as cursor:
            yield cursor


def _raw_rows_for_run(spec: CleanupSpec, pipeline_run_id: str) -> list[dict[str, Any]]:
    guard_sql = f'"{spec.guard_column}" AS guard_value' if spec.guard_column else "NULL::text AS guard_value"
    rows = fetchall(
        f"""
        SELECT DISTINCT "{spec.key_column}" AS source_pk, {guard_sql}
        FROM {spec.raw_schema}.{spec.raw_table}
        WHERE "_ingestion_run_id" = %s
          AND COALESCE("{spec.key_column}", '') <> ''
        ORDER BY "{spec.key_column}"
        """,
        [pipeline_run_id],
    )
    return rows


def _manifest_rows(cleanup_run_id: str, pipeline_run_id: str, spec: CleanupSpec, rows: list[dict[str, Any]]) -> int:
    manifested = 0
    copied_at = _utc_now()
    for row in rows:
        execute(
            """
            INSERT INTO control.transfer_cleanup_manifest
            (cleanup_run_id, pipeline_run_id, domain, source_table, source_pk, guard_column, guard_value,
             raw_schema, raw_table, copied_at, delete_status, deleted_at, delete_error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', NULL, '')
            """,
            [
                cleanup_run_id,
                pipeline_run_id,
                spec.domain,
                spec.source_table,
                str(row["source_pk"]),
                spec.guard_column or "",
                row.get("guard_value"),
                spec.raw_schema,
                spec.raw_table,
                copied_at,
            ],
        )
        manifested += 1
    return manifested


def _pending_manifests(cleanup_run_id: str, spec: CleanupSpec) -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT manifest_id, source_pk, guard_value
        FROM control.transfer_cleanup_manifest
        WHERE cleanup_run_id = %s
          AND source_table = %s
          AND delete_status = 'PENDING'
        ORDER BY manifest_id
        """,
        [cleanup_run_id, spec.source_table],
    )


def _delete_source_row(cursor: Any, spec: CleanupSpec, source_pk: str, guard_value: str | None) -> tuple[str, str]:
    if spec.guard_column:
        cursor.execute(
            f"""
            DELETE FROM `{spec.source_table}`
            WHERE `{spec.key_column}` = %s
              AND COALESCE(CAST(`{spec.guard_column}` AS CHAR), '') = COALESCE(%s, '')
            """,
            [source_pk, guard_value or ""],
        )
        if cursor.rowcount > 0:
            return "DELETED", ""
        cursor.execute(
            f"SELECT 1 AS exists_flag FROM `{spec.source_table}` WHERE `{spec.key_column}` = %s LIMIT 1",
            [source_pk],
        )
        if cursor.fetchone():
            return "GUARD_BLOCKED", "Source row changed after extraction; delete skipped."
        return "ALREADY_ABSENT", ""

    cursor.execute(
        f"DELETE FROM `{spec.source_table}` WHERE `{spec.key_column}` = %s",
        [source_pk],
    )
    if cursor.rowcount > 0:
        return "DELETED", ""
    return "ALREADY_ABSENT", ""


def _apply_deletes(cleanup_run_id: str, spec: CleanupSpec) -> dict[str, int]:
    pending = _pending_manifests(cleanup_run_id, spec)
    deleted = 0
    already_absent = 0
    guard_blocked = 0
    failed = 0

    if not pending:
        return {
            "rows_deleted": 0,
            "rows_already_absent": 0,
            "rows_guard_blocked": 0,
            "rows_failed": 0,
        }

    with source_mysql_cursor(spec.mysql_settings_name) as cursor:
        for row in pending:
            status = "FAILED"
            error_message = ""
            try:
                status, error_message = _delete_source_row(cursor, spec, row["source_pk"], row.get("guard_value"))
            except Exception as exc:
                error_message = str(exc)

            if status == "DELETED":
                deleted += 1
            elif status == "ALREADY_ABSENT":
                already_absent += 1
            elif status == "GUARD_BLOCKED":
                guard_blocked += 1
            elif status == "FAILED":
                failed += 1

            execute(
                """
                UPDATE control.transfer_cleanup_manifest
                SET delete_status = %s,
                    deleted_at = NOW()::text,
                    delete_error = %s
                WHERE manifest_id = %s
                """,
                [status, error_message, row["manifest_id"]],
            )

    return {
        "rows_deleted": deleted,
        "rows_already_absent": already_absent,
        "rows_guard_blocked": guard_blocked,
        "rows_failed": failed,
    }


def _cleanup_specs_for_run(cleanup_run_id: str, pipeline_run_id: str, specs: tuple[CleanupSpec, ...]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for spec in specs:
        rows = _raw_rows_for_run(spec, pipeline_run_id)
        manifested = _manifest_rows(cleanup_run_id, pipeline_run_id, spec, rows)
        delete_counts = _apply_deletes(cleanup_run_id, spec)
        step_status = "SUCCESS"
        if delete_counts["rows_failed"] > 0:
            step_status = "FAIL"
        elif delete_counts["rows_guard_blocked"] > 0:
            step_status = "PARTIAL_SUCCESS"
        log_cleanup_step(
            cleanup_run_id,
            spec.domain,
            pipeline_run_id,
            spec.source_table,
            step_status,
            rows_copied=len(rows),
            rows_manifested=manifested,
            rows_deleted=delete_counts["rows_deleted"],
            rows_already_absent=delete_counts["rows_already_absent"],
            rows_guard_blocked=delete_counts["rows_guard_blocked"],
            error_message=(
                "One or more source deletes failed."
                if step_status == "FAIL"
                else "Source guard prevented one or more deletes."
                if step_status == "PARTIAL_SUCCESS"
                else ""
            ),
        )
        summary[spec.source_table] = {
            "rows_copied": len(rows),
            "rows_manifested": manifested,
            **delete_counts,
            "status": step_status,
        }
    return summary


def _cleanup_status(cleanup_summary: dict[str, Any]) -> str:
    statuses = [item.get("status", "FAIL") for item in cleanup_summary.values()]
    if not statuses:
        return "SUCCESS"
    if all(status == "SUCCESS" for status in statuses):
        return "SUCCESS"
    if any(status == "FAIL" for status in statuses):
        return "FAIL"
    return "PARTIAL_SUCCESS"


def _run_domain(cleanup_run_id: str, domain: str) -> dict[str, Any]:
    if domain == "inclinic":
        pipeline_result = run_inclinic_pipeline(trigger_type="weekly_transfer_cleanup")
        cleanup_summary = _cleanup_specs_for_run(cleanup_run_id, pipeline_result["run_id"], INCLINIC_SPECS)
        cleanup_status = _cleanup_status(cleanup_summary)
        domain_status = pipeline_result["status"]
        if cleanup_status == "FAIL":
            domain_status = "FAIL"
        elif cleanup_status == "PARTIAL_SUCCESS" and domain_status == "SUCCESS":
            domain_status = "PARTIAL_SUCCESS"
        return {
            "status": domain_status,
            "pipeline_run_id": pipeline_result["run_id"],
            "pipeline_status": pipeline_result["status"],
            "cleanup": cleanup_summary,
        }

    if domain == "rfa":
        pipeline_result = run_sapa_pipeline(trigger_type="weekly_transfer_cleanup")
        cleanup_summary = _cleanup_specs_for_run(cleanup_run_id, pipeline_result["run_id"], RFA_SPECS)
        cleanup_status = _cleanup_status(cleanup_summary)
        return {
            "status": cleanup_status,
            "pipeline_run_id": pipeline_result["run_id"],
            "pipeline_status": "SUCCESS",
            "cleanup": cleanup_summary,
        }

    raise ValueError(f"Unsupported domain: {domain}")


def _overall_status(domain_results: dict[str, Any]) -> str:
    statuses = [result.get("status", "FAIL") for result in domain_results.values()]
    if all(status == "SUCCESS" for status in statuses):
        return "SUCCESS"
    if any(status == "SUCCESS" for status in statuses):
        return "PARTIAL_SUCCESS"
    return "FAIL"


def run_weekly_transfer_cleanup(domains: Iterable[str]) -> dict[str, Any]:
    normalized_domains = _normalize_domains(domains)
    cleanup_run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ensure_cleanup_tables()
    log_cleanup_run(cleanup_run_id, "RUNNING", normalized_domains, notes="{}")

    domain_results: dict[str, Any] = {}
    with cleanup_lock():
        for domain in normalized_domains:
            try:
                domain_results[domain] = _run_domain(cleanup_run_id, domain)
            except Exception as exc:
                domain_results[domain] = {
                    "status": "FAIL",
                    "error": str(exc),
                }

    status = _overall_status(domain_results)
    log_cleanup_run(cleanup_run_id, status, normalized_domains, notes=json.dumps(domain_results, default=str))
    return {
        "cleanup_run_id": cleanup_run_id,
        "status": status,
        "domains": domain_results,
    }
