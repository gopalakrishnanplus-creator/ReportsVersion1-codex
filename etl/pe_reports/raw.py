from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable

from django.conf import settings

from etl.pe_reports.control import get_watermark, log_step, upsert_watermark
from etl.pe_reports.mysql_master import MasterMySQLExtractionError, extract_rows as extract_master_rows
from etl.pe_reports.mysql_portal import PortalMySQLExtractionError, extract_rows as extract_portal_rows
from etl.pe_reports.specs import MASTER_TABLE_SPECS, PORTAL_TABLE_SPECS, RAW_AUDIT_COLUMNS, RAW_MASTER_SCHEMA, RAW_PORTAL_SCHEMA, SourceTableSpec
from etl.pe_reports.storage import append_rows, ensure_text_table
from etl.pe_reports.utils import clean_text, hash_fields, parse_datetime


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
    for spec in PORTAL_TABLE_SPECS.values():
        ensure_text_table(RAW_PORTAL_SCHEMA, spec.raw_table, spec.columns + RAW_AUDIT_COLUMNS)
    for spec in MASTER_TABLE_SPECS.values():
        ensure_text_table(RAW_MASTER_SCHEMA, spec.raw_table, spec.columns + RAW_AUDIT_COLUMNS)


def _ingest_specs(
    *,
    run_id: str,
    extracted_at: str,
    schema: str,
    specs: dict[str, SourceTableSpec],
    source_name: str,
    source_system_label: str,
    extractor: Callable[[str, list[str], str | None, str | None], list[dict[str, Any]]],
    error_type: type[Exception],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    max_watermarks: dict[str, str] = {}

    for name, spec in specs.items():
        lookback_days = spec.lookback_days or int(settings.PE_REPORTS["LOOKBACK_DAYS"])
        watermark_start = _watermark_start(source_name, name, spec.watermark_field, lookback_days)
        try:
            rows = extractor(spec.source_table, spec.columns, spec.watermark_field, watermark_start)
            prepared_rows: list[dict[str, Any]] = []
            max_watermark_value: str | None = None
            for row in rows:
                payload = {column: row.get(column) for column in spec.columns}
                payload.update(_audit_payload(run_id, source_system_label, name, extracted_at, [row.get(column) for column in spec.columns]))
                prepared_rows.append(payload)
                if spec.watermark_field:
                    candidate = clean_text(row.get(spec.watermark_field))
                    if candidate and (max_watermark_value is None or candidate > max_watermark_value):
                        max_watermark_value = candidate

            append_rows(schema, spec.raw_table, spec.columns + RAW_AUDIT_COLUMNS, prepared_rows)
            counts[name] = len(prepared_rows)
            if max_watermark_value:
                max_watermarks[name] = max_watermark_value
            current_watermark = get_watermark(source_name, name)
            previous_value = clean_text((current_watermark or {}).get("last_successful_watermark_value"))
            upsert_watermark(
                source_name,
                name,
                spec.watermark_field,
                max_watermark_value or previous_value,
                lookback_days,
                "incremental" if spec.watermark_field else "snapshot",
                run_id,
            )
            log_step(run_id, f"extract_{source_name}", name, "SUCCESS", rows_read=len(rows), rows_written=len(prepared_rows))
        except error_type as exc:  # type: ignore[arg-type]
            counts[name] = 0
            errors[name] = str(exc)
            log_step(run_id, f"extract_{source_name}", name, "FAIL", error_message=str(exc))

    return {"counts": counts, "errors": errors, "max_watermarks": max_watermarks}


def ingest_portal_sources(run_id: str, extracted_at: str) -> dict[str, Any]:
    ensure_raw_tables()
    return _ingest_specs(
        run_id=run_id,
        extracted_at=extracted_at,
        schema=RAW_PORTAL_SCHEMA,
        specs=PORTAL_TABLE_SPECS,
        source_name="portal",
        source_system_label="pe_portal_mysql",
        extractor=extract_portal_rows,
        error_type=PortalMySQLExtractionError,
    )


def ingest_master_sources(run_id: str, extracted_at: str) -> dict[str, Any]:
    ensure_raw_tables()
    return _ingest_specs(
        run_id=run_id,
        extracted_at=extracted_at,
        schema=RAW_MASTER_SCHEMA,
        specs=MASTER_TABLE_SPECS,
        source_name="master",
        source_system_label="pe_master_mysql",
        extractor=extract_master_rows,
        error_type=MasterMySQLExtractionError,
    )
