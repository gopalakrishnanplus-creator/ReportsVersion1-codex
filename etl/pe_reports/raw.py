from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable

from django.conf import settings

from etl.pe_reports.control import get_watermark, log_step, upsert_watermark
from etl.pe_reports.mysql_master import MasterMySQLExtractionError, extract_rows as extract_master_rows
from etl.pe_reports.mysql_portal import PortalMySQLExtractionError, extract_rows as extract_portal_rows
from etl.pe_reports.specs import MASTER_TABLE_SPECS, PORTAL_TABLE_SPECS, RAW_AUDIT_COLUMNS, RAW_MASTER_SCHEMA, RAW_PORTAL_SCHEMA, SourceTableSpec
from etl.pe_reports.storage import ensure_text_table, insert_new_source_rows
from etl.pe_reports.utils import clean_text, hash_fields, parse_datetime
from etl.v2_snapshot import record_v2_current_snapshot


def _legacy_v2_fallback_enabled() -> bool:
    return bool(settings.PE_REPORTS.get("ENABLE_LEGACY_V2_FALLBACKS", False))


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


def _filter_source_rows(rows: list[dict[str, Any]], spec: SourceTableSpec, source_table: str) -> list[dict[str, Any]]:
    if not spec.source_filters or source_table != spec.source_table:
        return rows
    return [
        row
        for row in rows
        if all(clean_text(row.get(column)) == expected for column, expected in spec.source_filters.items())
    ]


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
    skipped_counts: dict[str, int] = {}
    extracted_counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    max_watermarks: dict[str, str] = {}

    for name, spec in specs.items():
        is_v2_source = spec.source_table.lower().endswith("_v2")
        allow_legacy_fallback = is_v2_source and bool(spec.fallback_source_table) and _legacy_v2_fallback_enabled()
        lookback_days = spec.lookback_days or int(settings.PE_REPORTS["LOOKBACK_DAYS"])
        watermark_start = None if is_v2_source else _watermark_start(source_name, name, spec.watermark_field, lookback_days)
        try:
            source_batches: list[tuple[str, list[dict[str, Any]]]] = []
            try:
                primary_rows = extractor(spec.source_table, spec.columns, spec.watermark_field, watermark_start)
                source_batches.append((spec.source_table, _filter_source_rows(primary_rows, spec, spec.source_table)))
            except error_type:
                if not allow_legacy_fallback:
                    raise
            if allow_legacy_fallback:
                try:
                    fallback_rows = extractor(spec.fallback_source_table, spec.columns, spec.watermark_field, None)
                    source_batches.append((spec.fallback_source_table, fallback_rows))
                except error_type:
                    if not any(rows for _, rows in source_batches):
                        raise
            prepared_rows: list[dict[str, Any]] = []
            primary_prepared_rows: list[dict[str, Any]] = []
            max_watermark_value: str | None = None
            for effective_source_table, rows in source_batches:
                for row in rows:
                    payload = {column: row.get(column) for column in spec.columns}
                    payload.update(_audit_payload(run_id, source_system_label, effective_source_table, extracted_at, [row.get(column) for column in spec.columns]))
                    prepared_rows.append(payload)
                    if effective_source_table == spec.source_table:
                        primary_prepared_rows.append(payload)
                    if spec.watermark_field:
                        candidate = clean_text(row.get(spec.watermark_field))
                        if candidate and (max_watermark_value is None or candidate > max_watermark_value):
                            max_watermark_value = candidate

            fingerprint_columns = spec.columns + ["_source_table"] if is_v2_source or spec.fallback_source_table else spec.columns
            inserted_count = insert_new_source_rows(
                schema,
                spec.raw_table,
                spec.columns,
                RAW_AUDIT_COLUMNS,
                prepared_rows,
                fingerprint_columns=fingerprint_columns,
            )
            if is_v2_source and primary_prepared_rows:
                record_v2_current_snapshot(
                    raw_schema=schema,
                    raw_table=spec.raw_table,
                    source_table=spec.source_table,
                    key_columns=spec.key_columns,
                    rows=primary_prepared_rows,
                    run_id=run_id,
                    extracted_at=extracted_at,
                )
            counts[name] = inserted_count
            skipped_counts[name] = len(prepared_rows) - inserted_count
            extracted_counts[name] = len(prepared_rows)
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
                "snapshot" if is_v2_source else "incremental" if spec.watermark_field else "snapshot",
                run_id,
            )
            log_step(run_id, f"extract_{source_name}", name, "SUCCESS", rows_read=len(rows), rows_written=inserted_count)
        except error_type as exc:  # type: ignore[arg-type]
            counts[name] = 0
            skipped_counts[name] = 0
            extracted_counts[name] = 0
            errors[name] = str(exc)
            log_step(run_id, f"extract_{source_name}", name, "FAIL", error_message=str(exc))

    return {"counts": counts, "skipped_counts": skipped_counts, "extracted_counts": extracted_counts, "errors": errors, "max_watermarks": max_watermarks}


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
