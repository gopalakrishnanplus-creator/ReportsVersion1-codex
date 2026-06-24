from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from django.conf import settings

from etl.sapa_growth.specs import (
    API_TABLE_SPECS,
    BRONZE_SCHEMA,
    MYSQL_TABLE_SPECS,
    RAW_AUDIT_COLUMNS,
    RAW_API_SCHEMA,
    RAW_MYSQL_SCHEMA,
)
from etl.sapa_growth.storage import fetch_table, replace_table
from etl.v2_snapshot import current_v2_snapshot_keys, snapshot_row_key
from sapa_growth.logic import clean_text, hash_fields, parse_date, parse_datetime


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _coalesced_key(row: dict[str, Any], key_columns: list[str]) -> str:
    values = [clean_text(row.get(column)) for column in key_columns]
    if any(values):
        return "|".join(value or "" for value in values)
    return clean_text(row.get("_record_hash")) or hash_fields(*row.values())


def _ordering_key(row: dict[str, Any], watermark_field: str | None) -> tuple[Any, ...]:
    primary = parse_datetime(row.get(watermark_field)) if watermark_field else None
    ingested = parse_datetime(row.get("_ingested_at"))
    return (
        primary or datetime.min,
        ingested or datetime.min,
        clean_text(row.get("_record_hash")) or "",
    )


def _normalize_row(row: dict[str, Any], columns: list[str], table_name: str) -> dict[str, Any]:
    normalized = {column: clean_text(row.get(column)) for column in columns + RAW_AUDIT_COLUMNS}
    normalized["_bronze_deduped_at"] = _now_iso()
    normalized["_bronze_source_raw_ingested_at"] = clean_text(row.get("_ingested_at"))

    if table_name in {"redflags_patientsubmission", "gnd_gndpatientsubmission"}:
        normalized["submitted_at_parseable"] = "true" if parse_datetime(row.get("submitted_at")) else "false"
    if table_name == "redflags_followupreminder":
        for field_name in ("followup_date1", "followup_date2", "followup_date3", "first_followup_date"):
            normalized[f"{field_name}_parseable"] = "true" if parse_date(row.get(field_name)) else "false"
    if table_name == "redflags_metricevent":
        normalized["ts_parseable"] = "true" if parse_datetime(row.get("ts")) else "false"
    return normalized


def _dedup_rows(rows: list[dict[str, Any]], key_columns: list[str], watermark_field: str | None, columns: list[str], table_name: str) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: _ordering_key(item, watermark_field), reverse=True):
        key = _coalesced_key(row, key_columns)
        if key in deduped:
            continue
        deduped[key] = _normalize_row(row, columns, table_name)
    return list(deduped.values())


def _active_source_rows(
    rows: list[dict[str, Any]],
    source_table: str,
    key_columns: list[str],
    current_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    if source_table.lower().endswith("_v2"):
        rows = [row for row in rows if clean_text(row.get("_source_table")) == source_table]
    if current_keys is None:
        return rows
    return [row for row in rows if snapshot_row_key(row, key_columns) in current_keys]


def _legacy_v2_fallback_enabled() -> bool:
    return bool(settings.SAPA_ETL.get("ENABLE_LEGACY_V2_FALLBACKS", False))


def _fallback_source_tables_for_spec(spec) -> tuple[str, ...]:
    if _legacy_v2_fallback_enabled() or spec.current_snapshot:
        return spec.fallback_source_tables
    return ()


def _rows_for_source_table(rows: list[dict[str, Any]], source_table: str) -> list[dict[str, Any]]:
    source_rows = [row for row in rows if clean_text(row.get("_source_table")) == source_table]
    if source_rows or any(clean_text(row.get("_source_table")) for row in rows):
        return source_rows
    return rows


def _active_source_rows_for_spec(rows: list[dict[str, Any]], spec) -> list[dict[str, Any]]:
    fallback_source_tables = _fallback_source_tables_for_spec(spec)
    source_tables = (spec.source_table, *fallback_source_tables)
    active_rows_by_source: dict[str, list[dict[str, Any]]] = {}
    for source_table in source_tables:
        source_rows = _rows_for_source_table(rows, source_table)
        current_keys = (
            current_v2_snapshot_keys(RAW_MYSQL_SCHEMA, spec.raw_table, source_table)
            if spec.current_snapshot
            else None
        )
        active_rows_by_source[source_table] = _active_source_rows(source_rows, source_table, spec.key_columns, current_keys)

    primary_rows = active_rows_by_source.get(spec.source_table, [])
    if not fallback_source_tables:
        return primary_rows

    primary_keys = {
        _coalesced_key(row, spec.key_columns)
        for row in primary_rows
        if any(clean_text(row.get(column)) for column in spec.key_columns)
    }
    backfill_rows: list[dict[str, Any]] = []
    for source_table in fallback_source_tables:
        for row in active_rows_by_source.get(source_table, []):
            row_key = _coalesced_key(row, spec.key_columns)
            if row_key in primary_keys:
                continue
            primary_keys.add(row_key)
            backfill_rows.append(row)
    return primary_rows + backfill_rows


def build_bronze() -> dict[str, int]:
    counts: dict[str, int] = {}
    for name, spec in MYSQL_TABLE_SPECS.items():
        extra_columns = []
        if name in {"redflags_patientsubmission", "gnd_gndpatientsubmission"}:
            extra_columns.append("submitted_at_parseable")
        if name == "redflags_followupreminder":
            extra_columns.extend(
                [
                    "followup_date1_parseable",
                    "followup_date2_parseable",
                    "followup_date3_parseable",
                    "first_followup_date_parseable",
                ]
            )
        if name == "redflags_metricevent":
            extra_columns.append("ts_parseable")

        raw_rows = fetch_table(RAW_MYSQL_SCHEMA, spec.raw_table)
        raw_rows = _active_source_rows_for_spec(raw_rows, spec)
        output_rows = _dedup_rows(raw_rows, spec.key_columns, spec.watermark_field, spec.columns, name)
        replace_table(
            BRONZE_SCHEMA,
            name,
            spec.columns + RAW_AUDIT_COLUMNS + ["_bronze_deduped_at", "_bronze_source_raw_ingested_at"] + extra_columns,
            output_rows,
        )
        counts[name] = len(output_rows)

    for name, spec in API_TABLE_SPECS.items():
        raw_rows = fetch_table(RAW_API_SCHEMA, spec["raw_table"])
        output_rows = _dedup_rows(raw_rows, spec["key_columns"], "_ingested_at", spec["columns"], name)
        replace_table(
            BRONZE_SCHEMA,
            name,
            spec["columns"] + RAW_AUDIT_COLUMNS + ["_bronze_deduped_at", "_bronze_source_raw_ingested_at"],
            output_rows,
        )
        counts[name] = len(output_rows)

    return counts
