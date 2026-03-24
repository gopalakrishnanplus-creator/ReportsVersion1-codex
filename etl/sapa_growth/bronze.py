from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from etl.sapa_growth.specs import API_TABLE_SPECS, BRONZE_SCHEMA, MYSQL_TABLE_SPECS, RAW_AUDIT_COLUMNS, RAW_API_SCHEMA, RAW_MYSQL_SCHEMA
from etl.sapa_growth.storage import fetch_table, replace_table
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


def build_bronze() -> dict[str, int]:
    counts: dict[str, int] = {}
    for name, spec in MYSQL_TABLE_SPECS.items():
        raw_rows = fetch_table(RAW_MYSQL_SCHEMA, spec.raw_table)
        output_rows = _dedup_rows(raw_rows, spec.key_columns, spec.watermark_field, spec.columns, name)
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
