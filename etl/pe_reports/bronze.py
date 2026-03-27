from __future__ import annotations

from typing import Any

from etl.pe_reports.control import active_exclusion_rules
from etl.pe_reports.specs import BRONZE_SCHEMA, MASTER_TABLE_SPECS, PORTAL_TABLE_SPECS, RAW_AUDIT_COLUMNS, RAW_MASTER_SCHEMA, RAW_PORTAL_SCHEMA, SourceTableSpec
from etl.pe_reports.storage import fetch_table, replace_table
from etl.pe_reports.utils import clean_text, parse_datetime


def _ordering_key(row: dict[str, Any], watermark_field: str | None) -> tuple[Any, ...]:
    primary = parse_datetime(row.get(watermark_field)) if watermark_field else None
    secondary = parse_datetime(row.get("_ingested_at"))
    return (primary is not None, primary, secondary)


def _normalize_row(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    output = {}
    for column in columns:
        output[column] = clean_text(row.get(column))
    for column in RAW_AUDIT_COLUMNS:
        output[column] = clean_text(row.get(column))
    return output


def _is_excluded(table_name: str, row: dict[str, Any], rules: list[dict[str, Any]]) -> bool:
    for rule in rules:
        entity_name = clean_text(rule.get("entity_name"))
        field_name = clean_text(rule.get("field_name"))
        match_value = clean_text(rule.get("match_value"))
        if entity_name not in {table_name, "*"} or not field_name or match_value is None:
            continue
        if clean_text(row.get(field_name)) == match_value:
            return True
    return False


def _dedup_rows(rows: list[dict[str, Any]], spec: SourceTableSpec) -> list[dict[str, Any]]:
    if not spec.key_columns:
        return [_normalize_row(row, spec.columns) for row in rows]
    latest_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: _ordering_key(item, spec.watermark_field), reverse=True):
        key = tuple(clean_text(row.get(column)) for column in spec.key_columns)
        if key in latest_by_key:
            continue
        latest_by_key[key] = _normalize_row(row, spec.columns)
    return list(latest_by_key.values())


def _build_table(raw_schema: str, spec: SourceTableSpec, rules: list[dict[str, Any]]) -> int:
    raw_rows = fetch_table(raw_schema, spec.raw_table)
    deduped = _dedup_rows(raw_rows, spec)
    filtered = [row for row in deduped if not _is_excluded(spec.bronze_table, row, rules)]
    replace_table(BRONZE_SCHEMA, spec.bronze_table, spec.columns + RAW_AUDIT_COLUMNS, filtered)
    return len(filtered)


def build_bronze() -> dict[str, int]:
    rules = active_exclusion_rules()
    counts: dict[str, int] = {}
    for spec in PORTAL_TABLE_SPECS.values():
        counts[spec.bronze_table] = _build_table(RAW_PORTAL_SCHEMA, spec, rules)
    for spec in MASTER_TABLE_SPECS.values():
        counts[spec.bronze_table] = _build_table(RAW_MASTER_SCHEMA, spec, rules)
    return counts
