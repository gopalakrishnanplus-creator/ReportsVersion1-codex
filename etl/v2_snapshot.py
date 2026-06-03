from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from django.db import connection


SNAPSHOT_SCHEMA = "control"
SNAPSHOT_TABLE = "raw_v2_current_snapshot_keys"
ROW_KEY_SEPARATOR = "\x1f"


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def snapshot_row_key(row: dict[str, Any], key_columns: list[str]) -> str:
    values = [_clean(row.get(column)) for column in key_columns]
    if any(values):
        return ROW_KEY_SEPARATOR.join(values)
    return _clean(row.get("_record_hash"))


def _ensure_snapshot_table() -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(SNAPSHOT_SCHEMA)}")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_qident(SNAPSHOT_SCHEMA)}.{_qident(SNAPSHOT_TABLE)} (
                raw_schema TEXT NOT NULL,
                raw_table TEXT NOT NULL,
                source_table TEXT NOT NULL,
                row_key TEXT NOT NULL,
                last_run_id TEXT NOT NULL,
                extracted_at TEXT NOT NULL,
                PRIMARY KEY (raw_schema, raw_table, source_table, row_key)
            )
            """
        )


def record_v2_current_snapshot(
    *,
    raw_schema: str,
    raw_table: str,
    source_table: str,
    key_columns: list[str],
    rows: Iterable[dict[str, Any]],
    run_id: str,
    extracted_at: str,
) -> None:
    materialized = list(rows)
    row_keys = sorted({snapshot_row_key(row, key_columns) for row in materialized if snapshot_row_key(row, key_columns)})
    _ensure_snapshot_table()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            DELETE FROM {_qident(SNAPSHOT_SCHEMA)}.{_qident(SNAPSHOT_TABLE)}
            WHERE raw_schema = %s
              AND raw_table = %s
              AND source_table = %s
            """,
            [raw_schema, raw_table, source_table],
        )
        if not row_keys:
            return
        cursor.executemany(
            f"""
            INSERT INTO {_qident(SNAPSHOT_SCHEMA)}.{_qident(SNAPSHOT_TABLE)}
            (raw_schema, raw_table, source_table, row_key, last_run_id, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [(raw_schema, raw_table, source_table, row_key, run_id, extracted_at) for row_key in row_keys],
        )


def current_v2_snapshot_keys(raw_schema: str, raw_table: str, source_table: str) -> set[str] | None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", [f"{SNAPSHOT_SCHEMA}.{SNAPSHOT_TABLE}"])
        if cursor.fetchone()[0] is None:
            return None
        cursor.execute(
            f"""
            SELECT row_key
            FROM {_qident(SNAPSHOT_SCHEMA)}.{_qident(SNAPSHOT_TABLE)}
            WHERE raw_schema = %s
              AND raw_table = %s
              AND source_table = %s
            """,
            [raw_schema, raw_table, source_table],
        )
        return {str(row[0]) for row in cursor.fetchall()}
