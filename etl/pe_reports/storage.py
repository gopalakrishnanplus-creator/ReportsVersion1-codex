from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

from django.db import connection
from psycopg2.extras import execute_values


SOURCE_PAYLOAD_HASH_COLUMN = "_source_payload_hash"
SOURCE_ROW_INSERT_BATCH_SIZE = 1000


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def table_name(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def _payload_hash_sql(columns: list[str], relation: str | None = None) -> str:
    prefix = f"{qident(relation)}." if relation else ""
    parts = [f"{prefix}{qident(column)}::text" for column in columns]
    return f"md5(jsonb_build_array({', '.join(parts)})::text)"


def _raw_payload_index_name(schema: str, table: str) -> str:
    suffix = hashlib.md5(f"{schema}.{table}".encode("utf-8")).hexdigest()[:12]
    return f"raw_payload_hash_{suffix}_idx"


def ensure_schema(schema: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)}")


def table_exists(schema: str, table: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", [f"{schema}.{table}"])
        return cursor.fetchone()[0] is not None


def fetch_all(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        if cursor.description is None:
            return []
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_table(schema: str, table: str, order_by: str | None = None) -> list[dict[str, Any]]:
    ordering = f" ORDER BY {order_by}" if order_by else ""
    return fetch_all(f"SELECT * FROM {table_name(schema, table)}{ordering}")


def create_text_table(schema: str, table: str, columns: list[str]) -> None:
    ensure_schema(schema)
    column_sql = ", ".join(f"{qident(column)} TEXT" for column in columns)
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name(schema, table)}")
        cursor.execute(f"CREATE TABLE {table_name(schema, table)} ({column_sql})")


def ensure_text_table(schema: str, table: str, columns: list[str]) -> None:
    ensure_schema(schema)
    column_sql = ", ".join(f"{qident(column)} TEXT" for column in columns)
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name(schema, table)} ({column_sql})")
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            [schema, table],
        )
        existing_columns = {row[0] for row in cursor.fetchall()}
        for column in columns:
            if column in existing_columns:
                continue
            cursor.execute(
                f"ALTER TABLE {table_name(schema, table)} "
                f"ADD COLUMN {qident(column)} TEXT"
            )


def ensure_source_payload_hash(schema: str, table: str, source_columns: list[str]) -> None:
    table_ref = table_name(schema, table)
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {table_ref}
            SET {qident(SOURCE_PAYLOAD_HASH_COLUMN)} = {_payload_hash_sql(source_columns)}
            WHERE NULLIF({qident(SOURCE_PAYLOAD_HASH_COLUMN)}, '') IS NULL
            """
        )
        cursor.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {qident(_raw_payload_index_name(schema, table))}
            ON {table_ref} ({qident(SOURCE_PAYLOAD_HASH_COLUMN)})
            """
        )


def insert_rows(schema: str, table: str, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    if not materialized:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {table_name(schema, table)} "
        f"({', '.join(qident(column) for column in columns)}) "
        f"VALUES ({placeholders})"
    )
    with connection.cursor() as cursor:
        cursor.executemany(sql, [[row.get(column) for column in columns] for row in materialized])


def append_rows(schema: str, table: str, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    insert_rows(schema, table, columns, rows)


def insert_new_source_rows(schema: str, table: str, source_columns: list[str], audit_columns: list[str], rows: Iterable[dict[str, Any]]) -> int:
    materialized = list(rows)
    if not materialized:
        return 0

    all_columns = source_columns + audit_columns
    input_columns = source_columns + [column for column in audit_columns if column != SOURCE_PAYLOAD_HASH_COLUMN]
    ensure_text_table(schema, table, all_columns)
    ensure_source_payload_hash(schema, table, source_columns)

    table_ref = table_name(schema, table)
    input_column_sql = ", ".join(qident(column) for column in input_columns)
    insert_column_sql = ", ".join(qident(column) for column in all_columns)
    select_column_sql = ", ".join(
        "deduped.source_payload_hash" if column == SOURCE_PAYLOAD_HASH_COLUMN else f"deduped.{qident(column)}"
        for column in all_columns
    )
    payload_hash_sql = _payload_hash_sql(source_columns, relation="incoming")
    values = [[row.get(column) for column in input_columns] for row in materialized]

    query = f"""
        WITH incoming ({input_column_sql}) AS (
            VALUES %s
        ),
        payload AS (
            SELECT
                incoming.*,
                {payload_hash_sql} AS source_payload_hash
            FROM incoming
        ),
        deduped AS (
            SELECT
                payload.*,
                ROW_NUMBER() OVER (PARTITION BY source_payload_hash ORDER BY source_payload_hash) AS source_payload_rank
            FROM payload
        )
        INSERT INTO {table_ref} ({insert_column_sql})
        SELECT {select_column_sql}
        FROM deduped
        WHERE source_payload_rank = 1
          AND NOT EXISTS (
              SELECT 1
              FROM {table_ref} existing
              WHERE existing.{qident(SOURCE_PAYLOAD_HASH_COLUMN)} = deduped.source_payload_hash
          )
        RETURNING 1
    """
    with connection.cursor() as cursor:
        inserted = execute_values(cursor, query, values, page_size=SOURCE_ROW_INSERT_BATCH_SIZE, fetch=True)
    return len(inserted)


def replace_table(schema: str, table: str, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    create_text_table(schema, table, columns)
    insert_rows(schema, table, columns, rows)
