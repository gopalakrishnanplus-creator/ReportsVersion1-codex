from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from django.db import connection


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def table_name(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


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


def replace_table(schema: str, table: str, columns: list[str], rows: Iterable[dict[str, Any]]) -> None:
    create_text_table(schema, table, columns)
    insert_rows(schema, table, columns, rows)
