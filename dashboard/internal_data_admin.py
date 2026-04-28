from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.contrib import messages
from django.core import signing
from django.db import DatabaseError, connection, transaction
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.crypto import constant_time_compare
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods
from psycopg2 import sql


SESSION_KEY = "internal_data_admin_authenticated"
SESSION_USER_KEY = "internal_data_admin_username"
ROW_TOKEN_SALT = "dashboard.internal-data-admin.row"
PAGE_SIZE = 75
AUDIT_SCHEMA = "ops"
AUDIT_TABLE = "internal_dashboard_audit"


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: int
    default: str | None
    is_identity: bool
    is_generated: bool


@dataclass(frozen=True)
class TableInfo:
    schema: str
    name: str
    columns: list[ColumnInfo]
    primary_key: list[str]


def _admin_config() -> dict[str, str]:
    return getattr(settings, "INTERNAL_DATA_ADMIN", {})


def _is_authenticated(request: HttpRequest) -> bool:
    return bool(request.session.get(SESSION_KEY))


def _require_auth(request: HttpRequest):
    if not _is_authenticated(request):
        return redirect(f"{reverse('internal-data-admin-login')}?next={request.get_full_path()}")
    return None


def _fetch_dicts(query, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _execute(query, params: list[Any] | tuple[Any, ...] | None = None) -> None:
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])


def _is_relevant_schema(schema: str) -> bool:
    if schema in {"raw_server1", "raw_server2", "bronze", "silver", "gold_global", "control", "ops"}:
        return True
    return schema.startswith(("raw_", "silver_", "gold_"))


def _is_managed_table(schema: str, table: str) -> bool:
    return _is_relevant_schema(schema) and not (schema == AUDIT_SCHEMA and table == AUDIT_TABLE)


def _ensure_audit_table() -> None:
    _execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(AUDIT_SCHEMA)))
    _execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                action TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_locator JSONB,
                before_data JSONB,
                after_data JSONB,
                reason TEXT,
                actor TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(AUDIT_TABLE))
    )


def _table_exists(schema: str, table: str) -> bool:
    rows = _fetch_dicts(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
          AND table_type = 'BASE TABLE'
        LIMIT 1
        """,
        [schema, table],
    )
    return bool(rows)


def _list_tables() -> list[dict[str, Any]]:
    raw_tables = _fetch_dicts(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    )
    tables: list[dict[str, Any]] = []
    for row in raw_tables:
        schema = row["table_schema"]
        table = row["table_name"]
        if not _is_managed_table(schema, table):
            continue
        tables.append(
            {
                "schema": schema,
                "name": table,
                "row_count": _table_count(schema, table),
                "href": reverse("internal-data-admin-table", args=[schema, table]),
            }
        )
    return tables


def _table_count(schema: str, table: str) -> int | None:
    try:
        rows = _fetch_dicts(
            sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        return int(rows[0]["row_count"])
    except DatabaseError:
        return None


def _table_info(schema: str, table: str) -> TableInfo:
    if not _is_managed_table(schema, table) or not _table_exists(schema, table):
        raise Http404("Table is not available in the internal data dashboard.")

    column_rows = _fetch_dicts(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            ordinal_position,
            column_default,
            is_identity,
            is_generated
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    columns = [
        ColumnInfo(
            name=row["column_name"],
            data_type=row["data_type"],
            is_nullable=row["is_nullable"] == "YES",
            ordinal_position=int(row["ordinal_position"]),
            default=row["column_default"],
            is_identity=row["is_identity"] == "YES",
            is_generated=row["is_generated"] != "NEVER",
        )
        for row in column_rows
    ]
    pk_rows = _fetch_dicts(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_catalog = tc.constraint_catalog
         AND kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """,
        [schema, table],
    )
    return TableInfo(schema=schema, name=table, columns=columns, primary_key=[row["column_name"] for row in pk_rows])


def _display_columns(info: TableInfo) -> list[str]:
    priority = [
        "id",
        "brand_campaign_id",
        "campaign_id",
        "name",
        "title",
        "doctor_name",
        "state",
        "status",
        "_dq_status",
        "created_at",
        "updated_at",
    ]
    column_names = [column.name for column in info.columns]
    selected = [name for name in priority if name in column_names]
    for name in column_names:
        if name not in selected:
            selected.append(name)
        if len(selected) >= 12:
            break
    return selected


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _format_value(value: Any, limit: int = 120) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if len(text) > limit:
        return f"{text[:limit - 1]}..."
    return text


def _row_identity(info: TableInfo, row: dict[str, Any]) -> str:
    if info.primary_key:
        return ", ".join(f"{key}={_format_value(row.get(key), 48)}" for key in info.primary_key)
    return f"ctid={row.get('_row_ctid')}"


def _row_locator(info: TableInfo, row: dict[str, Any]) -> dict[str, Any]:
    if info.primary_key:
        return {
            "mode": "pk",
            "values": {key: "" if row.get(key) is None else str(row.get(key)) for key in info.primary_key},
        }
    return {"mode": "ctid", "ctid": str(row["_row_ctid"])}


def _sign_locator(info: TableInfo, row: dict[str, Any]) -> str:
    return signing.dumps(
        {"schema": info.schema, "table": info.name, "locator": _row_locator(info, row)},
        salt=ROW_TOKEN_SALT,
    )


def _load_locator(schema: str, table: str, token: str) -> dict[str, Any]:
    try:
        data = signing.loads(token, salt=ROW_TOKEN_SALT)
    except signing.BadSignature as exc:
        raise Http404("Invalid row locator.") from exc
    if data.get("schema") != schema or data.get("table") != table:
        raise Http404("Row locator does not match this table.")
    locator = data.get("locator")
    if not isinstance(locator, dict) or locator.get("mode") not in {"pk", "ctid"}:
        raise Http404("Row locator is malformed.")
    return locator


def _where_clause(locator: dict[str, Any]) -> tuple[sql.SQL, list[Any]]:
    if locator["mode"] == "ctid":
        return sql.SQL("ctid = %s::tid"), [locator["ctid"]]

    values = locator.get("values") or {}
    if not values:
        raise Http404("Primary key locator has no values.")
    clauses = [sql.SQL("{}::text = %s").format(sql.Identifier(column)) for column in values.keys()]
    return sql.SQL(" AND ").join(clauses), [str(value) for value in values.values()]


def _select_row(info: TableInfo, locator: dict[str, Any], lock: bool = False) -> dict[str, Any] | None:
    where_sql, params = _where_clause(locator)
    columns_sql = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
    query = sql.SQL("SELECT ctid::text AS _row_ctid, {} FROM {}.{} WHERE {} LIMIT 1").format(
        columns_sql,
        sql.Identifier(info.schema),
        sql.Identifier(info.name),
        where_sql,
    )
    if lock:
        query += sql.SQL(" FOR UPDATE")
    rows = _fetch_dicts(query, params)
    return rows[0] if rows else None


def _editable_columns(info: TableInfo) -> list[ColumnInfo]:
    return [
        column
        for column in info.columns
        if not column.is_identity and not column.is_generated and column.name not in info.primary_key
    ]


def _creatable_columns(info: TableInfo) -> list[ColumnInfo]:
    return [column for column in info.columns if not column.is_identity and not column.is_generated]


def _form_values(request: HttpRequest, columns: list[ColumnInfo]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for column in columns:
        if request.POST.get(f"__null__{column.name}") == "1":
            values[column.name] = None
        else:
            values[column.name] = request.POST.get(column.name, "")
    return values


def _audit(action: str, info: TableInfo, locator: dict[str, Any] | None, before: dict[str, Any] | None, after: dict[str, Any] | None, reason: str, actor: str) -> None:
    _ensure_audit_table()
    _execute(
        sql.SQL(
            """
            INSERT INTO {}.{}
              (action, schema_name, table_name, row_locator, before_data, after_data, reason, actor)
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s)
            """
        ).format(sql.Identifier(AUDIT_SCHEMA), sql.Identifier(AUDIT_TABLE)),
        [
            action,
            info.schema,
            info.name,
            json.dumps(_jsonable(locator or {})),
            json.dumps(_jsonable(before or {})),
            json.dumps(_jsonable(after or {})),
            reason,
            actor,
        ],
    )


def _foreign_key_dependencies(info: TableInfo, locator: dict[str, Any]) -> list[dict[str, Any]]:
    if locator.get("mode") != "pk":
        return []

    values = locator.get("values") or {}
    rows = _fetch_dicts(
        """
        SELECT
          tc.table_schema AS ref_schema,
          tc.table_name AS ref_table,
          kcu.column_name AS ref_column,
          ccu.column_name AS source_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_catalog = tc.constraint_catalog
         AND kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_catalog = tc.constraint_catalog
         AND ccu.constraint_schema = tc.constraint_schema
         AND ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_schema = %s
          AND ccu.table_name = %s
        """,
        [info.schema, info.name],
    )
    dependencies: list[dict[str, Any]] = []
    for row in rows:
        if not _is_managed_table(row["ref_schema"], row["ref_table"]):
            continue
        source_value = values.get(row["source_column"])
        if source_value is None:
            continue
        count = _count_column_value(row["ref_schema"], row["ref_table"], row["ref_column"], source_value)
        if count:
            dependencies.append({**row, "value": source_value, "count": count, "kind": "foreign key"})
    return dependencies


def _semantic_dependency_keys(info: TableInfo, row: dict[str, Any]) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    if info.name in {"campaign_campaign", "campaign_management_campaign"}:
        if row.get("id") is not None:
            keys.append(("campaign_id", str(row["id"])))
        if row.get("brand_campaign_id"):
            keys.append(("brand_campaign_id", str(row["brand_campaign_id"])))
    elif info.name == "collateral_management_collateral" and row.get("id") is not None:
        keys.append(("collateral_id", str(row["id"])))
    elif info.name == "campaign_fieldrep":
        if row.get("id") is not None:
            keys.append(("field_rep_id", str(row["id"])))
        if row.get("brand_supplied_field_rep_id"):
            keys.append(("field_rep_id", str(row["brand_supplied_field_rep_id"])))
    elif info.name == "doctor_viewer_doctor":
        if row.get("id") is not None:
            keys.append(("doctor_master_id_resolved", str(row["id"])))
    return list(dict.fromkeys(keys))


def _column_exists(schema: str, table: str, column: str) -> bool:
    rows = _fetch_dicts(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        [schema, table, column],
    )
    return bool(rows)


def _count_column_value(schema: str, table: str, column: str, value: Any) -> int:
    rows = _fetch_dicts(
        sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{} WHERE {}::text = %s").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(column),
        ),
        [str(value)],
    )
    return int(rows[0]["row_count"])


def _semantic_dependencies(info: TableInfo, row: dict[str, Any]) -> list[dict[str, Any]]:
    keys = _semantic_dependency_keys(info, row)
    if not keys:
        return []

    dependencies: list[dict[str, Any]] = []
    for table_ref in _list_tables():
        schema = table_ref["schema"]
        table = table_ref["name"]
        if schema == info.schema and table == info.name:
            continue
        for column, value in keys:
            if not _column_exists(schema, table, column):
                continue
            count = _count_column_value(schema, table, column, value)
            if count:
                dependencies.append(
                    {
                        "ref_schema": schema,
                        "ref_table": table,
                        "ref_column": column,
                        "source_column": column,
                        "value": value,
                        "count": count,
                        "kind": "semantic reference",
                    }
                )
    return dependencies


def _delete_dependencies(info: TableInfo, row: dict[str, Any], locator: dict[str, Any]) -> list[dict[str, Any]]:
    return _foreign_key_dependencies(info, locator) + _semantic_dependencies(info, row)


def _group_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for table in tables:
        groups.setdefault(table["schema"], []).append(table)
    return [{"schema": schema, "tables": rows} for schema, rows in groups.items()]


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_login(request: HttpRequest) -> HttpResponse:
    if _is_authenticated(request):
        return redirect("internal-data-admin-home")

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = request.POST.get("password") or ""
        config = _admin_config()
        if constant_time_compare(username, config.get("USERNAME", "")) and constant_time_compare(password, config.get("PASSWORD", "")):
            request.session[SESSION_KEY] = True
            request.session[SESSION_USER_KEY] = username
            return redirect(request.GET.get("next") or "internal-data-admin-home")
        error_message = "Invalid internal dashboard credentials."

    return render(request, "dashboard/internal_data_admin/login.html", {"error_message": error_message})


@never_cache
def internal_data_admin_logout(request: HttpRequest) -> HttpResponse:
    request.session.pop(SESSION_KEY, None)
    request.session.pop(SESSION_USER_KEY, None)
    return redirect("internal-data-admin-login")


@never_cache
def internal_data_admin_home(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    _ensure_audit_table()
    return render(
        request,
        "dashboard/internal_data_admin/home.html",
        {
            "table_groups": _group_tables(_list_tables()),
            "actor": request.session.get(SESSION_USER_KEY, "internal_admin"),
        },
    )


@never_cache
def internal_data_admin_table(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    page = max(int(request.GET.get("page") or "1"), 1)
    offset = (page - 1) * PAGE_SIZE
    display_columns = _display_columns(info)
    columns_sql = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
    if info.primary_key:
        order_sql = sql.SQL(" ORDER BY {}").format(sql.SQL(", ").join(sql.Identifier(column) for column in info.primary_key))
    else:
        order_sql = sql.SQL(" ORDER BY ctid DESC")
    rows = _fetch_dicts(
        sql.SQL("SELECT ctid::text AS _row_ctid, {} FROM {}.{}{} LIMIT %s OFFSET %s").format(
            columns_sql,
            sql.Identifier(info.schema),
            sql.Identifier(info.name),
            order_sql,
        ),
        [PAGE_SIZE + 1, offset],
    )
    has_next = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]
    prepared_rows = [
        {
            "token": _sign_locator(info, row),
            "identity": _row_identity(info, row),
            "cells": [{"name": column, "value": _format_value(row.get(column), 80)} for column in display_columns],
        }
        for row in rows
    ]
    return render(
        request,
        "dashboard/internal_data_admin/table.html",
        {
            "table": info,
            "rows": prepared_rows,
            "display_columns": display_columns,
            "row_count": _table_count(schema, table),
            "page": page,
            "previous_page": page - 1 if page > 1 else None,
            "next_page": page + 1 if has_next else None,
        },
    )


@never_cache
def internal_data_admin_row(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    dependencies = _delete_dependencies(info, row, locator)
    return render(
        request,
        "dashboard/internal_data_admin/row.html",
        {
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "fields": [{"name": column.name, "value": _format_value(row.get(column.name), 600)} for column in info.columns],
            "dependencies": dependencies[:20],
            "dependency_count": len(dependencies),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_new(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    columns = _creatable_columns(info)
    if request.method == "POST":
        values = _form_values(request, columns)
        reason = (request.POST.get("reason") or "").strip()
        if len(reason) < 8:
            messages.error(request, "Please provide a clear reason before creating a record.")
        else:
            try:
                with transaction.atomic():
                    column_names = list(values.keys())
                    if column_names:
                        returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                        query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING ctid::text AS _row_ctid, {}").format(
                            sql.Identifier(info.schema),
                            sql.Identifier(info.name),
                            sql.SQL(", ").join(sql.Identifier(column) for column in column_names),
                            sql.SQL(", ").join(sql.Placeholder() for _ in column_names),
                            returning_cols,
                        )
                        inserted = _fetch_dicts(query, list(values.values()))[0]
                    else:
                        returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                        inserted = _fetch_dicts(
                            sql.SQL("INSERT INTO {}.{} DEFAULT VALUES RETURNING ctid::text AS _row_ctid, {}").format(
                                sql.Identifier(info.schema),
                                sql.Identifier(info.name),
                                returning_cols,
                            )
                        )[0]
                    locator = _row_locator(info, inserted)
                    _audit("create", info, locator, None, inserted, reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                messages.success(request, "Record created and audited.")
                return redirect(f"{reverse('internal-data-admin-row', args=[schema, table])}?token={_sign_locator(info, inserted)}")
            except DatabaseError as exc:
                messages.error(request, f"Create failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/form.html",
        {
            "mode": "Create",
            "table": info,
            "fields": [
                {
                    "name": column.name,
                    "value": request.POST.get(column.name, "") if request.method == "POST" else "",
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "is_null": request.POST.get(f"__null__{column.name}") == "1",
                    "is_pk": column.name in info.primary_key,
                }
                for column in columns
            ],
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_edit(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    columns = _editable_columns(info)
    if request.method == "POST":
        values = _form_values(request, columns)
        reason = (request.POST.get("reason") or "").strip()
        if len(reason) < 8:
            messages.error(request, "Please provide a clear reason before editing a record.")
        elif not values:
            messages.error(request, "This table has no editable columns for this record.")
        else:
            try:
                with transaction.atomic():
                    before = _select_row(info, locator, lock=True)
                    if not before:
                        raise Http404("Record no longer exists.")
                    where_sql, where_params = _where_clause(locator)
                    assignments = sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(column)) for column in values.keys()
                    )
                    returning_cols = sql.SQL(", ").join(sql.Identifier(column.name) for column in info.columns)
                    updated = _fetch_dicts(
                        sql.SQL("UPDATE {}.{} SET {} WHERE {} RETURNING ctid::text AS _row_ctid, {}").format(
                            sql.Identifier(info.schema),
                            sql.Identifier(info.name),
                            assignments,
                            where_sql,
                            returning_cols,
                        ),
                        list(values.values()) + where_params,
                    )
                    if not updated:
                        raise Http404("Record no longer exists.")
                    _audit("update", info, locator, before, updated[0], reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                messages.success(request, "Record updated and audited.")
                return redirect(f"{reverse('internal-data-admin-row', args=[schema, table])}?token={_sign_locator(info, updated[0])}")
            except DatabaseError as exc:
                messages.error(request, f"Update failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/form.html",
        {
            "mode": "Edit",
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "fields": [
                {
                    "name": column.name,
                    "value": request.POST.get(column.name, "") if request.method == "POST" else "" if row.get(column.name) is None else str(row.get(column.name)),
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "is_null": request.POST.get(f"__null__{column.name}") == "1" if request.method == "POST" else row.get(column.name) is None,
                    "is_pk": column.name in info.primary_key,
                }
                for column in columns
            ],
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_delete(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    token = request.GET.get("token") or ""
    locator = _load_locator(schema, table, token)
    row = _select_row(info, locator)
    if not row:
        raise Http404("Record not found.")

    dependencies = _delete_dependencies(info, row, locator)
    phrase = f"DELETE {schema}.{table}"
    if request.method == "POST":
        confirmation = (request.POST.get("confirmation") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        if dependencies:
            messages.error(request, "Delete blocked because dependent records still reference this row.")
        elif confirmation != phrase:
            messages.error(request, f'Type "{phrase}" to confirm this delete.')
        elif len(reason) < 8:
            messages.error(request, "Please provide a clear reason before deleting a record.")
        else:
            try:
                with transaction.atomic():
                    before = _select_row(info, locator, lock=True)
                    if not before:
                        raise Http404("Record no longer exists.")
                    latest_dependencies = _delete_dependencies(info, before, locator)
                    if latest_dependencies:
                        messages.error(request, "Delete blocked because dependent records appeared while processing.")
                    else:
                        where_sql, where_params = _where_clause(locator)
                        _execute(
                            sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                                sql.Identifier(info.schema),
                                sql.Identifier(info.name),
                                where_sql,
                            ),
                            where_params,
                        )
                        _audit("delete", info, locator, before, None, reason, request.session.get(SESSION_USER_KEY, "internal_admin"))
                        messages.success(request, "Record deleted and audited.")
                        return redirect("internal-data-admin-table", schema=schema, table=table)
            except DatabaseError as exc:
                messages.error(request, f"Delete failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/delete.html",
        {
            "table": info,
            "token": token,
            "identity": _row_identity(info, row),
            "fields": [{"name": column.name, "value": _format_value(row.get(column.name), 240)} for column in info.columns[:16]],
            "dependencies": dependencies,
            "phrase": phrase,
            "can_delete": not dependencies,
        },
    )
