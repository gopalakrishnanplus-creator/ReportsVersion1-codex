from __future__ import annotations

import json
import re
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
LAYER_ORDER = {
    "raw": 1,
    "bronze": 2,
    "silver": 3,
    "gold": 4,
}
CLEANUP_SYSTEM_KEYS = ("inclinic", "sapa", "pe")
CLEANUP_LAYER_OPTIONS = [
    {"key": "raw", "label": "RAW and everything downstream"},
    {"key": "bronze", "label": "BRONZE, SILVER, and GOLD"},
    {"key": "silver", "label": "SILVER and GOLD"},
    {"key": "gold", "label": "GOLD only"},
]
LAYER_LABELS = {
    "raw": "RAW",
    "bronze": "BRONZE",
    "silver": "SILVER",
    "gold": "GOLD",
}
REGISTRY_TABLES = {
    "inclinic": [("gold_global", "campaign_registry")],
    "sapa": [("gold_sapa", "campaign_registry"), ("gold_sapa", "dim_campaign")],
    "pe": [("gold_pe_global", "campaign_registry")],
}
REGISTRY_MATCH_COLUMNS = [
    "brand_campaign_id",
    "campaign_id",
    "campaign_id_resolved",
    "pe_campaign_id",
    "rfa_campaign_id",
    "source_campaign_id",
    "master_campaign_id",
    "campaign_identifier",
    "campaign_key",
    "gold_schema_name",
    "schema_name",
    "campaign_schema",
]
REGISTRY_SCHEMA_COLUMNS = {"gold_schema_name", "schema_name", "campaign_schema"}
CLEANUP_MATCH_COLUMNS = [
    "brand_campaign_id",
    "campaign_id",
    "campaign_id_resolved",
    "pe_campaign_id",
    "rfa_campaign_id",
    "source_campaign_id",
    "master_campaign_id",
    "campaign_identifier",
    "campaign_key",
    "campaign_code",
    "campaign_uuid",
]


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


@dataclass(frozen=True)
class SystemProfile:
    key: str
    label: str
    short_label: str
    description: str
    cleanup_summary: str
    source_guidance: str
    cleanup_steps: list[str]


SYSTEM_PROFILES = {
    "inclinic": SystemProfile(
        key="inclinic",
        label="Inclinic Reporting",
        short_label="Inclinic",
        description="In-clinic sharing campaign data from MySQL source copies through RAW, BRONZE, SILVER, and campaign GOLD schemas.",
        cleanup_summary="Usually delete source/raw rows first, then rerun Inclinic ETL. Do not chase every derived row unless you are clearing stale report output before a rebuild.",
        source_guidance="Start with raw_server2 campaign, collateral, share, and transaction tables; use raw_server1 only for campaign and field-rep mapping rows.",
        cleanup_steps=[
            "Remove campaign identity rows from raw_server2.campaign_management_campaign and raw_server1.campaign_campaign.",
            "Remove engagement rows from raw_server2.sharing_management_collateraltransaction and raw_server2.sharing_management_sharelog.",
            "Remove campaign collateral bridge rows before collateral rows when the collateral belongs only to that dummy campaign.",
            "Rerun the Inclinic ETL so bronze, silver, gold_global, and gold_campaign_* rebuild from the cleaned source state.",
        ],
    ),
    "sapa": SystemProfile(
        key="sapa",
        label="SAPA / RFA",
        short_label="SAPA/RFA",
        description="Red Flag Alert and SAPA Growth data across raw_sapa_*, bronze_sapa, silver_sapa, and gold_sapa reporting tables.",
        cleanup_summary="Prefer deleting seeded/source rows from raw_sapa_mysql or raw_sapa_api and then rerunning the SAPA pipeline.",
        source_guidance="Use raw_sapa_mysql for campaign, doctor, clinic, screening, follow-up, reminder, and metric-event records; use raw_sapa_api for WordPress webinar/course/video fixture rows.",
        cleanup_steps=[
            "Delete test doctors, clinic mappings, and campaign rows from raw_sapa_mysql before derived SAPA tables.",
            "Delete matching WordPress fixture records from raw_sapa_api when course/webinar/video data was test-only.",
            "Rerun the SAPA/RFA ETL so bronze_sapa, silver_sapa, and gold_sapa are republished consistently.",
            "Delete directly from gold_sapa only when you need to clear stale dashboard output before a full rebuild.",
        ],
    ),
    "pe": SystemProfile(
        key="pe",
        label="Patient Education",
        short_label="PE",
        description="Patient Education campaign data across raw_pe_*, bronze_pe, silver_pe, gold_pe_global, and gold_pe_campaign_* schemas.",
        cleanup_summary="Prefer deleting raw PE campaign/share/playback rows and rerunning PE ETL; derived PE schemas should normally be rebuilt.",
        source_guidance="Use raw_pe_master for campaign, enrollment, doctor, brand, field-rep, trigger, and catalog records; use raw_pe_portal for share, playback, and banner-click activity.",
        cleanup_steps=[
            "Remove campaign and enrollment records from raw_pe_master when retiring a dummy PE campaign.",
            "Remove share, playback, and banner-click activity from raw_pe_portal for that test campaign.",
            "Only delete catalog/video/bundle records when they were created exclusively for the dummy campaign.",
            "Rerun PE ETL so bronze_pe, silver_pe, gold_pe_global, and gold_pe_campaign_* are rebuilt safely.",
        ],
    ),
    "shared": SystemProfile(
        key="shared",
        label="Shared Ops / Control",
        short_label="Shared",
        description="Operational tables used by multiple pipelines, including ETL run logs, rules, and dashboard audit records.",
        cleanup_summary="These tables control or describe pipeline behavior. Clean logs and rules deliberately; do not delete audit history casually.",
        source_guidance="Use control tables for run-log cleanup and ops tables for rules/configuration cleanup.",
        cleanup_steps=[
            "Delete old control.etl_run_log rows only when historical troubleshooting data is no longer needed.",
            "Delete or disable ops rules only after confirming the relevant pipeline no longer depends on them.",
            "The internal dashboard audit table is hidden from CRUD to preserve mutation history.",
        ],
    ),
}


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
    return schema.startswith(("raw_", "bronze_", "silver_", "gold_"))


def _is_managed_table(schema: str, table: str) -> bool:
    return _is_relevant_schema(schema) and not (schema == AUDIT_SCHEMA and table == AUDIT_TABLE)


def _system_key_for_schema(schema: str) -> str:
    if schema in {"raw_server1", "raw_server2", "bronze", "silver", "gold_global"} or schema.startswith("gold_campaign_"):
        return "inclinic"
    if schema in {"raw_sapa_mysql", "raw_sapa_api", "bronze_sapa", "silver_sapa", "gold_sapa", "gold_sapa_stage"}:
        return "sapa"
    if schema in {"raw_pe_master", "raw_pe_portal", "bronze_pe", "silver_pe", "gold_pe_global"} or schema.startswith("gold_pe_campaign_"):
        return "pe"
    if schema in {"control", "ops"}:
        return "shared"
    return "shared"


def _system_profile_for_schema(schema: str) -> SystemProfile:
    return SYSTEM_PROFILES[_system_key_for_schema(schema)]


def _layer_for_schema(schema: str) -> str:
    if schema.startswith("raw_") or schema in {"raw_server1", "raw_server2"}:
        return "RAW source copy"
    if schema.startswith("bronze") or schema == "bronze":
        return "BRONZE derived"
    if schema.startswith("silver") or schema == "silver":
        return "SILVER derived"
    if schema.startswith("gold"):
        return "GOLD report output"
    if schema == "control":
        return "CONTROL run metadata"
    if schema == "ops":
        return "OPS rules/config"
    return "Reporting table"


def _layer_key_for_schema(schema: str) -> str:
    if schema.startswith("raw_") or schema in {"raw_server1", "raw_server2"}:
        return "raw"
    if schema.startswith("bronze") or schema == "bronze":
        return "bronze"
    if schema.startswith("silver") or schema == "silver":
        return "silver"
    if schema.startswith("gold"):
        return "gold"
    return "other"


def _table_cleanup_note(info: TableInfo) -> str:
    layer = _layer_for_schema(info.schema)
    if layer == "RAW source copy":
        return "Best cleanup starting point. Delete dummy/test/source-removed records here, then rerun the matching ETL so derived layers regenerate."
    if layer in {"BRONZE derived", "SILVER derived", "GOLD report output"}:
        return "Derived table. You usually do not need to delete here table by table; clean RAW/source rows and rerun ETL. Direct delete is for emergency stale or corrupt derived output only."
    if info.schema == "control":
        return "Run metadata. Safe to prune old logs when no longer needed, but this does not remove campaign data."
    if info.schema == "ops":
        return "Operational rules/configuration. Deleting here can change future pipeline behavior, so use a clear reason."
    return "Review dependencies before changing this table."


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


def _managed_table_refs() -> list[dict[str, str]]:
    raw_tables = _fetch_dicts(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    )
    refs: list[dict[str, str]] = []
    for row in raw_tables:
        schema = row["table_schema"]
        table = row["table_name"]
        if _is_managed_table(schema, table):
            refs.append({"schema": schema, "name": table})
    return refs


def _list_tables() -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for row in _managed_table_refs():
        schema = row["schema"]
        table = row["name"]
        system = _system_profile_for_schema(schema)
        tables.append(
            {
                "schema": schema,
                "name": table,
                "row_count": _table_count(schema, table),
                "href": reverse("internal-data-admin-table", args=[schema, table]),
                "system_key": system.key,
                "system_label": system.short_label,
                "layer": _layer_for_schema(schema),
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


def _system_cards(tables: list[dict[str, Any]], selected_system: str) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for profile in SYSTEM_PROFILES.values():
        profile_tables = [table for table in tables if table["system_key"] == profile.key]
        cards.append(
            {
                "profile": profile,
                "table_count": len(profile_tables),
                "row_count": sum(table["row_count"] or 0 for table in profile_tables),
                "is_selected": selected_system == profile.key,
                "href": f"{reverse('internal-data-admin-home')}?system={profile.key}",
            }
        )
    return cards


def _selected_system(request: HttpRequest) -> str:
    requested = (request.GET.get("system") or "all").strip().lower()
    if requested in SYSTEM_PROFILES or requested == "all":
        return requested
    return "all"


def _current_system_context(schema: str) -> dict[str, Any]:
    profile = _system_profile_for_schema(schema)
    return {
        "profile": profile,
        "layer": _layer_for_schema(schema),
        "home_href": f"{reverse('internal-data-admin-home')}?system={profile.key}",
    }


def _load_selected_rows(info: TableInfo, tokens: list[str]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for token in tokens:
        if not token or token in seen:
            continue
        seen.add(token)
        locator = _load_locator(info.schema, info.name, token)
        row = _select_row(info, locator)
        if not row:
            continue
        dependencies = _delete_dependencies(info, row, locator)
        selected.append(
            {
                "token": token,
                "locator": locator,
                "row": row,
                "identity": _row_identity(info, row),
                "dependencies": dependencies,
                "dependency_count": len(dependencies),
            }
        )
    return selected


def _bulk_delete_selected(info: TableInfo, selected_rows: list[dict[str, Any]], reason: str, actor: str) -> int:
    deleted_count = 0
    with transaction.atomic():
        for selected in selected_rows:
            before = _select_row(info, selected["locator"], lock=True)
            if not before:
                continue
            latest_dependencies = _delete_dependencies(info, before, selected["locator"])
            if latest_dependencies:
                raise DatabaseError(f"Delete blocked for {selected['identity']} because dependencies appeared while processing.")
            where_sql, where_params = _where_clause(selected["locator"])
            _execute(
                sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                    sql.Identifier(info.schema),
                    sql.Identifier(info.name),
                    where_sql,
                ),
                where_params,
            )
            _audit("bulk_delete", info, selected["locator"], before, None, reason, actor)
            deleted_count += 1
    return deleted_count


def _cleanup_system_options() -> list[dict[str, str]]:
    return [
        {
            "key": key,
            "label": SYSTEM_PROFILES[key].label,
            "summary": SYSTEM_PROFILES[key].cleanup_summary,
        }
        for key in CLEANUP_SYSTEM_KEYS
    ]


def _cleanup_layer_option_label(layer_key: str) -> str:
    for option in CLEANUP_LAYER_OPTIONS:
        if option["key"] == layer_key:
            return option["label"]
    return LAYER_LABELS.get(layer_key, layer_key.upper())


def _dedupe_text_values(values: list[Any] | tuple[Any, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _normalize_cleanup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _cleanup_value_variants(values: list[str]) -> list[str]:
    variants: list[str] = []
    for value in values:
        text = value.strip()
        if not text:
            continue
        compact = re.sub(r"\s+", "", text)
        underscored = re.sub(r"[\s-]+", "_", text)
        dashed = re.sub(r"[\s_]+", "-", text)
        variants.extend(
            [
                text,
                compact,
                underscored,
                dashed,
                text.replace("-", "_"),
                text.replace("_", "-"),
                _normalize_cleanup_key(text),
            ]
        )
    return [value.lower() for value in _dedupe_text_values(variants)]


def _text_match_condition(columns: list[str], match_values: list[str]) -> tuple[sql.SQL, list[Any]]:
    clauses = [
        sql.SQL("LOWER(BTRIM({}::text)) = ANY(%s)").format(sql.Identifier(column))
        for column in columns
    ]
    return sql.SQL("({})").format(sql.SQL(" OR ").join(clauses)), [match_values for _ in columns]


def _registry_rows_for_entity(schema: str, table: str, match_values: list[str]) -> list[dict[str, Any]]:
    if not _table_exists(schema, table):
        return []
    info = _table_info(schema, table)
    column_names = [column.name for column in info.columns]
    match_columns = [column for column in REGISTRY_MATCH_COLUMNS if column in column_names]
    if not match_columns:
        return []

    select_columns = _dedupe_text_values(match_columns + [column for column in REGISTRY_SCHEMA_COLUMNS if column in column_names])
    where_sql, params = _text_match_condition(match_columns, match_values)
    return _fetch_dicts(
        sql.SQL("SELECT {} FROM {}.{} WHERE {} LIMIT 50").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in select_columns),
            sql.Identifier(schema),
            sql.Identifier(table),
            where_sql,
        ),
        params,
    )


def _campaign_gold_schema_prefixes(system_key: str) -> tuple[str, ...]:
    if system_key == "inclinic":
        return ("gold_campaign_",)
    if system_key == "pe":
        return ("gold_pe_campaign_",)
    return ()


def _matching_campaign_gold_schemas(system_key: str, values: list[str]) -> list[str]:
    prefixes = _campaign_gold_schema_prefixes(system_key)
    if not prefixes:
        return []

    normalized_values = {_normalize_cleanup_key(value) for value in values if value}
    rows = _fetch_dicts(
        """
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema
        """
    )
    schemas: list[str] = []
    for row in rows:
        schema = row["table_schema"]
        for prefix in prefixes:
            if not schema.startswith(prefix):
                continue
            suffix = schema[len(prefix):]
            if _normalize_cleanup_key(schema) in normalized_values or _normalize_cleanup_key(suffix) in normalized_values:
                schemas.append(schema)
    return schemas


def _cleanup_entity_scope(system_key: str, entity_key: str) -> dict[str, Any]:
    display_values = _dedupe_text_values([entity_key])
    scoped_gold_schemas: list[str] = []
    registry_sources: list[str] = []
    match_values = _cleanup_value_variants(display_values)

    for schema, table in REGISTRY_TABLES.get(system_key, []):
        rows = _registry_rows_for_entity(schema, table, match_values)
        if not rows:
            continue
        registry_sources.append(f"{schema}.{table}")
        for row in rows:
            for column, value in row.items():
                if value is None:
                    continue
                display_values = _dedupe_text_values(display_values + [value])
                if column in REGISTRY_SCHEMA_COLUMNS:
                    scoped_gold_schemas = _dedupe_text_values(scoped_gold_schemas + [value])
        match_values = _cleanup_value_variants(display_values)

    scoped_gold_schemas = _dedupe_text_values(
        scoped_gold_schemas + _matching_campaign_gold_schemas(system_key, display_values + scoped_gold_schemas)
    )
    display_values = _dedupe_text_values(display_values + scoped_gold_schemas)

    return {
        "display_values": display_values,
        "match_values": _cleanup_value_variants(display_values),
        "scoped_gold_schemas": scoped_gold_schemas,
        "registry_sources": registry_sources,
    }


def _cleanup_candidate_columns(info: TableInfo) -> list[str]:
    column_names = [column.name for column in info.columns]
    selected = [column for column in CLEANUP_MATCH_COLUMNS if column in column_names]

    for column in column_names:
        lowered = column.lower()
        if column in selected:
            continue
        if "campaign" in lowered and any(marker in lowered for marker in ("id", "key", "code", "uuid")):
            selected.append(column)

    campaign_table = "campaign" in info.name.lower() or info.name.lower() in {"campaign_registry", "dim_campaign"}
    if campaign_table:
        for column in ("underlying_key", "id"):
            if column in column_names and column not in selected:
                selected.append(column)

    return selected


def _cleanup_match_condition(info: TableInfo, scope: dict[str, Any]) -> dict[str, Any] | None:
    scoped_gold_schemas = set(scope["scoped_gold_schemas"])
    if info.schema in scoped_gold_schemas and info.schema.startswith(_campaign_gold_schema_prefixes(_system_key_for_schema(info.schema))):
        return {
            "where_sql": sql.SQL("TRUE"),
            "params": [],
            "match_columns": [],
            "scope_note": "entire matched campaign GOLD schema",
        }

    match_columns = _cleanup_candidate_columns(info)
    match_values = scope["match_values"]
    if not match_columns or not match_values:
        return None

    where_sql, params = _text_match_condition(match_columns, match_values)
    return {
        "where_sql": where_sql,
        "params": params,
        "match_columns": match_columns,
        "scope_note": "campaign/entity key columns",
    }


def _cleanup_count(info: TableInfo, where_sql: sql.SQL, params: list[Any]) -> int:
    rows = _fetch_dicts(
        sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{} WHERE {}").format(
            sql.Identifier(info.schema),
            sql.Identifier(info.name),
            where_sql,
        ),
        params,
    )
    return int(rows[0]["row_count"])


def _cleanup_confirmation_phrase(system_key: str, entity_key: str) -> str:
    return f"CLEANUP {system_key.upper()} {entity_key.strip()}"


def _cleanup_plan(system_key: str, start_layer: str, entity_key: str) -> dict[str, Any]:
    entity_key = entity_key.strip()
    if system_key not in CLEANUP_SYSTEM_KEYS:
        raise ValueError("Choose Inclinic, SAPA/RFA, or Patient Education before planning cleanup.")
    if start_layer not in LAYER_ORDER:
        raise ValueError("Choose a valid starting layer for cleanup.")
    if not entity_key:
        raise ValueError("Enter a campaign, schema, or source entity key before planning cleanup.")

    start_rank = LAYER_ORDER[start_layer]
    scope = _cleanup_entity_scope(system_key, entity_key)
    plan_rows: list[dict[str, Any]] = []

    for ref in _managed_table_refs():
        schema = ref["schema"]
        if _system_key_for_schema(schema) != system_key:
            continue
        layer_key = _layer_key_for_schema(schema)
        layer_rank = LAYER_ORDER.get(layer_key)
        if layer_rank is None or layer_rank < start_rank:
            continue

        info = _table_info(schema, ref["name"])
        match = _cleanup_match_condition(info, scope)
        if not match:
            continue

        count = _cleanup_count(info, match["where_sql"], match["params"])
        if count <= 0:
            continue

        plan_rows.append(
            {
                "schema": info.schema,
                "table": info.name,
                "layer_key": layer_key,
                "layer_label": LAYER_LABELS[layer_key],
                "delete_rank": layer_rank,
                "count": count,
                "match_columns": ", ".join(match["match_columns"]) if match["match_columns"] else "entire table",
                "scope_note": match["scope_note"],
                "where_sql": match["where_sql"],
                "params": match["params"],
            }
        )

    plan_rows.sort(key=lambda row: (-row["delete_rank"], row["schema"], row["table"]))
    layer_totals = []
    for layer_key in ("gold", "silver", "bronze", "raw"):
        total = sum(row["count"] for row in plan_rows if row["layer_key"] == layer_key)
        if total:
            layer_totals.append({"label": LAYER_LABELS[layer_key], "count": total})

    return {
        "system_key": system_key,
        "system_label": SYSTEM_PROFILES[system_key].label,
        "start_layer": start_layer,
        "start_layer_label": _cleanup_layer_option_label(start_layer),
        "entity_key": entity_key,
        "key_values": scope["display_values"],
        "scoped_gold_schemas": scope["scoped_gold_schemas"],
        "registry_sources": scope["registry_sources"],
        "rows": plan_rows,
        "table_count": len(plan_rows),
        "total_count": sum(row["count"] for row in plan_rows),
        "layer_totals": layer_totals,
    }


def _execute_hierarchy_cleanup(plan: dict[str, Any], reason: str, actor: str) -> dict[str, Any]:
    deleted_rows: list[dict[str, Any]] = []
    with transaction.atomic():
        for row in plan["rows"]:
            info = TableInfo(schema=row["schema"], name=row["table"], columns=[], primary_key=[])
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                        sql.Identifier(row["schema"]),
                        sql.Identifier(row["table"]),
                        row["where_sql"],
                    ),
                    row["params"],
                )
                deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0

            locator = {
                "mode": "hierarchy_cleanup",
                "system": plan["system_key"],
                "start_layer": plan["start_layer"],
                "entity_key": plan["entity_key"],
            }
            before = {
                "planned_count": row["count"],
                "deleted_count": deleted_count,
                "layer": row["layer_label"],
                "match_columns": row["match_columns"],
                "scope_note": row["scope_note"],
                "key_values": plan["key_values"],
                "scoped_gold_schemas": plan["scoped_gold_schemas"],
            }
            _audit("hierarchy_cleanup", info, locator, before, None, reason, actor)
            deleted_rows.append({**row, "deleted_count": deleted_count})

    return {
        "deleted_count": sum(row["deleted_count"] for row in deleted_rows),
        "table_count": len(deleted_rows),
        "rows": deleted_rows,
    }


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
    all_tables = _list_tables()
    selected_system = _selected_system(request)
    visible_tables = (
        all_tables
        if selected_system == "all"
        else [table for table in all_tables if table["system_key"] == selected_system]
    )
    selected_profile = SYSTEM_PROFILES.get(selected_system)
    return render(
        request,
        "dashboard/internal_data_admin/home.html",
        {
            "system_cards": _system_cards(all_tables, selected_system),
            "selected_system": selected_system,
            "selected_profile": selected_profile,
            "table_groups": _group_tables(visible_tables),
            "actor": request.session.get(SESSION_USER_KEY, "internal_admin"),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_cleanup(request: HttpRequest) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    _ensure_audit_table()
    values = request.POST if request.method == "POST" else request.GET
    selected_system = (values.get("system") or "inclinic").strip().lower()
    if selected_system not in CLEANUP_SYSTEM_KEYS:
        selected_system = "inclinic"
    selected_start_layer = (values.get("start_layer") or "raw").strip().lower()
    if selected_start_layer not in LAYER_ORDER:
        selected_start_layer = "raw"
    entity_key = (values.get("entity_key") or "").strip()

    plan = None
    phrase = _cleanup_confirmation_phrase(selected_system, entity_key) if entity_key else ""
    if request.method == "POST":
        cleanup_action = request.POST.get("cleanup_action") or "preview"
        try:
            plan = _cleanup_plan(selected_system, selected_start_layer, entity_key)
            phrase = _cleanup_confirmation_phrase(selected_system, entity_key)
        except (DatabaseError, ValueError) as exc:
            messages.error(request, f"Cleanup plan failed: {exc}")
            plan = None

        if cleanup_action == "execute" and plan:
            confirmation = (request.POST.get("confirmation") or "").strip()
            reason = (request.POST.get("reason") or "").strip()
            if plan["total_count"] <= 0:
                messages.error(request, "Cleanup execution blocked because the current plan has no matching records.")
            elif confirmation != phrase:
                messages.error(request, f'Type "{phrase}" to confirm hierarchy cleanup.')
            elif len(reason) < 8:
                messages.error(request, "Please provide a clear reason before running hierarchy cleanup.")
            else:
                try:
                    result = _execute_hierarchy_cleanup(
                        plan,
                        reason,
                        request.session.get(SESSION_USER_KEY, "internal_admin"),
                    )
                    messages.success(
                        request,
                        f"Hierarchy cleanup deleted {result['deleted_count']} records across {result['table_count']} tables.",
                    )
                    return redirect(
                        f"{reverse('internal-data-admin-cleanup')}?system={selected_system}&start_layer={selected_start_layer}"
                    )
                except DatabaseError as exc:
                    messages.error(request, f"Hierarchy cleanup failed and was rolled back: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/cleanup.html",
        {
            "system_options": _cleanup_system_options(),
            "layer_options": CLEANUP_LAYER_OPTIONS,
            "selected_system": selected_system,
            "selected_start_layer": selected_start_layer,
            "entity_key": entity_key,
            "plan": plan,
            "phrase": phrase,
        },
    )


@never_cache
def internal_data_admin_table(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
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
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
            "bulk_phrase": f"DELETE SELECTED {schema}.{table}",
        },
    )


@never_cache
def internal_data_admin_row(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
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
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def internal_data_admin_new(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
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
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
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
    system_context = _current_system_context(schema)
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
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
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
    system_context = _current_system_context(schema)
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
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )


@never_cache
@require_http_methods(["POST"])
def internal_data_admin_bulk_delete(request: HttpRequest, schema: str, table: str) -> HttpResponse:
    auth_redirect = _require_auth(request)
    if auth_redirect:
        return auth_redirect

    info = _table_info(schema, table)
    system_context = _current_system_context(schema)
    tokens = request.POST.getlist("row_token")
    selected_rows = _load_selected_rows(info, tokens)
    phrase = f"DELETE SELECTED {schema}.{table}"

    if not selected_rows:
        messages.error(request, "Select at least one record before using bulk delete.")
        return redirect("internal-data-admin-table", schema=schema, table=table)

    blocked_count = sum(1 for row in selected_rows if row["dependencies"])
    if request.POST.get("bulk_action") == "delete":
        confirmation = (request.POST.get("confirmation") or "").strip()
        reason = (request.POST.get("reason") or "").strip()
        if blocked_count:
            messages.error(request, "Bulk delete blocked because one or more selected records have dependencies.")
        elif confirmation != phrase:
            messages.error(request, f'Type "{phrase}" to confirm bulk delete.')
        elif len(reason) < 8:
            messages.error(request, "Please provide a clear reason before bulk deleting records.")
        else:
            try:
                deleted_count = _bulk_delete_selected(
                    info,
                    selected_rows,
                    reason,
                    request.session.get(SESSION_USER_KEY, "internal_admin"),
                )
                messages.success(request, f"Bulk deleted {deleted_count} records and audited each deletion.")
                return redirect("internal-data-admin-table", schema=schema, table=table)
            except DatabaseError as exc:
                messages.error(request, f"Bulk delete failed: {exc}")

    return render(
        request,
        "dashboard/internal_data_admin/bulk_delete.html",
        {
            "table": info,
            "selected_rows": selected_rows,
            "selected_count": len(selected_rows),
            "blocked_count": blocked_count,
            "phrase": phrase,
            "can_delete": blocked_count == 0,
            "system_context": system_context,
            "cleanup_note": _table_cleanup_note(info),
        },
    )
