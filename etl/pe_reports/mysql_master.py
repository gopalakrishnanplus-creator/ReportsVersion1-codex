from __future__ import annotations

from typing import Any

import pymysql
from django.conf import settings

from etl.pe_reports.utils import clean_text


class MasterMySQLExtractionError(RuntimeError):
    """Raised when PE master extraction fails."""


def _connection_params() -> dict[str, Any]:
    server_settings = settings.PE_MASTER_MYSQL
    params: dict[str, Any] = {
        "host": server_settings["HOST"],
        "port": int(server_settings["PORT"]),
        "user": server_settings["USER"],
        "password": server_settings["PASSWORD"],
        "database": server_settings["DATABASE"],
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
        "charset": "utf8mb4",
        "connect_timeout": int(server_settings.get("CONNECT_TIMEOUT", 10)),
        "read_timeout": int(server_settings.get("READ_TIMEOUT", 60)),
        "write_timeout": int(server_settings.get("WRITE_TIMEOUT", 60)),
    }
    ssl_mode = str(server_settings.get("SSL_MODE", "")).strip().lower()
    if ssl_mode in {"required", "verify_ca", "verify_identity"}:
        ssl_cfg: dict[str, Any] = {}
        ssl_ca = server_settings.get("SSL_CA")
        if ssl_ca:
            ssl_cfg["ca"] = ssl_ca
        params["ssl"] = ssl_cfg or {"check_hostname": ssl_mode == "verify_identity"}
    return params


def extract_rows(table: str, columns: list[str], watermark_field: str | None = None, watermark_start: str | None = None) -> list[dict[str, Any]]:
    try:
        rows: list[dict[str, Any]] = []
        with pymysql.connect(**_connection_params()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW COLUMNS FROM `{table}`")
                available_columns = {str(item["Field"]) for item in cursor.fetchall()}
                if not available_columns:
                    return []

                selected_columns = [column for column in columns if column in available_columns]
                missing_columns = [column for column in columns if column not in available_columns]
                if not selected_columns:
                    raise MasterMySQLExtractionError(f"None of the requested columns exist on table '{table}'")

                query = f"SELECT {', '.join(f'`{column}`' for column in selected_columns)} FROM `{table}`"
                params: list[Any] = []
                if watermark_field and clean_text(watermark_start) and watermark_field in available_columns:
                    query += f" WHERE `{watermark_field}` >= %s"
                    params.append(watermark_start)

                cursor.execute(query, params)
                while True:
                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    if missing_columns:
                        for row in batch:
                            for column in missing_columns:
                                row[column] = None
                    rows.extend(batch)
        return rows
    except Exception as exc:
        cfg = settings.PE_MASTER_MYSQL
        raise MasterMySQLExtractionError(
            f"PE master extract failed for table '{table}' on {cfg.get('HOST')}:{cfg.get('PORT')} "
            f"(db={cfg.get('DATABASE')}, user={cfg.get('USER')}): {exc}"
        ) from exc
