from __future__ import annotations

from typing import Any

import pymysql
from django.conf import settings


class MySQLExtractionError(RuntimeError):
    """Raised when source extraction from MySQL fails."""


def _connection_params(server_settings: dict[str, Any]) -> dict[str, Any]:
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


def extract_table(table: str) -> list[dict[str, Any]]:
    """Extract an entire source table from MySQL server 1."""
    try:
        with pymysql.connect(**_connection_params(settings.MYSQL_SERVER_1)) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{table}`")
                return list(cur.fetchall())
    except Exception as exc:
        host = settings.MYSQL_SERVER_1.get("HOST")
        port = settings.MYSQL_SERVER_1.get("PORT")
        user = settings.MYSQL_SERVER_1.get("USER")
        database = settings.MYSQL_SERVER_1.get("DATABASE")
        hint = (
            "Check host/port reachability, VPC/security-group access,"
            " credentials, and SSL settings (MYSQL_SERVER1_SSL_MODE / MYSQL_SERVER1_SSL_CA)."
        )
        raise MySQLExtractionError(
            f"mysql_server_1 extract failed for table '{table}' on {host}:{port} "
            f"(db={database}, user={user}): {exc}. {hint}"
        ) from exc
