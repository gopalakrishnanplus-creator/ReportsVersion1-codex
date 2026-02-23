from __future__ import annotations

from typing import Any

import pymysql
from django.conf import settings


class MySQLExtractionError(RuntimeError):
    """Raised when source extraction from MySQL fails."""


def _connection_params(server_settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "host": server_settings["HOST"],
        "port": int(server_settings["PORT"]),
        "user": server_settings["USER"],
        "password": server_settings["PASSWORD"],
        "database": server_settings["DATABASE"],
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
        "charset": "utf8mb4",
    }


def extract_table(table: str) -> list[dict[str, Any]]:
    """Extract an entire source table from MySQL server 1."""
    try:
        with pymysql.connect(**_connection_params(settings.MYSQL_SERVER_1)) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{table}`")
                return list(cur.fetchall())
    except Exception as exc:
        raise MySQLExtractionError(f"mysql_server_1 extract failed for table '{table}': {exc}") from exc
