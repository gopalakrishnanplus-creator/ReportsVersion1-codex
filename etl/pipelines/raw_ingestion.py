from datetime import datetime, timezone
from etl.connectors import mysql_server1, mysql_server2
from etl.connectors.postgres import execute
from etl.utils.normalization import hash_identity
from etl.utils.specs import SOURCE_TABLE_SPECS, AUDIT_COLUMNS


SCHEMA_BY_SERVER = {
    "mysql_server_1": "raw_server1",
    "mysql_server_2": "raw_server2",
}


def ensure_raw_tables() -> None:
    for server, tables in SOURCE_TABLE_SPECS.items():
        schema = SCHEMA_BY_SERVER[server]
        execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        for table, columns in tables.items():
            column_sql = ", ".join(f'"{c}" TEXT' for c in columns + AUDIT_COLUMNS)
            execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({column_sql});")


def _extract(server: str, table: str):
    if server == "mysql_server_1":
        return mysql_server1.extract_table(table)
    return mysql_server2.extract_table(table)


def ingest_raw(run_id: str) -> dict[str, int]:
    ensure_raw_tables()
    counts: dict[str, int] = {}
    extracted_at = datetime.now(timezone.utc).isoformat()
    for server, tables in SOURCE_TABLE_SPECS.items():
        schema = SCHEMA_BY_SERVER[server]
        for table, columns in tables.items():
            rows = _extract(server, table)
            inserted = 0
            for row in rows:
                source_values = [row.get(c) for c in columns]
                metadata = [
                    run_id,
                    extracted_at,
                    server,
                    table,
                    extracted_at,
                    extracted_at,
                    hash_identity(*source_values),
                    "false",
                    "PASS",
                    None,
                ]
                all_columns = columns + AUDIT_COLUMNS
                placeholders = ",".join(["%s"] * len(all_columns))
                quoted_cols = ",".join(f'"{c}"' for c in all_columns)
                execute(
                    f"INSERT INTO {schema}.{table} ({quoted_cols}) VALUES ({placeholders})",
                    source_values + metadata,
                )
                inserted += 1
            counts[f"{schema}.{table}"] = inserted
    return counts
