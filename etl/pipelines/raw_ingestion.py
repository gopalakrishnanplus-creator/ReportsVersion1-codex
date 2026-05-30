import hashlib
from datetime import datetime, timezone

from etl.connectors import mysql_server1, mysql_server2
from etl.connectors.postgres import cursor, execute
from etl.utils.normalization import hash_identity
from etl.utils.specs import AUDIT_COLUMNS, SOURCE_TABLE_SPECS

SOURCE_PAYLOAD_HASH_COLUMN = "_source_payload_hash"

SCHEMA_BY_SERVER = {
    "mysql_server_1": "raw_server1",
    "mysql_server_2": "raw_server2",
}


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _payload_hash_sql(columns: list[str], placeholder: bool = False) -> str:
    parts = ["%s::text" for _ in columns] if placeholder else [f"{_quote_identifier(column)}::text" for column in columns]
    return f"md5(jsonb_build_array({', '.join(parts)})::text)"


def _raw_payload_index_name(schema: str, table: str) -> str:
    suffix = hashlib.md5(f"{schema}.{table}".encode("utf-8")).hexdigest()[:12]
    return f"raw_payload_hash_{suffix}_idx"


def ensure_raw_tables() -> None:
    for server, tables in SOURCE_TABLE_SPECS.items():
        schema = SCHEMA_BY_SERVER[server]
        execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)};")
        for table, columns in tables.items():
            column_sql = ", ".join(f"{_quote_identifier(c)} TEXT" for c in columns + AUDIT_COLUMNS)
            table_ref = _qualified_table(schema, table)
            execute(f"CREATE TABLE IF NOT EXISTS {table_ref} ({column_sql});")
            for column in columns + AUDIT_COLUMNS:
                execute(f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS {_quote_identifier(column)} TEXT;")
            execute(
                f"""
                UPDATE {table_ref}
                SET {_quote_identifier(SOURCE_PAYLOAD_HASH_COLUMN)} = {_payload_hash_sql(columns)}
                WHERE NULLIF({_quote_identifier(SOURCE_PAYLOAD_HASH_COLUMN)}, '') IS NULL
                """
            )
            execute(
                f"""
                CREATE INDEX IF NOT EXISTS {_quote_identifier(_raw_payload_index_name(schema, table))}
                ON {table_ref} ({_quote_identifier(SOURCE_PAYLOAD_HASH_COLUMN)})
                """
            )


def _extract(server: str, table: str):
    if server == "mysql_server_1":
        return mysql_server1.extract_table(table)
    return mysql_server2.extract_table(table)


def _metadata_values(run_id: str, extracted_at: str, server: str, table: str, source_values: list[object]) -> dict[str, object]:
    return {
        "_ingestion_run_id": run_id,
        "_ingested_at": extracted_at,
        "_source_server": server,
        "_source_table": table,
        "_extract_started_at": extracted_at,
        "_extract_ended_at": extracted_at,
        "_record_hash": hash_identity(*source_values),
        "_is_deleted": "false",
        "_dq_status": "PASS",
        "_dq_errors": None,
    }


def _insert_raw_row(schema: str, table: str, columns: list[str], source_values: list[object], metadata: dict[str, object]) -> bool:
    table_ref = _qualified_table(schema, table)
    all_columns = columns + AUDIT_COLUMNS
    quoted_cols = ",".join(_quote_identifier(column) for column in all_columns)
    payload_hash_expr = _payload_hash_sql(columns, placeholder=True)
    select_parts = ["%s"] * len(columns)
    params = list(source_values)

    for column in AUDIT_COLUMNS:
        if column == SOURCE_PAYLOAD_HASH_COLUMN:
            select_parts.append("payload.source_payload_hash")
        else:
            select_parts.append("%s")
            params.append(metadata.get(column))

    query = f"""
        WITH payload AS (
            SELECT {payload_hash_expr} AS source_payload_hash
        )
        INSERT INTO {table_ref} ({quoted_cols})
        SELECT {", ".join(select_parts)}
        FROM payload
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table_ref} existing
            WHERE existing.{_quote_identifier(SOURCE_PAYLOAD_HASH_COLUMN)} = payload.source_payload_hash
        )
    """
    with cursor() as cur:
        cur.execute(query, list(source_values) + params)
        return cur.rowcount > 0


def ingest_raw(run_id: str) -> dict[str, object]:
    ensure_raw_tables()
    counts: dict[str, int] = {}
    skipped_counts: dict[str, int] = {}
    extracted_counts: dict[str, int] = {}
    table_errors: dict[str, str] = {}
    extracted_at = datetime.now(timezone.utc).isoformat()

    for server, tables in SOURCE_TABLE_SPECS.items():
        schema = SCHEMA_BY_SERVER[server]
        for table, columns in tables.items():
            table_key = f"{schema}.{table}"
            try:
                rows = _extract(server, table)
            except Exception as exc:
                counts[table_key] = 0
                table_errors[table_key] = str(exc)
                continue

            inserted = 0
            skipped = 0
            for row in rows:
                source_values = [row.get(c) for c in columns]
                metadata = _metadata_values(run_id, extracted_at, server, table, source_values)
                if _insert_raw_row(schema, table, columns, source_values, metadata):
                    inserted += 1
                else:
                    skipped += 1

            counts[table_key] = inserted
            skipped_counts[table_key] = skipped
            extracted_counts[table_key] = len(rows)

    return {
        "counts": counts,
        "skipped_counts": skipped_counts,
        "extracted_counts": extracted_counts,
        "errors": table_errors,
    }
