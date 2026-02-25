from etl.connectors.postgres import execute
from etl.utils.specs import SOURCE_TABLE_SPECS, AUDIT_COLUMNS


def ensure_bronze_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    for tables in SOURCE_TABLE_SPECS.values():
        for table, columns in tables.items():
            bronze_cols = columns + AUDIT_COLUMNS + ["_bronze_deduped_at", "_bronze_source_raw_ingested_at"]
            column_sql = ", ".join(f'"{c}" TEXT' for c in bronze_cols)
            execute(f"CREATE TABLE IF NOT EXISTS bronze.{table} ({column_sql});")


def _dedup_order_expression(columns: list[str]) -> str:
    """Build a safe ORDER BY expression for dedup without referencing missing columns."""
    precedence: list[str] = []
    if "updated_at" in columns:
        precedence.append('"updated_at"')
    if "created_at" in columns:
        precedence.append('"created_at"')
    precedence.append('"_ingested_at"')
    return f"COALESCE({', '.join(precedence)}) DESC, _record_hash DESC"


def build_bronze() -> None:
    ensure_bronze_tables()
    execute("CREATE SCHEMA IF NOT EXISTS ops;")
    execute(
        """
        CREATE TABLE IF NOT EXISTS ops.exclusion_rules (
            rule_id TEXT PRIMARY KEY,
            rule_type TEXT,
            rule_value TEXT,
            is_enabled TEXT,
            created_at TEXT,
            updated_at TEXT,
            notes TEXT
        );
        """
    )
    for server, tables in SOURCE_TABLE_SPECS.items():
        raw_schema = "raw_server1" if server == "mysql_server_1" else "raw_server2"
        for table, columns in tables.items():
            execute(f"TRUNCATE TABLE bronze.{table};")
            base_cols = ",".join(f'"{c}"' for c in columns + AUDIT_COLUMNS)
            dedup_order = _dedup_order_expression(columns)
            execute(
                f"""
                INSERT INTO bronze.{table} ({base_cols}, _bronze_deduped_at, _bronze_source_raw_ingested_at)
                SELECT {base_cols}, NOW()::text, "_ingested_at"
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY COALESCE("id", _record_hash)
                            ORDER BY {dedup_order}
                        ) AS rn
                    FROM {raw_schema}.{table}
                ) q
                WHERE rn = 1
                """
            )

    execute(
        """
        DELETE FROM bronze.sharing_management_collateraltransaction
        WHERE COALESCE(LOWER("brand_campaign_id"), '') LIKE '%test%'
           OR COALESCE("brand_campaign_id", '') = '';
        """
    )
