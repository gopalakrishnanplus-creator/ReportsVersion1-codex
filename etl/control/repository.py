from etl.connectors.postgres import execute


def ensure_control_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS control;")
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.etl_run_log (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            ended_at TEXT,
            status TEXT,
            trigger_type TEXT,
            notes TEXT
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.etl_step_log (
            run_id TEXT,
            step_name TEXT,
            source_table TEXT,
            started_at TEXT,
            ended_at TEXT,
            rows_read TEXT,
            rows_written TEXT,
            rows_rejected TEXT,
            status TEXT,
            error_message TEXT
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.etl_watermark (
            source_system TEXT,
            source_table TEXT,
            watermark_column TEXT,
            watermark_value TEXT,
            last_success_run_id TEXT,
            last_success_at TEXT,
            lookback_window_days TEXT,
            is_enabled TEXT,
            PRIMARY KEY (source_system, source_table)
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS control.dq_issue_log (
            run_id TEXT,
            layer TEXT,
            table_name TEXT,
            issue_type TEXT,
            issue_count TEXT,
            issue_sample TEXT,
            created_at TEXT
        );
        """
    )


def log_run(run_id: str, status: str, trigger_type: str = "manual", notes: str = "") -> None:
    execute(
        """
        INSERT INTO control.etl_run_log (run_id, started_at, ended_at, status, trigger_type, notes)
        VALUES (%s, NOW()::text, NOW()::text, %s, %s, %s)
        ON CONFLICT (run_id)
        DO UPDATE SET ended_at=NOW()::text, status=EXCLUDED.status, notes=EXCLUDED.notes
        """,
        [run_id, status, trigger_type, notes],
    )
