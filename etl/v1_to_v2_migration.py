from __future__ import annotations

import csv
import hashlib
import json
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable

from django.db import transaction

from etl.pe_reports.storage import fetch_all, qident, replace_table


RAW_V2_AUDIT_SCHEMA = "raw_v2_audit"
RAW_V2_AUDIT_TABLE = "v1_raw_row_migration_audit_v2"

V2_SOURCE_SCHEMAS = {
    "raw_v2_master",
    "raw_v2_inclinic",
    "raw_v2_pe_portal",
    RAW_V2_AUDIT_SCHEMA,
}

AUDIT_COLUMNS = [
    "run_id",
    "migrated_at",
    "source_schema",
    "source_table",
    "source_row_number",
    "source_pk_column",
    "source_pk_value",
    "source_payload_hash",
    "source_payload_json",
    "target_refs_json",
    "migration_status",
    "validation_status",
    "failure_reason",
]

SUCCESS_REPORT_COLUMNS = [
    "run_id",
    "source_schema",
    "source_table",
    "source_row_number",
    "source_pk_column",
    "source_pk_value",
    "source_payload_hash",
    "migration_status",
    "validation_status",
    "target_refs_json",
]

FAILURE_REPORT_COLUMNS = [
    "run_id",
    "source_schema",
    "source_table",
    "source_row_number",
    "source_pk_column",
    "source_pk_value",
    "source_payload_hash",
    "failure_reason",
    "traceback",
]

VALIDATION_REPORT_COLUMNS = [
    "check_name",
    "scope",
    "source_count",
    "target_count",
    "status",
    "details",
]

TARGET_ID_COLUMNS = [
    "id",
    "brand_uuid",
    "campaign_uuid",
    "field_rep_uuid",
    "campaign_field_rep_assignment_uuid",
    "doctor_uuid",
    "doctor_campaign_enrollment_uuid",
    "doctor_field_rep_roster_bridge_uuid",
    "exception_id",
    "inclinic_field_rep_identity_id",
    "inclinic_campaign_uuid",
    "assignment_uuid",
    "collateral_uuid",
    "campaign_collateral_uuid",
    "share_event_uuid",
    "transaction_uuid",
    "public_id",
    "campaign_id",
]


ProgressCallback = Callable[[str], None]


@dataclass
class MigrationPlan:
    run_id: str
    started_at: datetime
    source_data: dict[str, list[dict[str, Any]]]
    table_groups: dict[str, dict[str, list[dict[str, Any]]]]
    audit_rows: list[dict[str, Any]]
    failed_rows: list[dict[str, Any]]
    validation_rows: list[dict[str, Any]]
    source_counts: dict[str, int]
    v2_counts: dict[str, int]
    validation_status: str
    elapsed_seconds: float = 0.0
    reporting_result: dict[str, Any] = field(default_factory=dict)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=True, sort_keys=True)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def table_ref(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"


def all_columns(rows: list[dict[str, Any]], default: list[str] | None = None) -> list[str]:
    columns = list(dict.fromkeys(key for row in rows for key in row.keys()))
    return columns or list(default or ["id"])


def discover_v1_raw_tables() -> list[tuple[str, str]]:
    rows = fetch_all(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema LIKE 'raw_%%'
          AND table_schema NOT LIKE 'raw_v2_%%'
        ORDER BY table_schema, table_name
        """
    )
    return [
        (row["table_schema"], row["table_name"])
        for row in rows
        if row["table_schema"] not in V2_SOURCE_SCHEMAS
    ]


def table_columns(schema: str, table: str) -> list[str]:
    rows = fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        [schema, table],
    )
    return [row["column_name"] for row in rows]


def order_by_for_table(columns: list[str]) -> str:
    ordered = [column for column in ("id", "public_id", "_ingested_at", "created_at") if column in columns]
    if ordered:
        return ", ".join(qident(column) for column in ordered)
    return "ctid"


def load_v1_source_data(progress: ProgressCallback | None = None) -> dict[str, list[dict[str, Any]]]:
    source_data: dict[str, list[dict[str, Any]]] = {}
    tables = discover_v1_raw_tables()
    for index, (schema, table) in enumerate(tables, start=1):
        columns = table_columns(schema, table)
        order_by = order_by_for_table(columns)
        rows = fetch_all(f"SELECT * FROM {table_ref(schema, table)} ORDER BY {order_by}")
        dotted = f"{schema}.{table}"
        source_data[dotted] = rows
        if progress:
            progress(f"Loaded {index}/{len(tables)}: {dotted} ({len(rows)} rows)")
    return source_data


def build_v2_table_groups(
    source_data: dict[str, list[dict[str, Any]]],
    run_id: str,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    from etl import v2_builders as builders

    builders.RUN_ID = run_id
    master_v2 = builders.build_master_v2(source_data)
    inclinic_v2 = builders.build_inclinic_v2(source_data, master_v2)
    pe_portal_v2 = builders.build_pe_portal_v2(source_data)

    return {
        builders.RAW_V2_MASTER_SCHEMA: {
            table: rows for table, rows in master_v2.items() if table.endswith("_v2")
        },
        builders.RAW_V2_INCLINIC_SCHEMA: {
            table: rows for table, rows in inclinic_v2.items() if table.endswith("_v2")
        },
        builders.RAW_V2_PE_PORTAL_SCHEMA: {
            table if table.endswith("_v2") else f"{table}_v2": rows
            for table, rows in pe_portal_v2.items()
        },
    }


def target_record_ref(schema: str, table: str, row: dict[str, Any], row_number: int) -> str:
    for column in TARGET_ID_COLUMNS:
        value = clean(row.get(column)).strip()
        if value:
            return f"{schema}.{table}:{column}={value}"
    return f"{schema}.{table}:row_number={row_number}"


def source_hashes_from_v2_row(row: dict[str, Any]) -> set[str]:
    raw_payload = clean(row.get("raw_payload_json")).strip()
    if not raw_payload:
        return set()
    try:
        return {payload_hash(json.loads(raw_payload))}
    except Exception:
        return set()


def build_target_refs_by_source_hash(
    table_groups: dict[str, dict[str, list[dict[str, Any]]]]
) -> dict[str, list[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    for schema, tables in table_groups.items():
        for table, rows in tables.items():
            for row_number, row in enumerate(rows, start=1):
                target_ref = target_record_ref(schema, table, row, row_number)
                for source_hash in source_hashes_from_v2_row(row):
                    refs[source_hash].add(target_ref)
    return {source_hash: sorted(values) for source_hash, values in refs.items()}


def source_pk(row: dict[str, Any]) -> tuple[str, str]:
    for column in ("id", "public_id", "uuid", "campaign_id", "doctor_id"):
        value = clean(row.get(column)).strip()
        if value:
            return column, value
    return "", ""


def build_audit_rows(
    source_data: dict[str, list[dict[str, Any]]],
    target_refs_by_hash: dict[str, list[str]],
    run_id: str,
    migrated_at: str,
) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []
    for dotted, rows in source_data.items():
        schema, table = dotted.split(".", 1)
        for row_number, row in enumerate(rows, start=1):
            source_payload_hash = payload_hash(row)
            refs = target_refs_by_hash.get(source_payload_hash, [])
            pk_column, pk_value = source_pk(row)
            audit_rows.append(
                {
                    "run_id": run_id,
                    "migrated_at": migrated_at,
                    "source_schema": schema,
                    "source_table": table,
                    "source_row_number": str(row_number),
                    "source_pk_column": pk_column,
                    "source_pk_value": pk_value,
                    "source_payload_hash": source_payload_hash,
                    "source_payload_json": json_dumps(row),
                    "target_refs_json": json_dumps(refs),
                    "migration_status": "SEMANTIC_MAPPED" if refs else "AUDIT_PRESERVED_ONLY",
                    "validation_status": "PASS",
                    "failure_reason": "",
                }
            )
    return audit_rows


def validate_plan(
    source_data: dict[str, list[dict[str, Any]]],
    table_groups: dict[str, dict[str, list[dict[str, Any]]]],
    audit_rows: list[dict[str, Any]],
    failed_rows: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    validation_rows: list[dict[str, Any]] = []
    source_counts = {dotted: len(rows) for dotted, rows in sorted(source_data.items())}
    audit_counts: dict[str, int] = defaultdict(int)
    for row in audit_rows:
        audit_counts[f"{row['source_schema']}.{row['source_table']}"] += 1

    for dotted, source_count in source_counts.items():
        target_count = audit_counts.get(dotted, 0)
        validation_rows.append(
            {
                "check_name": "v1_row_audit_preservation",
                "scope": dotted,
                "source_count": str(source_count),
                "target_count": str(target_count),
                "status": "PASS" if source_count == target_count else "FAIL",
                "details": "Every V1 row must have one audit V2 row.",
            }
        )

    source_hashes = {payload_hash(row) for rows in source_data.values() for row in rows}
    missing_semantic_hashes: list[str] = []
    semantic_payload_count = 0
    for schema, tables in table_groups.items():
        for table, rows in tables.items():
            validation_rows.append(
                {
                    "check_name": "v2_table_row_count",
                    "scope": f"{schema}.{table}",
                    "source_count": "",
                    "target_count": str(len(rows)),
                    "status": "PASS",
                    "details": "Semantic V2 table planned for transactional replace.",
                }
            )
            for row in rows:
                row_hashes = source_hashes_from_v2_row(row)
                semantic_payload_count += len(row_hashes)
                for source_hash in row_hashes:
                    if source_hash not in source_hashes:
                        missing_semantic_hashes.append(source_hash)

    validation_rows.append(
        {
            "check_name": "semantic_v2_payload_source_match",
            "scope": "all_raw_payload_json_rows",
            "source_count": str(len(source_hashes)),
            "target_count": str(semantic_payload_count),
            "status": "PASS" if not missing_semantic_hashes else "FAIL",
            "details": (
                "All semantic V2 raw_payload_json values match a V1 source row hash."
                if not missing_semantic_hashes
                else f"Missing source hashes: {', '.join(sorted(set(missing_semantic_hashes))[:50])}"
            ),
        }
    )

    validation_rows.append(
        {
            "check_name": "failed_record_count",
            "scope": "all_sources",
            "source_count": str(sum(source_counts.values())),
            "target_count": str(len(failed_rows)),
            "status": "PASS" if not failed_rows else "FAIL",
            "details": "Technical migration failures must be zero before V2 switch.",
        }
    )

    overall = "PASS" if all(row["status"] == "PASS" for row in validation_rows) else "FAIL"
    return overall, validation_rows


def build_migration_plan(run_id: str, progress: ProgressCallback | None = None) -> MigrationPlan:
    started = utc_now()
    timer = monotonic()
    source_data = load_v1_source_data(progress)
    if progress:
        progress(f"Loaded {sum(len(rows) for rows in source_data.values())} V1 source rows.")
        progress("Building canonical V2 table rows.")
    table_groups = build_v2_table_groups(source_data, run_id)
    target_refs_by_hash = build_target_refs_by_source_hash(table_groups)
    audit_rows = build_audit_rows(source_data, target_refs_by_hash, run_id, iso_now())
    failed_rows: list[dict[str, Any]] = []
    validation_status, validation_rows = validate_plan(source_data, table_groups, audit_rows, failed_rows)
    v2_counts = {
        f"{schema}.{table}": len(rows)
        for schema, tables in table_groups.items()
        for table, rows in tables.items()
    }
    plan = MigrationPlan(
        run_id=run_id,
        started_at=started,
        source_data=source_data,
        table_groups=table_groups,
        audit_rows=audit_rows,
        failed_rows=failed_rows,
        validation_rows=validation_rows,
        source_counts={dotted: len(rows) for dotted, rows in sorted(source_data.items())},
        v2_counts=dict(sorted(v2_counts.items())),
        validation_status=validation_status,
        elapsed_seconds=monotonic() - timer,
    )
    return plan


def write_v2_tables(plan: MigrationPlan, progress: ProgressCallback | None = None) -> None:
    with transaction.atomic():
        for schema, tables in plan.table_groups.items():
            for table, rows in tables.items():
                columns = all_columns(rows)
                if progress:
                    progress(f"Replacing {schema}.{table} ({len(rows)} rows)")
                replace_table(schema, table, columns, rows)
        if progress:
            progress(f"Replacing {RAW_V2_AUDIT_SCHEMA}.{RAW_V2_AUDIT_TABLE} ({len(plan.audit_rows)} rows)")
        replace_table(RAW_V2_AUDIT_SCHEMA, RAW_V2_AUDIT_TABLE, AUDIT_COLUMNS, plan.audit_rows)


def rebuild_reporting_from_v2(run_id: str, progress: ProgressCallback | None = None) -> dict[str, Any]:
    from etl.pipelines.gold_aggregations import build_gold
    from etl.pipelines.v2_reporting import build_v2_reporting

    with transaction.atomic():
        if progress:
            progress("Rebuilding silver reporting tables from V2 raw tables.")
        result = {"v2_reporting": build_v2_reporting(run_id)}
        if progress:
            progress("Rebuilding gold campaign schemas from V2 silver tables.")
        build_gold(run_id)
        result["gold"] = {"status": "rebuilt"}
    return result


def ensure_report_dir(report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)


def write_csv_report(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def write_success_log(path: Path, plan: MigrationPlan, dry_run: bool, skipped_rebuild: bool) -> None:
    ended_at = utc_now()
    lines = [
        f"Run ID: {plan.run_id}",
        f"Started at: {plan.started_at.isoformat()}",
        f"Ended at: {ended_at.isoformat()}",
        f"Duration seconds: {plan.elapsed_seconds:.2f}",
        f"Dry run: {dry_run}",
        f"Skipped reporting rebuild: {skipped_rebuild}",
        "",
        "Source V1 counts:",
    ]
    lines.extend(f"  {name}: {count}" for name, count in plan.source_counts.items())
    lines.extend(["", "Destination V2 counts:"])
    lines.extend(f"  {name}: {count}" for name, count in plan.v2_counts.items())
    lines.extend(
        [
            "",
            f"Audit rows: {len(plan.audit_rows)}",
            f"Failed rows: {len(plan.failed_rows)}",
            f"Validation status: {plan.validation_status}",
            f"Overall migration result: {'PASS' if plan.validation_status == 'PASS' and not plan.failed_rows else 'FAIL'}",
        ]
    )
    if plan.reporting_result:
        lines.extend(["", "Reporting rebuild result:", json_dumps(plan.reporting_result)])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_failure_log(path: Path, failed_rows: list[dict[str, Any]], extra_traceback: str = "") -> None:
    lines = [f"Generated at: {iso_now()}", f"Failed record count: {len(failed_rows)}", ""]
    if failed_rows:
        for row in failed_rows:
            lines.append(json_dumps(row))
    else:
        lines.append("No failed records.")
    if extra_traceback:
        lines.extend(["", "Global traceback:", extra_traceback])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_final_summary(path: Path, plan: MigrationPlan, dry_run: bool) -> None:
    total_source = sum(plan.source_counts.values())
    total_v2 = sum(plan.v2_counts.values())
    status = "PASS" if plan.validation_status == "PASS" and not plan.failed_rows else "FAIL"
    lines = [
        "Final Validation Report",
        f"Run ID: {plan.run_id}",
        f"Dry run: {dry_run}",
        f"Total source records: {total_source}",
        f"Total migrated/audited V1 records: {len(plan.audit_rows)}",
        f"Total semantic V2 records: {total_v2}",
        f"Total failed records: {len(plan.failed_rows)}",
        f"Validation status: {plan.validation_status}",
        f"Overall migration result: {status}",
        "",
        "Record count comparison:",
    ]
    for row in plan.validation_rows:
        lines.append(
            f"{row['check_name']} | {row['scope']} | source={row['source_count']} "
            f"| target={row['target_count']} | {row['status']}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reports(
    report_dir: Path,
    plan: MigrationPlan,
    dry_run: bool,
    skipped_rebuild: bool,
    extra_traceback: str = "",
) -> dict[str, Path]:
    ensure_report_dir(report_dir)
    success_csv = report_dir / "successful_transfers.csv"
    failure_csv = report_dir / "failed_transfers.csv"
    validation_csv = report_dir / "validation_reconciliation_report.csv"
    success_log = report_dir / "successful_transfers.log.txt"
    failure_log = report_dir / "failed_transfers.log.txt"
    final_summary = report_dir / "final_validation_report.txt"

    write_csv_report(success_csv, SUCCESS_REPORT_COLUMNS, plan.audit_rows)
    write_csv_report(failure_csv, FAILURE_REPORT_COLUMNS, plan.failed_rows)
    write_csv_report(validation_csv, VALIDATION_REPORT_COLUMNS, plan.validation_rows)
    write_success_log(success_log, plan, dry_run=dry_run, skipped_rebuild=skipped_rebuild)
    write_failure_log(failure_log, plan.failed_rows, extra_traceback=extra_traceback)
    write_final_summary(final_summary, plan, dry_run=dry_run)
    return {
        "success_csv": success_csv,
        "failure_csv": failure_csv,
        "validation_csv": validation_csv,
        "success_log": success_log,
        "failure_log": failure_log,
        "final_summary": final_summary,
    }


def write_startup_failure_reports(report_dir: Path, run_id: str, error: BaseException, dry_run: bool) -> dict[str, Path]:
    ensure_report_dir(report_dir)
    failure = failed_global_row(run_id, error)
    validation_rows = [
        {
            "check_name": "global_migration_error",
            "scope": "startup_or_plan_build",
            "source_count": "",
            "target_count": "1",
            "status": "FAIL",
            "details": str(error),
        }
    ]
    success_csv = report_dir / "successful_transfers.csv"
    failure_csv = report_dir / "failed_transfers.csv"
    validation_csv = report_dir / "validation_reconciliation_report.csv"
    success_log = report_dir / "successful_transfers.log.txt"
    failure_log = report_dir / "failed_transfers.log.txt"
    final_summary = report_dir / "final_validation_report.txt"

    write_csv_report(success_csv, SUCCESS_REPORT_COLUMNS, [])
    write_csv_report(failure_csv, FAILURE_REPORT_COLUMNS, [failure])
    write_csv_report(validation_csv, VALIDATION_REPORT_COLUMNS, validation_rows)
    success_log.write_text(
        "\n".join(
            [
                f"Run ID: {run_id}",
                f"Generated at: {iso_now()}",
                f"Dry run: {dry_run}",
                "Validation status: FAIL",
                "Overall migration result: FAIL",
                "No successful transfers were recorded because the command failed before a full plan was built.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    write_failure_log(failure_log, [failure], extra_traceback=failure["traceback"])
    final_summary.write_text(
        "\n".join(
            [
                "Final Validation Report",
                f"Run ID: {run_id}",
                f"Dry run: {dry_run}",
                "Total source records: 0",
                "Total migrated/audited V1 records: 0",
                "Total semantic V2 records: 0",
                "Total failed records: 1",
                "Validation status: FAIL",
                "Overall migration result: FAIL",
                f"Failure reason: {error}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "success_csv": success_csv,
        "failure_csv": failure_csv,
        "validation_csv": validation_csv,
        "success_log": success_log,
        "failure_log": failure_log,
        "final_summary": final_summary,
    }


def failed_global_row(run_id: str, error: BaseException) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "source_schema": "",
        "source_table": "",
        "source_row_number": "",
        "source_pk_column": "",
        "source_pk_value": "",
        "source_payload_hash": "",
        "failure_reason": str(error),
        "traceback": traceback.format_exc(),
    }
