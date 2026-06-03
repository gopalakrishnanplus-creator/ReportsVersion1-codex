from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import connection


GENERATED_SCHEMA_PATTERNS = (
    "gold_campaign_%",
    "gold_pe_campaign_%",
    "gold_sapa_campaign_%",
)


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


class Command(BaseCommand):
    help = "Audit generated campaign reporting tables and optionally drop only zero-row tables."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--drop-empty",
            action="store_true",
            help="Drop generated campaign tables whose exact row count is zero. Dry-run when omitted.",
        )
        parser.add_argument(
            "--drop-empty-schemas",
            action="store_true",
            help="After dropping empty tables, drop generated campaign schemas that contain no base tables.",
        )

    def handle(self, *args, **options) -> None:
        drop_empty = bool(options["drop_empty"])
        drop_empty_schemas = bool(options["drop_empty_schemas"])
        tables = self._generated_tables()
        empty_tables: list[tuple[str, str]] = []
        nonempty_count = 0

        for schema, table in tables:
            row_count = self._table_count(schema, table)
            if row_count == 0:
                empty_tables.append((schema, table))
                action = "DROP" if drop_empty else "DRY-RUN"
                self.stdout.write(f"{action}: {schema}.{table} has 0 rows")
                if drop_empty:
                    self._drop_table(schema, table)
            else:
                nonempty_count += 1

        dropped_schemas = 0
        if drop_empty and drop_empty_schemas:
            for schema in self._empty_generated_schemas():
                self.stdout.write(f"DROP SCHEMA: {schema} has no base tables")
                self._drop_schema(schema)
                dropped_schemas += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Checked {len(tables)} generated tables: {len(empty_tables)} empty, {nonempty_count} non-empty"
                + (f", {dropped_schemas} empty schemas dropped" if drop_empty and drop_empty_schemas else "")
            )
        )

    def _generated_tables(self) -> list[tuple[str, str]]:
        where_sql = " OR ".join(["table_schema LIKE %s"] * len(GENERATED_SCHEMA_PATTERNS))
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND ({where_sql})
                ORDER BY table_schema, table_name
                """,
                list(GENERATED_SCHEMA_PATTERNS),
            )
            return [(str(schema), str(table)) for schema, table in cursor.fetchall()]

    def _empty_generated_schemas(self) -> list[str]:
        where_sql = " OR ".join(["s.schema_name LIKE %s"] * len(GENERATED_SCHEMA_PATTERNS))
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT s.schema_name
                FROM information_schema.schemata s
                LEFT JOIN information_schema.tables t
                  ON t.table_schema = s.schema_name
                 AND t.table_type = 'BASE TABLE'
                WHERE ({where_sql})
                GROUP BY s.schema_name
                HAVING COUNT(t.table_name) = 0
                ORDER BY s.schema_name
                """,
                list(GENERATED_SCHEMA_PATTERNS),
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def _table_count(self, schema: str, table: str) -> int:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {_qident(schema)}.{_qident(table)}")
            return int(cursor.fetchone()[0])

    def _drop_table(self, schema: str, table: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {_qident(schema)}.{_qident(table)}")

    def _drop_schema(self, schema: str) -> None:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {_qident(schema)}")
