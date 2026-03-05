from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from django.db import connection
from django.db.utils import OperationalError, ProgrammingError
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from etl.utils.specs import SOURCE_TABLE_SPECS


def _fetch_dicts(sql: str, params=None):
    with connection.cursor() as cursor:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _row_has_week_data(row: dict[str, Any]) -> bool:
    metrics = (
        _to_float(row.get("doctors_reached_unique")),
        _to_float(row.get("doctors_opened_unique")),
        _to_float(row.get("video_viewed_50_unique")),
        _to_float(row.get("pdf_download_unique")),
        _to_float(row.get("doctors_consumed_unique")),
    )
    return any(v > 0 for v in metrics)


def _safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return (num / den) * 100.0


def _health_color(score: float) -> str:
    if score < 40:
        return "red"
    if score < 60:
        return "yellow"
    return "green"


def _format_schedule_date(value: Any) -> str | None:
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(txt[:19], fmt).strftime("%b %d, %Y")
        except ValueError:
            continue
    return txt


def _campaign_credentials(brand_campaign_id: str) -> dict[str, str]:
    suffix = brand_campaign_id.replace("-", "")
    return {
        "username": f"brand_{suffix[:6]}",
        "password": f"report_{suffix[-4:]}",
    }


def _normalize_campaign_id(value: Any) -> str:
    return str(value or "").strip()


def _build_media_logo_url(company_logo_path: Any) -> str | None:
    raw = str(company_logo_path or "").strip()
    if not raw or raw.lower() == "null":
        return None
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://inclinic.inditech.co.in/media/{raw.lstrip('/')}"


def _campaign_list() -> list[dict[str, Any]]:
    """Return campaign list for menu page.

    If ETL/GOLD tables are not created yet (fresh deploy), return an empty list
    instead of crashing the entire dashboard route.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('gold_global.campaign_registry')")
            registry_exists = cursor.fetchone()[0] is not None
        if not registry_exists:
            return []

        return _fetch_dicts(
            """
            WITH campaign_candidates AS (
                SELECT
                  r.brand_campaign_id,
                  r.gold_schema_name,
                  MIN(
                    CASE
                        WHEN cm.name IS NULL OR btrim(cm.name) = '' OR lower(btrim(cm.name)) = 'null'
                        THEN NULL
                        ELSE cm.name
                    END
                  ) AS cm_campaign_name,
                  MIN(
                    CASE
                        WHEN cc.name IS NULL OR btrim(cc.name) = '' OR lower(btrim(cc.name)) = 'null'
                        THEN NULL
                        ELSE cc.name
                    END
                  ) AS cc_campaign_name
                FROM gold_global.campaign_registry r
                LEFT JOIN silver.map_brand_campaign_to_campaign m ON m.brand_campaign_id = r.brand_campaign_id
                LEFT JOIN bronze.campaign_campaign cc
                  ON cc.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                LEFT JOIN bronze.campaign_management_campaign cm
                  ON regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(r.brand_campaign_id)), '-', '', 'g')
                  OR cm.id::text = btrim(r.brand_campaign_id)
                  OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                GROUP BY r.brand_campaign_id, r.gold_schema_name
            )
            SELECT
              brand_campaign_id,
              gold_schema_name,
              COALESCE(cm_campaign_name, cc_campaign_name) AS campaign_name
            FROM campaign_candidates
            WHERE COALESCE(cm_campaign_name, cc_campaign_name) IS NOT NULL
              AND lower(COALESCE(cm_campaign_name, cc_campaign_name)) NOT LIKE 'test%'
              AND lower(COALESCE(cm_campaign_name, cc_campaign_name)) NOT LIKE '%dummy%'
            ORDER BY COALESCE(cm_campaign_name, cc_campaign_name)
            """
        )
    except (ProgrammingError, OperationalError):
        return []



def _table_exists(schema: str, table: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s)", [f"{schema}.{table}"])
        return cursor.fetchone()[0] is not None


def _table_count(schema: str, table: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM {schema}.{table}')
        return int(cursor.fetchone()[0])


def _build_debug_snapshot() -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "layers": [],
        "latest_run": None,
        "errors": [],
    }

    try:
        layer_specs = {
            "raw_server1": list(SOURCE_TABLE_SPECS.get("mysql_server_1", {}).keys()),
            "raw_server2": list(SOURCE_TABLE_SPECS.get("mysql_server_2", {}).keys()),
            "bronze": list(SOURCE_TABLE_SPECS.get("mysql_server_1", {}).keys()) + list(SOURCE_TABLE_SPECS.get("mysql_server_2", {}).keys()),
            "silver": [
                "dim_field_rep",
                "dim_doctor",
                "dim_collateral",
                "bridge_campaign_collateral_schedule",
                "fact_collateral_transaction",
                "map_brand_campaign_to_campaign",
                "bridge_brand_campaign_doctor_base",
                "doctor_action_first_seen",
            ],
            "gold_global": [
                "campaign_registry",
                "campaign_health_history",
                "benchmark_last_10_campaigns",
            ],
            "control": ["etl_run_log"],
        }

        for schema, tables in layer_specs.items():
            schema_rows = []
            for table in tables:
                try:
                    exists = _table_exists(schema, table)
                    row_count = _table_count(schema, table) if exists else 0
                    schema_rows.append({
                        "table": table,
                        "exists": exists,
                        "row_count": row_count,
                    })
                except Exception as exc:
                    schema_rows.append({
                        "table": table,
                        "exists": False,
                        "row_count": 0,
                        "error": str(exc),
                    })
                    snapshot["errors"].append(f"{schema}.{table}: {exc}")

            snapshot["layers"].append({"schema": schema, "tables": schema_rows})

        # Count campaign schemas for quick GOLD visibility
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.schemata
                WHERE schema_name LIKE 'gold_campaign_%'
            """)
            snapshot["gold_campaign_schema_count"] = int(cursor.fetchone()[0])

        # Latest ETL run metadata
        if _table_exists("control", "etl_run_log"):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run_id, started_at, ended_at, status, notes
                    FROM control.etl_run_log
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
                if row:
                    parsed_notes = None
                    notes_errors = []
                    notes_value = row[4]
                    if notes_value:
                        try:
                            parsed_notes = json.loads(notes_value)
                            notes_errors = list((parsed_notes.get("errors") or {}).items())[:20]
                        except (TypeError, ValueError):
                            parsed_notes = None

                    snapshot["latest_run"] = {
                        "run_id": row[0],
                        "started_at": row[1],
                        "ended_at": row[2],
                        "status": row[3],
                        "notes": notes_value,
                        "notes_summary": (parsed_notes or {}).get("summary"),
                        "notes_errors": notes_errors,
                    }

        # Per-campaign GOLD table diagnostics
        snapshot["campaign_schema_tables"] = []
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name LIKE 'gold_campaign_%'
                ORDER BY schema_name
                """
            )
            campaign_schemas = [r[0] for r in cursor.fetchall()]

        for schema_name in campaign_schemas:
            tables = []
            for table_name in ["fact_doctor_collateral_latest", "kpi_weekly_summary", "weekly_action_items"]:
                try:
                    exists = _table_exists(schema_name, table_name)
                    row_count = _table_count(schema_name, table_name) if exists else 0
                    tables.append({
                        "table": table_name,
                        "exists": exists,
                        "row_count": row_count,
                    })
                except Exception as exc:
                    tables.append({
                        "table": table_name,
                        "exists": False,
                        "row_count": 0,
                        "error": str(exc),
                    })
                    snapshot["errors"].append(f"{schema_name}.{table_name}: {exc}")

            snapshot["campaign_schema_tables"].append({
                "schema": schema_name,
                "tables": tables,
            })

    except Exception as exc:
        snapshot["errors"].append(str(exc))

    return snapshot

def menu_page(request: HttpRequest) -> HttpResponse:
    campaigns = _campaign_list()
    return render(request, "dashboard/menu.html", {"campaigns": campaigns})


def etl_debug_page(request: HttpRequest) -> HttpResponse:
    debug_snapshot = _build_debug_snapshot()
    return render(request, "dashboard/debug.html", {"debug_snapshot": debug_snapshot})


def campaign_login(request: HttpRequest, brand_campaign_id: str) -> HttpResponse:
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    campaigns = {_normalize_campaign_id(c["brand_campaign_id"]): c for c in _campaign_list()}
    campaign = campaigns.get(normalized_campaign_id)
    if not campaign:
        return redirect("menu")

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        expected = _campaign_credentials(normalized_campaign_id)
        if username == expected["username"] and password == expected["password"]:
            request.session[f"auth_{normalized_campaign_id}"] = True
            return redirect("campaign-overview-specific", brand_campaign_id=normalized_campaign_id)
        error_message = "Invalid brand credentials"

    return render(
        request,
        "dashboard/login.html",
        {
            "campaign": campaign,
            "error_message": error_message,
            "credential_hint": f"Username: brand_{normalized_campaign_id.replace('-', '')[:6]} / Password: report_{normalized_campaign_id.replace('-', '')[-4:]}",
        },
    )


def _build_report_context(selected_campaign: str, week_filter: int | None = None) -> dict[str, Any]:
    selected_schema = None
    all_weekly_rows: list[dict[str, Any]] = []
    weekly_rows: list[dict[str, Any]] = []
    error_message = None
    state_attention: list[dict[str, Any]] = []
    schedule_text = "Schedule unavailable"
    collateral_name = "N/A"
    brand_name = "Apex"
    brand_logo_text = "apex"
    company_logo_url = None

    action_panel = {
        "primary_issue": "No issue detected",
        "who_should_act": "Field Team Lead",
        "actions": ["Continue current execution and monitor weekly movement."],
    }
    collateral_cards = {"current": {}, "best": {}, "benchmark": {}}

    context_metrics = {
        "campaign_health": 0.0,
        "campaign_wow": 0.0,
        "campaign_benchmark_label": "Insufficient Data",
        "campaign_color": "red",
        "weekly_health": 0.0,
        "weekly_wow": 0.0,
        "weekly_benchmark_label": "Insufficient Data",
        "weekly_color": "red",
        "kpi_reached": 0,
        "kpi_opened": 0,
        "kpi_video": 0,
        "kpi_pdf": 0,
        "kpi_reached_pct": 0,
        "kpi_opened_pct": 0,
        "kpi_video_pct": 0,
        "kpi_pdf_pct": 0,
        "week_of": "Week -",
    }

    try:
        selected_campaign = _normalize_campaign_id(selected_campaign)
        schema_rows = _fetch_dicts(
            """
            SELECT brand_campaign_id, gold_schema_name
            FROM gold_global.campaign_registry
            WHERE btrim(brand_campaign_id) = btrim(%s)
            """,
            [selected_campaign],
        )
        if not schema_rows:
            schema_rows = _fetch_dicts(
                """
                SELECT brand_campaign_id, gold_schema_name
                FROM gold_global.campaign_registry
                WHERE lower(btrim(brand_campaign_id)) = lower(btrim(%s))
                """,
                [selected_campaign],
            )
        if not schema_rows:
            return {"error_message": f"Campaign schema not found for {selected_campaign}", **context_metrics}

        selected_campaign = _normalize_campaign_id(schema_rows[0]["brand_campaign_id"])
        selected_schema = schema_rows[0]["gold_schema_name"]
        all_weekly_rows = _fetch_dicts(f"SELECT * FROM {selected_schema}.kpi_weekly_summary ORDER BY week_index")
        available_weekly_rows = [r for r in all_weekly_rows if _row_has_week_data(r)]

        week_options = sorted({_to_int(r.get("week_index")) for r in available_weekly_rows if _to_int(r.get("week_index")) > 0})
        if week_filter and week_filter not in week_options:
            week_filter = None

        if week_filter:
            weekly_rows = [r for r in available_weekly_rows if _to_int(r.get("week_index")) == week_filter]
        else:
            weekly_rows = list(available_weekly_rows)

        schedule_rows = _fetch_dicts(
            """
            WITH campaign_row AS (
                SELECT
                    cm.id,
                    cm.brand_campaign_id,
                    cm.brand_name,
                    cm.company_logo,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            CASE
                                WHEN regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g') THEN 1
                                WHEN cm.id::text = btrim(%s) THEN 2
                                ELSE 3
                            END,
                            cm.id DESC
                    ) AS rn
                FROM bronze.campaign_management_campaign cm
                LEFT JOIN silver.map_brand_campaign_to_campaign m
                  ON regexp_replace(lower(btrim(m.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                WHERE
                    regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                    OR cm.id::text = btrim(%s)
                    OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
            ),
            campaign_source AS (
                SELECT id, brand_campaign_id, brand_name, company_logo
                FROM campaign_row
                WHERE rn = 1
            )
            SELECT
                MIN(
                    CASE
                        WHEN cc.start_date IS NULL OR btrim(cc.start_date) = '' OR lower(btrim(cc.start_date)) = 'null'
                        THEN NULL
                        ELSE cc.start_date::date
                    END
                ) AS schedule_start_date,
                MAX(
                    CASE
                        WHEN cc.end_date IS NULL OR btrim(cc.end_date) = '' OR lower(btrim(cc.end_date)) = 'null'
                        THEN NULL
                        ELSE cc.end_date::date
                    END
                ) AS schedule_end_date,
                MIN(NULLIF(c.title, '')) AS collateral_title,
                MIN(
                    CASE
                        WHEN cs.brand_name IS NULL OR btrim(cs.brand_name) = '' OR lower(btrim(cs.brand_name)) = 'null'
                        THEN NULL
                        ELSE cs.brand_name
                    END
                ) AS brand_name,
                MIN(
                    CASE
                        WHEN cs.company_logo IS NULL OR btrim(cs.company_logo) = '' OR lower(btrim(cs.company_logo)) = 'null'
                        THEN NULL
                        ELSE cs.company_logo
                    END
                ) AS company_logo
            FROM campaign_source cs
            LEFT JOIN bronze.collateral_management_campaigncollateral cc ON cc.campaign_id::text = cs.id::text
            LEFT JOIN bronze.collateral_management_collateral c ON c.id = cc.collateral_id
            """,
            [selected_campaign, selected_campaign, selected_campaign, selected_campaign, selected_campaign],
        )
        if schedule_rows:
            start = _format_schedule_date(schedule_rows[0].get("schedule_start_date"))
            end = _format_schedule_date(schedule_rows[0].get("schedule_end_date"))
            if start and end:
                schedule_text = f"{start} - {end}"
            collateral_name = schedule_rows[0].get("collateral_title") or collateral_name
            brand_name = schedule_rows[0].get("brand_name") or brand_name
            company_logo_url = _build_media_logo_url(schedule_rows[0].get("company_logo"))

        if not company_logo_url:
            fallback_logo = _fetch_dicts(
                """
                WITH matched_campaign AS (
                    SELECT
                        cm.company_logo,
                        ROW_NUMBER() OVER (
                            ORDER BY
                                CASE
                                    WHEN regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g') THEN 1
                                    WHEN cm.id::text = btrim(%s) THEN 2
                                    ELSE 3
                                END,
                                cm.id DESC
                        ) AS rn
                    FROM bronze.campaign_management_campaign cm
                    LEFT JOIN silver.map_brand_campaign_to_campaign m
                      ON regexp_replace(lower(btrim(m.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                    WHERE
                        regexp_replace(lower(btrim(cm.brand_campaign_id)), '-', '', 'g') = regexp_replace(lower(btrim(%s)), '-', '', 'g')
                        OR cm.id::text = btrim(%s)
                        OR cm.id::text = NULLIF(btrim(m.campaign_id_resolved), '')
                )
                SELECT company_logo
                FROM matched_campaign
                WHERE rn = 1
                """,
                [selected_campaign, selected_campaign, selected_campaign, selected_campaign, selected_campaign],
            )
            if fallback_logo:
                company_logo_url = _build_media_logo_url(fallback_logo[0].get("company_logo"))

        if collateral_name in {"", "N/A", "Collateral"}:
            fallback_collateral = _fetch_dicts(
                """
                SELECT MIN(NULLIF(c.title, '')) AS collateral_title
                FROM silver.fact_collateral_transaction t
                LEFT JOIN bronze.collateral_management_collateral c ON c.id = t.collateral_id
                WHERE t.brand_campaign_id = %s
                """,
                [selected_campaign],
            )
            if fallback_collateral:
                collateral_name = fallback_collateral[0].get("collateral_title") or collateral_name

        if weekly_rows:
            latest_week = weekly_rows[-1]
            total_doctors = _to_float(latest_week.get("total_doctors_in_campaign"))

            latest_reached = _to_float(latest_week.get("doctors_reached_unique"))
            latest_opened = _to_float(latest_week.get("doctors_opened_unique"))
            latest_video = _to_float(latest_week.get("video_viewed_50_unique"))
            latest_pdf = _to_float(latest_week.get("pdf_download_unique"))
            latest_consumed = _to_float(latest_week.get("doctors_consumed_unique"))

            reached_pct_total = _safe_pct(latest_reached, total_doctors)
            opened_pct_reached = _safe_pct(latest_opened, latest_reached)
            video_pct_opened = _safe_pct(latest_video, latest_opened)
            pdf_pct_opened = _safe_pct(latest_pdf, latest_opened)
            consumed_pct_opened = _safe_pct(latest_consumed, latest_opened)

            current_week_idx = _to_int(latest_week.get("week_index"), 1)
            prev_week = None
            if current_week_idx > 1:
                prev_candidates = [r for r in available_weekly_rows if _to_int(r.get("week_index")) == current_week_idx - 1]
                prev_week = prev_candidates[-1] if prev_candidates else None

            campaign_health = sum(_to_float(r.get("weekly_health_score")) for r in available_weekly_rows) / max(len(available_weekly_rows), 1)
            weekly_health = _to_float(latest_week.get("weekly_health_score"))
            wow_campaign = campaign_health - (
                sum(_to_float(r.get("weekly_health_score")) for r in available_weekly_rows[:-1]) / max(len(available_weekly_rows[:-1]), 1)
                if len(available_weekly_rows) > 1
                else campaign_health
            )
            wow_weekly = weekly_health - _to_float(prev_week.get("weekly_health_score")) if prev_week else 0.0

            state_rows = _fetch_dicts(
                f"""
                WITH fact_enriched AS (
                    SELECT
                      f.doctor_identity_key,
                      COALESCE(NULLIF(f.state_normalized,''), NULLIF(fr.state_normalized,''), 'UNKNOWN') AS state_normalized,
                      f.reached_first_ts,
                      f.opened_first_ts
                    FROM {selected_schema}.fact_doctor_collateral_latest f
                    LEFT JOIN silver.dim_field_rep fr
                      ON fr.id::text = NULLIF(btrim(f.field_rep_id_resolved), '')
                ),
                x AS (
                    SELECT
                      state_normalized,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE reached_first_ts IS NOT NULL
                          AND reached_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS reached,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE opened_first_ts IS NOT NULL
                          AND opened_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS opened,
                      COUNT(DISTINCT doctor_identity_key) AS total_state
                    FROM fact_enriched
                    GROUP BY 1
                )
                SELECT state_normalized,reached,opened,total_state
                FROM x
                WHERE state_normalized <> 'UNKNOWN'
                ORDER BY
                  CASE
                    WHEN reached=0 OR total_state=0 THEN 0
                    ELSE ((LEAST((reached / NULLIF((total_state/4.0),0)),1.0)
                      + (opened / NULLIF(reached,0))
                      + (opened / NULLIF(opened,0))) / 3.0) * 100
                  END ASC,
                  state_normalized ASC
                LIMIT 3
                """,
                [
                    latest_week.get("week_start_date"),
                    latest_week.get("week_end_date"),
                    latest_week.get("week_start_date"),
                    latest_week.get("week_end_date"),
                ],
            )

            state_attention = []
            for row in state_rows:
                reached = _to_float(row.get("reached"))
                opened = _to_float(row.get("opened"))
                total_state = _to_float(row.get("total_state"))
                reached_pct = min(_safe_pct(reached, total_state / 4.0 if total_state else 0), 100.0)
                open_pct = _safe_pct(opened, reached)
                state_health = ((reached_pct / 100.0) + (open_pct / 100.0) + (open_pct / 100.0)) / 3.0 * 100
                label = "Low" if state_health < 40 else "Medium" if state_health < 60 else "Good"
                state_attention.append(
                    {
                        "state": row.get("state_normalized"),
                        "open_pct": round(open_pct, 1),
                        "reached_pct": round(reached_pct, 1),
                        "label": label,
                    }
                )

            weakest = min(
                [
                    ("OPEN", opened_pct_reached),
                    ("CONSUMPTION", consumed_pct_opened),
                    ("REACH", reached_pct_total),
                ],
                key=lambda x: x[1],
            )[0]

            if weakest == "OPEN":
                action_panel = {
                    "primary_issue": f"Low Open Rate in {len(state_attention)} States",
                    "who_should_act": "Field Team Lead",
                    "actions": [
                        "Improve pitch and preview text to increase engagement.",
                        "Resend campaign to unopened doctors with updated messaging.",
                    ],
                }
            elif weakest == "CONSUMPTION":
                action_panel = {
                    "primary_issue": "Low Consumption Conversion",
                    "who_should_act": "Content + Field Team",
                    "actions": [
                        "Improve content hook and opening CTA for stronger consumption.",
                        "Prioritize follow-up with doctors who opened but did not consume.",
                    ],
                }
            else:
                action_panel = {
                    "primary_issue": "Low Reach Coverage",
                    "who_should_act": "Field Team Lead",
                    "actions": [
                        "Increase resend cadence for unreached doctor cohorts.",
                        "Ensure field reps cover low reach clusters first.",
                    ],
                }

            weekly_best = max(available_weekly_rows, key=lambda r: _to_float(r.get("weekly_health_score")))
            bench_rows = _fetch_dicts(
                """
                SELECT avg_campaign_health_score
                FROM gold_global.benchmark_last_10_campaigns
                ORDER BY as_of_date DESC
                LIMIT 1
                """
            )
            benchmark_health = _to_float(bench_rows[0]["avg_campaign_health_score"]) if bench_rows else 0.0

            collateral_cards["current"] = {
                "title": collateral_name,
                "reached": _to_int(latest_reached),
                "opened": _to_int(latest_opened),
                "video": _to_int(latest_video),
                "pdf": _to_int(latest_pdf),
                "reached_pct": round(reached_pct_total, 1),
                "opened_pct": round(opened_pct_reached, 1),
                "video_pct": round(video_pct_opened, 1),
                "pdf_pct": round(pdf_pct_opened, 1),
            }
            collateral_cards["best"] = {
                "title": f"Week {weekly_best.get('week_index')} Best",
                "reached": _to_int(_to_float(weekly_best.get("doctors_reached_unique"))),
                "opened": _to_int(_to_float(weekly_best.get("doctors_opened_unique"))),
                "video": _to_int(_to_float(weekly_best.get("video_viewed_50_unique"))),
                "pdf": _to_int(_to_float(weekly_best.get("pdf_download_unique"))),
                "reached_pct": round(_safe_pct(_to_float(weekly_best.get("doctors_reached_unique")), total_doctors), 1),
                "opened_pct": round(_safe_pct(_to_float(weekly_best.get("doctors_opened_unique")), _to_float(weekly_best.get("doctors_reached_unique"))), 1),
                "video_pct": round(_safe_pct(_to_float(weekly_best.get("video_viewed_50_unique")), _to_float(weekly_best.get("doctors_opened_unique"))), 1),
                "pdf_pct": round(_safe_pct(_to_float(weekly_best.get("pdf_download_unique")), _to_float(weekly_best.get("doctors_opened_unique"))), 1),
            }
            benchmark_metric_rows = _fetch_dicts(
                """
                WITH recent_campaigns AS (
                    SELECT DISTINCT brand_campaign_id
                    FROM gold_global.campaign_health_history
                    ORDER BY brand_campaign_id DESC
                    LIMIT 10
                ),
                campaign_doctor_base AS (
                    SELECT b.brand_campaign_id, COUNT(DISTINCT b.doctor_identity_key) AS total_doctors
                    FROM silver.bridge_brand_campaign_doctor_base b
                    JOIN recent_campaigns r ON r.brand_campaign_id = b.brand_campaign_id
                    GROUP BY b.brand_campaign_id
                ),
                campaign_actions AS (
                    SELECT
                        a.brand_campaign_id,
                        COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.reached_first_ts,'') IS NOT NULL) AS reached,
                        COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.opened_first_ts,'') IS NOT NULL) AS opened,
                        COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.video_gt_50_first_ts,'') IS NOT NULL) AS video,
                        COUNT(DISTINCT a.doctor_identity_key) FILTER (WHERE NULLIF(a.pdf_download_first_ts,'') IS NOT NULL) AS pdf
                    FROM silver.doctor_action_first_seen a
                    JOIN recent_campaigns r ON r.brand_campaign_id = a.brand_campaign_id
                    GROUP BY a.brand_campaign_id
                ),
                campaign_stats AS (
                    SELECT
                        x.brand_campaign_id,
                        x.reached,
                        x.opened,
                        x.video,
                        x.pdf,
                        CASE WHEN d.total_doctors=0 THEN 0 ELSE (x.reached::numeric / d.total_doctors) * 100 END AS reached_pct,
                        CASE WHEN x.reached=0 THEN 0 ELSE (x.opened::numeric / x.reached) * 100 END AS opened_pct,
                        CASE WHEN x.opened=0 THEN 0 ELSE (x.video::numeric / x.opened) * 100 END AS video_pct,
                        CASE WHEN x.opened=0 THEN 0 ELSE (x.pdf::numeric / x.opened) * 100 END AS pdf_pct,
                        (
                          CASE WHEN d.total_doctors=0 THEN 0 ELSE (x.reached::numeric / d.total_doctors) END
                          + CASE WHEN x.reached=0 THEN 0 ELSE (x.opened::numeric / x.reached) END
                          + CASE WHEN x.opened=0 THEN 0 ELSE ((GREATEST(x.video, x.pdf))::numeric / x.opened) END
                        ) / 3.0 * 100 AS health_score
                    FROM campaign_actions x
                    JOIN campaign_doctor_base d ON d.brand_campaign_id = x.brand_campaign_id
                )
                SELECT *
                FROM campaign_stats
                ORDER BY health_score DESC, reached DESC, opened DESC
                LIMIT 1
                """
            )
            bm = benchmark_metric_rows[0] if benchmark_metric_rows else {}
            benchmark_reached_pct = round(_to_float(bm.get("reached_pct")), 1)
            benchmark_opened_pct = round(_to_float(bm.get("opened_pct")), 1)
            benchmark_video_pct = round(_to_float(bm.get("video_pct")), 1)
            benchmark_pdf_pct = round(_to_float(bm.get("pdf_pct")), 1)

            collateral_cards["benchmark"] = {
                "title": "Benchmark Best (Last 10 Campaigns)",
                "reached": _to_int(bm.get("reached")),
                "opened": _to_int(bm.get("opened")),
                "video": _to_int(bm.get("video")),
                "pdf": _to_int(bm.get("pdf")),
                "reached_pct": benchmark_reached_pct,
                "opened_pct": benchmark_opened_pct,
                "video_pct": benchmark_video_pct,
                "pdf_pct": benchmark_pdf_pct,
                "benchmark_health": round(_to_float(bm.get("health_score"), benchmark_health), 1),
            }

            context_metrics = {
                "campaign_health": round(campaign_health, 1),
                "campaign_wow": round(wow_campaign, 1),
                "campaign_benchmark_label": "Above Average" if campaign_health >= benchmark_health else "Below Average",
                "campaign_color": _health_color(campaign_health),
                "weekly_health": round(weekly_health, 1),
                "weekly_wow": round(wow_weekly, 1),
                "weekly_benchmark_label": "Average" if 40 <= weekly_health < 60 else ("Good" if weekly_health >= 60 else "Low"),
                "weekly_color": _health_color(weekly_health),
                "kpi_reached": _to_int(latest_reached),
                "kpi_opened": _to_int(latest_opened),
                "kpi_video": _to_int(latest_video),
                "kpi_pdf": _to_int(latest_pdf),
                "kpi_reached_pct": round(reached_pct_total, 1),
                "kpi_opened_pct": round(opened_pct_reached, 1),
                "kpi_video_pct": round(video_pct_opened, 1),
                "kpi_pdf_pct": round(pdf_pct_opened, 1),
                "week_of": f"Week {current_week_idx} ({latest_week.get('week_start_date')} to {latest_week.get('week_end_date')})",
            }

    except Exception as exc:
        error_message = str(exc)

    trend_source_rows = weekly_rows if weekly_rows else []
    trend_labels = [f"Week {r.get('week_index')}" for r in trend_source_rows]
    reached_pct_series = [_safe_pct(_to_float(r.get("doctors_reached_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in trend_source_rows]
    opened_pct_series = [_safe_pct(_to_float(r.get("doctors_opened_unique")), _to_float(r.get("doctors_reached_unique"))) for r in trend_source_rows]
    pdf_pct_series = [_safe_pct(_to_float(r.get("pdf_download_unique")), _to_float(r.get("doctors_opened_unique"))) for r in trend_source_rows]
    video_pct_series = [_safe_pct(_to_float(r.get("video_viewed_50_unique")), _to_float(r.get("doctors_opened_unique"))) for r in trend_source_rows]

    week_options = sorted({_to_int(r.get("week_index")) for r in all_weekly_rows if _row_has_week_data(r) and _to_int(r.get("week_index")) > 0})

    if selected_campaign:
        if not brand_name or brand_name == "Apex":
            brand_name = f"Brand {selected_campaign[:8]}"
        brand_logo_text = (brand_name or selected_campaign).strip()

    return {
        "selected_campaign": selected_campaign,
        "brand_name": brand_name,
        "brand_logo_text": brand_logo_text,
        "company_logo_url": company_logo_url,
        "selected_schema": selected_schema,
        "weekly_rows": weekly_rows,
        "error_message": error_message,
        "schedule_text": schedule_text,
        "collateral_name": collateral_name,
        "state_attention": state_attention,
        "action_panel": action_panel,
        "collateral_cards": collateral_cards,
        "trend_labels": trend_labels,
        "reached_pct_series": [round(v, 1) for v in reached_pct_series],
        "opened_pct_series": [round(v, 1) for v in opened_pct_series],
        "pdf_pct_series": [round(v, 1) for v in pdf_pct_series],
        "video_pct_series": [round(v, 1) for v in video_pct_series],
        "week_options": week_options,
        "selected_week": week_filter,
        **context_metrics,
    }


def campaign_overview(request: HttpRequest, brand_campaign_id: str | None = None):
    if not brand_campaign_id:
        return redirect("menu")

    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not request.session.get(f"auth_{normalized_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None

    context = _build_report_context(normalized_campaign_id, week_filter)
    return render(request, "dashboard/overview.html", context)


def export_report(request: HttpRequest, brand_campaign_id: str):
    normalized_campaign_id = _normalize_campaign_id(brand_campaign_id)
    if not request.session.get(f"auth_{normalized_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=normalized_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    context = _build_report_context(normalized_campaign_id, week_filter)
    context["export_mode"] = True
    return render(request, "dashboard/overview.html", context)
