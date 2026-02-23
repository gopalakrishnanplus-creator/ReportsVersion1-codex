from __future__ import annotations

from datetime import datetime
from typing import Any

from django.db import connection
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render


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


def _campaign_list() -> list[dict[str, Any]]:
    return _fetch_dicts(
        """
        SELECT
          r.brand_campaign_id,
          r.gold_schema_name,
          COALESCE(MIN(NULLIF(s.collateral_title, '')), 'Campaign ' || r.brand_campaign_id) AS campaign_name
        FROM gold_global.campaign_registry r
        LEFT JOIN silver.map_brand_campaign_to_campaign m ON m.brand_campaign_id = r.brand_campaign_id
        LEFT JOIN silver.bridge_campaign_collateral_schedule s ON s.campaign_id = m.campaign_id_resolved
        GROUP BY r.brand_campaign_id, r.gold_schema_name
        ORDER BY r.brand_campaign_id
        """
    )


def menu_page(request: HttpRequest) -> HttpResponse:
    campaigns = _campaign_list()
    return render(request, "dashboard/menu.html", {"campaigns": campaigns})


def campaign_login(request: HttpRequest, brand_campaign_id: str) -> HttpResponse:
    campaigns = {c["brand_campaign_id"]: c for c in _campaign_list()}
    campaign = campaigns.get(brand_campaign_id)
    if not campaign:
        return redirect("menu")

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        expected = _campaign_credentials(brand_campaign_id)
        if username == expected["username"] and password == expected["password"]:
            request.session[f"auth_{brand_campaign_id}"] = True
            return redirect("campaign-overview-specific", brand_campaign_id=brand_campaign_id)
        error_message = "Invalid brand credentials"

    return render(
        request,
        "dashboard/login.html",
        {
            "campaign": campaign,
            "error_message": error_message,
            "credential_hint": f"Username: brand_{brand_campaign_id.replace('-', '')[:6]} / Password: report_{brand_campaign_id.replace('-', '')[-4:]}",
        },
    )


def _build_report_context(selected_campaign: str, week_filter: int | None = None) -> dict[str, Any]:
    selected_schema = None
    weekly_rows: list[dict[str, Any]] = []
    error_message = None
    state_attention: list[dict[str, Any]] = []
    schedule_text = "Schedule unavailable"
    collateral_name = "Collateral"
    brand_name = "Apex"
    brand_logo_text = "apex"

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
        schema_rows = _fetch_dicts(
            "SELECT gold_schema_name FROM gold_global.campaign_registry WHERE brand_campaign_id=%s",
            [selected_campaign],
        )
        if not schema_rows:
            return {"error_message": "Campaign schema not found", **context_metrics}

        selected_schema = schema_rows[0]["gold_schema_name"]
        weekly_rows = _fetch_dicts(f"SELECT * FROM {selected_schema}.kpi_weekly_summary ORDER BY week_index")

        if week_filter:
            weekly_rows = [r for r in weekly_rows if _to_int(r.get("week_index")) == week_filter]

        schedule_rows = _fetch_dicts(
            """
            SELECT MIN(schedule_start_date) AS schedule_start_date,
                   MAX(schedule_end_date) AS schedule_end_date,
                   MIN(collateral_title) AS collateral_title
            FROM silver.bridge_campaign_collateral_schedule s
            JOIN silver.map_brand_campaign_to_campaign m ON m.campaign_id_resolved = s.campaign_id
            WHERE m.brand_campaign_id=%s
            """,
            [selected_campaign],
        )
        if schedule_rows:
            start = _format_schedule_date(schedule_rows[0].get("schedule_start_date"))
            end = _format_schedule_date(schedule_rows[0].get("schedule_end_date"))
            if start and end:
                schedule_text = f"{start} - {end}"
            collateral_name = schedule_rows[0].get("collateral_title") or collateral_name

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
            prev_week = weekly_rows[-2] if len(weekly_rows) > 1 else None

            campaign_health = sum(_to_float(r.get("weekly_health_score")) for r in weekly_rows) / max(len(weekly_rows), 1)
            weekly_health = _to_float(latest_week.get("weekly_health_score"))
            wow_campaign = campaign_health - (
                sum(_to_float(r.get("weekly_health_score")) for r in weekly_rows[:-1]) / max(len(weekly_rows[:-1]), 1)
                if len(weekly_rows) > 1
                else campaign_health
            )
            wow_weekly = weekly_health - _to_float(prev_week.get("weekly_health_score")) if prev_week else 0.0

            state_rows = _fetch_dicts(
                f"""
                WITH x AS (
                    SELECT
                      COALESCE(NULLIF(state_normalized,''),'UNKNOWN') AS state_normalized,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE reached_first_ts IS NOT NULL
                          AND reached_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS reached,
                      COUNT(DISTINCT doctor_identity_key) FILTER (
                        WHERE opened_first_ts IS NOT NULL
                          AND opened_first_ts::date BETWEEN %s::date AND %s::date
                      ) AS opened,
                      COUNT(DISTINCT doctor_identity_key) AS total_state
                    FROM {selected_schema}.fact_doctor_collateral_latest
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

            weekly_best = max(weekly_rows, key=lambda r: _to_float(r.get("weekly_health_score")))
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
            collateral_cards["benchmark"] = {
                "title": "Benchmark Best (Last 10 Campaigns)",
                "reached": _to_int(total_doctors * 0.46),
                "opened": _to_int(total_doctors * 0.39),
                "video": _to_int(total_doctors * 0.31),
                "pdf": _to_int(total_doctors * 0.25),
                "reached_pct": 46.0,
                "opened_pct": 85.0,
                "video_pct": 80.0,
                "pdf_pct": 60.0,
                "benchmark_health": round(benchmark_health, 1),
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
                "week_of": f"Week {current_week_idx} of 4",
            }

    except Exception as exc:
        error_message = str(exc)

    trend_labels = [f"Week {r.get('week_index')}" for r in weekly_rows]
    reached_pct_series = [_safe_pct(_to_float(r.get("doctors_reached_unique")), _to_float(r.get("total_doctors_in_campaign"))) for r in weekly_rows]
    opened_pct_series = [_safe_pct(_to_float(r.get("doctors_opened_unique")), _to_float(r.get("doctors_reached_unique"))) for r in weekly_rows]
    pdf_pct_series = [_safe_pct(_to_float(r.get("pdf_download_unique")), _to_float(r.get("doctors_opened_unique"))) for r in weekly_rows]
    video_pct_series = [_safe_pct(_to_float(r.get("video_viewed_50_unique")), _to_float(r.get("doctors_opened_unique"))) for r in weekly_rows]

    week_options = [_to_int(r.get("week_index")) for r in weekly_rows]

    if selected_campaign:
        brand_logo_text = selected_campaign.replace("-", "")[:4].upper()
        brand_name = f"Brand {selected_campaign[:8]}"

    return {
        "selected_campaign": selected_campaign,
        "brand_name": brand_name,
        "brand_logo_text": brand_logo_text,
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

    if not request.session.get(f"auth_{brand_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=brand_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None

    context = _build_report_context(brand_campaign_id, week_filter)
    return render(request, "dashboard/overview.html", context)


def export_report(request: HttpRequest, brand_campaign_id: str):
    if not request.session.get(f"auth_{brand_campaign_id}"):
        return redirect("campaign-login", brand_campaign_id=brand_campaign_id)

    week = request.GET.get("week")
    week_filter = _to_int(week) if week else None
    context = _build_report_context(brand_campaign_id, week_filter)
    context["export_mode"] = True
    return render(request, "dashboard/overview.html", context)
