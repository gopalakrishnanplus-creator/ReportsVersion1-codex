from __future__ import annotations

from django.db import connection
from django.shortcuts import render


def _fetch_dicts(sql: str, params=None):
    with connection.cursor() as cursor:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def campaign_overview(request, brand_campaign_id: str | None = None):
    campaigns = []
    weekly_rows = []
    selected_campaign = brand_campaign_id
    selected_schema = None
    error_message = None

    try:
        campaigns = _fetch_dicts(
            "SELECT brand_campaign_id, gold_schema_name FROM gold_global.campaign_registry ORDER BY brand_campaign_id"
        )

        if not selected_campaign and campaigns:
            selected_campaign = campaigns[0]["brand_campaign_id"]

        if selected_campaign:
            schema = _fetch_dicts(
                "SELECT gold_schema_name FROM gold_global.campaign_registry WHERE brand_campaign_id=%s",
                [selected_campaign],
            )
            if schema:
                selected_schema = schema[0]["gold_schema_name"]
                weekly_rows = _fetch_dicts(
                    f"SELECT * FROM {selected_schema}.kpi_weekly_summary ORDER BY week_index"
                )
    except Exception as exc:
        error_message = str(exc)

    current_week = weekly_rows[-1] if weekly_rows else {}
    campaign_health = sum(_to_float(r.get("weekly_health_score")) for r in weekly_rows)
    campaign_health = campaign_health / len(weekly_rows) if weekly_rows else 0.0

    trend_labels = [f"W{r.get('week_index')}" for r in weekly_rows]
    reached_series = [_to_float(r.get("doctors_reached_unique")) for r in weekly_rows]
    opened_series = [_to_float(r.get("doctors_opened_unique")) for r in weekly_rows]
    consumed_series = [_to_float(r.get("doctors_consumed_unique")) for r in weekly_rows]
    health_series = [_to_float(r.get("weekly_health_score")) for r in weekly_rows]

    return render(
        request,
        "dashboard/overview.html",
        {
            "campaigns": campaigns,
            "selected_campaign": selected_campaign,
            "selected_schema": selected_schema,
            "weekly_rows": weekly_rows,
            "current_week": current_week,
            "campaign_health": round(campaign_health, 1),
            "trend_labels": trend_labels,
            "reached_series": reached_series,
            "opened_series": opened_series,
            "consumed_series": consumed_series,
            "health_series": health_series,
            "error_message": error_message,
        },
    )
