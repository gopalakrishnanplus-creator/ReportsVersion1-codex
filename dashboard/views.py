from django.shortcuts import render
from django.db import connection


def _fetch_dicts(sql: str, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params or [])
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def campaign_overview(request, brand_campaign_id: str | None = None):
    campaigns = []
    weekly_rows = []
    selected_campaign = brand_campaign_id
    try:
        campaigns = _fetch_dicts("SELECT brand_campaign_id, gold_schema_name FROM gold_global.campaign_registry ORDER BY brand_campaign_id")
        if not selected_campaign and campaigns:
            selected_campaign = campaigns[0]["brand_campaign_id"]

        if selected_campaign:
            schema = _fetch_dicts("SELECT gold_schema_name FROM gold_global.campaign_registry WHERE brand_campaign_id=%s", [selected_campaign])
            if schema:
                weekly_rows = _fetch_dicts(f'SELECT * FROM {schema[0]["gold_schema_name"]}.kpi_weekly_summary ORDER BY week_index')
    except Exception:
        pass

    return render(
        request,
        "dashboard/overview.html",
        {
            "campaigns": campaigns,
            "selected_campaign": selected_campaign,
            "weekly_rows": weekly_rows,
        },
    )
