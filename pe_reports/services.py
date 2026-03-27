from __future__ import annotations

import csv
from io import BytesIO
from math import ceil
from typing import Any

from django.http import Http404, HttpRequest, HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas as pdf_canvas

from etl.pe_reports.control import get_thresholds, log_export
from etl.pe_reports.specs import GOLD_GLOBAL_SCHEMA
from etl.pe_reports.storage import fetch_all, fetch_table, table_exists
from etl.pe_reports.utils import clean_text, normalize_campaign_id, slugify
from pe_reports.reporting import build_dashboard_payload, current_filters_query, metric_dataset


def parse_filters(query_params: Any) -> dict[str, str | None]:
    return {
        "week": clean_text(query_params.get("week")),
        "state": clean_text(query_params.get("state")),
        "field_rep_id": clean_text(query_params.get("field_rep_id")),
        "doctor_key": clean_text(query_params.get("doctor_key")),
        "language_code": clean_text(query_params.get("language_code")),
        "share_type": clean_text(query_params.get("share_type")),
        "therapy_area": clean_text(query_params.get("therapy_area")),
        "trigger": clean_text(query_params.get("trigger")),
        "bundle": clean_text(query_params.get("bundle")),
    }


def _global_rows(table: str) -> list[dict[str, Any]]:
    if not table_exists(GOLD_GLOBAL_SCHEMA, table):
        return []
    return fetch_table(GOLD_GLOBAL_SCHEMA, table)


def _campaign_registry_row(campaign_id: str) -> dict[str, Any] | None:
    normalized = normalize_campaign_id(campaign_id)
    for row in _global_rows("campaign_registry"):
        if clean_text(row.get("campaign_id_normalized")) == normalized:
            return row
        if normalize_campaign_id(row.get("campaign_id_original")) == normalized:
            return row
    return None


def _latest_refresh() -> dict[str, Any] | None:
    rows = _global_rows("refresh_status")
    if not rows:
        return None
    rows.sort(key=lambda row: clean_text(row.get("published_at")) or "", reverse=True)
    return rows[0]


def _schema_rows(schema: str, table: str) -> list[dict[str, Any]]:
    if not table_exists(schema, table):
        return []
    return fetch_table(schema, table)


def _filter_options(schema: str) -> dict[str, list[dict[str, Any]]]:
    return {
        "weeks": _schema_rows(schema, "dim_filter_week"),
        "states": _schema_rows(schema, "dim_filter_state"),
        "field_reps": _schema_rows(schema, "dim_filter_field_rep"),
        "doctors": _schema_rows(schema, "dim_filter_doctor"),
        "languages": _schema_rows(schema, "dim_filter_language"),
        "share_types": _schema_rows(schema, "dim_filter_share_type"),
        "therapy_areas": _schema_rows(schema, "dim_filter_therapy_area"),
        "triggers": _schema_rows(schema, "dim_filter_trigger"),
        "bundles": _schema_rows(schema, "dim_filter_bundle"),
    }


def _metric_href(campaign_id: str, metric: str, filters: dict[str, str | None]) -> str:
    query = current_filters_query(filters)
    suffix = f"?{query}" if query else ""
    return f"/pe-reports/campaign/{campaign_id}/details/{metric}/{suffix}"


def _benchmark_best_summary() -> dict[str, Any] | None:
    refresh = _latest_refresh()
    if refresh is None:
        return None
    as_of_date = clean_text(refresh.get("as_of_date"))
    if not as_of_date:
        return None
    history_rows = [row for row in _global_rows("campaign_health_history") if clean_text(row.get("as_of_date")) == as_of_date]
    if not history_rows:
        return None
    best = max(history_rows, key=lambda row: float(row.get("campaign_health_score") or 0))
    registry = _campaign_registry_row(clean_text(best.get("campaign_id_original")) or clean_text(best.get("campaign_id_normalized")) or "")
    if not registry:
        return None
    schema = clean_text(registry.get("gold_schema_name"))
    summary_rows = _schema_rows(schema or "", "kpi_campaign_health_summary")
    if not summary_rows:
        return None
    return summary_rows[0]


def _load_campaign_dataset(campaign_id: str) -> dict[str, Any]:
    registry = _campaign_registry_row(campaign_id)
    if registry is None:
        raise Http404("Unknown Patient Education campaign")
    schema = clean_text(registry.get("gold_schema_name"))
    if not schema:
        raise Http404("Patient Education dashboard schema is not registered")

    refresh = _latest_refresh()
    weekly_rows = _schema_rows(schema, "kpi_weekly_summary")
    summary_rows = _schema_rows(schema, "kpi_campaign_health_summary")
    if not weekly_rows or not summary_rows:
        return {
            "ready": False,
            "registry": registry,
            "schema": schema,
            "refresh": refresh,
            "filter_options": _filter_options(schema),
        }

    return {
        "ready": True,
        "registry": registry,
        "schema": schema,
        "refresh": refresh,
        "weekly_rows": weekly_rows,
        "summary_row": summary_rows[0],
        "enrollment_rows": _schema_rows(schema, "rpt_enrollment_detail"),
        "share_rows": _schema_rows(schema, "rpt_share_detail"),
        "playback_rows": _schema_rows(schema, "rpt_playback_detail"),
        "video_rows": _schema_rows(schema, "rpt_content_video_detail"),
        "filter_options": _filter_options(schema),
    }


def menu_context() -> dict[str, Any]:
    registry_rows = _global_rows("campaign_registry")
    refresh = _latest_refresh()
    campaigns = []
    for row in sorted(registry_rows, key=lambda item: (clean_text(item.get("campaign_name")) or "", clean_text(item.get("brand_name")) or "")):
        campaign_id = clean_text(row.get("campaign_id_original")) or clean_text(row.get("campaign_id_normalized"))
        campaigns.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": clean_text(row.get("campaign_name")) or campaign_id,
                "brand_name": clean_text(row.get("brand_name")) or "",
                "href": f"/pe-reports/campaign/{campaign_id}/login/",
                "dashboard_href": f"/pe-reports/campaign/{campaign_id}/",
                "access_href": f"/pe-reports/campaign/{campaign_id}/access/",
                "email_href": f"/pe-reports/campaign/{campaign_id}/send-access-email/",
            }
        )
    return {
        "ready": bool(campaigns),
        "campaigns": campaigns,
        "refresh": refresh,
    }


def dashboard_context(campaign_id: str, filters: dict[str, str | None]) -> dict[str, Any]:
    dataset = _load_campaign_dataset(campaign_id)
    if not dataset["ready"]:
        return {
            "ready": False,
            "campaign_id": campaign_id,
            "filters": filters,
            "filter_options": dataset["filter_options"],
            "registry": dataset["registry"],
            "refresh": dataset["refresh"],
            "filters_query": current_filters_query(filters),
            "export_filename": f"pe-dashboard-{slugify(campaign_id)}.pdf",
        }

    benchmark_best = _benchmark_best_summary()
    payload = build_dashboard_payload(
        dataset["registry"],
        filters,
        dataset["weekly_rows"],
        dataset["summary_row"],
        dataset["enrollment_rows"],
        dataset["share_rows"],
        dataset["video_rows"],
        get_thresholds(),
        benchmark_best_row=benchmark_best,
    )
    current_week = payload["current_week_row"] or {}
    summary = payload["campaign_summary"]
    previous_week = None
    weekly_rows_all = payload["weekly_rows_all"]
    if current_week:
        current_index = int(current_week.get("week_index") or 0)
        previous_week = next((row for row in weekly_rows_all if int(row.get("week_index") or 0) == current_index - 1), None)

    filters_query = payload["filters_query"]
    current_suffix = f"?{filters_query}" if filters_query else ""
    benchmark = payload["benchmark_row"] or {}
    best_week = payload["best_week_row"] or {}
    state_attention = []
    for row in payload["state_attention_rows"]:
        health = float(row.get("weekly_state_health_score") or 0)
        state_attention.append(
            {
                "state": row.get("state"),
                "activation_pct": row.get("activation_pct_state"),
                "engagement_50_pct": row.get("engagement_50_pct_state"),
                "label": "Low" if health < 40 else "Medium" if health < 60 else "Good",
            }
        )

    sharing_cards = [
        {
            "title": "Enrolled Doctors",
            "value": summary.get("enrolled_doctors_current", 0),
            "subtitle": "Enrolled / Supported",
            "delta": int(current_week.get("enrolled_doctors_current") or 0) - int(previous_week.get("enrolled_doctors_current") or 0) if previous_week else 0,
            "href": _metric_href(campaign_id, "enrolled_doctors", filters),
        },
        {
            "title": "Doctors Sharing",
            "value": current_week.get("doctors_sharing_unique", 0),
            "subtitle": "Sharing / Enrolled",
            "delta": current_week.get("wow_doctors_sharing_unique_delta", 0),
            "href": _metric_href(campaign_id, "doctors_sharing", filters),
        },
        {
            "title": "Total Shares",
            "value": current_week.get("shares_total", 0),
            "subtitle": "Shares this week / campaign-to-date",
            "delta": current_week.get("wow_shares_total_delta", 0),
            "href": _metric_href(campaign_id, "total_shares", filters),
        },
        {
            "title": "Unique Caregivers Reached",
            "value": current_week.get("unique_recipient_references", 0),
            "subtitle": "Recipient refs / Shares",
            "delta": current_week.get("wow_unique_recipient_references_delta", 0),
            "href": _metric_href(campaign_id, "unique_recipients", filters),
        },
    ]
    playback_cards = [
        {
            "title": "Shares Played",
            "value": current_week.get("shares_played", 0),
            "delta": current_week.get("wow_shares_played_delta", 0),
            "href": _metric_href(campaign_id, "shares_played", filters),
        },
        {
            "title": "Viewed >50%",
            "value": current_week.get("shares_viewed_50", 0),
            "delta": current_week.get("wow_shares_viewed_50_delta", 0),
            "href": _metric_href(campaign_id, "shares_viewed_50", filters),
        },
        {
            "title": "Completed 100%",
            "value": current_week.get("shares_viewed_100", 0),
            "delta": current_week.get("wow_shares_viewed_100_delta", 0),
            "href": _metric_href(campaign_id, "shares_viewed_100", filters),
        },
        {
            "title": "Video / Bundle Shares",
            "value": f"{current_week.get('video_shares', 0)} / {current_week.get('bundle_shares', 0)}",
            "delta": 0,
            "href": _metric_href(campaign_id, "video_shares", filters),
        },
    ]

    return {
        "ready": True,
        "campaign_id": clean_text(dataset["registry"].get("campaign_id_original")) or campaign_id,
        "campaign_name": clean_text(dataset["registry"].get("campaign_name")) or campaign_id,
        "brand_name": clean_text(dataset["registry"].get("brand_name")) or "",
        "bundle_name": clean_text(dataset["registry"].get("campaign_name")) or clean_text(dataset["summary_row"].get("bundle_display_name")) or "",
        "registry": dataset["registry"],
        "refresh": dataset["refresh"],
        "filters": filters,
        "filters_query": filters_query,
        "filter_options": dataset["filter_options"],
        "summary": summary,
        "current_week": current_week,
        "state_attention": state_attention,
        "field_rep_attention": payload["field_rep_attention_rows"][:3],
        "action_row": payload["action_row"],
        "sharing_cards": sharing_cards,
        "playback_cards": playback_cards,
        "comparison_cards": {
            "current": summary,
            "best_week": best_week,
            "benchmark": benchmark,
        },
        "trend_labels": [f"Week {row.get('week_index')}" for row in weekly_rows_all],
        "trend_activation": [row.get("activation_pct", 0) for row in weekly_rows_all],
        "trend_play_rate": [row.get("play_rate_pct", 0) for row in weekly_rows_all],
        "trend_engagement_50": [row.get("engagement_50_pct", 0) for row in weekly_rows_all],
        "trend_completion": [row.get("completion_pct", 0) for row in weekly_rows_all],
        "weekly_rows": payload["weekly_rows"],
        "top_videos_shared": payload["video_rankings"][:5],
        "top_videos_viewed_50": payload["video_viewed_50_rankings"][:5],
        "top_bundles_shared": payload["bundle_rankings"][:5],
        "top_languages": payload["language_rankings"][:5],
        "detail_links": {
            "state_attention": _metric_href(campaign_id, "state_attention", filters),
            "field_rep_attention": _metric_href(campaign_id, "field_rep_attention", filters),
            "top_videos_shared": _metric_href(campaign_id, "top_videos_shared", filters),
            "top_videos_viewed_50": _metric_href(campaign_id, "top_videos_viewed_50", filters),
            "top_bundles_shared": _metric_href(campaign_id, "top_bundles_shared", filters),
            "languages": _metric_href(campaign_id, "languages", filters),
        },
        "dashboard_href": f"/pe-reports/campaign/{campaign_id}/{current_suffix}",
        "export_filename": f"patient-education-{slugify(clean_text(dataset['registry'].get('campaign_name')) or campaign_id)}-{clean_text((dataset['refresh'] or {}).get('as_of_date')) or 'report'}.pdf",
    }


def detail_context(campaign_id: str, metric: str, filters: dict[str, str | None], page: int = 1, per_page: int = 25) -> dict[str, Any]:
    dataset = _load_campaign_dataset(campaign_id)
    if not dataset["ready"]:
        raise Http404("Patient Education dashboard data has not been published yet")
    payload = build_dashboard_payload(
        dataset["registry"],
        filters,
        dataset["weekly_rows"],
        dataset["summary_row"],
        dataset["enrollment_rows"],
        dataset["share_rows"],
        dataset["video_rows"],
        get_thresholds(),
        benchmark_best_row=_benchmark_best_summary(),
    )
    try:
        title, columns, rows = metric_dataset(metric, payload)
    except KeyError as exc:
        raise Http404("Unknown metric") from exc
    total_rows = len(rows)
    page = max(page, 1)
    start = (page - 1) * per_page
    end = start + per_page
    return {
        "metric": metric,
        "title": title,
        "campaign_id": clean_text(dataset["registry"].get("campaign_id_original")) or campaign_id,
        "campaign_name": clean_text(dataset["registry"].get("campaign_name")) or campaign_id,
        "columns": columns,
        "rows": rows[start:end],
        "row_count": total_rows,
        "page": page,
        "page_count": max(1, ceil(total_rows / per_page)) if total_rows else 1,
        "filters": filters,
        "filters_query": current_filters_query(filters),
        "last_updated": clean_text((dataset["refresh"] or {}).get("published_at")) or "",
        "as_of_date": clean_text((dataset["refresh"] or {}).get("as_of_date")) or "",
        "back_href": f"/pe-reports/campaign/{campaign_id}/{f'?{current_filters_query(filters)}' if current_filters_query(filters) else ''}",
    }


def export_detail_csv(campaign_id: str, metric: str, filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    dataset = _load_campaign_dataset(campaign_id)
    if not dataset["ready"]:
        raise Http404("Patient Education dashboard data has not been published yet")
    payload = build_dashboard_payload(
        dataset["registry"],
        filters,
        dataset["weekly_rows"],
        dataset["summary_row"],
        dataset["enrollment_rows"],
        dataset["share_rows"],
        dataset["video_rows"],
        get_thresholds(),
        benchmark_best_row=_benchmark_best_summary(),
    )
    try:
        title, columns, rows = metric_dataset(metric, payload)
    except KeyError as exc:
        raise Http404("Unknown metric") from exc
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="pe-{slugify(metric)}.csv"'
    writer = csv.writer(response)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(column, "") for column in columns])
    log_export(metric, clean_text(dataset["registry"].get("campaign_id_original")) or campaign_id, f"/pe-reports/campaign/{campaign_id}/details/{metric}/export/", current_filters_query(filters), len(rows), getattr(getattr(request, "session", None), "session_key", None))
    return response


def export_dashboard_pdf(campaign_id: str, filters: dict[str, str | None], request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    snapshot = request.FILES.get("snapshot")
    if snapshot is None:
        return HttpResponseBadRequest("Missing dashboard snapshot")
    image_bytes = snapshot.read()
    if not image_bytes:
        return HttpResponseBadRequest("Empty dashboard snapshot")

    image_reader = ImageReader(BytesIO(image_bytes))
    image_width, image_height = image_reader.getSize()
    if not image_width or not image_height:
        return HttpResponseBadRequest("Invalid dashboard snapshot")

    dataset = _load_campaign_dataset(campaign_id)
    if not dataset["ready"]:
        raise Http404("Patient Education dashboard data has not been published yet")
    as_of_date = clean_text((dataset["refresh"] or {}).get("as_of_date")) or "report"
    filename = f"patient-education-{slugify(clean_text(dataset['registry'].get('campaign_name')) or campaign_id)}-{as_of_date}.pdf"

    buffer = BytesIO()
    pdf = pdf_canvas.Canvas(buffer, pagesize=(float(image_width), float(image_height)))
    pdf.drawImage(image_reader, 0, 0, width=float(image_width), height=float(image_height), preserveAspectRatio=True, mask="auto")
    pdf.showPage()
    pdf.save()
    buffer.seek(0)

    response = HttpResponse(buffer.getvalue(), content_type="application/pdf")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    log_export("dashboard-pdf", clean_text(dataset["registry"].get("campaign_id_original")) or campaign_id, f"/pe-reports/campaign/{campaign_id}/export/dashboard.pdf", current_filters_query(filters), 0, getattr(getattr(request, "session", None), "session_key", None))
    return response


def etl_debug_context() -> dict[str, Any]:
    refresh = _latest_refresh()
    registry_rows = _global_rows("campaign_registry")
    dq_rows = fetch_all(
        """
        SELECT run_id, layer, table_name, issue_type, issue_count, created_at
        FROM control.pe_dq_issue_log
        ORDER BY created_at DESC
        LIMIT 20
        """
    )
    run_rows = fetch_all(
        """
        SELECT run_id, started_at, ended_at, status, trigger_type, notes
        FROM control.pe_etl_run_log
        ORDER BY started_at DESC
        LIMIT 10
        """
    )
    return {
        "ready": True,
        "refresh": refresh,
        "campaign_count": len(registry_rows),
        "runs": run_rows,
        "dq_issues": dq_rows,
    }
