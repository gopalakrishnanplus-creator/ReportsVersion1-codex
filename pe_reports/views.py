from __future__ import annotations

from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from pe_reports.services import (
    _campaign_registry_row,
    dashboard_context,
    detail_context,
    etl_debug_context,
    export_dashboard_pdf,
    export_detail_csv,
    menu_context,
    parse_filters,
)
from etl.pe_reports.utils import clean_text
from reporting.access import absolute_url, access_email_history, authenticate_session, build_report_access, is_authenticated, send_access_email, validate_credentials


def campaign_menu(request: HttpRequest) -> HttpResponse:
    return render(request, "pe_reports/menu.html", menu_context())


def campaign_login(request: HttpRequest, campaign_id: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    if registry is None:
        messages.error(request, "That Patient Education campaign is not available.")
        return redirect("pe_reports:menu")

    canonical_campaign_id = clean_text(registry.get("campaign_id_original")) or campaign_id
    if is_authenticated(request, "pe", canonical_campaign_id):
        return redirect("pe_reports:dashboard", campaign_id=canonical_campaign_id)

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        if validate_credentials("pe", canonical_campaign_id, username, password):
            authenticate_session(request, "pe", canonical_campaign_id)
            return redirect("pe_reports:dashboard", campaign_id=canonical_campaign_id)
        error_message = "Invalid campaign credentials"

    return render(
        request,
        "pe_reports/login.html",
        {
            "campaign_id": canonical_campaign_id,
            "campaign_name": clean_text(registry.get("campaign_name")) or canonical_campaign_id,
            "brand_name": clean_text(registry.get("brand_name")) or "",
            "error_message": error_message,
        },
    )


def campaign_access_page(request: HttpRequest, campaign_id: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    if registry is None:
        messages.error(request, "That Patient Education campaign is not available.")
        return redirect("pe_reports:menu")
    canonical_campaign_id = clean_text(registry.get("campaign_id_original")) or campaign_id
    return render(
        request,
        "pe_reports/access.html",
        {
            "campaign_id": canonical_campaign_id,
            "campaign_name": clean_text(registry.get("campaign_name")) or canonical_campaign_id,
            "brand_name": clean_text(registry.get("brand_name")) or "",
            "history_rows": access_email_history("pe", canonical_campaign_id),
        },
    )


def send_access_email_view(request: HttpRequest, campaign_id: str) -> HttpResponse:
    if request.method != "POST":
        return redirect("pe_reports:access", campaign_id=campaign_id)
    registry = _campaign_registry_row(campaign_id)
    if registry is None:
        messages.error(request, "That Patient Education campaign is not available.")
        return redirect("pe_reports:menu")

    canonical_campaign_id = clean_text(registry.get("campaign_id_original")) or campaign_id
    access = build_report_access("pe", canonical_campaign_id)
    recipient_email = request.POST.get("recipient_email", "")
    try:
        send_access_email(
            report_key="pe",
            recipient_email=recipient_email,
            access_url=absolute_url(request, f"/pe-reports/campaign/{canonical_campaign_id}/login/"),
            report_name=clean_text(registry.get("campaign_name")) or canonical_campaign_id,
            scope_label="Campaign",
            scope_id=canonical_campaign_id,
            username=access.username,
            password=access.password,
            brand_name=clean_text(registry.get("brand_name")) or "",
        )
    except Exception as exc:
        messages.error(request, f"PE access email could not be sent: {exc}")
        return redirect("pe_reports:access", campaign_id=canonical_campaign_id)

    messages.success(request, f"PE access email sent to {(recipient_email or '').strip()}.")
    return redirect("pe_reports:access", campaign_id=canonical_campaign_id)


def dashboard(request: HttpRequest, campaign_id: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    canonical_campaign_id = clean_text((registry or {}).get("campaign_id_original")) or campaign_id
    if not is_authenticated(request, "pe", canonical_campaign_id):
        return redirect("pe_reports:login", campaign_id=canonical_campaign_id)
    filters = parse_filters(request.GET)
    return render(request, "pe_reports/dashboard.html", dashboard_context(canonical_campaign_id, filters))


def detail_view(request: HttpRequest, campaign_id: str, metric: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    canonical_campaign_id = clean_text((registry or {}).get("campaign_id_original")) or campaign_id
    if not is_authenticated(request, "pe", canonical_campaign_id):
        return redirect("pe_reports:login", campaign_id=canonical_campaign_id)
    filters = parse_filters(request.GET)
    page = int(request.GET.get("page", "1") or "1")
    return render(request, "pe_reports/detail.html", detail_context(canonical_campaign_id, metric, filters, page=page))


def detail_export(request: HttpRequest, campaign_id: str, metric: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    canonical_campaign_id = clean_text((registry or {}).get("campaign_id_original")) or campaign_id
    if not is_authenticated(request, "pe", canonical_campaign_id):
        return redirect("pe_reports:login", campaign_id=canonical_campaign_id)
    filters = parse_filters(request.GET)
    return export_detail_csv(canonical_campaign_id, metric, filters, request)


def dashboard_export(request: HttpRequest, campaign_id: str) -> HttpResponse:
    registry = _campaign_registry_row(campaign_id)
    canonical_campaign_id = clean_text((registry or {}).get("campaign_id_original")) or campaign_id
    if not is_authenticated(request, "pe", canonical_campaign_id):
        return redirect("pe_reports:login", campaign_id=canonical_campaign_id)
    filters = parse_filters(request.GET)
    return export_dashboard_pdf(canonical_campaign_id, filters, request)


def etl_debug(request: HttpRequest) -> HttpResponse:
    return render(request, "pe_reports/debug.html", etl_debug_context())
