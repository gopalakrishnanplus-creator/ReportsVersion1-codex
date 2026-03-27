from __future__ import annotations

from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render

from sapa_growth.services import (
    certified_context,
    dashboard_context,
    detail_context,
    export_certified_csv,
    export_dashboard_pdf,
    export_detail_csv,
    parse_certified_filters,
    parse_global_filters,
)
from reporting.access import absolute_url, authenticate_session, build_report_access, is_authenticated, send_access_email, validate_credentials


SAPA_SCOPE_KEY = "growth-clinic"


def login(request: HttpRequest) -> HttpResponse:
    if is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:dashboard")

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        if validate_credentials("sapa", SAPA_SCOPE_KEY, username, password):
            authenticate_session(request, "sapa", SAPA_SCOPE_KEY)
            return redirect("sapa_growth:dashboard")
        error_message = "Invalid report credentials"

    return render(
        request,
        "sapa_growth/login.html",
        {
            "error_message": error_message,
            "program_name": "SAPA Growth Clinic Program",
        },
    )


def send_access_email_view(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("sapa_growth:login")
    recipient_email = request.POST.get("recipient_email", "")
    try:
        access = absolute_url(request, "/sapa-growth/login/")
        credentials = build_report_access("sapa", SAPA_SCOPE_KEY)
        send_access_email(
            report_key="sapa",
            recipient_email=recipient_email,
            access_url=access,
            report_name="SAPA Growth Clinic Program",
            scope_label="Program",
            scope_id="sapa-growth",
            username=credentials.username,
            password=credentials.password,
            brand_name="SAPA",
        )
    except Exception as exc:
        messages.error(request, f"SAPA access email could not be sent: {exc}")
        return redirect("sapa_growth:login")

    messages.success(request, f"SAPA access email sent to {(recipient_email or '').strip()}.")
    return redirect("sapa_growth:login")


def dashboard(request: HttpRequest) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    filters = parse_global_filters(request.GET)
    context = dashboard_context(filters)
    return render(request, "sapa_growth/dashboard.html", context)


def certified_clinics_partial(request: HttpRequest) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    global_filters = parse_global_filters(request.GET)
    local_filters = parse_certified_filters(request.GET, global_filters)
    context = certified_context(global_filters, local_filters)
    return render(request, "sapa_growth/_certified_list.html", context)


def detail_view(request: HttpRequest, metric: str) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    filters = parse_global_filters(request.GET)
    page = int(request.GET.get("page", "1") or "1")
    context = detail_context(metric, filters, page=page)
    return render(request, "sapa_growth/detail.html", context)


def dashboard_export(request: HttpRequest) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    filters = parse_global_filters(request.GET)
    return export_dashboard_pdf(filters, request)


def detail_export(request: HttpRequest, metric: str) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    filters = parse_global_filters(request.GET)
    return export_detail_csv(metric, filters, request)


def certified_export(request: HttpRequest) -> HttpResponse:
    if not is_authenticated(request, "sapa", SAPA_SCOPE_KEY):
        return redirect("sapa_growth:login")
    global_filters = parse_global_filters(request.GET)
    local_filters = parse_certified_filters(request.GET, global_filters)
    return export_certified_csv(global_filters, local_filters, request)
