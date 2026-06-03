from __future__ import annotations

from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse

from sapa_growth.services import (
    _latest_refresh,
    campaign_options,
    certified_context,
    dashboard_context,
    detail_context,
    export_certified_csv,
    export_dashboard_pdf,
    export_detail_csv,
    parse_certified_filters,
    parse_global_filters,
)
from reporting.access import absolute_url, access_email_history, authenticate_session, build_report_access, is_authenticated, send_access_email, validate_credentials


SAPA_SCOPE_KEY = "growth-clinic"


def _campaign_scope_key(campaign_key: str | None) -> str:
    campaign_key = (campaign_key or "").strip()
    return f"{SAPA_SCOPE_KEY}:{campaign_key}" if campaign_key else SAPA_SCOPE_KEY


def _campaign_route(name: str, campaign_key: str | None, **kwargs: str) -> str:
    if campaign_key:
        names = {
            "login": "sapa_growth:campaign-login",
            "access": "sapa_growth:campaign-access",
            "dashboard": "sapa_growth:campaign-dashboard",
            "detail": "sapa_growth:campaign-detail",
            "detail-export": "sapa_growth:campaign-detail-export",
            "dashboard-export": "sapa_growth:campaign-dashboard-export",
            "certified-clinics": "sapa_growth:campaign-certified-clinics",
            "certified-export": "sapa_growth:campaign-certified-export",
            "send-access-email": "sapa_growth:campaign-send-access-email",
        }
        return reverse(names[name], kwargs={"campaign_key": campaign_key, **kwargs})
    names = {
        "login": "sapa_growth:login",
        "access": "sapa_growth:access",
        "dashboard": "sapa_growth:dashboard",
        "detail": "sapa_growth:detail",
        "detail-export": "sapa_growth:detail-export",
        "dashboard-export": "sapa_growth:dashboard-export",
        "certified-clinics": "sapa_growth:certified-clinics",
        "certified-export": "sapa_growth:certified-export",
        "send-access-email": "sapa_growth:send-access-email",
    }
    return reverse(names[name], kwargs=kwargs)


def menu(request: HttpRequest) -> HttpResponse:
    campaigns = campaign_options()
    launcher_rows = []
    for option in campaigns:
        campaign_key = (option.get("underlying_key") or "").strip()
        launcher_rows.append(
            {
                "campaign_key": campaign_key,
                "campaign_label": option.get("display_label") or campaign_key or "SAPA Growth Clinic Program",
                "login_href": _campaign_route("login", campaign_key or None),
                "dashboard_href": _campaign_route("dashboard", campaign_key or None),
                "access_href": _campaign_route("access", campaign_key or None),
                "email_href": _campaign_route("send-access-email", campaign_key or None),
            }
        )
    return render(
        request,
        "sapa_growth/menu.html",
        {
            "program_name": "SAPA Growth Clinic Program",
            "refresh": _latest_refresh(),
            "campaigns": launcher_rows,
        },
    )


def login(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not campaign_key:
        return redirect(reverse("sapa_growth:menu"))
    scope_key = _campaign_scope_key(campaign_key)
    if is_authenticated(request, "sapa", scope_key):
        return redirect(_campaign_route("dashboard", campaign_key))

    error_message = None
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        password = (request.POST.get("password") or "").strip()
        if validate_credentials("sapa", scope_key, username, password):
            authenticate_session(request, "sapa", scope_key)
            return redirect(_campaign_route("dashboard", campaign_key))
        error_message = "Invalid report credentials"

    selected_campaign = next((row for row in campaign_options() if (row.get("underlying_key") or "").strip() == (campaign_key or "").strip()), None)
    return render(
        request,
        "sapa_growth/login.html",
        {
            "error_message": error_message,
            "program_name": (selected_campaign or {}).get("display_label") or "SAPA Growth Clinic Program",
            "menu_href": reverse("sapa_growth:menu"),
            "campaign_key": campaign_key,
        },
    )


def access_page(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not campaign_key:
        return redirect(reverse("sapa_growth:menu"))
    selected_campaign = next((row for row in campaign_options() if (row.get("underlying_key") or "").strip() == (campaign_key or "").strip()), None)
    return render(
        request,
        "sapa_growth/access.html",
        {
            "program_name": (selected_campaign or {}).get("display_label") or "SAPA Growth Clinic Program",
            "history_rows": access_email_history("sapa", _campaign_scope_key(campaign_key)),
            "login_href": _campaign_route("login", campaign_key),
            "menu_href": reverse("sapa_growth:menu"),
            "send_access_href": _campaign_route("send-access-email", campaign_key),
        },
    )


def send_access_email_view(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not campaign_key:
        return redirect(reverse("sapa_growth:menu"))
    if request.method != "POST":
        return redirect(_campaign_route("access", campaign_key))
    recipient_email = request.POST.get("recipient_email", "")
    try:
        access = absolute_url(request, _campaign_route("login", campaign_key))
        credentials = build_report_access("sapa", _campaign_scope_key(campaign_key))
        selected_campaign = next((row for row in campaign_options() if (row.get("underlying_key") or "").strip() == (campaign_key or "").strip()), None)
        send_access_email(
            report_key="sapa",
            recipient_email=recipient_email,
            access_url=access,
            report_name=(selected_campaign or {}).get("display_label") or "SAPA Growth Clinic Program",
            scope_label="Campaign" if campaign_key else "Program",
            scope_id=campaign_key or "sapa-growth",
            username=credentials.username,
            password=credentials.password,
            brand_name=(selected_campaign or {}).get("display_label") or "SAPA",
        )
    except Exception as exc:
        messages.error(request, f"SAPA access email could not be sent: {exc}")
        return redirect(_campaign_route("access", campaign_key))

    messages.success(request, f"SAPA access email sent to {(recipient_email or '').strip()}.")
    return redirect(_campaign_route("access", campaign_key))


def dashboard(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not campaign_key:
        return redirect(reverse("sapa_growth:menu"))
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    context = dashboard_context(filters)
    return render(request, "sapa_growth/dashboard.html", context)


def certified_clinics_partial(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    global_filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    local_filters = parse_certified_filters(request.GET, global_filters)
    context = certified_context(global_filters, local_filters)
    return render(request, "sapa_growth/_certified_list.html", context)


def detail_view(request: HttpRequest, metric: str, campaign_key: str | None = None) -> HttpResponse:
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    page = int(request.GET.get("page", "1") or "1")
    context = detail_context(metric, filters, page=page)
    return render(request, "sapa_growth/detail.html", context)


def dashboard_export(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    return export_dashboard_pdf(filters, request)


def detail_export(request: HttpRequest, metric: str, campaign_key: str | None = None) -> HttpResponse:
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    return export_detail_csv(metric, filters, request)


def certified_export(request: HttpRequest, campaign_key: str | None = None) -> HttpResponse:
    if not is_authenticated(request, "sapa", _campaign_scope_key(campaign_key)):
        return redirect(_campaign_route("login", campaign_key))
    global_filters = parse_global_filters(request.GET, campaign_key=campaign_key)
    local_filters = parse_certified_filters(request.GET, global_filters)
    return export_certified_csv(global_filters, local_filters, request)
