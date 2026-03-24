from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

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


def dashboard(request: HttpRequest) -> HttpResponse:
    filters = parse_global_filters(request.GET)
    context = dashboard_context(filters)
    return render(request, "sapa_growth/dashboard.html", context)


def certified_clinics_partial(request: HttpRequest) -> HttpResponse:
    global_filters = parse_global_filters(request.GET)
    local_filters = parse_certified_filters(request.GET, global_filters)
    context = certified_context(global_filters, local_filters)
    return render(request, "sapa_growth/_certified_list.html", context)


def detail_view(request: HttpRequest, metric: str) -> HttpResponse:
    filters = parse_global_filters(request.GET)
    page = int(request.GET.get("page", "1") or "1")
    context = detail_context(metric, filters, page=page)
    return render(request, "sapa_growth/detail.html", context)


def dashboard_export(request: HttpRequest) -> HttpResponse:
    filters = parse_global_filters(request.GET)
    return export_dashboard_pdf(filters, request)


def detail_export(request: HttpRequest, metric: str) -> HttpResponse:
    filters = parse_global_filters(request.GET)
    return export_detail_csv(metric, filters, request)


def certified_export(request: HttpRequest) -> HttpResponse:
    global_filters = parse_global_filters(request.GET)
    local_filters = parse_certified_filters(request.GET, global_filters)
    return export_certified_csv(global_filters, local_filters, request)
