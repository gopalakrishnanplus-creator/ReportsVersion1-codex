from __future__ import annotations

import logging
from typing import Callable

from django.db import DatabaseError
from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET

from reporting.api_services import build_in_clinic_rows, build_patient_education_rows, build_red_flag_alert_rows
from reporting.campaign_performance import (
    CampaignPerformanceNotFound,
    build_campaign_performance_page_payload,
    build_campaign_performance_payload,
)


logger = logging.getLogger(__name__)


def _payload_response(subsystem: str, rows: list[dict[str, object]], *, status: int = 200, detail: str | None = None) -> JsonResponse:
    results = []
    for index, row in enumerate(rows, start=1):
        item = {"id": index}
        item.update(row)
        results.append(item)
    payload: dict[str, object] = {
        "subsystem": subsystem,
        "count": len(results),
        "results": results,
    }
    if detail:
        payload["detail"] = detail
    return JsonResponse(payload, status=status)


def _render_api(subsystem: str, builder: Callable[[], list[dict[str, object]]]) -> JsonResponse:
    try:
        return _payload_response(subsystem, builder())
    except DatabaseError:
        logger.exception("Unified reporting API database failure for %s", subsystem)
        return _payload_response(subsystem, [], status=503, detail="Reporting data is currently unavailable.")
    except Exception:
        logger.exception("Unified reporting API failed for %s", subsystem)
        return _payload_response(subsystem, [], status=500, detail="Unexpected reporting API error.")


@require_GET
def red_flag_alert_api(_request: HttpRequest) -> JsonResponse:
    return _render_api("red_flag_alert", build_red_flag_alert_rows)


@require_GET
def in_clinic_api(_request: HttpRequest) -> JsonResponse:
    return _render_api("in_clinic", build_in_clinic_rows)


@require_GET
def patient_education_api(_request: HttpRequest) -> JsonResponse:
    return _render_api("patient_education", build_patient_education_rows)


@require_GET
def campaign_performance_api(request: HttpRequest, campaign_id: str | None = None) -> JsonResponse:
    requested_campaign_id = str(campaign_id or request.GET.get("campaign_id") or "").strip()
    if not requested_campaign_id:
        return JsonResponse(
            {
                "detail": "campaign_id is required.",
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=400,
        )
    try:
        payload = build_campaign_performance_payload(requested_campaign_id)
        payload["requested_campaign_id"] = requested_campaign_id
        return JsonResponse(payload)
    except CampaignPerformanceNotFound as exc:
        return JsonResponse(
            {
                "detail": str(exc),
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=404,
        )
    except DatabaseError:
        logger.exception("Campaign performance API database failure for %s", requested_campaign_id)
        return JsonResponse(
            {
                "detail": "Campaign performance data is currently unavailable.",
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=503,
        )
    except Exception:
        logger.exception("Campaign performance API failed for %s", requested_campaign_id)
        return JsonResponse(
            {
                "detail": "Unexpected campaign performance API error.",
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=500,
        )


@require_GET
def campaign_performance_page_api(request: HttpRequest, campaign_id: str | None = None) -> JsonResponse:
    requested_campaign_id = str(campaign_id or request.GET.get("campaign_id") or "").strip()
    if not requested_campaign_id:
        return JsonResponse(
            {
                "detail": "campaign_id is required.",
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=400,
        )
    try:
        payload = build_campaign_performance_page_payload(requested_campaign_id)
        payload["requested_campaign_id"] = requested_campaign_id
        return JsonResponse(payload)
    except CampaignPerformanceNotFound as exc:
        return JsonResponse(
            {
                "detail": str(exc),
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=404,
        )
    except DatabaseError:
        logger.exception("Campaign performance page API database failure for %s", requested_campaign_id)
        return JsonResponse(
            {
                "detail": "Campaign performance page data is currently unavailable.",
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=503,
        )
    except Exception:
        logger.exception("Campaign performance page API failed for %s", requested_campaign_id)
        return JsonResponse(
            {
                "detail": "Unexpected campaign performance page API error.",
                "requested_campaign_id": requested_campaign_id,
                "system_count": 0,
                "available_systems": [],
                "sections": [],
            },
            status=500,
        )
