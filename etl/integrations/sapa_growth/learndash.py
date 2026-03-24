from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests
from django.conf import settings


class LearnDashIntegrationError(RuntimeError):
    """Raised when SAPA WordPress/LearnDash extraction fails."""


class LearnDashClient:
    def __init__(self) -> None:
        self.config = settings.SAPA_WORDPRESS

    def _fixture_path(self, action: str, suffix: str = "") -> Path:
        fixture_dir = self.config.get("FIXTURE_DIR") or settings.SAPA_SOURCE_FIXTURE_DIR
        filename = f"{action}{suffix}.json"
        return Path(fixture_dir) / filename

    def _load_fixture(self, action: str, suffix: str = "") -> dict[str, Any]:
        path = self._fixture_path(action, suffix=suffix)
        if not path.exists():
            raise LearnDashIntegrationError(f"Missing WordPress fixture: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _fetch(self, action: str, params: dict[str, Any] | None = None, fixture_suffix: str = "") -> dict[str, Any]:
        params = dict(params or {})
        backend = (self.config.get("BACKEND") or "http").lower()
        if backend == "fixture":
            return self._load_fixture(action, suffix=fixture_suffix)

        base_url = (self.config.get("BASE_URL") or "").strip()
        api_secret = (self.config.get("API_SECRET") or "").strip()
        if not base_url or not api_secret:
            raise LearnDashIntegrationError("SAPA WordPress credentials are not configured")

        params["ld_api"] = action
        params["secret"] = api_secret
        retries = max(int(self.config.get("RETRY_COUNT", 2)), 0)
        timeout = int(self.config.get("TIMEOUT", 30))
        last_error: Exception | None = None

        for attempt in range(retries + 1):
            try:
                response = requests.get(base_url, params=params, timeout=timeout)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise LearnDashIntegrationError(f"Unexpected payload type for action={action}")
                return payload
            except Exception as exc:
                last_error = exc
                if attempt >= retries:
                    break
                time.sleep(min(2 ** attempt, 5))

        raise LearnDashIntegrationError(f"LearnDash fetch failed for action={action}: {last_error}") from last_error

    def get_webinar_registrations(self) -> list[dict[str, Any]]:
        payload = self._fetch("webinar_registrations")
        return list(payload.get("data") or [])

    def get_course_summary(self, course_id: int) -> dict[str, Any]:
        payload = self._fetch("course_summary", {"course_id": course_id}, fixture_suffix=f"_{course_id}")
        return dict(payload.get("data") or {})

    def get_course_breakdown(self, course_id: int) -> list[dict[str, Any]]:
        payload = self._fetch("course_breakdown", {"course_id": course_id}, fixture_suffix=f"_{course_id}")
        return list(payload.get("data") or [])
