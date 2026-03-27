from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.template.loader import render_to_string
from django.utils.crypto import constant_time_compare


@dataclass(frozen=True)
class ReportAccess:
    report_key: str
    scope_key: str
    username: str
    password: str
    session_key: str


REPORT_THEMES = {
    "inclinic": {
        "primary": "#1f3258",
        "secondary": "#4f7dd1",
        "heading": "#16233c",
        "accent": "#e45757",
        "background": "#f4f7fb",
        "label": "In-Clinic Sharing Report",
    },
    "pe": {
        "primary": "#2aa7a1",
        "secondary": "#3fb6af",
        "heading": "#2f3e9e",
        "accent": "#e45757",
        "background": "#f6fafb",
        "label": "Patient Education Report",
    },
    "sapa": {
        "primary": "#1f8c84",
        "secondary": "#55c2b3",
        "heading": "#284037",
        "accent": "#da594f",
        "background": "#f3f8f3",
        "label": "SAPA Growth Clinic Dashboard",
    },
}


def _tokenize(value: Any) -> str:
    token = re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())
    return token or "access"


def build_report_access(report_key: str, scope_key: Any) -> ReportAccess:
    normalized_report = _tokenize(report_key)
    normalized_scope = _tokenize(scope_key)
    if normalized_report == "inclinic":
        username = f"brand_{normalized_scope[:6]}"
        password = f"report_{normalized_scope[-4:]}"
    elif normalized_report == "pe":
        username = f"pe_{normalized_scope[:6]}"
        password = f"report_{normalized_scope[-4:]}"
    else:
        username = f"sapa_{normalized_scope[:6]}"
        password = f"report_{normalized_scope[-4:]}"
    return ReportAccess(
        report_key=normalized_report,
        scope_key=normalized_scope,
        username=username,
        password=password,
        session_key=f"auth_{normalized_report}_{normalized_scope}",
    )


def is_authenticated(request: Any, report_key: str, scope_key: Any) -> bool:
    access = build_report_access(report_key, scope_key)
    return bool(getattr(request, "session", {}).get(access.session_key))


def authenticate_session(request: Any, report_key: str, scope_key: Any) -> None:
    access = build_report_access(report_key, scope_key)
    request.session[access.session_key] = True


def validate_credentials(report_key: str, scope_key: Any, username: str, password: str) -> bool:
    access = build_report_access(report_key, scope_key)
    return constant_time_compare((username or "").strip(), access.username) and constant_time_compare((password or "").strip(), access.password)


def validate_recipient_email(value: str) -> str:
    email = (value or "").strip().lower()
    if not email:
        raise ValueError("Receiver email is required.")
    try:
        validate_email(email)
    except ValidationError as exc:
        raise ValueError("Enter a valid receiver email address.") from exc
    return email


def _public_base_url() -> str:
    return str(settings.REPORTS_EMAIL.get("PUBLIC_BASE_URL") or "").rstrip("/")


def absolute_url(request: Any, relative_path: str) -> str:
    if _public_base_url():
        return f"{_public_base_url()}/{str(relative_path).lstrip('/')}"
    return request.build_absolute_uri(relative_path)


def _access_log_path() -> Path:
    return Path(str(settings.REPORTS_EMAIL.get("ACCESS_LOG_PATH") or "")).expanduser()


def log_access_email(
    *,
    report_key: str,
    scope_id: str,
    scope_label: str,
    report_name: str,
    recipient_email: str,
    brand_name: str,
    access_url: str,
) -> None:
    log_path = _access_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "report_key": _tokenize(report_key),
        "scope_id": str(scope_id),
        "scope_label": scope_label,
        "report_name": report_name,
        "recipient_email": recipient_email,
        "brand_name": brand_name,
        "access_url": access_url,
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def access_email_history(report_key: str, scope_id: str) -> list[dict[str, Any]]:
    log_path = _access_log_path()
    if not log_path.exists():
        return []
    normalized_report = _tokenize(report_key)
    rows: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if _tokenize(record.get("report_key")) != normalized_report:
                continue
            if str(record.get("scope_id") or "") != str(scope_id):
                continue
            rows.append(record)
    rows.sort(key=lambda row: str(row.get("sent_at") or ""), reverse=True)
    return rows


def send_access_email(
    *,
    report_key: str,
    recipient_email: str,
    access_url: str,
    report_name: str,
    scope_label: str,
    scope_id: str,
    username: str,
    password: str,
    brand_name: str = "",
) -> None:
    email = validate_recipient_email(recipient_email)
    config = settings.REPORTS_EMAIL
    api_key = str(config.get("SENDGRID_API_KEY") or "").strip()
    from_email = str(config.get("FROM_EMAIL") or "").strip()
    from_name = str(config.get("FROM_NAME") or "Inditech Reports").strip()
    reply_to = str(config.get("REPLY_TO") or "").strip()
    timeout = int(config.get("TIMEOUT") or 20)

    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY is not configured.")
    if not from_email:
        raise RuntimeError("REPORTS_EMAIL_FROM is not configured.")

    theme = REPORT_THEMES.get(report_key, REPORT_THEMES["inclinic"])
    report_label = theme["label"]
    subject = f"{report_label} access for {report_name}"
    html_content = render_to_string(
        "reporting/access_email.html",
        {
            "report_label": report_label,
            "report_name": report_name,
            "scope_label": scope_label,
            "scope_id": scope_id,
            "brand_name": brand_name,
            "access_url": access_url,
            "username": username,
            "password": password,
            "primary_color": theme["primary"],
            "secondary_color": theme["secondary"],
            "heading_color": theme["heading"],
            "accent_color": theme["accent"],
            "background_color": theme["background"],
        },
    )
    text_content = (
        f"{report_label}\n\n"
        f"{scope_label}: {report_name}\n"
        f"Reference ID: {scope_id}\n"
        f"Brand: {brand_name or '-'}\n\n"
        f"Access page: {access_url}\n"
        f"Username: {username}\n"
        f"Password: {password}\n"
    )

    payload: dict[str, Any] = {
        "personalizations": [{"to": [{"email": email}]}],
        "from": {"email": from_email, "name": from_name},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": text_content},
            {"type": "text/html", "value": html_content},
        ],
    }
    if reply_to:
        payload["reply_to"] = {"email": reply_to}

    response = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"SendGrid rejected the message ({response.status_code}).")
    log_access_email(
        report_key=report_key,
        scope_id=scope_id,
        scope_label=scope_label,
        report_name=report_name,
        recipient_email=email,
        brand_name=brand_name,
        access_url=access_url,
    )
