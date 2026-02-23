import hashlib
import re
from datetime import datetime
from typing import Any


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    txt = str(value).strip()
    return txt or None


def normalize_phone(value: Any, minimum_digits: int = 7) -> str | None:
    txt = normalize_text(value)
    if not txt:
        return None
    has_plus = txt.startswith("+")
    digits = re.sub(r"\D", "", txt)
    if len(digits) < minimum_digits:
        return None
    return f"+{digits}" if has_plus else digits


def normalize_email(value: Any) -> str | None:
    txt = normalize_text(value)
    if not txt:
        return None
    email = txt.lower()
    if "@" not in email:
        return None
    local, domain = email.split("@", 1)
    if not local or "." not in domain:
        return None
    return email


def parse_timestamp(value: Any) -> datetime | None:
    txt = normalize_text(value)
    if not txt:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_bool(value: Any) -> bool | None:
    txt = normalize_text(value)
    if txt is None:
        return None
    if txt.lower() in {"1", "true", "t", "yes", "y"}:
        return True
    if txt.lower() in {"0", "false", "f", "no", "n"}:
        return False
    return None


def parse_float(value: Any) -> float | None:
    txt = normalize_text(value)
    if txt is None:
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def hash_identity(*parts: Any) -> str:
    joined = "|".join(str(p) for p in parts if normalize_text(p) is not None)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
