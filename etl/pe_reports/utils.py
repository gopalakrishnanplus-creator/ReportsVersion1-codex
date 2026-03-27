from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable


NULL_LIKE = {"", "null", "none", "nil", "n/a", "na"}


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in NULL_LIKE:
        return None
    return text or None


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (AttributeError, TypeError, ValueError):
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except (AttributeError, TypeError, ValueError):
        return default


def normalize_phone(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    if len(digits) > 10 and digits.startswith("91"):
        digits = digits[-10:]
    return digits


def normalize_email(value: Any) -> str | None:
    text = clean_text(value)
    if text is None or "@" not in text:
        return None
    return text.lower()


def normalize_identifier(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    return re.sub(r"[^a-z0-9]", "", text.lower()) or None


def normalize_campaign_id(value: Any) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    return text.lower().replace("-", "")


def campaign_schema_name(campaign_id_normalized: str) -> str:
    base = re.sub(r"[^a-z0-9]", "_", (campaign_id_normalized or "").lower())
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "unknown"
    if not base[0].isalpha():
        base = f"c_{base}"
    return f"gold_pe_campaign_{base}"


def slugify(value: Any) -> str:
    text = clean_text(value) or "unknown"
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "unknown"


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = clean_text(value)
    if text is None:
        return None
    normalized = text.replace("T", " ").replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized[:32], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = parse_datetime(value)
    return parsed.date() if parsed else None


def iso_datetime(value: Any) -> str | None:
    parsed = parse_datetime(value)
    return parsed.isoformat(sep=" ") if parsed else None


def iso_date(value: Any) -> str | None:
    parsed = parse_date(value)
    return parsed.isoformat() if parsed else None


def hash_fields(*values: Any) -> str:
    payload = json.dumps([str(v) if v is not None else None for v in values], ensure_ascii=True)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def safe_pct(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return (numerator / denominator) * 100.0


def health_color(score: float | int | None) -> str:
    value = as_float(score)
    if value < 40:
        return "red"
    if value < 60:
        return "yellow"
    return "green"


def week_end_saturday(value: date | datetime | Any) -> date | None:
    current = parse_date(value)
    if current is None:
        return None
    delta = (5 - current.weekday()) % 7
    return current + timedelta(days=delta)


def week_start_sunday(week_end: date | datetime | Any) -> date | None:
    current = parse_date(week_end)
    if current is None:
        return None
    return current - timedelta(days=6)


def iter_week_ranges(start_value: Any, end_value: Any) -> list[tuple[date, date]]:
    start_date = parse_date(start_value)
    end_date = parse_date(end_value)
    if start_date is None or end_date is None or start_date > end_date:
        return []
    current_end = week_end_saturday(start_date) or start_date
    ranges: list[tuple[date, date]] = []
    while current_end <= end_date:
        ranges.append((week_start_sunday(current_end) or current_end, current_end))
        current_end += timedelta(days=7)
    if not ranges or ranges[-1][1] < end_date:
        final_end = week_end_saturday(end_date) or end_date
        ranges.append((week_start_sunday(final_end) or final_end, final_end))
    return ranges


def first_non_empty(*values: Any) -> str | None:
    for value in values:
        cleaned = clean_text(value)
        if cleaned is not None:
            return cleaned
    return None


def unique_preserving_order(values: Iterable[Any]) -> list[Any]:
    output: list[Any] = []
    seen: set[Any] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output

