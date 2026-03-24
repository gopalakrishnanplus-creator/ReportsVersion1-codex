from __future__ import annotations

import json
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from django.conf import settings

from sapa_growth.logic import clean_text

SUPPORTED_VIDEO_HOSTS = {
    "youtu.be",
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "vimeo.com",
    "www.vimeo.com",
    "player.vimeo.com",
}

YOUTUBE_HOSTS = {
    "youtu.be",
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
}

VIMEO_HOSTS = {
    "vimeo.com",
    "www.vimeo.com",
    "player.vimeo.com",
}


def supported_video_link(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if text.startswith("//"):
        text = f"https:{text}"
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if parsed.netloc.lower() not in SUPPORTED_VIDEO_HOSTS:
        return ""
    return text


def _oembed_endpoint(video_url: str) -> str:
    host = urlparse(video_url).netloc.lower()
    if host in YOUTUBE_HOSTS:
        return f"https://www.youtube.com/oembed?{urlencode({'url': video_url, 'format': 'json'})}"
    if host in VIMEO_HOSTS:
        return f"https://vimeo.com/api/oembed.json?{urlencode({'url': video_url})}"
    return ""


@lru_cache(maxsize=512)
def resolve_video_metadata(video_url: Any) -> dict[str, str]:
    normalized_url = supported_video_link(video_url)
    empty = {
        "video_url": "",
        "video_title": "",
        "preferred_display_label": "",
    }
    if not normalized_url:
        return empty

    fallback = {
        "video_url": normalized_url,
        "video_title": "",
        "preferred_display_label": normalized_url,
    }
    endpoint = _oembed_endpoint(normalized_url)
    if not endpoint:
        return fallback

    timeout = int(getattr(settings, "SAPA_VIDEO_METADATA_TIMEOUT", 8))
    request = Request(
        endpoint,
        headers={
            "Accept": "application/json",
            "User-Agent": "InclinicReporting-SAPA/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except Exception:
        return fallback

    title = clean_text(payload.get("title"))
    if not title:
        return fallback
    return {
        "video_url": normalized_url,
        "video_title": title,
        "preferred_display_label": title,
    }
