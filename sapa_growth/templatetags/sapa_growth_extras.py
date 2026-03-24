from django import template
from urllib.parse import urlparse


register = template.Library()


@register.filter
def get_item(value, key):
    if isinstance(value, dict):
        return value.get(key, "")
    return ""


@register.filter
def is_external_url(value):
    if not value:
        return False
    parsed = urlparse(str(value))
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
