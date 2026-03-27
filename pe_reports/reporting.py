from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from etl.pe_reports.gold import (
    _action_rows,
    _bundle_rankings,
    _doctor_activity_rows,
    _field_rep_summary_rows,
    _language_summary_rows,
    _state_summary_rows,
    _video_rankings,
    compute_health_components,
)
from etl.pe_reports.utils import as_float, as_int, clean_text, health_color, week_end_saturday


def current_filters_query(filters: dict[str, str | None]) -> str:
    return urlencode({key: value for key, value in filters.items() if value})


def _date_or_none(value: Any) -> date | None:
    raw = clean_text(value)
    if not raw:
        return None
    for chunk in (raw[:10], raw[:19], raw):
        try:
            return date.fromisoformat(chunk[:10])
        except ValueError:
            pass
        try:
            return datetime.fromisoformat(chunk.replace("Z", "+00:00")).date()
        except ValueError:
            continue
    return None


def _month_key(value: Any) -> str:
    current = _date_or_none(value)
    return current.strftime("%Y-%m") if current else ""


def _month_bounds(month_key: str) -> tuple[date | None, date | None]:
    normalized = clean_text(month_key)
    if len(normalized) != 7:
        return None, None
    try:
        start = date.fromisoformat(f"{normalized}-01")
    except ValueError:
        return None, None
    next_month = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    end = next_month - timedelta(days=1)
    return start, end


def month_label(month_key: str) -> str:
    start, _ = _month_bounds(month_key)
    return start.strftime("%b %Y") if start else month_key


def month_filter_options(weekly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in weekly_rows:
        period_key = _month_key(row.get("week_end_date") or row.get("week_start_date"))
        if not period_key:
            continue
        start_date = clean_text(row.get("week_start_date"))
        end_date = clean_text(row.get("week_end_date"))
        bucket = grouped.setdefault(
            period_key,
            {
                "underlying_key": period_key,
                "display_label": month_label(period_key),
                "sort_key": period_key,
                "week_count": 0,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        bucket["week_count"] += 1
        if start_date and (not bucket.get("start_date") or start_date < bucket["start_date"]):
            bucket["start_date"] = start_date
        if end_date and (not bucket.get("end_date") or end_date > bucket["end_date"]):
            bucket["end_date"] = end_date
    options = sorted(grouped.values(), key=lambda row: clean_text(row.get("sort_key")) or "", reverse=True)
    for option in options:
        option["display_label"] = f"{option['display_label']} ({option['week_count']} weeks)"
    return options


def selected_month_key(filters: dict[str, str | None], weekly_rows: list[dict[str, Any]]) -> str | None:
    explicit = clean_text(filters.get("month"))
    options = month_filter_options(weekly_rows)
    valid_keys = {clean_text(option.get("underlying_key")) for option in options}
    if explicit and explicit in valid_keys:
        return explicit
    if not options:
        return None
    return clean_text(options[0].get("underlying_key"))


def filter_weekly_rows_by_month(weekly_rows: list[dict[str, Any]], month: str | None) -> list[dict[str, Any]]:
    if not month:
        return list(weekly_rows)
    return [row for row in weekly_rows if _month_key(row.get("week_end_date") or row.get("week_start_date")) == month]


def bounded_weekly_rows(weekly_rows: list[dict[str, Any]], as_of_date: Any) -> list[dict[str, Any]]:
    cutoff = week_end_saturday(as_of_date) or _date_or_none(as_of_date)
    if cutoff is None:
        return list(weekly_rows)
    output: list[dict[str, Any]] = []
    for row in weekly_rows:
        week_end = _date_or_none(row.get("week_end_date"))
        if week_end and week_end > cutoff:
            continue
        output.append(row)
    return output


def apply_enrollment_filters(rows: list[dict[str, Any]], filters: dict[str, str | None], *, up_to_period_end: str | None = None) -> list[dict[str, Any]]:
    state = clean_text(filters.get("state"))
    field_rep_id = clean_text(filters.get("field_rep_id"))
    doctor_key = clean_text(filters.get("doctor_key"))
    output = []
    for row in rows:
        if state and clean_text(row.get("state")) != state:
            continue
        if field_rep_id and clean_text(row.get("field_rep_id_resolved")) != field_rep_id:
            continue
        if doctor_key and clean_text(row.get("doctor_key")) != doctor_key:
            continue
        if up_to_period_end and clean_text(row.get("enrolled_at_ts")) and str(row.get("enrolled_at_ts"))[:10] > up_to_period_end:
            continue
        output.append(row)
    return output


def apply_share_filters(rows: list[dict[str, Any]], filters: dict[str, str | None]) -> list[dict[str, Any]]:
    predicates = {
        "state": ("state",),
        "field_rep_id": ("field_rep_id", "field_rep_id_resolved"),
        "doctor_key": ("doctor_key",),
        "language_code": ("language_code",),
        "share_type": ("shared_item_type",),
        "therapy_area": ("therapy_area_name",),
        "trigger": ("trigger_name",),
        "bundle": ("video_cluster_code",),
    }
    month = clean_text(filters.get("month"))
    output = []
    for row in rows:
        if month:
            row_month = _month_key(row.get("shared_at_ts")) or _month_key(row.get("week_end_date"))
            if row_month != month:
                continue
        keep = True
        for filter_key, row_keys in predicates.items():
            filter_value = clean_text(filters.get(filter_key))
            row_value = None
            for row_key in row_keys:
                row_value = clean_text(row.get(row_key))
                if row_value:
                    break
            if filter_value and row_value != filter_value:
                keep = False
                break
        if keep:
            output.append(row)
    return output


def recompute_weekly_rows(
    campaign: dict[str, Any],
    weekly_template_rows: list[dict[str, Any]],
    enrollment_rows: list[dict[str, Any]],
    share_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    previous = None
    sorted_rows = sorted(weekly_template_rows, key=lambda row: as_int(row.get("week_index"), default=0))
    for template in sorted_rows:
        week_end = clean_text(template.get("week_end_date"))
        week_start = clean_text(template.get("week_start_date"))
        cohort = [row for row in share_rows if clean_text(row.get("week_end_date")) == week_end]
        enrolled = len({row.get("doctor_key") for row in enrollment_rows if clean_text(row.get("doctor_key")) and (not week_end or str(row.get("enrolled_at_ts") or "")[:10] <= week_end)})
        sharing = len({row.get("doctor_key") for row in cohort if clean_text(row.get("doctor_key"))})
        shares_total = len(cohort)
        unique_recips = len({row.get("recipient_reference") for row in cohort if clean_text(row.get("recipient_reference"))})
        shares_played = len([row for row in cohort if clean_text(row.get("is_played")) == "true"])
        shares_viewed_50 = len([row for row in cohort if clean_text(row.get("is_viewed_50")) == "true"])
        shares_viewed_100 = len([row for row in cohort if clean_text(row.get("is_viewed_100")) == "true"])
        video_shares = len([row for row in cohort if clean_text(row.get("shared_item_type")) == "video"])
        bundle_shares = len([row for row in cohort if clean_text(row.get("shared_item_type")) == "cluster"])
        health = compute_health_components(
            enrolled_doctors_current=enrolled,
            doctors_sharing_unique=sharing,
            shares_total=shares_total,
            shares_played=shares_played,
            shares_viewed_50=shares_viewed_50,
            shares_viewed_100=shares_viewed_100,
        )
        row = {
            "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
            "week_index": template.get("week_index"),
            "week_start_date": week_start,
            "week_end_date": week_end,
            "campaign_start_date": clean_text(campaign.get("start_date")),
            "campaign_end_date": clean_text(campaign.get("end_date")),
            "bundle_display_name": clean_text(campaign.get("local_video_cluster_name")),
            "enrolled_doctors_current": enrolled,
            "doctors_sharing_unique": sharing,
            "shares_total": shares_total,
            "unique_recipient_references": unique_recips,
            "shares_played": shares_played,
            "shares_viewed_50": shares_viewed_50,
            "shares_viewed_100": shares_viewed_100,
            "video_shares": video_shares,
            "bundle_shares": bundle_shares,
            "activation_pct": health["activation_pct"],
            "play_rate_pct": health["play_rate_pct"],
            "engagement_50_pct": health["engagement_50_pct"],
            "completion_pct": health["completion_pct"],
            "weekly_health_score": health["campaign_health_score"],
            "wow_doctors_sharing_unique_delta": sharing - as_int((previous or {}).get("doctors_sharing_unique")),
            "wow_shares_total_delta": shares_total - as_int((previous or {}).get("shares_total")),
            "wow_unique_recipient_references_delta": unique_recips - as_int((previous or {}).get("unique_recipient_references")),
            "wow_shares_played_delta": shares_played - as_int((previous or {}).get("shares_played")),
            "wow_shares_viewed_50_delta": shares_viewed_50 - as_int((previous or {}).get("shares_viewed_50")),
            "wow_shares_viewed_100_delta": shares_viewed_100 - as_int((previous or {}).get("shares_viewed_100")),
            "wow_weekly_health_score_delta": round(as_float(health["campaign_health_score"]) - as_float((previous or {}).get("weekly_health_score")), 2),
            "health_color": health_color(health["campaign_health_score"]),
            "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
        }
        output.append(row)
        previous = row
    return output


def cumulative_summary(campaign_summary_row: dict[str, Any], enrollment_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]]) -> dict[str, Any]:
    enrolled = len({row.get("doctor_key") for row in enrollment_rows if clean_text(row.get("doctor_key"))})
    sharing = len({row.get("doctor_key") for row in share_rows if clean_text(row.get("doctor_key"))})
    shares_total = len(share_rows)
    recips = len({row.get("recipient_reference") for row in share_rows if clean_text(row.get("recipient_reference"))})
    played = len([row for row in share_rows if clean_text(row.get("is_played")) == "true"])
    viewed_50 = len([row for row in share_rows if clean_text(row.get("is_viewed_50")) == "true"])
    viewed_100 = len([row for row in share_rows if clean_text(row.get("is_viewed_100")) == "true"])
    video_shares = len([row for row in share_rows if clean_text(row.get("shared_item_type")) == "video"])
    bundle_shares = len([row for row in share_rows if clean_text(row.get("shared_item_type")) == "cluster"])
    health = compute_health_components(
        enrolled_doctors_current=enrolled,
        doctors_sharing_unique=sharing,
        shares_total=shares_total,
        shares_played=played,
        shares_viewed_50=viewed_50,
        shares_viewed_100=viewed_100,
    )
    return {
        "campaign_id_original": clean_text(campaign_summary_row.get("campaign_id_original")),
        "as_of_date": clean_text(campaign_summary_row.get("as_of_date")),
        "enrolled_doctors_current": enrolled,
        "doctors_sharing_unique_cumulative": sharing,
        "shares_total_cumulative": shares_total,
        "unique_recipient_references_cumulative": recips,
        "shares_played_cumulative": played,
        "shares_viewed_50_cumulative": viewed_50,
        "shares_viewed_100_cumulative": viewed_100,
        "video_shares_cumulative": video_shares,
        "bundle_shares_cumulative": bundle_shares,
        "activation_pct": health["activation_pct"],
        "play_rate_pct": health["play_rate_pct"],
        "engagement_50_pct": health["engagement_50_pct"],
        "completion_pct": health["completion_pct"],
        "campaign_health_score": health["campaign_health_score"],
        "wow_campaign_health_score_delta": clean_text(campaign_summary_row.get("wow_campaign_health_score_delta")) or "0",
        "benchmark_avg_campaign_health_score": clean_text(campaign_summary_row.get("benchmark_avg_campaign_health_score")) or "0",
        "benchmark_label": clean_text(campaign_summary_row.get("benchmark_label")) or "Average",
        "health_color": health_color(health["campaign_health_score"]),
        "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
    }


def build_dashboard_payload(
    campaign: dict[str, Any],
    filters: dict[str, str | None],
    weekly_template_rows: list[dict[str, Any]],
    campaign_summary_row: dict[str, Any],
    enrollment_rows: list[dict[str, Any]],
    share_rows: list[dict[str, Any]],
    video_rows: list[dict[str, Any]],
    thresholds: dict[str, float],
    benchmark_best_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    weekly_template_rows = bounded_weekly_rows(weekly_template_rows, campaign_summary_row.get("as_of_date"))
    selected_month = selected_month_key(filters, weekly_template_rows)
    effective_filters = dict(filters)
    if selected_month:
        effective_filters["month"] = selected_month
    _, selected_month_end = _month_bounds(selected_month or "")
    visible_week_rows = filter_weekly_rows_by_month(weekly_template_rows, selected_month)
    filtered_enrollment_rows = apply_enrollment_filters(
        enrollment_rows,
        effective_filters,
        up_to_period_end=selected_month_end.isoformat() if selected_month_end else None,
    )
    filtered_share_rows = apply_share_filters(share_rows, effective_filters)
    filtered_video_rows = apply_share_filters(video_rows, effective_filters)

    weekly_rows_all = recompute_weekly_rows(campaign, visible_week_rows, filtered_enrollment_rows, filtered_share_rows)
    current_week_row = weekly_rows_all[-1] if weekly_rows_all else {}
    current_week_end = clean_text((current_week_row or {}).get("week_end_date"))
    state_rows_all = _state_summary_rows(filtered_enrollment_rows, filtered_share_rows, weekly_rows_all)
    rep_rows_all = _field_rep_summary_rows(filtered_enrollment_rows, filtered_share_rows, weekly_rows_all, {})
    language_rows_all = _language_summary_rows(filtered_share_rows, weekly_rows_all)
    action_rows_all = _action_rows(weekly_rows_all, state_rows_all, rep_rows_all, thresholds, "dashboard", clean_text(campaign_summary_row.get("as_of_date")) or "")
    state_attention_rows = [row for row in state_rows_all if clean_text(row.get("week_end_date")) == current_week_end]
    rep_attention_rows = [row for row in rep_rows_all if clean_text(row.get("week_end_date")) == current_week_end]
    state_attention_rows.sort(key=lambda row: (as_float(row.get("weekly_state_health_score")), row.get("state") or ""))
    rep_attention_rows.sort(key=lambda row: (as_float(row.get("weekly_rep_health_score")), row.get("field_rep_name") or ""))
    current_action_row = next((row for row in action_rows_all if clean_text(row.get("week_end_date")) == current_week_end), action_rows_all[-1] if action_rows_all else {})
    language_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in filtered_share_rows:
        language_groups[clean_text(row.get("language_code")) or "Unknown"].append(row)
    language_ranking_rows = [
        {
            "language_code": language,
            "shares_total": len(rows),
            "shares_viewed_50": len([row for row in rows if clean_text(row.get("is_viewed_50")) == "true"]),
            "engagement_50_pct": round((len([row for row in rows if clean_text(row.get("is_viewed_50")) == "true"]) / len(rows)) * 100, 2) if rows else 0,
        }
        for language, rows in language_groups.items()
    ]
    language_ranking_rows.sort(key=lambda row: (-as_int(row.get("shares_total")), row.get("language_code") or ""))

    summary_row = cumulative_summary(campaign_summary_row, filtered_enrollment_rows, filtered_share_rows)
    best_week_row = max(weekly_rows_all, key=lambda row: as_float(row.get("weekly_health_score")), default={})
    benchmark_row = benchmark_best_row or {}
    video_rankings = _video_rankings(filtered_video_rows)
    video_viewed_50_rankings = sorted(video_rankings, key=lambda row: (-as_int(row.get("viewed_50_share_count")), row.get("preferred_display_label") or ""))
    bundle_rankings = _bundle_rankings(filtered_share_rows)

    return {
        "selected_month": selected_month,
        "selected_month_label": month_label(selected_month or "") if selected_month else "",
        "month_options": month_filter_options(weekly_template_rows),
        "weekly_rows_all": weekly_rows_all,
        "weekly_rows": weekly_rows_all,
        "current_week_row": current_week_row,
        "campaign_summary": summary_row,
        "state_attention_rows": state_attention_rows[:3],
        "field_rep_attention_rows": rep_attention_rows[:3],
        "state_rows_all": state_rows_all,
        "field_rep_rows_all": rep_rows_all,
        "language_rows_all": language_rows_all,
        "action_row": current_action_row,
        "video_rankings": video_rankings,
        "video_viewed_50_rankings": video_viewed_50_rankings,
        "bundle_rankings": bundle_rankings,
        "language_rankings": language_ranking_rows,
        "best_week_row": best_week_row,
        "benchmark_row": benchmark_row,
        "filtered_share_rows": filtered_share_rows,
        "filtered_enrollment_rows": filtered_enrollment_rows,
        "filtered_video_rows": filtered_video_rows,
        "doctor_activity_rows": _doctor_activity_rows(filtered_enrollment_rows, filtered_share_rows),
        "filters_query": current_filters_query(effective_filters),
    }


def metric_dataset(metric: str, payload: dict[str, Any]) -> tuple[str, list[str], list[dict[str, Any]]]:
    share_rows = payload["filtered_share_rows"]
    enrollment_rows = payload["filtered_enrollment_rows"]
    doctor_rows = payload["doctor_activity_rows"]
    state_rows = payload["state_rows_all"]
    rep_rows = payload["field_rep_rows_all"]
    language_rows = payload["language_rankings"]
    if metric == "enrolled_doctors":
        return (
            "Enrolled Doctors",
            ["doctor_id", "full_name", "clinic_name", "city", "district", "state", "field_rep_id_resolved", "enrolled_at_ts"],
            enrollment_rows,
        )
    if metric == "doctors_sharing":
        rows = [row for row in doctor_rows if as_int(row.get("shares_total_cumulative")) > 0]
        return ("Doctors Sharing", ["doctor_id", "doctor_display_name", "state", "field_rep_id", "shares_total_cumulative", "last_shared_at_ts"], rows)
    if metric == "total_shares":
        return ("Total Shares", ["share_public_id", "doctor_id", "doctor_display_name", "shared_item_type", "shared_item_name", "language_code", "recipient_reference", "shared_at_ts"], share_rows)
    if metric == "unique_recipients":
        rows = list({clean_text(row.get("recipient_reference")): row for row in share_rows if clean_text(row.get("recipient_reference"))}.values())
        return ("Unique Caregivers Reached", ["recipient_reference", "doctor_id", "doctor_display_name", "shared_item_name", "language_code", "shared_at_ts"], rows)
    if metric == "shares_played":
        rows = [row for row in share_rows if clean_text(row.get("is_played")) == "true"]
        return ("Shares Played", ["share_public_id", "doctor_id", "doctor_display_name", "shared_item_name", "play_first_ts", "shared_at_ts"], rows)
    if metric == "shares_viewed_50":
        rows = [row for row in share_rows if clean_text(row.get("is_viewed_50")) == "true"]
        return ("Viewed >50%", ["share_public_id", "doctor_id", "doctor_display_name", "shared_item_name", "view_50_first_ts", "shared_at_ts"], rows)
    if metric == "shares_viewed_100":
        rows = [row for row in share_rows if clean_text(row.get("is_viewed_100")) == "true"]
        return ("Completed 100%", ["share_public_id", "doctor_id", "doctor_display_name", "shared_item_name", "view_100_first_ts", "shared_at_ts"], rows)
    if metric == "video_shares":
        rows = [row for row in share_rows if clean_text(row.get("shared_item_type")) == "video"]
        return ("Video Shares", ["share_public_id", "doctor_id", "doctor_display_name", "video_display_label", "language_code", "shared_at_ts"], rows)
    if metric == "bundle_shares":
        rows = [row for row in share_rows if clean_text(row.get("shared_item_type")) == "cluster"]
        return ("Bundle Shares", ["share_public_id", "doctor_id", "doctor_display_name", "video_cluster_display_label", "language_code", "shared_at_ts"], rows)
    if metric == "state_attention":
        rows = sorted(state_rows, key=lambda row: (as_float(row.get("weekly_state_health_score")), row.get("state") or ""))
        return ("States Requiring Attention", ["state", "enrolled_doctors_state", "doctors_sharing_unique_state", "shares_total_state", "activation_pct_state", "engagement_50_pct_state", "weekly_state_health_score"], rows)
    if metric == "field_rep_attention":
        rows = sorted(rep_rows, key=lambda row: (as_float(row.get("weekly_rep_health_score")), row.get("field_rep_name") or ""))
        return ("Field Reps Requiring Attention", ["field_rep_name", "enrolled_doctors_rep", "doctors_sharing_unique_rep", "shares_total_rep", "activation_pct_rep", "engagement_50_pct_rep", "weekly_rep_health_score"], rows)
    if metric == "top_videos_shared":
        return ("Top Videos Shared", ["video_code", "preferred_display_label", "shares_count", "played_share_count", "viewed_50_share_count", "viewed_100_share_count"], payload["video_rankings"])
    if metric == "top_videos_viewed_50":
        return ("Top Videos Viewed >50%", ["video_code", "preferred_display_label", "viewed_50_share_count", "shares_count", "viewed_100_share_count"], payload["video_viewed_50_rankings"])
    if metric == "top_bundles_shared":
        return ("Top Bundles Shared", ["video_cluster_code", "preferred_display_label", "shares_count", "played_share_count", "viewed_50_share_count", "viewed_100_share_count"], payload["bundle_rankings"])
    if metric == "languages":
        return ("Languages Used", ["language_code", "shares_total", "shares_viewed_50", "engagement_50_pct"], language_rows)
    raise KeyError(metric)
