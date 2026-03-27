from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from statistics import median
from typing import Any

from django.db import connection, transaction

from etl.pe_reports.control import get_thresholds
from etl.pe_reports.specs import GOLD_GLOBAL_SCHEMA, SILVER_SCHEMA
from etl.pe_reports.storage import ensure_schema, fetch_table, qident, replace_table, table_exists
from etl.pe_reports.utils import (
    as_float,
    as_int,
    campaign_schema_name,
    clean_text,
    first_non_empty,
    health_color,
    iso_date,
    safe_pct,
    week_end_saturday,
    week_start_sunday,
)


GLOBAL_DEFAULT_COLUMNS: dict[str, list[str]] = {
    "campaign_registry": [
        "campaign_id_original",
        "campaign_id_normalized",
        "gold_schema_name",
        "campaign_name",
        "brand_name",
        "first_seen_ts",
        "last_seen_ts",
        "_created_at",
        "_updated_at",
    ],
    "campaign_health_history": [
        "campaign_id_original",
        "campaign_id_normalized",
        "as_of_date",
        "campaign_health_score",
        "activation_pct",
        "play_rate_pct",
        "engagement_50_pct",
        "completion_pct",
        "enrolled_doctors_current",
        "_loaded_at",
    ],
    "benchmark_last_10_campaigns": [
        "as_of_date",
        "campaign_count",
        "avg_campaign_health_score",
        "p50_campaign_health_score",
        "p75_campaign_health_score",
        "avg_activation_pct",
        "avg_play_rate_pct",
        "avg_engagement_50_pct",
        "avg_completion_pct",
        "_computed_at",
    ],
    "refresh_status": ["publish_id", "published_at", "as_of_date", "status", "notes"],
}

CAMPAIGN_DEFAULT_COLUMNS: dict[str, list[str]] = {
    "fact_share_latest": [
        "share_public_id",
        "doctor_key",
        "doctor_id",
        "doctor_display_name",
        "shared_item_type",
        "shared_item_code",
        "shared_item_name",
        "language_code",
        "recipient_reference",
        "shared_at_ts",
        "week_index",
        "week_start_date",
        "week_end_date",
        "is_played",
        "is_viewed_50",
        "is_viewed_100",
        "play_first_ts",
        "view_50_first_ts",
        "view_100_first_ts",
        "video_code",
        "video_display_label",
        "video_cluster_code",
        "video_cluster_display_label",
        "therapy_area_name",
        "trigger_name",
        "city",
        "district",
        "state",
        "field_rep_id",
        "field_rep_external_id",
        "campaign_id_original",
        "campaign_id_normalized",
        "campaign_attribution_method",
        "_as_of_run_id",
        "_as_of_ts",
    ],
    "kpi_weekly_summary": [
        "campaign_id_original",
        "week_index",
        "week_start_date",
        "week_end_date",
        "campaign_start_date",
        "campaign_end_date",
        "bundle_display_name",
        "enrolled_doctors_current",
        "doctors_sharing_unique",
        "shares_total",
        "unique_recipient_references",
        "shares_played",
        "shares_viewed_50",
        "shares_viewed_100",
        "video_shares",
        "bundle_shares",
        "activation_pct",
        "play_rate_pct",
        "engagement_50_pct",
        "completion_pct",
        "weekly_health_score",
        "wow_doctors_sharing_unique_delta",
        "wow_shares_total_delta",
        "wow_unique_recipient_references_delta",
        "wow_shares_played_delta",
        "wow_shares_viewed_50_delta",
        "wow_shares_viewed_100_delta",
        "wow_weekly_health_score_delta",
        "health_color",
        "insufficient_data_flag",
    ],
    "kpi_campaign_health_summary": [
        "campaign_id_original",
        "as_of_date",
        "enrolled_doctors_current",
        "doctors_sharing_unique_cumulative",
        "shares_total_cumulative",
        "unique_recipient_references_cumulative",
        "shares_played_cumulative",
        "shares_viewed_50_cumulative",
        "shares_viewed_100_cumulative",
        "video_shares_cumulative",
        "bundle_shares_cumulative",
        "activation_pct",
        "play_rate_pct",
        "engagement_50_pct",
        "completion_pct",
        "campaign_health_score",
        "wow_campaign_health_score_delta",
        "benchmark_avg_campaign_health_score",
        "benchmark_label",
        "health_color",
        "insufficient_data_flag",
    ],
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stringify_row(row: dict[str, Any]) -> dict[str, str]:
    return {key: "" if value is None else str(value) for key, value in row.items()}


def _replace_stage_table(schema: str, table: str, rows: list[dict[str, Any]], default_columns: list[str]) -> None:
    ensure_schema(schema)
    replace_table(schema, f"{table}__stage", list(rows[0].keys()) if rows else default_columns, [_stringify_row(row) for row in rows])


def _publish_schema_tables(schema: str, table_names: list[str]) -> None:
    ensure_schema(schema)
    with connection.cursor() as cursor:
        for table in table_names:
            stage_table = f"{table}__stage"
            if not table_exists(schema, stage_table):
                continue
            backup_table = f"{table}__prev"
            cursor.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(backup_table)}")
            if table_exists(schema, table):
                cursor.execute(f"ALTER TABLE {qident(schema)}.{qident(table)} RENAME TO {qident(backup_table)}")
            cursor.execute(f"ALTER TABLE {qident(schema)}.{qident(stage_table)} RENAME TO {qident(table)}")
            cursor.execute(f"DROP TABLE IF EXISTS {qident(schema)}.{qident(backup_table)}")


def compute_health_components(
    *,
    enrolled_doctors_current: int,
    doctors_sharing_unique: int,
    shares_total: int,
    shares_played: int,
    shares_viewed_50: int,
    shares_viewed_100: int,
) -> dict[str, float | bool]:
    activation_pct = safe_pct(doctors_sharing_unique, enrolled_doctors_current)
    play_rate_pct = safe_pct(shares_played, shares_total)
    engagement_50_pct = safe_pct(shares_viewed_50, shares_total)
    completion_pct = safe_pct(shares_viewed_100, shares_total)
    insufficient = enrolled_doctors_current == 0
    campaign_health_score = (activation_pct + play_rate_pct + engagement_50_pct + completion_pct) / 4.0 if not insufficient else 0.0
    return {
        "activation_pct": round(activation_pct, 2),
        "play_rate_pct": round(play_rate_pct, 2),
        "engagement_50_pct": round(engagement_50_pct, 2),
        "completion_pct": round(completion_pct, 2),
        "campaign_health_score": round(campaign_health_score, 2),
        "insufficient_data_flag": insufficient,
    }


def build_benchmark_row(health_rows: list[dict[str, Any]], as_of_date: str, computed_at: str) -> dict[str, Any]:
    eligible = [row for row in health_rows if clean_text(row.get("insufficient_data_flag")) != "true"]
    if not eligible:
        return {
            "as_of_date": as_of_date,
            "campaign_count": 0,
            "avg_campaign_health_score": 0,
            "p50_campaign_health_score": 0,
            "p75_campaign_health_score": 0,
            "avg_activation_pct": 0,
            "avg_play_rate_pct": 0,
            "avg_engagement_50_pct": 0,
            "avg_completion_pct": 0,
            "_computed_at": computed_at,
        }
    scores = sorted(as_float(row.get("campaign_health_score")) for row in eligible)
    p50 = median(scores)
    p75_index = max(0, min(len(scores) - 1, round((len(scores) - 1) * 0.75)))
    return {
        "as_of_date": as_of_date,
        "campaign_count": len(eligible),
        "avg_campaign_health_score": round(sum(scores) / len(scores), 2),
        "p50_campaign_health_score": round(float(p50), 2),
        "p75_campaign_health_score": round(float(scores[p75_index]), 2),
        "avg_activation_pct": round(sum(as_float(row.get("activation_pct")) for row in eligible) / len(eligible), 2),
        "avg_play_rate_pct": round(sum(as_float(row.get("play_rate_pct")) for row in eligible) / len(eligible), 2),
        "avg_engagement_50_pct": round(sum(as_float(row.get("engagement_50_pct")) for row in eligible) / len(eligible), 2),
        "avg_completion_pct": round(sum(as_float(row.get("completion_pct")) for row in eligible) / len(eligible), 2),
        "_computed_at": computed_at,
    }


def _date_or_none(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except Exception:
        return None


def _campaign_week_ranges(campaign: dict[str, Any], base_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]], as_of_date: date) -> list[tuple[date, date]]:
    period_end = week_end_saturday(as_of_date) or as_of_date
    candidates: list[date] = []
    for value in (
        campaign.get("start_date"),
        campaign.get("end_date"),
        *[row.get("enrolled_at_ts") for row in base_rows],
        *[row.get("shared_at_ts") for row in share_rows],
    ):
        parsed = _date_or_none(str(value)[:10]) if value else None
        if parsed:
            candidates.append(parsed)
    candidates = [item for item in candidates if item <= period_end]
    if not candidates:
        week_end = period_end
        return [(week_start_sunday(week_end) or week_end, week_end)]

    start_date = min(candidates)
    end_date = period_end
    first_week_end = week_end_saturday(start_date) or start_date
    ranges: list[tuple[date, date]] = []
    current_week_end = first_week_end
    while current_week_end <= end_date:
        ranges.append((week_start_sunday(current_week_end) or current_week_end, current_week_end))
        current_week_end = current_week_end.fromordinal(current_week_end.toordinal() + 7)
    return ranges


def _share_with_funnel(share_rows: list[dict[str, Any]], funnel_rows: list[dict[str, Any]], run_id: str, published_at: str) -> list[dict[str, Any]]:
    funnel_by_share = {clean_text(row.get("share_public_id")): row for row in funnel_rows if clean_text(row.get("share_public_id"))}
    merged_rows: list[dict[str, Any]] = []
    for share in share_rows:
        share_id = clean_text(share.get("share_public_id"))
        funnel = funnel_by_share.get(share_id or "", {})
        shared_date = _date_or_none(str(share.get("shared_at_ts"))[:10]) if clean_text(share.get("shared_at_ts")) else None
        week_end = week_end_saturday(shared_date) if shared_date else None
        merged_rows.append(
            {
                "share_public_id": share_id,
                "doctor_key": clean_text(share.get("doctor_key")),
                "doctor_id": clean_text(share.get("doctor_id")),
                "doctor_display_name": clean_text(share.get("doctor_name_snapshot")) or clean_text(share.get("doctor_id")),
                "shared_item_type": clean_text(share.get("shared_item_type")),
                "shared_item_code": clean_text(share.get("shared_item_code")),
                "shared_item_name": clean_text(share.get("shared_item_name")),
                "language_code": clean_text(share.get("language_code")),
                "recipient_reference": clean_text(share.get("recipient_reference")),
                "shared_at_ts": clean_text(share.get("shared_at_ts")),
                "week_start_date": iso_date(week_start_sunday(week_end) if week_end else None),
                "week_end_date": iso_date(week_end),
                "is_played": clean_text((funnel or {}).get("is_played")) or "false",
                "is_viewed_50": clean_text((funnel or {}).get("is_viewed_50")) or "false",
                "is_viewed_100": clean_text((funnel or {}).get("is_viewed_100")) or "false",
                "play_first_ts": clean_text((funnel or {}).get("play_first_ts")),
                "view_50_first_ts": clean_text((funnel or {}).get("view_50_first_ts")),
                "view_100_first_ts": clean_text((funnel or {}).get("view_100_first_ts")),
                "video_code": clean_text(share.get("video_code")),
                "video_display_label": clean_text(share.get("video_display_label")),
                "video_cluster_code": clean_text(share.get("video_cluster_code")),
                "video_cluster_display_label": clean_text(share.get("video_cluster_display_label")),
                "therapy_area_name": clean_text(share.get("therapy_area_name")),
                "trigger_name": clean_text(share.get("trigger_name")),
                "city": clean_text(share.get("city")),
                "district": clean_text(share.get("district")),
                "state": clean_text(share.get("state")) or "Unknown",
                "field_rep_id": clean_text(share.get("field_rep_id_resolved")),
                "field_rep_external_id": clean_text(share.get("field_rep_external_id")),
                "campaign_id_original": clean_text(share.get("campaign_id_original")),
                "campaign_id_normalized": clean_text(share.get("campaign_id_normalized")),
                "campaign_attribution_method": clean_text(share.get("campaign_attribution_method")),
                "_as_of_run_id": run_id,
                "_as_of_ts": published_at,
            }
        )
    return merged_rows


def _weekly_summary_rows(campaign: dict[str, Any], base_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    as_of_date = date.today()
    week_ranges = _campaign_week_ranges(campaign, base_rows, share_rows, as_of_date)
    summary_rows: list[dict[str, Any]] = []
    previous_row: dict[str, Any] | None = None
    for week_index, (week_start, week_end) in enumerate(week_ranges, start=1):
        cohort_shares = [row for row in share_rows if _date_or_none(str(row.get("shared_at_ts"))[:10]) and week_start <= _date_or_none(str(row.get("shared_at_ts"))[:10]) <= week_end]
        enrolled_current = len({row.get("doctor_key") for row in base_rows if _date_or_none(str(row.get("enrolled_at_ts"))[:10]) and _date_or_none(str(row.get("enrolled_at_ts"))[:10]) <= week_end})
        doctors_sharing_unique = len({row.get("doctor_key") for row in cohort_shares if clean_text(row.get("doctor_key"))})
        shares_total = len(cohort_shares)
        unique_recipient_references = len({row.get("recipient_reference") for row in cohort_shares if clean_text(row.get("recipient_reference"))})
        shares_played = len([row for row in cohort_shares if clean_text(row.get("is_played")) == "true"])
        shares_viewed_50 = len([row for row in cohort_shares if clean_text(row.get("is_viewed_50")) == "true"])
        shares_viewed_100 = len([row for row in cohort_shares if clean_text(row.get("is_viewed_100")) == "true"])
        video_shares = len([row for row in cohort_shares if clean_text(row.get("shared_item_type")) == "video"])
        bundle_shares = len([row for row in cohort_shares if clean_text(row.get("shared_item_type")) == "cluster"])
        health = compute_health_components(
            enrolled_doctors_current=enrolled_current,
            doctors_sharing_unique=doctors_sharing_unique,
            shares_total=shares_total,
            shares_played=shares_played,
            shares_viewed_50=shares_viewed_50,
            shares_viewed_100=shares_viewed_100,
        )
        row = {
            "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
            "week_index": week_index,
            "week_start_date": week_start.isoformat(),
            "week_end_date": week_end.isoformat(),
            "campaign_start_date": clean_text(campaign.get("start_date")),
            "campaign_end_date": clean_text(campaign.get("end_date")),
            "bundle_display_name": clean_text(campaign.get("local_video_cluster_name")),
            "enrolled_doctors_current": enrolled_current,
            "doctors_sharing_unique": doctors_sharing_unique,
            "shares_total": shares_total,
            "unique_recipient_references": unique_recipient_references,
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
            "wow_doctors_sharing_unique_delta": doctors_sharing_unique - as_int((previous_row or {}).get("doctors_sharing_unique")),
            "wow_shares_total_delta": shares_total - as_int((previous_row or {}).get("shares_total")),
            "wow_unique_recipient_references_delta": unique_recipient_references - as_int((previous_row or {}).get("unique_recipient_references")),
            "wow_shares_played_delta": shares_played - as_int((previous_row or {}).get("shares_played")),
            "wow_shares_viewed_50_delta": shares_viewed_50 - as_int((previous_row or {}).get("shares_viewed_50")),
            "wow_shares_viewed_100_delta": shares_viewed_100 - as_int((previous_row or {}).get("shares_viewed_100")),
            "wow_weekly_health_score_delta": round(as_float(health["campaign_health_score"]) - as_float((previous_row or {}).get("weekly_health_score")), 2),
            "health_color": health_color(health["campaign_health_score"]),
            "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
        }
        summary_rows.append(row)
        previous_row = row

    for row in summary_rows:
        matching = [item for item in summary_rows if item["week_end_date"] == row["week_end_date"]]
        if matching:
            row["week_index"] = matching[0]["week_index"]
    return summary_rows


def _dimension_rows_from_table(rows: list[dict[str, Any]], key_field: str, label_field: str | None = None) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for row in rows:
        key = clean_text(row.get(key_field))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(
            {
                "underlying_key": key,
                "display_label": clean_text(row.get(label_field or key_field)) or key,
                "sort_key": clean_text(row.get(label_field or key_field)) or key,
            }
        )
    output.sort(key=lambda item: item["sort_key"])
    return output


def _state_summary_rows(base_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]], weekly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for weekly_row in weekly_rows:
        week_start = _date_or_none(weekly_row.get("week_start_date"))
        week_end = _date_or_none(weekly_row.get("week_end_date"))
        if week_start is None or week_end is None:
            continue
        states = {clean_text(row.get("state")) or "Unknown" for row in base_rows} | {clean_text(row.get("state")) or "Unknown" for row in share_rows}
        state_rows_for_week: list[dict[str, Any]] = []
        for state in sorted(states):
            state_base = [row for row in base_rows if (clean_text(row.get("state")) or "Unknown") == state and _date_or_none(str(row.get("enrolled_at_ts"))[:10]) and _date_or_none(str(row.get("enrolled_at_ts"))[:10]) <= week_end]
            cohort = [row for row in share_rows if (clean_text(row.get("state")) or "Unknown") == state and _date_or_none(str(row.get("shared_at_ts"))[:10]) and week_start <= _date_or_none(str(row.get("shared_at_ts"))[:10]) <= week_end]
            enrolled = len({row.get("doctor_key") for row in state_base})
            sharing = len({row.get("doctor_key") for row in cohort if clean_text(row.get("doctor_key"))})
            shares_total = len(cohort)
            played = len([row for row in cohort if clean_text(row.get("is_played")) == "true"])
            viewed_50 = len([row for row in cohort if clean_text(row.get("is_viewed_50")) == "true"])
            viewed_100 = len([row for row in cohort if clean_text(row.get("is_viewed_100")) == "true"])
            health = compute_health_components(
                enrolled_doctors_current=enrolled,
                doctors_sharing_unique=sharing,
                shares_total=shares_total,
                shares_played=played,
                shares_viewed_50=viewed_50,
                shares_viewed_100=viewed_100,
            )
            state_rows_for_week.append(
                {
                    "state": state,
                    "week_start_date": week_start.isoformat(),
                    "week_end_date": week_end.isoformat(),
                    "week_index": weekly_row.get("week_index"),
                    "enrolled_doctors_state": enrolled,
                    "doctors_sharing_unique_state": sharing,
                    "shares_total_state": shares_total,
                    "shares_played_state": played,
                    "shares_viewed_50_state": viewed_50,
                    "shares_viewed_100_state": viewed_100,
                    "activation_pct_state": health["activation_pct"],
                    "play_rate_pct_state": health["play_rate_pct"],
                    "engagement_50_pct_state": health["engagement_50_pct"],
                    "completion_pct_state": health["completion_pct"],
                    "weekly_state_health_score": health["campaign_health_score"],
                    "health_color": health_color(health["campaign_health_score"]),
                    "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
                }
            )
        ranked = sorted(state_rows_for_week, key=lambda item: (as_float(item.get("weekly_state_health_score")), item.get("state") or ""))
        for rank, row in enumerate(ranked, start=1):
            row["state_rank_asc"] = rank
            row["is_bottom_3_flag"] = "true" if rank <= 3 else "false"
        output.extend(ranked)
    return output


def _field_rep_summary_rows(base_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]], weekly_rows: list[dict[str, Any]], field_rep_lookup: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    rep_ids = {clean_text(row.get("field_rep_id_resolved")) or "Unassigned" for row in base_rows} | {
        clean_text(row.get("field_rep_id")) or "Unassigned" for row in share_rows
    }
    for weekly_row in weekly_rows:
        week_start = _date_or_none(weekly_row.get("week_start_date"))
        week_end = _date_or_none(weekly_row.get("week_end_date"))
        if week_start is None or week_end is None:
            continue
        week_rows: list[dict[str, Any]] = []
        for rep_id in sorted(rep_ids):
            rep_base = [row for row in base_rows if (clean_text(row.get("field_rep_id_resolved")) or "Unassigned") == rep_id and _date_or_none(str(row.get("enrolled_at_ts"))[:10]) and _date_or_none(str(row.get("enrolled_at_ts"))[:10]) <= week_end]
            cohort = [row for row in share_rows if (clean_text(row.get("field_rep_id")) or "Unassigned") == rep_id and _date_or_none(str(row.get("shared_at_ts"))[:10]) and week_start <= _date_or_none(str(row.get("shared_at_ts"))[:10]) <= week_end]
            enrolled = len({row.get("doctor_key") for row in rep_base})
            sharing = len({row.get("doctor_key") for row in cohort if clean_text(row.get("doctor_key"))})
            shares_total = len(cohort)
            played = len([row for row in cohort if clean_text(row.get("is_played")) == "true"])
            viewed_50 = len([row for row in cohort if clean_text(row.get("is_viewed_50")) == "true"])
            viewed_100 = len([row for row in cohort if clean_text(row.get("is_viewed_100")) == "true"])
            health = compute_health_components(
                enrolled_doctors_current=enrolled,
                doctors_sharing_unique=sharing,
                shares_total=shares_total,
                shares_played=played,
                shares_viewed_50=viewed_50,
                shares_viewed_100=viewed_100,
            )
            lookup = field_rep_lookup.get(rep_id, {})
            week_rows.append(
                {
                    "field_rep_id": rep_id if rep_id != "Unassigned" else "",
                    "field_rep_external_id": clean_text(lookup.get("brand_supplied_field_rep_id")),
                    "field_rep_name": clean_text(lookup.get("full_name")) or rep_id,
                    "week_start_date": week_start.isoformat(),
                    "week_end_date": week_end.isoformat(),
                    "week_index": weekly_row.get("week_index"),
                    "enrolled_doctors_rep": enrolled,
                    "doctors_sharing_unique_rep": sharing,
                    "shares_total_rep": shares_total,
                    "shares_played_rep": played,
                    "shares_viewed_50_rep": viewed_50,
                    "shares_viewed_100_rep": viewed_100,
                    "activation_pct_rep": health["activation_pct"],
                    "play_rate_pct_rep": health["play_rate_pct"],
                    "engagement_50_pct_rep": health["engagement_50_pct"],
                    "completion_pct_rep": health["completion_pct"],
                    "weekly_rep_health_score": health["campaign_health_score"],
                }
            )
        ranked = sorted(week_rows, key=lambda item: (as_float(item.get("weekly_rep_health_score")), item.get("field_rep_name") or ""))
        for rank, row in enumerate(ranked, start=1):
            row["rep_rank_asc"] = rank
            row["is_bottom_3_flag"] = "true" if rank <= 3 else "false"
        output.extend(ranked)
    return output


def _language_summary_rows(share_rows: list[dict[str, Any]], weekly_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for weekly_row in weekly_rows:
        week_start = _date_or_none(weekly_row.get("week_start_date"))
        week_end = _date_or_none(weekly_row.get("week_end_date"))
        if week_start is None or week_end is None:
            continue
        cohort = [row for row in share_rows if _date_or_none(str(row.get("shared_at_ts"))[:10]) and week_start <= _date_or_none(str(row.get("shared_at_ts"))[:10]) <= week_end]
        total_shares = len(cohort)
        languages = {clean_text(row.get("language_code")) or "Unknown" for row in cohort} or {"Unknown"}
        for language in sorted(languages):
            rows = [row for row in cohort if (clean_text(row.get("language_code")) or "Unknown") == language]
            shares_total = len(rows)
            played = len([row for row in rows if clean_text(row.get("is_played")) == "true"])
            viewed_50 = len([row for row in rows if clean_text(row.get("is_viewed_50")) == "true"])
            viewed_100 = len([row for row in rows if clean_text(row.get("is_viewed_100")) == "true"])
            output.append(
                {
                    "language_code": language,
                    "week_start_date": week_start.isoformat(),
                    "week_end_date": week_end.isoformat(),
                    "week_index": weekly_row.get("week_index"),
                    "shares_total": shares_total,
                    "shares_played": played,
                    "shares_viewed_50": viewed_50,
                    "shares_viewed_100": viewed_100,
                    "share_pct": round(safe_pct(shares_total, total_shares), 2),
                    "engagement_50_pct": round(safe_pct(viewed_50, shares_total), 2),
                    "completion_pct": round(safe_pct(viewed_100, shares_total), 2),
                }
            )
    return output


def _video_rankings(video_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in video_rows:
        video_code = clean_text(row.get("video_code"))
        if not video_code:
            continue
        current = grouped.setdefault(
            video_code,
            {
                "video_code": video_code,
                "preferred_display_label": clean_text(row.get("preferred_display_label")) or video_code,
                "shares_count": 0,
                "played_share_count": 0,
                "viewed_50_share_count": 0,
                "viewed_100_share_count": 0,
                "latest_interaction_ts": clean_text(row.get("shared_at_ts")) or "",
            },
        )
        current["shares_count"] += 1
        current["played_share_count"] += 1 if clean_text(row.get("is_played")) == "true" else 0
        current["viewed_50_share_count"] += 1 if clean_text(row.get("is_viewed_50")) == "true" else 0
        current["viewed_100_share_count"] += 1 if clean_text(row.get("is_viewed_100")) == "true" else 0
        latest = max(
            clean_text(row.get("view_100_first_ts")) or "",
            clean_text(row.get("view_50_first_ts")) or "",
            clean_text(row.get("play_first_ts")) or "",
            clean_text(row.get("shared_at_ts")) or "",
        )
        if latest > current["latest_interaction_ts"]:
            current["latest_interaction_ts"] = latest
    return sorted(grouped.values(), key=lambda item: (-as_int(item.get("shares_count")), item.get("preferred_display_label") or ""))


def _bundle_rankings(share_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in share_rows:
        if clean_text(row.get("shared_item_type")) != "cluster":
            continue
        bundle_code = clean_text(row.get("video_cluster_code"))
        if not bundle_code:
            continue
        current = grouped.setdefault(
            bundle_code,
            {
                "video_cluster_code": bundle_code,
                "preferred_display_label": clean_text(row.get("video_cluster_display_label")) or bundle_code,
                "shares_count": 0,
                "played_share_count": 0,
                "viewed_50_share_count": 0,
                "viewed_100_share_count": 0,
                "latest_interaction_ts": clean_text(row.get("shared_at_ts")) or "",
            },
        )
        current["shares_count"] += 1
        current["played_share_count"] += 1 if clean_text(row.get("is_played")) == "true" else 0
        current["viewed_50_share_count"] += 1 if clean_text(row.get("is_viewed_50")) == "true" else 0
        current["viewed_100_share_count"] += 1 if clean_text(row.get("is_viewed_100")) == "true" else 0
        latest = max(
            clean_text(row.get("view_100_first_ts")) or "",
            clean_text(row.get("view_50_first_ts")) or "",
            clean_text(row.get("play_first_ts")) or "",
            clean_text(row.get("shared_at_ts")) or "",
        )
        if latest > current["latest_interaction_ts"]:
            current["latest_interaction_ts"] = latest
    return sorted(grouped.values(), key=lambda item: (-as_int(item.get("shares_count")), item.get("preferred_display_label") or ""))


def _doctor_activity_rows(base_rows: list[dict[str, Any]], share_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_by_doctor = {clean_text(row.get("doctor_key")): row for row in base_rows if clean_text(row.get("doctor_key"))}
    share_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in share_rows:
        doctor_key = clean_text(row.get("doctor_key"))
        if doctor_key:
            share_groups[doctor_key].append(row)

    output: list[dict[str, Any]] = []
    for doctor_key in sorted(set(base_by_doctor) | set(share_groups)):
        base = base_by_doctor.get(doctor_key, {})
        shares = share_groups.get(doctor_key, [])
        output.append(
            {
                "doctor_key": doctor_key,
                "doctor_id": clean_text(base.get("doctor_id")) or clean_text((shares[0] if shares else {}).get("doctor_id")),
                "doctor_display_name": clean_text(base.get("full_name")) or clean_text((shares[0] if shares else {}).get("doctor_display_name")) or doctor_key,
                "clinic_name": clean_text(base.get("clinic_name")),
                "city": clean_text(base.get("city")) or clean_text((shares[0] if shares else {}).get("city")),
                "district": clean_text(base.get("district")) or clean_text((shares[0] if shares else {}).get("district")),
                "state": clean_text(base.get("state")) or clean_text((shares[0] if shares else {}).get("state")),
                "field_rep_id": clean_text(base.get("field_rep_id_resolved")) or clean_text((shares[0] if shares else {}).get("field_rep_id")),
                "field_rep_external_id": clean_text(base.get("field_rep_external_id")) or clean_text((shares[0] if shares else {}).get("field_rep_external_id")),
                "enrolled_at_ts": clean_text(base.get("enrolled_at_ts")),
                "is_enrolled_flag": clean_text(base.get("is_enrolled_flag")) or "false",
                "shares_total_cumulative": len(shares),
                "unique_recipient_references_cumulative": len({row.get("recipient_reference") for row in shares if clean_text(row.get("recipient_reference"))}),
                "shares_played_cumulative": len([row for row in shares if clean_text(row.get("is_played")) == "true"]),
                "shares_viewed_50_cumulative": len([row for row in shares if clean_text(row.get("is_viewed_50")) == "true"]),
                "shares_viewed_100_cumulative": len([row for row in shares if clean_text(row.get("is_viewed_100")) == "true"]),
                "video_shares_cumulative": len([row for row in shares if clean_text(row.get("shared_item_type")) == "video"]),
                "bundle_shares_cumulative": len([row for row in shares if clean_text(row.get("shared_item_type")) == "cluster"]),
                "last_shared_at_ts": max((clean_text(row.get("shared_at_ts")) or "" for row in shares), default=""),
            }
        )
    return output


def _action_rows(weekly_rows: list[dict[str, Any]], state_rows: list[dict[str, Any]], rep_rows: list[dict[str, Any]], thresholds: dict[str, float], run_id: str, generated_at: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    activation_threshold = thresholds.get("low_activation_pct", 40)
    play_threshold = thresholds.get("low_play_rate_pct", 40)
    engagement_threshold = thresholds.get("low_engagement_50_pct", 40)
    completion_threshold = thresholds.get("low_completion_pct", 40)
    multi_state_threshold = as_int(thresholds.get("multi_state_alert_count", 2), default=2)

    for row in weekly_rows:
        week_end_date = clean_text(row.get("week_end_date"))
        state_slice = [item for item in state_rows if clean_text(item.get("week_end_date")) == week_end_date]
        rep_slice = [item for item in rep_rows if clean_text(item.get("week_end_date")) == week_end_date]
        low_activation_states = [item for item in state_slice if as_float(item.get("activation_pct_state")) < activation_threshold]
        low_health_reps = [item for item in rep_slice if as_float(item.get("weekly_rep_health_score")) < activation_threshold]
        issue = {
            "week_end_date": week_end_date,
            "primary_issue_code": "stable_execution",
            "primary_issue_title": "Campaign execution is stable",
            "who_should_act_role": "Campaign Manager",
            "recommended_action_1": "Continue current execution and monitor weekly movement.",
            "recommended_action_2": "Review content rankings for language and bundle mix shifts.",
            "recommended_action_3": "Watch attention states for any new downward movement.",
            "affected_state_count": len(low_activation_states),
            "affected_field_rep_count": len(low_health_reps),
            "_generated_at": generated_at,
            "_generated_by_run_id": run_id,
        }
        if len(low_activation_states) >= multi_state_threshold:
            issue.update(
                {
                    "primary_issue_code": "low_activation_multi_state",
                    "primary_issue_title": f"Low activation in {len(low_activation_states)} states",
                    "who_should_act_role": "Field Team Lead",
                    "recommended_action_1": "Prioritize doctor follow-up in the weakest states first.",
                    "recommended_action_2": "Review enrollment-to-share conversion with the assigned field reps.",
                    "recommended_action_3": "Confirm campaign bundle awareness with newly enrolled doctors.",
                }
            )
        elif as_float(row.get("play_rate_pct")) < play_threshold:
            issue.update(
                {
                    "primary_issue_code": "low_play_rate",
                    "primary_issue_title": "Low share play rate this week",
                    "who_should_act_role": "Field Team Lead",
                    "recommended_action_1": "Tighten the doctor script used at the point of sharing.",
                    "recommended_action_2": "Focus on high-intent doctors with no recent played shares.",
                    "recommended_action_3": "Verify the selected language mix matches campaign demand.",
                }
            )
        elif as_float(row.get("engagement_50_pct")) < engagement_threshold:
            issue.update(
                {
                    "primary_issue_code": "low_engagement_50",
                    "primary_issue_title": "Low >50% engagement this week",
                    "who_should_act_role": "Content Team",
                    "recommended_action_1": "Review the opening sequencing of the highest-volume content.",
                    "recommended_action_2": "Promote better-performing bundles in underperforming states.",
                    "recommended_action_3": "Check whether language choice is affecting completion depth.",
                }
            )
        elif as_float(row.get("completion_pct")) < completion_threshold:
            issue.update(
                {
                    "primary_issue_code": "low_completion",
                    "primary_issue_title": "Low completion rate this week",
                    "who_should_act_role": "Content Team",
                    "recommended_action_1": "Review long-form content causing drop-off before completion.",
                    "recommended_action_2": "Favor bundles with stronger mid-video retention.",
                    "recommended_action_3": "Brief field reps on the completion gap for follow-up calls.",
                }
            )
        output.append(issue)
    return output


def build_gold(run_id: str, source_status: str = "SUCCESS", notes: str = "") -> dict[str, Any]:
    published_at = _now_iso()
    as_of_date = date.today().isoformat()
    thresholds = get_thresholds()

    dim_campaign_rows = fetch_table(SILVER_SCHEMA, "dim_campaign")
    field_rep_rows = fetch_table(SILVER_SCHEMA, "dim_field_rep")
    base_rows = fetch_table(SILVER_SCHEMA, "bridge_campaign_doctor_base")
    share_rows = fetch_table(SILVER_SCHEMA, "fact_share_activity")
    playback_rows = fetch_table(SILVER_SCHEMA, "fact_share_playback_event")
    funnel_rows = fetch_table(SILVER_SCHEMA, "fact_share_funnel_first_seen")
    video_rows = fetch_table(SILVER_SCHEMA, "fact_video_view")
    enrollment_rows = fetch_table(SILVER_SCHEMA, "fact_campaign_enrollment")

    field_rep_lookup = {clean_text(row.get("field_rep_id")): row for row in field_rep_rows if clean_text(row.get("field_rep_id"))}
    existing_history = fetch_table(GOLD_GLOBAL_SCHEMA, "campaign_health_history") if table_exists(GOLD_GLOBAL_SCHEMA, "campaign_health_history") else []
    existing_benchmarks = fetch_table(GOLD_GLOBAL_SCHEMA, "benchmark_last_10_campaigns") if table_exists(GOLD_GLOBAL_SCHEMA, "benchmark_last_10_campaigns") else []

    relevant_campaigns = []
    for campaign in dim_campaign_rows:
        campaign_id_normalized = clean_text(campaign.get("campaign_id_normalized"))
        if not campaign_id_normalized:
            continue
        has_publisher = clean_text(campaign.get("publisher_campaign_present_flag")) == "true"
        has_enrollment = any(clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized for row in enrollment_rows)
        has_share = any(clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized for row in share_rows)
        if has_publisher or has_enrollment or has_share:
            relevant_campaigns.append(campaign)

    global_registry_rows: list[dict[str, Any]] = []
    global_health_rows_current: list[dict[str, Any]] = []
    published_campaign_schemas: list[str] = []
    campaign_table_map: dict[str, list[str]] = {}

    for campaign in relevant_campaigns:
        campaign_id_normalized = clean_text(campaign.get("campaign_id_normalized"))
        if not campaign_id_normalized:
            continue
        schema_name = campaign_schema_name(campaign_id_normalized)
        published_campaign_schemas.append(schema_name)
        campaign_table_map[schema_name] = []

        campaign_base_rows = [row for row in base_rows if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized]
        campaign_share_rows_source = [row for row in share_rows if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized and clean_text(row.get("is_campaign_attributed_flag")) == "true"]
        campaign_share_rows = _share_with_funnel(campaign_share_rows_source, [row for row in funnel_rows if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized], run_id, published_at)
        week_lookup = {
            clean_text(row.get("week_end_date")): row.get("week_index")
            for row in _weekly_summary_rows(campaign, campaign_base_rows, campaign_share_rows)
        }
        for row in campaign_share_rows:
            row["week_index"] = week_lookup.get(clean_text(row.get("week_end_date")), 1)

        weekly_rows = _weekly_summary_rows(campaign, campaign_base_rows, campaign_share_rows)
        state_rows = _state_summary_rows(campaign_base_rows, campaign_share_rows, weekly_rows)
        rep_rows = _field_rep_summary_rows(campaign_base_rows, campaign_share_rows, weekly_rows, field_rep_lookup)
        language_rows = _language_summary_rows(campaign_share_rows, weekly_rows)
        video_detail_rows = [
            dict(row, week_end_date=clean_text(next((share.get("week_end_date") for share in campaign_share_rows if clean_text(share.get("share_public_id")) == clean_text(row.get("share_public_id"))), "")))
            for row in video_rows
            if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized
        ]
        video_rank_rows = _video_rankings(video_detail_rows)
        bundle_rank_rows = _bundle_rankings(campaign_share_rows)

        cumulative_enrolled = len({row.get("doctor_key") for row in campaign_base_rows if clean_text(row.get("doctor_key"))})
        cumulative_doctors_sharing = len({row.get("doctor_key") for row in campaign_share_rows if clean_text(row.get("doctor_key"))})
        cumulative_shares = len(campaign_share_rows)
        cumulative_recips = len({row.get("recipient_reference") for row in campaign_share_rows if clean_text(row.get("recipient_reference"))})
        cumulative_played = len([row for row in campaign_share_rows if clean_text(row.get("is_played")) == "true"])
        cumulative_viewed_50 = len([row for row in campaign_share_rows if clean_text(row.get("is_viewed_50")) == "true"])
        cumulative_viewed_100 = len([row for row in campaign_share_rows if clean_text(row.get("is_viewed_100")) == "true"])
        cumulative_video_shares = len([row for row in campaign_share_rows if clean_text(row.get("shared_item_type")) == "video"])
        cumulative_bundle_shares = len([row for row in campaign_share_rows if clean_text(row.get("shared_item_type")) == "cluster"])
        health = compute_health_components(
            enrolled_doctors_current=cumulative_enrolled,
            doctors_sharing_unique=cumulative_doctors_sharing,
            shares_total=cumulative_shares,
            shares_played=cumulative_played,
            shares_viewed_50=cumulative_viewed_50,
            shares_viewed_100=cumulative_viewed_100,
        )
        previous_history_row = next(
            (
                row
                for row in sorted(existing_history, key=lambda item: clean_text(item.get("as_of_date")) or "", reverse=True)
                if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized
            ),
            None,
        )
        campaign_health_summary_row = {
            "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
            "as_of_date": as_of_date,
            "enrolled_doctors_current": cumulative_enrolled,
            "doctors_sharing_unique_cumulative": cumulative_doctors_sharing,
            "shares_total_cumulative": cumulative_shares,
            "unique_recipient_references_cumulative": cumulative_recips,
            "shares_played_cumulative": cumulative_played,
            "shares_viewed_50_cumulative": cumulative_viewed_50,
            "shares_viewed_100_cumulative": cumulative_viewed_100,
            "video_shares_cumulative": cumulative_video_shares,
            "bundle_shares_cumulative": cumulative_bundle_shares,
            "activation_pct": health["activation_pct"],
            "play_rate_pct": health["play_rate_pct"],
            "engagement_50_pct": health["engagement_50_pct"],
            "completion_pct": health["completion_pct"],
            "campaign_health_score": health["campaign_health_score"],
            "wow_campaign_health_score_delta": round(as_float(health["campaign_health_score"]) - as_float((previous_history_row or {}).get("campaign_health_score")), 2),
            "benchmark_avg_campaign_health_score": 0,
            "benchmark_label": "Insufficient Data" if health["insufficient_data_flag"] else "Pending Benchmark",
            "health_color": health_color(health["campaign_health_score"]),
            "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
        }
        global_health_rows_current.append(
            {
                "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
                "campaign_id_normalized": campaign_id_normalized,
                "as_of_date": as_of_date,
                "campaign_health_score": health["campaign_health_score"],
                "activation_pct": health["activation_pct"],
                "play_rate_pct": health["play_rate_pct"],
                "engagement_50_pct": health["engagement_50_pct"],
                "completion_pct": health["completion_pct"],
                "enrolled_doctors_current": cumulative_enrolled,
                "insufficient_data_flag": "true" if health["insufficient_data_flag"] else "false",
                "_loaded_at": published_at,
            }
        )

        action_rows = _action_rows(weekly_rows, state_rows, rep_rows, thresholds, run_id, published_at)
        latest_state_rows = [row for row in state_rows if clean_text(row.get("is_bottom_3_flag")) == "true"]
        latest_rep_rows = [row for row in rep_rows if clean_text(row.get("is_bottom_3_flag")) == "true"]
        doctor_activity_rows = _doctor_activity_rows(campaign_base_rows, campaign_share_rows)
        playback_detail_rows = [dict(row, week_end_date=clean_text(next((share.get("week_end_date") for share in campaign_share_rows if clean_text(share.get("share_public_id")) == clean_text(row.get("share_public_id"))), ""))) for row in playback_rows if clean_text(row.get("campaign_id_normalized")) == campaign_id_normalized]
        bundle_detail_rows = [row for row in campaign_share_rows if clean_text(row.get("shared_item_type")) == "cluster"]
        language_detail_rows = language_rows

        registry_row = {
            "campaign_id_original": clean_text(campaign.get("campaign_id_original")),
            "campaign_id_normalized": campaign_id_normalized,
            "gold_schema_name": schema_name,
            "campaign_name": clean_text(campaign.get("campaign_name")),
            "brand_name": clean_text(campaign.get("brand_name")),
            "first_seen_ts": clean_text(campaign.get("created_at_ts")) or published_at,
            "last_seen_ts": published_at,
            "_created_at": clean_text(campaign.get("created_at_ts")) or published_at,
            "_updated_at": published_at,
        }
        global_registry_rows.append(registry_row)

        _replace_stage_table(schema_name, "fact_share_latest", campaign_share_rows, CAMPAIGN_DEFAULT_COLUMNS["fact_share_latest"])
        _replace_stage_table(schema_name, "kpi_weekly_summary", weekly_rows, CAMPAIGN_DEFAULT_COLUMNS["kpi_weekly_summary"])
        _replace_stage_table(schema_name, "kpi_campaign_health_summary", [campaign_health_summary_row], CAMPAIGN_DEFAULT_COLUMNS["kpi_campaign_health_summary"])
        _replace_stage_table(schema_name, "state_weekly_summary", state_rows, ["state"])
        _replace_stage_table(schema_name, "field_rep_weekly_summary", rep_rows, ["field_rep_id"])
        _replace_stage_table(schema_name, "language_weekly_summary", language_rows, ["language_code"])
        _replace_stage_table(schema_name, "content_video_rankings", video_rank_rows, ["video_code"])
        _replace_stage_table(schema_name, "content_bundle_rankings", bundle_rank_rows, ["video_cluster_code"])
        _replace_stage_table(schema_name, "weekly_action_items", action_rows, ["week_end_date"])
        _replace_stage_table(schema_name, "rpt_enrollment_detail", campaign_base_rows, ["doctor_key"])
        _replace_stage_table(schema_name, "rpt_share_detail", campaign_share_rows, CAMPAIGN_DEFAULT_COLUMNS["fact_share_latest"])
        _replace_stage_table(schema_name, "rpt_playback_detail", playback_detail_rows, ["source_playback_id"])
        _replace_stage_table(schema_name, "rpt_doctor_activity_current", doctor_activity_rows, ["doctor_key"])
        _replace_stage_table(schema_name, "rpt_state_detail", latest_state_rows, ["state"])
        _replace_stage_table(schema_name, "rpt_field_rep_detail", latest_rep_rows, ["field_rep_id"])
        _replace_stage_table(schema_name, "rpt_content_video_detail", video_detail_rows, ["share_public_id"])
        _replace_stage_table(schema_name, "rpt_content_bundle_detail", bundle_detail_rows, ["share_public_id"])
        _replace_stage_table(schema_name, "rpt_language_detail", language_detail_rows, ["language_code"])
        _replace_stage_table(
            schema_name,
            "dim_filter_week",
            [
                {
                    "underlying_key": clean_text(row.get("week_end_date")),
                    "display_label": f"Week {row.get('week_index')} ({row.get('week_start_date')} to {row.get('week_end_date')})",
                    "sort_key": clean_text(row.get("week_end_date")),
                }
                for row in weekly_rows
            ],
            ["underlying_key", "display_label", "sort_key"],
        )
        _replace_stage_table(schema_name, "dim_filter_state", _dimension_rows_from_table(campaign_base_rows + campaign_share_rows, "state"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_field_rep", _dimension_rows_from_table(campaign_base_rows, "field_rep_id_resolved"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_doctor", _dimension_rows_from_table(doctor_activity_rows, "doctor_key", "doctor_display_name"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_language", _dimension_rows_from_table(campaign_share_rows, "language_code"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_share_type", _dimension_rows_from_table(campaign_share_rows, "shared_item_type"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_therapy_area", _dimension_rows_from_table(video_detail_rows + campaign_share_rows, "therapy_area_name"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_trigger", _dimension_rows_from_table(video_detail_rows + campaign_share_rows, "trigger_name"), ["underlying_key", "display_label", "sort_key"])
        _replace_stage_table(schema_name, "dim_filter_bundle", _dimension_rows_from_table(bundle_detail_rows + campaign_share_rows, "video_cluster_code", "video_cluster_display_label"), ["underlying_key", "display_label", "sort_key"])

        campaign_table_map[schema_name].extend(
            [
                "fact_share_latest",
                "kpi_weekly_summary",
                "kpi_campaign_health_summary",
                "state_weekly_summary",
                "field_rep_weekly_summary",
                "language_weekly_summary",
                "content_video_rankings",
                "content_bundle_rankings",
                "weekly_action_items",
                "rpt_enrollment_detail",
                "rpt_share_detail",
                "rpt_playback_detail",
                "rpt_doctor_activity_current",
                "rpt_state_detail",
                "rpt_field_rep_detail",
                "rpt_content_video_detail",
                "rpt_content_bundle_detail",
                "rpt_language_detail",
                "dim_filter_week",
                "dim_filter_state",
                "dim_filter_field_rep",
                "dim_filter_doctor",
                "dim_filter_language",
                "dim_filter_share_type",
                "dim_filter_therapy_area",
                "dim_filter_trigger",
                "dim_filter_bundle",
            ]
        )

    benchmark_source = []
    for row in global_health_rows_current:
        matching_campaign = next((item for item in relevant_campaigns if clean_text(item.get("campaign_id_normalized")) == clean_text(row.get("campaign_id_normalized"))), {})
        benchmark_source.append(dict(row, _campaign_sort_date=first_non_empty(clean_text(matching_campaign.get("start_date")), clean_text(matching_campaign.get("created_at_ts")), "0000-00-00")))
    benchmark_source = sorted(benchmark_source, key=lambda item: item.get("_campaign_sort_date") or "", reverse=True)[:10]
    benchmark_row = build_benchmark_row(
        [
            dict(
                row,
                insufficient_data_flag=next(
                    (
                        summary.get("insufficient_data_flag")
                        for summary in [r for r in global_health_rows_current if clean_text(r.get("campaign_id_normalized")) == clean_text(row.get("campaign_id_normalized"))]
                    ),
                    "false",
                ),
            )
            for row in benchmark_source
        ],
        as_of_date,
        published_at,
    )

    for schema_name in published_campaign_schemas:
        if table_exists(schema_name, "kpi_campaign_health_summary__stage"):
            rows = fetch_table(schema_name, "kpi_campaign_health_summary__stage")
            if rows:
                rows[0]["benchmark_avg_campaign_health_score"] = benchmark_row["avg_campaign_health_score"]
                score = as_float(rows[0].get("campaign_health_score"))
                rows[0]["benchmark_label"] = (
                    "Above Average"
                    if score > as_float(benchmark_row.get("avg_campaign_health_score"))
                    else "Below Average" if score < as_float(benchmark_row.get("avg_campaign_health_score")) else "Average"
                )
                _replace_stage_table(schema_name, "kpi_campaign_health_summary", rows, CAMPAIGN_DEFAULT_COLUMNS["kpi_campaign_health_summary"])

    new_history = [row for row in existing_history if clean_text(row.get("as_of_date")) != as_of_date]
    new_history.extend(global_health_rows_current)
    new_benchmarks = [row for row in existing_benchmarks if clean_text(row.get("as_of_date")) != as_of_date]
    new_benchmarks.append(benchmark_row)

    _replace_stage_table(GOLD_GLOBAL_SCHEMA, "campaign_registry", global_registry_rows, GLOBAL_DEFAULT_COLUMNS["campaign_registry"])
    _replace_stage_table(GOLD_GLOBAL_SCHEMA, "campaign_health_history", new_history, GLOBAL_DEFAULT_COLUMNS["campaign_health_history"])
    _replace_stage_table(GOLD_GLOBAL_SCHEMA, "benchmark_last_10_campaigns", new_benchmarks, GLOBAL_DEFAULT_COLUMNS["benchmark_last_10_campaigns"])
    _replace_stage_table(
        GOLD_GLOBAL_SCHEMA,
        "refresh_status",
        [{"publish_id": run_id, "published_at": published_at, "as_of_date": as_of_date, "status": source_status, "notes": notes}],
        GLOBAL_DEFAULT_COLUMNS["refresh_status"],
    )

    with transaction.atomic():
        _publish_schema_tables(GOLD_GLOBAL_SCHEMA, list(GLOBAL_DEFAULT_COLUMNS.keys()))
        for schema_name, table_names in campaign_table_map.items():
            _publish_schema_tables(schema_name, table_names)

    return {
        "as_of_date": as_of_date,
        "published_at": published_at,
        "campaigns_built": len(relevant_campaigns),
        "campaign_schemas": published_campaign_schemas,
        "tables": list(GLOBAL_DEFAULT_COLUMNS.keys()),
        "benchmark_population_count": benchmark_row["campaign_count"],
    }
