import re
from etl.connectors.postgres import execute, fetchall


def normalize_schema_name(brand_campaign_id: str) -> str:
    normalized = re.sub(r"[^a-z0-9]", "_", (brand_campaign_id or "").lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized or not normalized[0].isalpha():
        normalized = f"c_{normalized}" if normalized else "c_unknown"
    return f"gold_campaign_{normalized}"


def ensure_global_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS gold_global;")
    execute(
        """
        CREATE TABLE IF NOT EXISTS gold_global.campaign_registry (
            brand_campaign_id TEXT PRIMARY KEY,
            gold_schema_name TEXT NOT NULL,
            campaign_id_resolved TEXT,
            first_seen_ts TEXT,
            last_seen_ts TEXT,
            _created_at TEXT,
            _updated_at TEXT
        );
        """
    )
    execute(
        """
        CREATE TABLE IF NOT EXISTS gold_global.campaign_health_history (
            brand_campaign_id TEXT,
            as_of_date TEXT,
            campaign_health_score NUMERIC,
            health_color TEXT,
            total_doctors_in_campaign NUMERIC,
            _loaded_at TEXT,
            PRIMARY KEY (brand_campaign_id, as_of_date)
        );
        """
    )


def build_gold(run_id: str) -> None:
    ensure_global_tables()
    campaigns = fetchall(
        """
        SELECT brand_campaign_id, campaign_id_resolved
        FROM silver.map_brand_campaign_to_campaign
        WHERE _dq_status = 'PASS' AND COALESCE(brand_campaign_id,'') <> ''
        """
    )

    for row in campaigns:
        brand_campaign_id = row["brand_campaign_id"]
        schema = normalize_schema_name(brand_campaign_id)
        execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        execute(
            """
            INSERT INTO gold_global.campaign_registry
            (brand_campaign_id, gold_schema_name, campaign_id_resolved, first_seen_ts, last_seen_ts, _created_at, _updated_at)
            VALUES (%s,%s,%s,NOW()::text,NOW()::text,NOW()::text,NOW()::text)
            ON CONFLICT (brand_campaign_id)
            DO UPDATE SET gold_schema_name=EXCLUDED.gold_schema_name,
                          campaign_id_resolved=EXCLUDED.campaign_id_resolved,
                          last_seen_ts=NOW()::text,
                          _updated_at=NOW()::text
            """,
            [brand_campaign_id, schema, row["campaign_id_resolved"]],
        )

        execute(f"DROP TABLE IF EXISTS {schema}.fact_doctor_collateral_latest;")
        execute(
            f"""
            CREATE TABLE {schema}.fact_doctor_collateral_latest AS
            SELECT
                f.brand_campaign_id,
                f.collateral_id,
                f.doctor_identity_key,
                MAX(f.doctor_master_id_resolved) AS doctor_master_id_resolved,
                MAX(f.field_rep_id) AS field_rep_id_resolved,
                MAX(b.state_normalized) AS state_normalized,
                CASE WHEN MIN(a.reached_first_ts) IS NOT NULL THEN 1 ELSE 0 END AS is_reached,
                CASE WHEN MIN(a.opened_first_ts) IS NOT NULL THEN 1 ELSE 0 END AS is_opened,
                CASE WHEN MIN(a.video_gt_50_first_ts) IS NOT NULL THEN 1 ELSE 0 END AS is_video_viewed_gt_50,
                CASE WHEN MIN(a.pdf_download_first_ts) IS NOT NULL THEN 1 ELSE 0 END AS is_pdf_downloaded,
                CASE WHEN MIN(a.video_gt_50_first_ts) IS NOT NULL OR MIN(a.pdf_download_first_ts) IS NOT NULL THEN 1 ELSE 0 END AS is_consumed,
                MIN(a.reached_first_ts) AS reached_first_ts,
                MIN(a.opened_first_ts) AS opened_first_ts,
                MIN(a.video_gt_50_first_ts) AS video_gt_50_first_ts,
                MIN(a.pdf_download_first_ts) AS pdf_download_first_ts,
                MAX(a.last_activity_ts) AS last_activity_ts,
                MAX(f.id) AS source_latest_transaction_id,
                '{run_id}'::text AS _as_of_run_id,
                NOW()::text AS _as_of_ts
            FROM silver.fact_collateral_transaction f
            JOIN silver.doctor_action_first_seen a
              ON a.brand_campaign_id=f.brand_campaign_id
             AND a.collateral_id=f.collateral_id
             AND a.doctor_identity_key=f.doctor_identity_key
            LEFT JOIN silver.bridge_brand_campaign_doctor_base b
              ON b.brand_campaign_id=f.brand_campaign_id AND b.doctor_identity_key=f.doctor_identity_key
            WHERE f.brand_campaign_id = %s
            GROUP BY f.brand_campaign_id, f.collateral_id, f.doctor_identity_key
            """,
            [brand_campaign_id],
        )

        execute(f"DROP TABLE IF EXISTS {schema}.kpi_weekly_summary;")
        execute(
            f"""
            CREATE TABLE {schema}.kpi_weekly_summary AS
            WITH base AS (
                SELECT COUNT(DISTINCT doctor_identity_key) AS total_doctors_in_campaign
                FROM silver.bridge_brand_campaign_doctor_base WHERE brand_campaign_id=%s
            ),
            weeks AS (
                SELECT generate_series(date_trunc('week', CURRENT_DATE)::date,
                                       date_trunc('week', CURRENT_DATE)::date + interval '21 day',
                                       interval '7 day')::date AS week_start_date
            ),
            agg AS (
                SELECT
                    w.week_start_date,
                    (w.week_start_date + interval '6 day')::date AS week_end_date,
                    ROW_NUMBER() OVER (ORDER BY w.week_start_date) AS week_index,
                    COUNT(DISTINCT f.doctor_identity_key) FILTER (WHERE f.reached_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date) AS doctors_reached_unique,
                    COUNT(DISTINCT f.doctor_identity_key) FILTER (WHERE f.opened_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date) AS doctors_opened_unique,
                    COUNT(DISTINCT f.doctor_identity_key) FILTER (WHERE f.video_gt_50_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date) AS video_viewed_50_unique,
                    COUNT(DISTINCT f.doctor_identity_key) FILTER (WHERE f.pdf_download_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date) AS pdf_download_unique,
                    COUNT(DISTINCT f.doctor_identity_key) FILTER (WHERE (f.video_gt_50_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date) OR (f.pdf_download_first_ts::date BETWEEN w.week_start_date AND (w.week_start_date + interval '6 day')::date)) AS doctors_consumed_unique
                FROM weeks w
                LEFT JOIN {schema}.fact_doctor_collateral_latest f ON TRUE
                GROUP BY w.week_start_date
            )
            SELECT
                %s::text AS brand_campaign_id,
                week_index,
                week_start_date,
                week_end_date,
                doctors_reached_unique,
                doctors_opened_unique,
                video_viewed_50_unique,
                pdf_download_unique,
                doctors_consumed_unique,
                b.total_doctors_in_campaign,
                (b.total_doctors_in_campaign / 4.0) AS weekly_doctor_base,
                LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / 4.0),0) END, 1.0) AS weekly_reached_pct,
                CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END AS weekly_opened_pct,
                CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END AS weekly_consumption_pct,
                ((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / 4.0),0) END, 1.0)
                + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100 AS weekly_health_score,
                CASE
                    WHEN (((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / 4.0),0) END, 1.0)
                    + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                    + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100) < 40 THEN 'Red'
                    WHEN (((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / 4.0),0) END, 1.0)
                    + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                    + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100) < 60 THEN 'Yellow'
                    ELSE 'Green'
                END AS health_color,
                CASE WHEN b.total_doctors_in_campaign=0 THEN 1 ELSE 0 END AS insufficient_data_flag
            FROM agg CROSS JOIN base b
            """,
            [brand_campaign_id, brand_campaign_id],
        )

        execute(f"CREATE TABLE IF NOT EXISTS {schema}.weekly_action_items AS SELECT * FROM {schema}.kpi_weekly_summary WHERE false;")

        execute(
            f"""
            INSERT INTO gold_global.campaign_health_history
            (brand_campaign_id, as_of_date, campaign_health_score, health_color, total_doctors_in_campaign, _loaded_at)
            SELECT
                %s,
                CURRENT_DATE::text,
                AVG(weekly_health_score),
                CASE WHEN AVG(weekly_health_score) < 40 THEN 'Red' WHEN AVG(weekly_health_score) < 60 THEN 'Yellow' ELSE 'Green' END,
                MAX(total_doctors_in_campaign),
                NOW()::text
            FROM {schema}.kpi_weekly_summary
            ON CONFLICT (brand_campaign_id, as_of_date)
            DO UPDATE SET campaign_health_score=EXCLUDED.campaign_health_score,
                          health_color=EXCLUDED.health_color,
                          total_doctors_in_campaign=EXCLUDED.total_doctors_in_campaign,
                          _loaded_at=EXCLUDED._loaded_at
            """,
            [brand_campaign_id],
        )

    execute(
        """
        CREATE TABLE IF NOT EXISTS gold_global.benchmark_last_10_campaigns AS
        SELECT
            CURRENT_DATE::text AS as_of_date,
            'GLOBAL_LAST_10'::text AS benchmark_group_key,
            COUNT(*)::int AS campaign_count,
            AVG(campaign_health_score) AS avg_campaign_health_score,
            percentile_cont(0.5) within group(order by campaign_health_score) AS p50_campaign_health_score,
            percentile_cont(0.75) within group(order by campaign_health_score) AS p75_campaign_health_score,
            NOW()::text AS _computed_at
        FROM (
            SELECT campaign_health_score
            FROM gold_global.campaign_health_history
            WHERE as_of_date = CURRENT_DATE::text
            ORDER BY _loaded_at DESC
            LIMIT 10
        ) q;
        """
    )
