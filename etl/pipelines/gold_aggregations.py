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
            WITH latest_tx AS (
                SELECT DISTINCT ON (brand_campaign_id, collateral_id, doctor_identity_key)
                    brand_campaign_id,
                    collateral_id,
                    doctor_identity_key,
                    id AS source_latest_transaction_id,
                    doctor_master_id_resolved,
                    field_rep_id
                FROM silver.fact_collateral_transaction
                WHERE brand_campaign_id = %s
                ORDER BY brand_campaign_id, collateral_id, doctor_identity_key, COALESCE(updated_at_ts, created_at_ts, transaction_date_ts) DESC, id DESC
            )
            SELECT
                a.brand_campaign_id,
                a.collateral_id,
                a.doctor_identity_key,
                tx.doctor_master_id_resolved,
                tx.field_rep_id AS field_rep_id_resolved,
                COALESCE(b.state_normalized, 'UNKNOWN') AS state_normalized,
                CASE WHEN a.reached_first_ts IS NOT NULL THEN 1 ELSE 0 END AS is_reached,
                CASE WHEN a.opened_first_ts IS NOT NULL THEN 1 ELSE 0 END AS is_opened,
                CASE WHEN a.video_gt_50_first_ts IS NOT NULL THEN 1 ELSE 0 END AS is_video_viewed_gt_50,
                CASE WHEN a.pdf_download_first_ts IS NOT NULL THEN 1 ELSE 0 END AS is_pdf_downloaded,
                CASE WHEN a.video_gt_50_first_ts IS NOT NULL OR a.pdf_download_first_ts IS NOT NULL THEN 1 ELSE 0 END AS is_consumed,
                a.reached_first_ts,
                a.opened_first_ts,
                a.video_gt_50_first_ts,
                a.pdf_download_first_ts,
                a.last_activity_ts,
                tx.source_latest_transaction_id,
                '{run_id}'::text AS _as_of_run_id,
                NOW()::text AS _as_of_ts
            FROM silver.doctor_action_first_seen a
            LEFT JOIN latest_tx tx
              ON tx.brand_campaign_id=a.brand_campaign_id
             AND tx.collateral_id=a.collateral_id
             AND tx.doctor_identity_key=a.doctor_identity_key
            LEFT JOIN silver.bridge_brand_campaign_doctor_base b
              ON b.brand_campaign_id=a.brand_campaign_id AND b.doctor_identity_key=a.doctor_identity_key
            WHERE a.brand_campaign_id = %s
            """,
            [brand_campaign_id, brand_campaign_id],
        )

        execute(f"DROP TABLE IF EXISTS {schema}.kpi_weekly_summary;")
        execute(
            f"""
            CREATE TABLE {schema}.kpi_weekly_summary AS
            WITH base AS (
                SELECT COUNT(DISTINCT doctor_identity_key) AS total_doctors_in_campaign
                FROM silver.bridge_brand_campaign_doctor_base WHERE brand_campaign_id=%s
            ),
            source_events AS (
                SELECT
                    COALESCE(NULLIF(doctor_master_id_resolved,''), doctor_identity_key, source_latest_transaction_id::text) AS doctor_key,
                    CASE WHEN reached_first_ts IS NULL OR btrim(reached_first_ts) = '' OR lower(btrim(reached_first_ts)) = 'null' THEN NULL ELSE reached_first_ts::date END AS reached_first_date,
                    CASE WHEN opened_first_ts IS NULL OR btrim(opened_first_ts) = '' OR lower(btrim(opened_first_ts)) = 'null' THEN NULL ELSE opened_first_ts::date END AS opened_first_date,
                    CASE WHEN video_gt_50_first_ts IS NULL OR btrim(video_gt_50_first_ts) = '' OR lower(btrim(video_gt_50_first_ts)) = 'null' THEN NULL ELSE video_gt_50_first_ts::date END AS video_gt_50_first_date,
                    CASE WHEN pdf_download_first_ts IS NULL OR btrim(pdf_download_first_ts) = '' OR lower(btrim(pdf_download_first_ts)) = 'null' THEN NULL ELSE pdf_download_first_ts::date END AS pdf_download_first_date
                FROM {schema}.fact_doctor_collateral_latest
            ),
            fact_normalized AS (
                SELECT
                    doctor_key,
                    reached_first_date,
                    opened_first_date,
                    video_gt_50_first_date,
                    pdf_download_first_date,
                    (
                        SELECT MIN(activity_date)
                        FROM (
                            VALUES
                                (reached_first_date),
                                (opened_first_date),
                                (video_gt_50_first_date),
                                (pdf_download_first_date)
                        ) AS dates(activity_date)
                        WHERE activity_date IS NOT NULL
                    ) AS first_activity_date
                FROM source_events
            ),
            activity_bounds AS (
                SELECT
                    MIN(first_activity_date) AS first_activity_date,
                    MAX(first_activity_date) AS last_activity_date
                FROM fact_normalized
            ),
            schedule_source AS (
                SELECT
                    MIN(schedule_start_date) AS schedule_start_date,
                    MAX(schedule_end_date) AS schedule_end_date
                FROM silver.bridge_campaign_collateral_schedule
                WHERE campaign_id_resolved::text = NULLIF(btrim(%s), '')
            ),
            schedule_bounds AS (
                SELECT
                    COALESCE(s.schedule_start_date, a.first_activity_date, CURRENT_DATE)::date AS schedule_start_date,
                    GREATEST(
                        COALESCE(s.schedule_end_date, a.last_activity_date, s.schedule_start_date, CURRENT_DATE)::date,
                        COALESCE(s.schedule_start_date, a.first_activity_date, CURRENT_DATE)::date
                    ) AS schedule_end_date
                FROM activity_bounds a
                CROSS JOIN schedule_source s
            ),
            weeks AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY week_start)::int AS week_index,
                    week_start::date AS week_start_date,
                    LEAST((week_start + interval '6 day')::date, b.schedule_end_date)::date AS week_end_date
                FROM schedule_bounds b
                CROSS JOIN LATERAL generate_series(
                    b.schedule_start_date,
                    b.schedule_end_date,
                    interval '7 day'
                ) AS gs(week_start)
            ),
            agg AS (
                SELECT
                    w.week_index,
                    w.week_start_date,
                    w.week_end_date,
                    COUNT(DISTINCT f.doctor_key) FILTER (
                        WHERE f.reached_first_date BETWEEN w.week_start_date AND w.week_end_date
                           OR f.opened_first_date BETWEEN w.week_start_date AND w.week_end_date
                           OR f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date
                           OR f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date
                    ) AS doctors_reached_unique,
                    COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.opened_first_date BETWEEN w.week_start_date AND w.week_end_date) AS doctors_opened_unique,
                    COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date) AS video_viewed_50_unique,
                    COUNT(DISTINCT f.doctor_key) FILTER (WHERE f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date) AS pdf_download_unique,
                    COUNT(DISTINCT f.doctor_key) FILTER (WHERE (f.video_gt_50_first_date BETWEEN w.week_start_date AND w.week_end_date) OR (f.pdf_download_first_date BETWEEN w.week_start_date AND w.week_end_date)) AS doctors_consumed_unique
                FROM weeks w
                LEFT JOIN fact_normalized f ON TRUE
                GROUP BY w.week_index, w.week_start_date, w.week_end_date
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
                (b.total_doctors_in_campaign / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric) AS weekly_doctor_base,
                LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric),0) END, 1.0) AS weekly_reached_pct,
                CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END AS weekly_opened_pct,
                CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END AS weekly_consumption_pct,
                ((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric),0) END, 1.0)
                + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100 AS weekly_health_score,
                CASE
                    WHEN (((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric),0) END, 1.0)
                    + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                    + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100) < 40 THEN 'Red'
                    WHEN (((LEAST(CASE WHEN b.total_doctors_in_campaign=0 THEN 0 ELSE doctors_reached_unique / NULLIF((b.total_doctors_in_campaign / GREATEST((SELECT COUNT(*) FROM weeks), 1)::numeric),0) END, 1.0)
                    + CASE WHEN doctors_reached_unique=0 THEN 0 ELSE doctors_opened_unique::numeric / doctors_reached_unique END
                    + CASE WHEN doctors_opened_unique=0 THEN 0 ELSE doctors_consumed_unique::numeric / doctors_opened_unique END) / 3.0) * 100) < 60 THEN 'Yellow'
                    ELSE 'Green'
                END AS health_color,
                CASE WHEN b.total_doctors_in_campaign=0 THEN 1 ELSE 0 END AS insufficient_data_flag
            FROM agg CROSS JOIN base b
            """,
            [brand_campaign_id, row["campaign_id_resolved"], brand_campaign_id],
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
