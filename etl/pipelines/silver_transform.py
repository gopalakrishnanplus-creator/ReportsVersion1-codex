from etl.connectors.postgres import execute


def ensure_silver_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS silver;")


def build_silver(run_id: str) -> None:
    ensure_silver_tables()
    execute("DROP TABLE IF EXISTS silver.dim_field_rep;")
    execute(
        """
        CREATE TABLE silver.dim_field_rep AS
        WITH unified AS (
            SELECT
                id::text AS id,
                NULLIF(btrim(full_name), '') AS full_name,
                NULLIF(btrim(phone_number), '') AS phone_number,
                NULLIF(btrim(brand_supplied_field_rep_id), '') AS brand_supplied_field_rep_id,
                CASE WHEN lower(COALESCE(is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active,
                NULLIF(btrim(password_hash), '') AS password_hash,
                created_at,
                updated_at,
                brand_id,
                user_id,
                NULLIF(btrim(state), '') AS state,
                NULL::text AS campaign_id,
                'campaign_fieldrep'::text AS source_table
            FROM bronze.campaign_fieldrep
            UNION ALL
            SELECT
                id::text AS id,
                NULL::text AS full_name,
                NULL::text AS phone_number,
                NULLIF(btrim(field_rep_id), '') AS brand_supplied_field_rep_id,
                'true'::text AS is_active,
                NULL::text AS password_hash,
                created_at,
                NULL::text AS updated_at,
                NULL::text AS brand_id,
                NULL::text AS user_id,
                NULLIF(btrim(state), '') AS state,
                NULLIF(btrim(campaign_id), '') AS campaign_id,
                'campaign_campaignfieldrep'::text AS source_table
            FROM bronze.campaign_campaignfieldrep
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY lower(regexp_replace(COALESCE(NULLIF(btrim(brand_supplied_field_rep_id),''), btrim(id)), '[^a-zA-Z0-9]', '', 'g'))
                    ORDER BY
                        CASE WHEN state IS NULL OR lower(state) = 'null' THEN 1 ELSE 0 END,
                        COALESCE(NULLIF(updated_at,''), created_at) DESC NULLS LAST,
                        id DESC
                ) AS rn
            FROM unified
        )
        SELECT
            id,
            full_name,
            phone_number,
            brand_supplied_field_rep_id,
            is_active,
            password_hash,
            created_at,
            updated_at,
            brand_id,
            user_id,
            state,
            regexp_replace(COALESCE(phone_number,''), '[^0-9+]', '', 'g') AS field_rep_phone_normalized,
            NULL::text AS field_rep_email_best,
            COALESCE(NULLIF(initcap(trim(state)), ''), 'UNKNOWN') AS state_normalized,
            CASE WHEN lower(COALESCE(is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active_flag,
            created_at::text AS created_at_ts,
            updated_at::text AS updated_at_ts,
            campaign_id,
            COALESCE(NULLIF(brand_supplied_field_rep_id,''), id::text) AS source_field_rep_id,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM ranked
        WHERE rn = 1
        """
    )

    execute("DROP TABLE IF EXISTS silver.dim_doctor;")
    execute(
        """
        CREATE TABLE silver.dim_doctor AS
        SELECT
            d.id,
            d.name,
            d.phone,
            d.rep_id,
            d.source,
            regexp_replace(COALESCE(d.phone,''), '[^0-9+]', '', 'g') AS doctor_phone_normalized,
            'doctor_id'::text AS doctor_identity_source,
            md5(COALESCE(d.id, regexp_replace(COALESCE(d.phone,''), '[^0-9+]', '', 'g'))) AS doctor_identity_key,
            d.rep_id AS rep_id_normalized,
            fr.id AS field_rep_id_resolved,
            COALESCE(fr.state_normalized, 'UNKNOWN') AS state_normalized,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM bronze.doctor_viewer_doctor d
        LEFT JOIN silver.dim_field_rep fr
          ON lower(COALESCE(NULLIF(btrim(fr.source_field_rep_id),''), btrim(fr.id::text)))
           = lower(NULLIF(btrim(d.rep_id), ''))
        """
    )

    execute("DROP TABLE IF EXISTS silver.dim_collateral;")
    execute(
        """
        CREATE TABLE silver.dim_collateral AS
        SELECT
            *,
            CASE WHEN lower(COALESCE(is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active_flag,
            upload_date::text AS upload_date_ts,
            created_at::text AS created_at_ts,
            updated_at::text AS updated_at_ts,
            webinar_date::text AS webinar_date_dt,
            COALESCE(NULLIF(title,''), id) AS collateral_display_name,
            CASE WHEN COALESCE(file,'')='' AND COALESCE(vimeo_url,'')='' THEN '1' ELSE '0' END AS content_missing_flag,
            NOW()::text AS _silver_updated_at
        FROM bronze.collateral_management_collateral
        """
    )

    execute("DROP TABLE IF EXISTS silver.bridge_campaign_collateral_schedule;")
    execute(
        """
        CREATE TABLE silver.bridge_campaign_collateral_schedule AS
        SELECT
            cc.*,
            cc.start_date::text AS schedule_start_ts,
            cc.end_date::text AS schedule_end_ts,
            CASE
                WHEN cc.start_date IS NULL THEN NULL
                WHEN btrim(cc.start_date) = '' THEN NULL
                WHEN lower(btrim(cc.start_date)) = 'null' THEN NULL
                ELSE cc.start_date::date
            END AS schedule_start_date,
            CASE
                WHEN cc.end_date IS NULL THEN NULL
                WHEN btrim(cc.end_date) = '' THEN NULL
                WHEN lower(btrim(cc.end_date)) = 'null' THEN NULL
                ELSE cc.end_date::date
            END AS schedule_end_date,
            CASE WHEN cc.start_date IS NULL OR cc.end_date IS NULL THEN '1' ELSE '0' END AS schedule_missing_flag,
            cc.campaign_id AS campaign_id_resolved,
            c.type AS collateral_type,
            c.title AS collateral_title,
            NOW()::text AS _silver_updated_at
        FROM bronze.collateral_management_campaigncollateral cc
        LEFT JOIN silver.dim_collateral c ON c.id = cc.collateral_id
        """
    )

    execute("DROP TABLE IF EXISTS silver.fact_collateral_transaction;")
    execute(
        f"""
        CREATE TABLE silver.fact_collateral_transaction AS
        SELECT
            t.*,
            regexp_replace(COALESCE(t.doctor_number,''), '[^0-9+]', '', 'g') AS doctor_phone_normalized,
            md5(COALESCE(NULLIF(t.doctor_unique_id,''), NULLIF(regexp_replace(COALESCE(t.doctor_number,''), '[^0-9+]', '', 'g'),''), t.id)) AS doctor_identity_key,
            d.id AS doctor_master_id_resolved,
            CASE WHEN lower(COALESCE(t.has_viewed,'')) IN ('1','true','t','yes') THEN '1' ELSE '0' END AS has_viewed_flag,
            CASE WHEN lower(COALESCE(t.downloaded_pdf,'')) IN ('1','true','t','yes') THEN '1' ELSE '0' END AS downloaded_pdf_flag,
            CASE
                WHEN lower(COALESCE(t.video_view_gt_50,'')) IN ('1','true','t','yes') THEN '1'
                WHEN NULLIF(t.last_video_percentage,'') IS NOT NULL AND t.last_video_percentage::float >= 50 THEN '1'
                WHEN NULLIF(t.video_watch_percentage,'') IS NOT NULL AND t.video_watch_percentage::float >= 50 THEN '1'
                WHEN t.video_gt_50_at IS NOT NULL THEN '1'
                ELSE '0'
            END AS video_view_gt_50_flag,
            CASE WHEN t.last_video_percentage IS NULL OR btrim(t.last_video_percentage) = '' OR lower(btrim(t.last_video_percentage)) = 'null' THEN NULL ELSE t.last_video_percentage::float END AS last_video_percentage_num,
            CASE WHEN t.video_watch_percentage IS NULL OR btrim(t.video_watch_percentage) = '' OR lower(btrim(t.video_watch_percentage)) = 'null' THEN NULL ELSE t.video_watch_percentage::float END AS video_watch_percentage_num,
            CASE WHEN t.pdf_last_page IS NULL OR btrim(t.pdf_last_page) = '' OR lower(btrim(t.pdf_last_page)) = 'null' THEN NULL ELSE t.pdf_last_page::float END AS pdf_last_page_num,
            CASE WHEN t.pdf_total_pages IS NULL OR btrim(t.pdf_total_pages) = '' OR lower(btrim(t.pdf_total_pages)) = 'null' THEN NULL ELSE t.pdf_total_pages::float END AS pdf_total_pages_num,
            t.created_at::text AS created_at_ts,
            t.updated_at::text AS updated_at_ts,
            t.transaction_date::text AS transaction_date_ts,
            t.sent_at::text AS sent_at_ts,
            t.viewed_at::text AS viewed_at_ts,
            t.first_viewed_at::text AS first_viewed_at_ts,
            t.viewed_last_page_at::text AS viewed_last_page_at_ts,
            t.video_lt_50_at::text AS video_lt_50_at_ts,
            t.video_gt_50_at::text AS video_gt_50_at_ts,
            t.video_100_at::text AS video_100_at_ts,
            t.last_viewed_at::text AS last_viewed_at_ts,
            COALESCE(t.sent_at, t.transaction_date, t.created_at)::text AS reached_event_ts,
            COALESCE(t.first_viewed_at, t.viewed_at)::text AS opened_event_ts,
            COALESCE(t.video_gt_50_at, t.last_viewed_at, t.updated_at)::text AS video_gt_50_event_ts,
            COALESCE(t.viewed_last_page_at, t.updated_at)::text AS pdf_download_event_ts,
            NOW()::text AS _silver_updated_at,
            '{run_id}'::text AS _as_of_run_id
        FROM bronze.sharing_management_collateraltransaction t
        LEFT JOIN silver.dim_doctor d
          ON regexp_replace(COALESCE(d.phone,''), '[^0-9+]', '', 'g') = regexp_replace(COALESCE(t.doctor_number,''), '[^0-9+]', '', 'g')
        """
    )

    execute("DROP TABLE IF EXISTS silver.fact_share_log;")
    execute(
        f"""
        CREATE TABLE silver.fact_share_log AS
        SELECT
            s.*,
            regexp_replace(COALESCE(s.doctor_identifier,''), '[^0-9+]', '', 'g') AS doctor_identifier_normalized,
            md5(COALESCE(NULLIF(s.doctor_identifier,''), s.id::text)) AS doctor_identity_key,
            COALESCE(s.share_timestamp, s.created_at)::text AS reached_event_ts,
            s.share_timestamp::text AS share_timestamp_ts,
            s.created_at::text AS created_at_ts,
            s.updated_at::text AS updated_at_ts,
            NOW()::text AS _silver_updated_at,
            '{run_id}'::text AS _as_of_run_id
        FROM bronze.sharing_management_sharelog s
        """
    )

    execute("DROP TABLE IF EXISTS silver.map_brand_campaign_to_campaign;")
    execute(
        """
        CREATE TABLE silver.map_brand_campaign_to_campaign AS
        WITH campaign_ids AS (
            SELECT brand_campaign_id FROM silver.fact_collateral_transaction
            UNION
            SELECT brand_campaign_id FROM silver.fact_share_log
        )
        SELECT
            c.brand_campaign_id,
            COALESCE(MIN(coll.campaign_id), MIN(cm.id::text)) AS campaign_id_resolved,
            COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) AS distinct_campaign_id_count,
            CASE WHEN COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) <= 1 THEN 'PASS' ELSE 'FAIL' END AS _dq_status,
            CASE WHEN COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) <= 1 THEN NULL ELSE 'Campaign mapping inconsistency' END AS _dq_errors,
            NOW()::text AS _silver_updated_at
        FROM campaign_ids c
        LEFT JOIN bronze.campaign_management_campaign cm ON cm.brand_campaign_id = c.brand_campaign_id
        LEFT JOIN silver.dim_collateral coll ON coll.id IN (
            SELECT DISTINCT collateral_id FROM silver.fact_collateral_transaction t WHERE t.brand_campaign_id = c.brand_campaign_id
            UNION
            SELECT DISTINCT collateral_id FROM silver.fact_share_log s WHERE s.brand_campaign_id = c.brand_campaign_id
        )
        GROUP BY c.brand_campaign_id
        """
    )

    execute("DROP TABLE IF EXISTS silver.bridge_brand_campaign_doctor_base;")
    execute(
        """
        CREATE TABLE silver.bridge_brand_campaign_doctor_base AS
        SELECT DISTINCT
            x.brand_campaign_id,
            x.doctor_identity_key,
            d.id AS doctor_master_id_resolved,
            x.field_rep_id_resolved,
            COALESCE(fr.state_normalized, d.state_normalized, 'UNKNOWN') AS state_normalized,
            x.inclusion_reason,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM (
            SELECT
                t.brand_campaign_id,
                t.doctor_identity_key,
                t.field_rep_id AS field_rep_id_resolved,
                'OBSERVED_IN_TRANSACTION'::text AS inclusion_reason
            FROM silver.fact_collateral_transaction t
            UNION
            SELECT
                s.brand_campaign_id,
                s.doctor_identity_key,
                s.field_rep_id::text AS field_rep_id_resolved,
                'OBSERVED_IN_SHARELOG'::text AS inclusion_reason
            FROM silver.fact_share_log s
        ) x
        LEFT JOIN silver.dim_doctor d ON d.doctor_identity_key = x.doctor_identity_key
        LEFT JOIN silver.dim_field_rep fr
          ON lower(COALESCE(NULLIF(btrim(fr.source_field_rep_id),''), btrim(fr.id::text)))
           = lower(NULLIF(btrim(x.field_rep_id_resolved), ''))
        """
    )

    execute("DROP TABLE IF EXISTS silver.doctor_action_first_seen;")
    execute(
        """
        CREATE TABLE silver.doctor_action_first_seen AS
        WITH tx AS (
            SELECT
                brand_campaign_id,
                collateral_id,
                doctor_identity_key,
                MIN(NULLIF(reached_event_ts,'')) AS reached_first_tx_ts,
                MIN(NULLIF(opened_event_ts,'')) AS opened_first_ts,
                MIN(NULLIF(video_gt_50_event_ts,'')) FILTER (WHERE video_view_gt_50_flag='1') AS video_gt_50_first_ts,
                MIN(NULLIF(COALESCE(viewed_last_page_at_ts, updated_at_ts),'')) FILTER (WHERE downloaded_pdf_flag='1') AS pdf_download_first_ts,
                MAX(COALESCE(NULLIF(updated_at_ts,''), NULLIF(last_viewed_at_ts,''))) AS last_activity_tx_ts
            FROM silver.fact_collateral_transaction
            GROUP BY brand_campaign_id, collateral_id, doctor_identity_key
        ),
        share AS (
            SELECT
                brand_campaign_id,
                collateral_id,
                doctor_identity_key,
                MIN(NULLIF(reached_event_ts,'')) AS reached_first_share_ts,
                MAX(NULLIF(updated_at_ts,'')) AS last_activity_share_ts
            FROM silver.fact_share_log
            GROUP BY brand_campaign_id, collateral_id, doctor_identity_key
        ),
        keys AS (
            SELECT brand_campaign_id, collateral_id, doctor_identity_key FROM tx
            UNION
            SELECT brand_campaign_id, collateral_id, doctor_identity_key FROM share
        )
        SELECT
            k.brand_campaign_id,
            k.collateral_id,
            k.doctor_identity_key,
            COALESCE(share.reached_first_share_ts, tx.reached_first_tx_ts) AS reached_first_ts,
            tx.opened_first_ts,
            tx.video_gt_50_first_ts,
            tx.pdf_download_first_ts,
            COALESCE(tx.last_activity_tx_ts, share.last_activity_share_ts) AS last_activity_ts,
            NOW()::text AS _silver_updated_at
        FROM keys k
        LEFT JOIN tx ON tx.brand_campaign_id = k.brand_campaign_id AND tx.collateral_id = k.collateral_id AND tx.doctor_identity_key = k.doctor_identity_key
        LEFT JOIN share ON share.brand_campaign_id = k.brand_campaign_id AND share.collateral_id = k.collateral_id AND share.doctor_identity_key = k.doctor_identity_key
        """
    )
