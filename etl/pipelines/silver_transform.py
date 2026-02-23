from etl.connectors.postgres import execute


def ensure_silver_tables() -> None:
    execute("CREATE SCHEMA IF NOT EXISTS silver;")


def build_silver(run_id: str) -> None:
    ensure_silver_tables()
    execute("DROP TABLE IF EXISTS silver.dim_field_rep;")
    execute(
        """
        CREATE TABLE silver.dim_field_rep AS
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
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM bronze.campaign_fieldrep
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
        LEFT JOIN silver.dim_field_rep fr ON fr.id = d.rep_id
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
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
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
            cc.start_date::date AS schedule_start_date,
            cc.end_date::date AS schedule_end_date,
            CASE WHEN cc.start_date IS NULL OR cc.end_date IS NULL THEN '1' ELSE '0' END AS schedule_missing_flag,
            cc.campaign_id AS campaign_id_resolved,
            c.type AS collateral_type,
            c.title AS collateral_title,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
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
            NULLIF(t.last_video_percentage,'')::float AS last_video_percentage_num,
            NULLIF(t.video_watch_percentage,'')::float AS video_watch_percentage_num,
            NULLIF(t.pdf_last_page,'')::float AS pdf_last_page_num,
            NULLIF(t.pdf_total_pages,'')::float AS pdf_total_pages_num,
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
            '{run_id}'::text AS _as_of_run_id,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM bronze.sharing_management_collateraltransaction t
        LEFT JOIN silver.dim_doctor d
          ON regexp_replace(COALESCE(d.phone,''), '[^0-9+]', '', 'g') = regexp_replace(COALESCE(t.doctor_number,''), '[^0-9+]', '', 'g')
        """
    )

    execute("DROP TABLE IF EXISTS silver.map_brand_campaign_to_campaign;")
    execute(
        """
        CREATE TABLE silver.map_brand_campaign_to_campaign AS
        SELECT
            t.brand_campaign_id,
            MIN(c.campaign_id) AS campaign_id_resolved,
            COUNT(DISTINCT c.campaign_id) AS distinct_campaign_id_count,
            CASE WHEN COUNT(DISTINCT c.campaign_id)=1 THEN 'PASS' ELSE 'FAIL' END AS _dq_status,
            CASE WHEN COUNT(DISTINCT c.campaign_id)=1 THEN NULL ELSE 'Campaign mapping inconsistency' END AS _dq_errors,
            NOW()::text AS _silver_updated_at
        FROM silver.fact_collateral_transaction t
        LEFT JOIN silver.dim_collateral c ON c.id = t.collateral_id
        GROUP BY t.brand_campaign_id
        """
    )

    execute("DROP TABLE IF EXISTS silver.bridge_brand_campaign_doctor_base;")
    execute(
        """
        CREATE TABLE silver.bridge_brand_campaign_doctor_base AS
        SELECT DISTINCT
            t.brand_campaign_id,
            t.doctor_identity_key,
            t.doctor_master_id_resolved,
            t.field_rep_id AS field_rep_id_resolved,
            COALESCE(fr.state_normalized, 'UNKNOWN') AS state_normalized,
            CASE WHEN d.id IS NOT NULL THEN 'MASTER_BY_REP' ELSE 'OBSERVED_IN_TRANSACTION' END AS inclusion_reason,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM silver.fact_collateral_transaction t
        LEFT JOIN silver.dim_doctor d ON d.doctor_identity_key = t.doctor_identity_key
        LEFT JOIN silver.dim_field_rep fr ON fr.id = t.field_rep_id
        """
    )

    execute("DROP TABLE IF EXISTS silver.doctor_action_first_seen;")
    execute(
        """
        CREATE TABLE silver.doctor_action_first_seen AS
        SELECT
            brand_campaign_id,
            collateral_id,
            doctor_identity_key,
            MIN(NULLIF(reached_event_ts,'')) AS reached_first_ts,
            MIN(NULLIF(opened_event_ts,'')) AS opened_first_ts,
            MIN(NULLIF(video_gt_50_event_ts,'')) FILTER (WHERE video_view_gt_50_flag='1') AS video_gt_50_first_ts,
            MIN(NULLIF(COALESCE(viewed_last_page_at_ts, updated_at_ts),'')) FILTER (WHERE downloaded_pdf_flag='1') AS pdf_download_first_ts,
            MAX(COALESCE(NULLIF(updated_at_ts,''), NULLIF(last_viewed_at_ts,''))) AS last_activity_ts,
            NOW()::text AS _silver_updated_at
        FROM silver.fact_collateral_transaction
        GROUP BY brand_campaign_id, collateral_id, doctor_identity_key
        """
    )
