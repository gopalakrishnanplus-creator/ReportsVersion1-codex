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
                cfr.id::text AS id,
                COALESCE(
                    NULLIF(btrim(cfr.full_name), ''),
                    NULLIF(btrim(concat_ws(' ', NULLIF(au.first_name, ''), NULLIF(au.last_name, ''))), ''),
                    NULLIF(btrim(au.username), '')
                ) AS full_name,
                NULLIF(btrim(cfr.phone_number), '') AS phone_number,
                NULLIF(btrim(cfr.brand_supplied_field_rep_id), '') AS brand_supplied_field_rep_id,
                CASE WHEN lower(COALESCE(cfr.is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active,
                NULLIF(btrim(cfr.password_hash), '') AS password_hash,
                cfr.created_at,
                cfr.updated_at,
                cfr.brand_id,
                cfr.user_id,
                NULLIF(btrim(cfr.state), '') AS state,
                NULLIF(btrim(au.email), '') AS email,
                NULL::text AS campaign_id,
                'campaign_fieldrep'::text AS source_table,
                COALESCE(
                    NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''),
                    NULLIF(btrim(cfr.id::text), ''),
                    NULLIF(btrim(au.username), '')
                ) AS source_field_rep_id
            FROM bronze.campaign_fieldrep cfr
            LEFT JOIN bronze.auth_user au
              ON au.id::text = cfr.user_id::text
            UNION ALL
            SELECT
                ccf.field_rep_id::text AS id,
                COALESCE(
                    NULLIF(btrim(cfr.full_name), ''),
                    NULLIF(btrim(concat_ws(' ', NULLIF(au.first_name, ''), NULLIF(au.last_name, ''))), ''),
                    NULLIF(btrim(au.username), '')
                ) AS full_name,
                NULLIF(btrim(cfr.phone_number), '') AS phone_number,
                COALESCE(NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''), NULLIF(btrim(ccf.field_rep_id), '')) AS brand_supplied_field_rep_id,
                'true'::text AS is_active,
                NULLIF(btrim(cfr.password_hash), '') AS password_hash,
                ccf.created_at,
                cfr.updated_at,
                cfr.brand_id,
                cfr.user_id,
                NULLIF(btrim(cfr.state), '') AS state,
                NULLIF(btrim(au.email), '') AS email,
                NULLIF(btrim(ccf.campaign_id), '') AS campaign_id,
                'campaign_campaignfieldrep'::text AS source_table,
                COALESCE(NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''), NULLIF(btrim(ccf.field_rep_id), '')) AS source_field_rep_id
            FROM bronze.campaign_campaignfieldrep ccf
            LEFT JOIN bronze.campaign_fieldrep cfr
              ON cfr.id::text = ccf.field_rep_id::text
            LEFT JOIN bronze.auth_user au
              ON au.id::text = cfr.user_id::text
            UNION ALL
            SELECT
                u.id::text AS id,
                COALESCE(
                    NULLIF(btrim(concat_ws(' ', NULLIF(u.first_name, ''), NULLIF(u.last_name, ''))), ''),
                    NULLIF(btrim(u.username), ''),
                    NULLIF(btrim(u.email), '')
                ) AS full_name,
                NULLIF(btrim(u.phone_number), '') AS phone_number,
                NULLIF(btrim(u.field_id), '') AS brand_supplied_field_rep_id,
                CASE
                    WHEN lower(COALESCE(u.is_active, u.active, '')) IN ('1','true','t','yes') THEN 'true'
                    ELSE 'false'
                END AS is_active,
                NULL::text AS password_hash,
                u.date_joined AS created_at,
                u.last_login AS updated_at,
                NULL::text AS brand_id,
                u.id::text AS user_id,
                NULL::text AS state,
                NULLIF(btrim(u.email), '') AS email,
                NULL::text AS campaign_id,
                'user_management_user'::text AS source_table,
                COALESCE(NULLIF(btrim(u.field_id), ''), NULLIF(btrim(u.id::text), ''), NULLIF(btrim(u.email), ''), NULLIF(btrim(u.username), '')) AS source_field_rep_id
            FROM bronze.user_management_user u
            UNION ALL
            SELECT
                sfr.id::text AS id,
                COALESCE(NULLIF(btrim(sfr.email), ''), NULLIF(btrim(sfr.gmail), ''), NULLIF(btrim(sfr.field_id), '')) AS full_name,
                NULLIF(btrim(sfr.whatsapp_number), '') AS phone_number,
                NULLIF(btrim(sfr.field_id), '') AS brand_supplied_field_rep_id,
                CASE WHEN lower(COALESCE(sfr.is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active,
                NULL::text AS password_hash,
                sfr.created_at,
                sfr.updated_at,
                NULL::text AS brand_id,
                NULL::text AS user_id,
                NULL::text AS state,
                COALESCE(NULLIF(btrim(sfr.email), ''), NULLIF(btrim(sfr.gmail), '')) AS email,
                NULL::text AS campaign_id,
                'sharing_management_fieldrepresentative'::text AS source_table,
                COALESCE(
                    NULLIF(btrim(sfr.field_id), ''),
                    NULLIF(btrim(sfr.id::text), ''),
                    NULLIF(btrim(sfr.email), ''),
                    NULLIF(btrim(sfr.gmail), ''),
                    NULLIF(btrim(sfr.whatsapp_number), '')
                ) AS source_field_rep_id
            FROM bronze.sharing_management_fieldrepresentative sfr
            UNION ALL
            SELECT
                au.id::text AS id,
                COALESCE(
                    NULLIF(btrim(concat_ws(' ', NULLIF(au.first_name, ''), NULLIF(au.last_name, ''))), ''),
                    NULLIF(btrim(au.username), ''),
                    NULLIF(btrim(au.email), '')
                ) AS full_name,
                NULL::text AS phone_number,
                NULLIF(btrim(au.username), '') AS brand_supplied_field_rep_id,
                CASE WHEN lower(COALESCE(au.is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active,
                NULL::text AS password_hash,
                au.date_joined AS created_at,
                au.last_login AS updated_at,
                NULL::text AS brand_id,
                au.id::text AS user_id,
                NULL::text AS state,
                NULLIF(btrim(au.email), '') AS email,
                NULL::text AS campaign_id,
                'auth_user'::text AS source_table,
                COALESCE(NULLIF(btrim(au.username), ''), NULLIF(btrim(au.id::text), ''), NULLIF(btrim(au.email), '')) AS source_field_rep_id
            FROM bronze.auth_user au
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY lower(regexp_replace(COALESCE(NULLIF(btrim(source_field_rep_id),''), NULLIF(btrim(brand_supplied_field_rep_id),''), btrim(id)), '[^a-zA-Z0-9]', '', 'g'))
                    ORDER BY
                        CASE WHEN state IS NULL OR lower(state) = 'null' THEN 1 ELSE 0 END,
                        CASE source_table
                            WHEN 'campaign_fieldrep' THEN 0
                            WHEN 'campaign_campaignfieldrep' THEN 1
                            WHEN 'sharing_management_fieldrepresentative' THEN 2
                            WHEN 'user_management_user' THEN 3
                            ELSE 4
                        END,
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
            email AS field_rep_email_best,
            COALESCE(NULLIF(initcap(trim(state)), ''), 'UNKNOWN') AS state_normalized,
            CASE WHEN lower(COALESCE(is_active,'')) IN ('1','true','t','yes') THEN 'true' ELSE 'false' END AS is_active_flag,
            created_at::text AS created_at_ts,
            updated_at::text AS updated_at_ts,
            campaign_id,
            source_table,
            COALESCE(NULLIF(source_field_rep_id,''), NULLIF(brand_supplied_field_rep_id,''), id::text) AS source_field_rep_id,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM ranked
        WHERE rn = 1
        """
    )

    execute("DROP TABLE IF EXISTS silver.map_field_rep_identity;")
    execute(
        """
        CREATE TABLE silver.map_field_rep_identity AS
        WITH master_reps AS (
            SELECT
                cfr.id::text AS canonical_field_rep_id,
                COALESCE(NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''), cfr.id::text) AS field_rep_display_id,
                COALESCE(
                    NULLIF(btrim(cfr.full_name), ''),
                    NULLIF(btrim(concat_ws(' ', NULLIF(au.first_name, ''), NULLIF(au.last_name, ''))), ''),
                    NULLIF(btrim(au.username), ''),
                    NULLIF(btrim(cfr.brand_supplied_field_rep_id), ''),
                    cfr.id::text
                ) AS field_rep_name,
                NULLIF(btrim(cfr.brand_supplied_field_rep_id), '') AS brand_supplied_field_rep_id,
                NULLIF(btrim(cfr.user_id), '') AS auth_user_id,
                NULLIF(btrim(au.email), '') AS auth_email,
                NULLIF(btrim(au.username), '') AS auth_username,
                COALESCE(NULLIF(initcap(btrim(cfr.state)), ''), 'UNKNOWN') AS state_normalized,
                cfr.updated_at,
                cfr.created_at
            FROM bronze.campaign_fieldrep cfr
            LEFT JOIN bronze.auth_user au
              ON au.id::text = cfr.user_id::text
        ),
        strict_local_users AS (
            SELECT DISTINCT
                mr.canonical_field_rep_id,
                uu.id::text AS local_user_id,
                NULLIF(btrim(uu.field_id), '') AS local_field_id,
                NULLIF(btrim(uu.email), '') AS local_email,
                NULLIF(btrim(uu.username), '') AS local_username
            FROM master_reps mr
            JOIN bronze.user_management_user uu
              ON (
                  mr.brand_supplied_field_rep_id IS NOT NULL
                  AND lower(regexp_replace(NULLIF(btrim(uu.field_id), ''), '[^a-zA-Z0-9]', '', 'g'))
                    = lower(regexp_replace(mr.brand_supplied_field_rep_id, '[^a-zA-Z0-9]', '', 'g'))
              )
              OR (
                  mr.auth_email IS NOT NULL
                  AND lower(regexp_replace(NULLIF(btrim(uu.email), ''), '[^a-zA-Z0-9]', '', 'g'))
                    = lower(regexp_replace(mr.auth_email, '[^a-zA-Z0-9]', '', 'g'))
              )
              OR (
                  mr.auth_username IS NOT NULL
                  AND lower(regexp_replace(NULLIF(btrim(uu.username), ''), '[^a-zA-Z0-9]', '', 'g'))
                    = lower(regexp_replace(mr.auth_username, '[^a-zA-Z0-9]', '', 'g'))
              )
        ),
        strict_legacy_reps AS (
            SELECT DISTINCT
                mr.canonical_field_rep_id,
                sfr.id::text AS legacy_rep_id,
                NULLIF(btrim(sfr.field_id), '') AS legacy_field_id,
                NULLIF(btrim(sfr.email), '') AS legacy_email,
                NULLIF(btrim(sfr.gmail), '') AS legacy_gmail,
                NULLIF(btrim(sfr.whatsapp_number), '') AS legacy_whatsapp
            FROM master_reps mr
            JOIN bronze.sharing_management_fieldrepresentative sfr
              ON (
                  mr.brand_supplied_field_rep_id IS NOT NULL
                  AND lower(regexp_replace(NULLIF(btrim(sfr.field_id), ''), '[^a-zA-Z0-9]', '', 'g'))
                    = lower(regexp_replace(mr.brand_supplied_field_rep_id, '[^a-zA-Z0-9]', '', 'g'))
              )
              OR (
                  mr.auth_email IS NOT NULL
                  AND (
                    lower(regexp_replace(NULLIF(btrim(sfr.email), ''), '[^a-zA-Z0-9]', '', 'g'))
                      = lower(regexp_replace(mr.auth_email, '[^a-zA-Z0-9]', '', 'g'))
                    OR lower(regexp_replace(NULLIF(btrim(sfr.gmail), ''), '[^a-zA-Z0-9]', '', 'g'))
                      = lower(regexp_replace(mr.auth_email, '[^a-zA-Z0-9]', '', 'g'))
                  )
              )
        ),
        alias_rows AS (
            SELECT canonical_field_rep_id, field_rep_display_id, field_rep_name, brand_supplied_field_rep_id, state_normalized, 'campaign_fieldrep_id'::text AS alias_type, canonical_field_rep_id AS alias_value, 80 AS match_rank FROM master_reps
            UNION ALL SELECT canonical_field_rep_id, field_rep_display_id, field_rep_name, brand_supplied_field_rep_id, state_normalized, 'brand_field_id', brand_supplied_field_rep_id, 20 FROM master_reps WHERE brand_supplied_field_rep_id IS NOT NULL
            UNION ALL SELECT canonical_field_rep_id, field_rep_display_id, field_rep_name, brand_supplied_field_rep_id, state_normalized, 'auth_user_id', auth_user_id, 70 FROM master_reps WHERE auth_user_id IS NOT NULL
            UNION ALL SELECT canonical_field_rep_id, field_rep_display_id, field_rep_name, brand_supplied_field_rep_id, state_normalized, 'auth_email', auth_email, 10 FROM master_reps WHERE auth_email IS NOT NULL
            UNION ALL SELECT canonical_field_rep_id, field_rep_display_id, field_rep_name, brand_supplied_field_rep_id, state_normalized, 'auth_username', auth_username, 30 FROM master_reps WHERE auth_username IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'local_user_id', slu.local_user_id, 40 FROM strict_local_users slu JOIN master_reps mr ON mr.canonical_field_rep_id = slu.canonical_field_rep_id WHERE slu.local_user_id IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'local_field_id', slu.local_field_id, 20 FROM strict_local_users slu JOIN master_reps mr ON mr.canonical_field_rep_id = slu.canonical_field_rep_id WHERE slu.local_field_id IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'local_email', slu.local_email, 10 FROM strict_local_users slu JOIN master_reps mr ON mr.canonical_field_rep_id = slu.canonical_field_rep_id WHERE slu.local_email IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'local_username', slu.local_username, 30 FROM strict_local_users slu JOIN master_reps mr ON mr.canonical_field_rep_id = slu.canonical_field_rep_id WHERE slu.local_username IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'legacy_rep_id', slr.legacy_rep_id, 50 FROM strict_legacy_reps slr JOIN master_reps mr ON mr.canonical_field_rep_id = slr.canonical_field_rep_id WHERE slr.legacy_rep_id IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'legacy_field_id', slr.legacy_field_id, 20 FROM strict_legacy_reps slr JOIN master_reps mr ON mr.canonical_field_rep_id = slr.canonical_field_rep_id WHERE slr.legacy_field_id IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'legacy_email', slr.legacy_email, 10 FROM strict_legacy_reps slr JOIN master_reps mr ON mr.canonical_field_rep_id = slr.canonical_field_rep_id WHERE slr.legacy_email IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'legacy_gmail', slr.legacy_gmail, 10 FROM strict_legacy_reps slr JOIN master_reps mr ON mr.canonical_field_rep_id = slr.canonical_field_rep_id WHERE slr.legacy_gmail IS NOT NULL
            UNION ALL SELECT mr.canonical_field_rep_id, mr.field_rep_display_id, mr.field_rep_name, mr.brand_supplied_field_rep_id, mr.state_normalized, 'legacy_whatsapp', slr.legacy_whatsapp, 60 FROM strict_legacy_reps slr JOIN master_reps mr ON mr.canonical_field_rep_id = slr.canonical_field_rep_id WHERE slr.legacy_whatsapp IS NOT NULL
        ),
        normalized_alias_rows AS (
            SELECT
                canonical_field_rep_id,
                field_rep_display_id,
                field_rep_name,
                brand_supplied_field_rep_id,
                state_normalized,
                alias_type,
                alias_value,
                lower(regexp_replace(NULLIF(btrim(alias_value), ''), '[^a-zA-Z0-9]', '', 'g')) AS alias_key,
                match_rank
            FROM alias_rows
            WHERE NULLIF(btrim(alias_value), '') IS NOT NULL
        )
        SELECT DISTINCT ON (alias_type, alias_key, canonical_field_rep_id)
            canonical_field_rep_id,
            field_rep_display_id,
            field_rep_name,
            brand_supplied_field_rep_id,
            state_normalized,
            alias_type,
            alias_value,
            alias_key,
            match_rank,
            NOW()::text AS _silver_updated_at
        FROM normalized_alias_rows
        ORDER BY alias_type, alias_key, canonical_field_rep_id, match_rank
        """
    )

    execute("DROP TABLE IF EXISTS silver.dim_doctor;")
    execute(
        """
        CREATE TABLE silver.dim_doctor AS
        WITH unified AS (
            SELECT
                d.id::text AS id,
                NULLIF(btrim(d.name), '') AS name,
                NULLIF(btrim(d.phone), '') AS phone,
                NULLIF(btrim(d.rep_id), '') AS rep_id,
                NULLIF(btrim(d.source), '') AS source,
                'doctor_viewer_doctor'::text AS source_table
            FROM bronze.doctor_viewer_doctor d
        ),
        normalized AS (
            SELECT
                *,
                regexp_replace(COALESCE(phone,''), '[^0-9+]', '', 'g') AS doctor_phone_normalized,
                lower(regexp_replace(COALESCE(rep_id,''), '[^a-zA-Z0-9]', '', 'g')) AS rep_key
            FROM unified
        )
        SELECT
            n.id,
            n.name,
            n.phone,
            n.rep_id,
            n.source,
            n.doctor_phone_normalized,
            CASE WHEN NULLIF(n.doctor_phone_normalized, '') IS NOT NULL THEN 'phone' ELSE n.source_table END AS doctor_identity_source,
            md5(COALESCE(NULLIF(n.doctor_phone_normalized, ''), n.source_table || ':' || COALESCE(n.id, ''))) AS doctor_identity_key,
            n.rep_id AS rep_id_normalized,
            mi.canonical_field_rep_id AS field_rep_id_resolved,
            COALESCE(mi.state_normalized, 'UNKNOWN') AS state_normalized,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM normalized n
        LEFT JOIN LATERAL (
            SELECT
                mi.canonical_field_rep_id,
                mi.state_normalized,
                mi.match_rank
            FROM silver.map_field_rep_identity mi
            WHERE n.rep_key <> ''
              AND mi.alias_key = n.rep_key
              AND mi.alias_type IN ('local_user_id', 'local_field_id', 'legacy_rep_id', 'legacy_field_id', 'brand_field_id')
            ORDER BY
                mi.match_rank,
                mi.canonical_field_rep_id
            LIMIT 1
        ) mi ON TRUE
        """
    )

    execute("DROP TABLE IF EXISTS silver.dim_prefilled_doctor;")
    execute(
        """
        CREATE TABLE silver.dim_prefilled_doctor AS
        SELECT
            id,
            NULLIF(btrim(full_name), '') AS full_name,
            NULLIF(btrim(email), '') AS email,
            NULLIF(btrim(phone), '') AS phone,
            regexp_replace(COALESCE(phone,''), '[^0-9+]', '', 'g') AS doctor_phone_normalized,
            NULLIF(btrim(specialty), '') AS specialty,
            NULLIF(btrim(city), '') AS city,
            NOW()::text AS _silver_updated_at
        FROM bronze.prefilled_doctor
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

    execute("DROP TABLE IF EXISTS silver.fact_share_log;")
    execute(
        f"""
        CREATE TABLE silver.fact_share_log AS
        SELECT
            s.*,
            regexp_replace(COALESCE(s.doctor_identifier,''), '[^0-9+]', '', 'g') AS doctor_identifier_normalized,
            md5(COALESCE(NULLIF(regexp_replace(COALESCE(s.doctor_identifier,''), '[^0-9+]', '', 'g'),''), NULLIF(s.doctor_identifier,''), s.id::text)) AS doctor_identity_key,
            COALESCE(s.share_timestamp, s.created_at)::text AS reached_event_ts,
            s.share_timestamp::text AS share_timestamp_ts,
            s.created_at::text AS created_at_ts,
            s.updated_at::text AS updated_at_ts,
            NOW()::text AS _silver_updated_at,
            '{run_id}'::text AS _as_of_run_id
        FROM bronze.sharing_management_sharelog s
        """
    )

    execute("DROP TABLE IF EXISTS silver.fact_collateral_transaction;")
    execute(
        f"""
        CREATE TABLE silver.fact_collateral_transaction AS
        WITH collateral_campaign AS (
            SELECT
                cc.collateral_id,
                MIN(NULLIF(btrim(cm.brand_campaign_id), '')) AS resolved_brand_campaign_id,
                COUNT(DISTINCT NULLIF(btrim(cm.brand_campaign_id), '')) AS brand_campaign_count
            FROM silver.bridge_campaign_collateral_schedule cc
            LEFT JOIN bronze.campaign_management_campaign cm
              ON cm.id::text = cc.campaign_id_resolved
            GROUP BY cc.collateral_id
        ),
        normalized AS (
            SELECT
                t.*,
                NULLIF(regexp_replace(COALESCE(t.doctor_number,''), '[^0-9+]', '', 'g'), '') AS doctor_phone_normalized_value,
                COALESCE(
                    NULLIF(btrim(t.transaction_date), ''),
                    NULLIF(btrim(t.sent_at), ''),
                    NULLIF(btrim(t.created_at), ''),
                    NULLIF(btrim(t.updated_at), ''),
                    NULLIF(btrim(t.id), '')
                ) AS transaction_time_value,
                COALESCE(
                    NULLIF(btrim(t.brand_campaign_id), ''),
                    NULLIF(btrim(linked_share.brand_campaign_id), ''),
                    CASE WHEN cc.brand_campaign_count = 1 THEN cc.resolved_brand_campaign_id ELSE NULL END
                ) AS brand_campaign_id_resolved_value,
                COALESCE(
                    rep_from_txid.brand_supplied_field_rep_id,
                    rep_from_email.brand_supplied_field_rep_id,
                    rep_from_id.brand_supplied_field_rep_id,
                    NULLIF(btrim(t.field_rep_unique_id), ''),
                    NULLIF(btrim(t.field_rep_id), '')
                ) AS brand_supplied_field_rep_id_resolved,
                COALESCE(
                    rep_from_txid.canonical_field_rep_id,
                    rep_from_email.canonical_field_rep_id,
                    rep_from_id.canonical_field_rep_id
                ) AS field_rep_master_id_resolved
            FROM bronze.sharing_management_collateraltransaction t
            LEFT JOIN LATERAL (
                SELECT s.*
                FROM silver.fact_share_log s
                WHERE NULLIF(btrim(COALESCE(t.sm_engagement_id, t.share_management_engagement_id)), '') IS NOT NULL
                  AND s.id::text = COALESCE(NULLIF(btrim(t.sm_engagement_id), ''), NULLIF(btrim(t.share_management_engagement_id), ''))
                ORDER BY COALESCE(s.updated_at_ts, s.created_at_ts, s.share_timestamp_ts) DESC NULLS LAST, s.id DESC
                LIMIT 1
            ) linked_share ON TRUE
            LEFT JOIN collateral_campaign cc
              ON cc.collateral_id::text = t.collateral_id::text
            LEFT JOIN LATERAL (
                SELECT mi.canonical_field_rep_id, mi.brand_supplied_field_rep_id
                FROM silver.map_field_rep_identity mi
                WHERE mi.alias_key = lower(regexp_replace(NULLIF(btrim(split_part(t.transaction_id, '-', 1)), ''), '[^a-zA-Z0-9]', '', 'g'))
                  AND mi.alias_type IN ('brand_field_id', 'local_field_id', 'legacy_field_id')
                ORDER BY mi.match_rank, mi.canonical_field_rep_id
                LIMIT 1
            ) rep_from_txid ON TRUE
            LEFT JOIN LATERAL (
                SELECT mi.canonical_field_rep_id, mi.brand_supplied_field_rep_id
                FROM silver.map_field_rep_identity mi
                WHERE mi.alias_key = lower(regexp_replace(NULLIF(btrim(t.field_rep_email), ''), '[^a-zA-Z0-9]', '', 'g'))
                  AND mi.alias_type IN ('local_email', 'auth_email', 'legacy_email', 'legacy_gmail')
                ORDER BY mi.match_rank, mi.canonical_field_rep_id
                LIMIT 1
            ) rep_from_email ON TRUE
            LEFT JOIN LATERAL (
                SELECT mi.canonical_field_rep_id, mi.brand_supplied_field_rep_id
                FROM silver.map_field_rep_identity mi
                WHERE mi.alias_key = lower(regexp_replace(NULLIF(btrim(t.field_rep_id), ''), '[^a-zA-Z0-9]', '', 'g'))
                  AND mi.alias_type IN ('local_user_id', 'brand_field_id', 'legacy_rep_id', 'legacy_field_id')
                ORDER BY mi.match_rank, mi.canonical_field_rep_id
                LIMIT 1
            ) rep_from_id ON TRUE
        )
        SELECT
            t.id,
            COALESCE(
                NULLIF(btrim(t.transaction_id), ''),
                NULLIF(
                    concat_ws(
                        '-',
                        NULLIF(btrim(t.brand_supplied_field_rep_id_resolved), ''),
                        t.doctor_phone_normalized_value,
                        NULLIF(btrim(t.collateral_id), ''),
                        NULLIF(regexp_replace(COALESCE(t.transaction_time_value, ''), '[^0-9A-Za-z]+', '', 'g'), '')
                    ),
                    ''
                ),
                t.id
            ) AS transaction_id,
            NULLIF(btrim(t.transaction_id), '') AS source_transaction_id,
            t.brand_campaign_id_resolved_value AS brand_campaign_id,
            t.field_rep_id,
            t.field_rep_unique_id,
            t.doctor_name,
            t.doctor_number,
            t.doctor_unique_id,
            t.collateral_id,
            t.transaction_date,
            t.has_viewed,
            t.downloaded_pdf,
            t.pdf_completed,
            t.video_view_lt_50,
            t.video_view_gt_50,
            t.video_completed,
            t.pdf_total_pages,
            t.last_video_percentage,
            t.pdf_last_page,
            t.doctor_viewer_engagement_id,
            t.share_management_engagement_id,
            t.video_tracking_last_event_id,
            t.created_at,
            t.updated_at,
            t.sent_at,
            t.viewed_at,
            t.first_viewed_at,
            t.viewed_last_page_at,
            t.video_lt_50_at,
            t.video_gt_50_at,
            t.video_100_at,
            t.last_viewed_at,
            t.dv_engagement_id,
            t.field_rep_email,
            t.share_channel,
            t.sm_engagement_id,
            t.video_watch_percentage,
            t._ingestion_run_id,
            t._ingested_at,
            t._source_server,
            t._source_table,
            t._extract_started_at,
            t._extract_ended_at,
            t._record_hash,
            t._is_deleted,
            t._dq_status,
            t._dq_errors,
            t.doctor_phone_normalized_value AS doctor_phone_normalized,
            md5(COALESCE(NULLIF(t.doctor_unique_id,''), t.doctor_phone_normalized_value, t.id)) AS doctor_identity_key,
            md5(
                COALESCE(
                    NULLIF(btrim(t.transaction_id), ''),
                    NULLIF(
                        concat_ws(
                            '-',
                            NULLIF(btrim(t.brand_supplied_field_rep_id_resolved), ''),
                            t.doctor_phone_normalized_value,
                            NULLIF(btrim(t.collateral_id), ''),
                            NULLIF(regexp_replace(COALESCE(t.transaction_time_value, ''), '[^0-9A-Za-z]+', '', 'g'), '')
                        ),
                        ''
                    ),
                    t.id
                )
            ) AS transaction_identity_key,
            d.id AS doctor_master_id_resolved,
            t.field_rep_master_id_resolved,
            t.brand_supplied_field_rep_id_resolved,
            CASE WHEN lower(COALESCE(t.has_viewed,'')) IN ('1','true','t','yes') THEN '1' ELSE '0' END AS has_viewed_flag,
            CASE WHEN lower(COALESCE(t.downloaded_pdf,'')) IN ('1','true','t','yes') THEN '1' ELSE '0' END AS downloaded_pdf_flag,
            CASE WHEN lower(COALESCE(t.pdf_completed,'')) IN ('1','true','t','yes') THEN '1' ELSE '0' END AS pdf_completed_flag,
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
            CASE
                WHEN lower(COALESCE(t.downloaded_pdf,'')) IN ('1','true','t','yes')
                  OR lower(COALESCE(t.pdf_completed,'')) IN ('1','true','t','yes')
                  OR t.viewed_last_page_at IS NOT NULL
                THEN COALESCE(t.viewed_last_page_at, t.updated_at)
                ELSE NULL
            END::text AS pdf_download_event_ts,
            NOW()::text AS _silver_updated_at,
            '{run_id}'::text AS _as_of_run_id
        FROM normalized t
        LEFT JOIN silver.dim_doctor d
          ON d.doctor_phone_normalized = t.doctor_phone_normalized_value
        """
    )

    execute("DROP TABLE IF EXISTS silver.map_brand_campaign_to_campaign;")
    execute(
        """
        CREATE TABLE silver.map_brand_campaign_to_campaign AS
        WITH campaign_ids AS (
            SELECT brand_campaign_id FROM silver.fact_collateral_transaction WHERE COALESCE(NULLIF(btrim(brand_campaign_id), ''), '') <> ''
            UNION
            SELECT brand_campaign_id FROM silver.fact_share_log WHERE COALESCE(NULLIF(btrim(brand_campaign_id), ''), '') <> ''
        )
        SELECT
            c.brand_campaign_id,
            COALESCE(MIN(coll.campaign_id), MIN(cm.id::text)) AS campaign_id_resolved,
            COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) AS distinct_campaign_id_count,
            CASE WHEN COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) <= 1 THEN 'PASS' ELSE 'FAIL' END AS _dq_status,
            CASE WHEN COUNT(DISTINCT COALESCE(cm.id::text, coll.campaign_id)) <= 1 THEN NULL ELSE 'Campaign mapping inconsistency' END AS _dq_errors,
            NOW()::text AS _silver_updated_at
        FROM campaign_ids c
        LEFT JOIN bronze.campaign_management_campaign cm
          ON lower(regexp_replace(COALESCE(NULLIF(btrim(cm.brand_campaign_id), ''), ''), '[^a-zA-Z0-9]', '', 'g'))
           = lower(regexp_replace(COALESCE(NULLIF(btrim(c.brand_campaign_id), ''), ''), '[^a-zA-Z0-9]', '', 'g'))
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
        WITH campaign_rep_aliases AS (
            SELECT DISTINCT
                m.brand_campaign_id,
                ccf.field_rep_id::text AS master_field_rep_id,
                mi.alias_key AS rep_key,
                COALESCE(mi.state_normalized, NULLIF(initcap(btrim(cfr.state)), ''), 'UNKNOWN') AS state_normalized
            FROM silver.map_brand_campaign_to_campaign m
            JOIN bronze.campaign_campaignfieldrep ccf
              ON lower(regexp_replace(NULLIF(btrim(ccf.campaign_id), ''), '[^a-zA-Z0-9]', '', 'g')) IN (
                    lower(regexp_replace(COALESCE(NULLIF(btrim(m.campaign_id_resolved), ''), ''), '[^a-zA-Z0-9]', '', 'g')),
                    lower(regexp_replace(COALESCE(NULLIF(btrim(m.brand_campaign_id), ''), ''), '[^a-zA-Z0-9]', '', 'g'))
                 )
            LEFT JOIN bronze.campaign_fieldrep cfr
              ON cfr.id::text = ccf.field_rep_id::text
            JOIN silver.map_field_rep_identity mi
              ON mi.canonical_field_rep_id = ccf.field_rep_id::text
             AND mi.alias_type IN ('local_user_id', 'local_field_id', 'brand_field_id', 'auth_email', 'auth_username', 'local_email', 'local_username', 'legacy_rep_id', 'legacy_field_id', 'legacy_email', 'legacy_gmail', 'legacy_whatsapp')
            WHERE mi.alias_key <> ''
            UNION ALL
            SELECT DISTINCT
                m.brand_campaign_id,
                ca.field_rep_id::text AS master_field_rep_id,
                lower(regexp_replace(NULLIF(btrim(alias_value), ''), '[^a-zA-Z0-9]', '', 'g')) AS rep_key,
                'UNKNOWN'::text AS state_normalized
            FROM silver.map_brand_campaign_to_campaign m
            JOIN bronze.campaign_management_campaignassignment ca
              ON NULLIF(btrim(ca.campaign_id), '') = NULLIF(btrim(m.campaign_id_resolved), '')
            LEFT JOIN bronze.user_management_user uu
              ON uu.id::text = ca.field_rep_id::text
            CROSS JOIN LATERAL (
                VALUES
                    (ca.field_rep_id::text),
                    (uu.id::text),
                    (uu.field_id),
                    (uu.username),
                    (uu.email)
            ) aliases(alias_value)
            WHERE NULLIF(btrim(alias_value), '') IS NOT NULL
            UNION ALL
            SELECT DISTINCT
                m.brand_campaign_id,
                afc.field_rep_id::text AS master_field_rep_id,
                lower(regexp_replace(NULLIF(btrim(alias_value), ''), '[^a-zA-Z0-9]', '', 'g')) AS rep_key,
                'UNKNOWN'::text AS state_normalized
            FROM silver.map_brand_campaign_to_campaign m
            JOIN bronze.admin_dashboard_fieldrepcampaign afc
              ON NULLIF(btrim(afc.campaign_id), '') = NULLIF(btrim(m.campaign_id_resolved), '')
            LEFT JOIN bronze.user_management_user uu
              ON uu.id::text = afc.field_rep_id::text
            CROSS JOIN LATERAL (
                VALUES
                    (afc.field_rep_id::text),
                    (uu.id::text),
                    (uu.field_id),
                    (uu.username),
                    (uu.email)
            ) aliases(alias_value)
            WHERE NULLIF(btrim(alias_value), '') IS NOT NULL
        ),
        campaign_rep_state AS (
            SELECT DISTINCT brand_campaign_id, rep_key, state_normalized
            FROM campaign_rep_aliases
        )
        SELECT DISTINCT
            x.brand_campaign_id,
            x.doctor_identity_key,
            d.id AS doctor_master_id_resolved,
            x.field_rep_id_resolved,
            COALESCE(
                crs.state_normalized,
                fr.state_normalized,
                d.state_normalized,
                'UNKNOWN'
            ) AS state_normalized,
            x.inclusion_reason,
            NOW()::text AS _silver_updated_at,
            'PASS'::text AS _dq_status,
            NULL::text AS _dq_errors
        FROM (
            SELECT
                cra.brand_campaign_id,
                d.doctor_identity_key,
                cra.master_field_rep_id AS field_rep_id_resolved,
                'ASSIGNED_TO_CAMPAIGN_REP'::text AS inclusion_reason
            FROM campaign_rep_aliases cra
            JOIN silver.dim_doctor d
              ON d.field_rep_id_resolved = cra.master_field_rep_id
              OR lower(regexp_replace(NULLIF(btrim(d.rep_id_normalized), ''), '[^a-zA-Z0-9]', '', 'g')) = cra.rep_key
            UNION
            SELECT
                t.brand_campaign_id,
                t.doctor_identity_key,
                COALESCE(t.field_rep_master_id_resolved, t.field_rep_id) AS field_rep_id_resolved,
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
        LEFT JOIN silver.dim_doctor d
          ON d.doctor_identity_key = x.doctor_identity_key
        LEFT JOIN silver.dim_field_rep fr
          ON lower(regexp_replace(COALESCE(NULLIF(btrim(fr.source_field_rep_id),''), btrim(fr.id::text)), '[^a-zA-Z0-9]', '', 'g'))
           = lower(regexp_replace(NULLIF(btrim(x.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
        LEFT JOIN campaign_rep_state crs
          ON crs.brand_campaign_id = x.brand_campaign_id
         AND crs.rep_key = lower(regexp_replace(NULLIF(btrim(x.field_rep_id_resolved), ''), '[^a-zA-Z0-9]', '', 'g'))
        """
    )

    execute("DROP TABLE IF EXISTS silver.doctor_action_first_seen;")
    execute(
        """
        CREATE TABLE silver.doctor_action_first_seen AS
        WITH latest_transaction_rows AS (
            SELECT DISTINCT ON (transaction_identity_key)
                *
            FROM silver.fact_collateral_transaction
            ORDER BY
                transaction_identity_key,
                COALESCE(NULLIF(updated_at_ts,''), NULLIF(last_viewed_at_ts,''), NULLIF(created_at_ts,''), NULLIF(transaction_date_ts,''), NULLIF(_ingested_at,'')) DESC NULLS LAST,
                id DESC
        ),
        tx AS (
            SELECT
                brand_campaign_id,
                collateral_id,
                doctor_identity_key,
                MIN(NULLIF(reached_event_ts,'')) AS reached_first_tx_ts,
                MIN(NULLIF(opened_event_ts,'')) AS opened_first_ts,
                MIN(NULLIF(video_gt_50_event_ts,'')) FILTER (WHERE video_view_gt_50_flag='1') AS video_gt_50_first_ts,
                MIN(NULLIF(COALESCE(viewed_last_page_at_ts, updated_at_ts),'')) FILTER (
                    WHERE downloaded_pdf_flag='1'
                       OR pdf_completed_flag='1'
                       OR NULLIF(viewed_last_page_at_ts,'') IS NOT NULL
                ) AS pdf_download_first_ts,
                MAX(COALESCE(NULLIF(updated_at_ts,''), NULLIF(last_viewed_at_ts,''))) AS last_activity_tx_ts
            FROM latest_transaction_rows
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
            COALESCE(share.reached_first_share_ts, tx.reached_first_tx_ts, tx.opened_first_ts, tx.video_gt_50_first_ts, tx.pdf_download_first_ts) AS reached_first_ts,
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
