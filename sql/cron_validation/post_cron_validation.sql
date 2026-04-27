\set ON_ERROR_STOP on
\pset pager off
\timing on

-- Usage example:
-- psql "$DATABASE_URL" \
--   -v job_name=weekly_transfer_cleanup \
--   -v run_log_table=control.etl_run_log \
--   -v watch_tables_csv=raw_server2.sharing_management_collateraltransaction,bronze.sharing_management_collateraltransaction,silver.fact_collateral_transaction,bronze_sapa.redflags_patientsubmission,bronze_sapa.gnd_gndpatientsubmission,silver_sapa.fact_screening_submission \
--   -v log_dir=./var/log/cron_validation \
--   -f sql/cron_validation/post_cron_validation.sql

\if :{?job_name}
\else
\set job_name weekly_transfer_cleanup
\endif

\if :{?run_log_table}
\else
\set run_log_table control.etl_run_log
\endif

\if :{?watch_tables_csv}
\else
\set watch_tables_csv raw_server2.sharing_management_collateraltransaction,bronze.sharing_management_collateraltransaction,silver.fact_collateral_transaction,bronze_sapa.redflags_patientsubmission,bronze_sapa.gnd_gndpatientsubmission,silver_sapa.fact_screening_submission
\endif

\if :{?log_dir}
\else
\set log_dir ./var/log/cron_validation
\endif

\! mkdir -p :log_dir

SELECT
    :'log_dir' || '/' || :'job_name' || '_post_' || to_char(clock_timestamp(), 'YYYYMMDD_HH24MISS') || '.txt' AS log_file
\gset

\o :log_file

\qecho ============================================
\qecho POST-CRON VALIDATION START
\qecho Job Name      : :job_name
\qecho Run Log Table : :run_log_table
\qecho Watch Tables  : :watch_tables_csv
\qecho Log File      : :log_file
\qecho ============================================

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.cron_validation_checkpoint (
    checkpoint_id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    snapshot_phase TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    run_log_table TEXT NOT NULL,
    run_log_started_at TIMESTAMPTZ NULL,
    run_log_ended_at TIMESTAMPTZ NULL,
    run_log_status TEXT NULL,
    validation_status TEXT NULL,
    validation_message TEXT NULL,
    notes JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT cron_validation_checkpoint_phase_chk
        CHECK (snapshot_phase IN ('PRE', 'POST'))
);

CREATE INDEX IF NOT EXISTS idx_cron_validation_checkpoint_job_phase
    ON control.cron_validation_checkpoint (job_name, snapshot_phase, captured_at DESC);

CREATE TABLE IF NOT EXISTS control.cron_validation_table_snapshot (
    checkpoint_id BIGINT NOT NULL REFERENCES control.cron_validation_checkpoint(checkpoint_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    table_exists BOOLEAN NOT NULL DEFAULT FALSE,
    row_count BIGINT NULL,
    marker_column TEXT NULL,
    marker_value TEXT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (checkpoint_id, table_name)
);

CREATE OR REPLACE FUNCTION control.capture_cron_validation_snapshot(
    p_job_name TEXT,
    p_phase TEXT,
    p_run_log_table TEXT,
    p_watch_tables TEXT[]
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_checkpoint_id BIGINT;
    v_run_log_regclass REGCLASS;
    v_run_started TIMESTAMPTZ;
    v_run_ended TIMESTAMPTZ;
    v_run_status TEXT;
    v_table_name TEXT;
    v_table_regclass REGCLASS;
    v_schema_name TEXT;
    v_relation_name TEXT;
    v_marker_column TEXT;
    v_row_count BIGINT;
    v_marker_value TEXT;
BEGIN
    IF upper(p_phase) NOT IN ('PRE', 'POST') THEN
        RAISE EXCEPTION 'Unsupported snapshot phase: %', p_phase;
    END IF;

    SELECT to_regclass(p_run_log_table) INTO v_run_log_regclass;

    IF v_run_log_regclass IS NULL THEN
        v_run_status := 'RUN_LOG_TABLE_NOT_FOUND';
    ELSE
        BEGIN
            EXECUTE format(
                'SELECT NULLIF(started_at, '''')::timestamptz,
                        NULLIF(ended_at, '''')::timestamptz,
                        status
                 FROM %s
                 ORDER BY COALESCE(NULLIF(ended_at, '''')::timestamptz, NULLIF(started_at, '''')::timestamptz) DESC NULLS LAST
                 LIMIT 1',
                v_run_log_regclass
            )
            INTO v_run_started, v_run_ended, v_run_status;
        EXCEPTION
            WHEN undefined_column THEN
                v_run_status := 'RUN_LOG_MISSING_EXPECTED_COLUMNS';
                v_run_started := NULL;
                v_run_ended := NULL;
        END;
    END IF;

    INSERT INTO control.cron_validation_checkpoint (
        job_name,
        snapshot_phase,
        run_log_table,
        run_log_started_at,
        run_log_ended_at,
        run_log_status,
        notes
    )
    VALUES (
        p_job_name,
        upper(p_phase),
        p_run_log_table,
        v_run_started,
        v_run_ended,
        v_run_status,
        jsonb_build_object('watch_tables', to_jsonb(p_watch_tables))
    )
    RETURNING checkpoint_id INTO v_checkpoint_id;

    FOREACH v_table_name IN ARRAY p_watch_tables LOOP
        v_table_name := btrim(v_table_name);
        IF v_table_name = '' THEN
            CONTINUE;
        END IF;

        SELECT to_regclass(v_table_name) INTO v_table_regclass;

        IF v_table_regclass IS NULL THEN
            INSERT INTO control.cron_validation_table_snapshot (
                checkpoint_id,
                table_name,
                table_exists,
                row_count,
                marker_column,
                marker_value
            )
            VALUES (
                v_checkpoint_id,
                v_table_name,
                FALSE,
                NULL,
                NULL,
                NULL
            );
            CONTINUE;
        END IF;

        SELECT n.nspname, c.relname
        INTO v_schema_name, v_relation_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = v_table_regclass;

        SELECT column_name
        INTO v_marker_column
        FROM information_schema.columns
        WHERE table_schema = v_schema_name
          AND table_name = v_relation_name
          AND column_name = ANY (
              ARRAY[
                  '_ingested_at',
                  '_extract_ended_at',
                  'updated_at',
                  'created_at',
                  'submitted_at',
                  'transaction_date',
                  'ts',
                  'ended_at'
              ]
          )
        ORDER BY array_position(
            ARRAY[
                '_ingested_at',
                '_extract_ended_at',
                'updated_at',
                'created_at',
                'submitted_at',
                'transaction_date',
                'ts',
                'ended_at'
            ],
            column_name
        )
        LIMIT 1;

        EXECUTE format('SELECT count(*) FROM %s', v_table_regclass)
        INTO v_row_count;

        IF v_marker_column IS NOT NULL THEN
            EXECUTE format('SELECT max(%I)::text FROM %s', v_marker_column, v_table_regclass)
            INTO v_marker_value;
        ELSE
            v_marker_value := NULL;
        END IF;

        INSERT INTO control.cron_validation_table_snapshot (
            checkpoint_id,
            table_name,
            table_exists,
            row_count,
            marker_column,
            marker_value
        )
        VALUES (
            v_checkpoint_id,
            v_table_name,
            TRUE,
            v_row_count,
            v_marker_column,
            v_marker_value
        );
    END LOOP;

    RETURN v_checkpoint_id;
END;
$$;

SELECT control.capture_cron_validation_snapshot(
    :'job_name',
    'POST',
    :'run_log_table',
    string_to_array(:'watch_tables_csv', ',')
) AS post_checkpoint_id
\gset

WITH latest_pre AS (
    SELECT checkpoint_id
    FROM control.cron_validation_checkpoint
    WHERE job_name = :'job_name'
      AND snapshot_phase = 'PRE'
      AND checkpoint_id < :post_checkpoint_id
    ORDER BY captured_at DESC
    LIMIT 1
),
pre_checkpoint AS (
    SELECT *
    FROM control.cron_validation_checkpoint
    WHERE checkpoint_id = (SELECT checkpoint_id FROM latest_pre)
),
post_checkpoint AS (
    SELECT *
    FROM control.cron_validation_checkpoint
    WHERE checkpoint_id = :post_checkpoint_id
),
pre_snapshots AS (
    SELECT *
    FROM control.cron_validation_table_snapshot
    WHERE checkpoint_id = (SELECT checkpoint_id FROM latest_pre)
),
post_snapshots AS (
    SELECT *
    FROM control.cron_validation_table_snapshot
    WHERE checkpoint_id = :post_checkpoint_id
),
table_comparison AS (
    SELECT
        COALESCE(pre.table_name, post.table_name) AS table_name,
        pre.table_exists AS pre_exists,
        post.table_exists AS post_exists,
        pre.row_count AS pre_row_count,
        post.row_count AS post_row_count,
        COALESCE(post.marker_column, pre.marker_column) AS marker_column,
        pre.marker_value AS pre_marker_value,
        post.marker_value AS post_marker_value,
        (
            pre.table_exists IS DISTINCT FROM post.table_exists
            OR pre.row_count IS DISTINCT FROM post.row_count
            OR COALESCE(pre.marker_value, '') IS DISTINCT FROM COALESCE(post.marker_value, '')
        ) AS changed_flag
    FROM pre_snapshots pre
    FULL OUTER JOIN post_snapshots post USING (table_name)
),
summary AS (
    SELECT
        pre_cp.checkpoint_id AS pre_checkpoint_id,
        post_cp.checkpoint_id AS post_checkpoint_id,
        pre_cp.run_log_started_at AS pre_run_log_started_at,
        pre_cp.run_log_ended_at AS pre_run_log_ended_at,
        pre_cp.run_log_status AS pre_run_log_status,
        post_cp.run_log_started_at AS post_run_log_started_at,
        post_cp.run_log_ended_at AS post_run_log_ended_at,
        post_cp.run_log_status AS post_run_log_status,
        CASE
            WHEN pre_cp.checkpoint_id IS NULL THEN FALSE
            WHEN COALESCE(post_cp.run_log_ended_at, post_cp.run_log_started_at) IS NULL THEN FALSE
            WHEN COALESCE(pre_cp.run_log_ended_at, pre_cp.run_log_started_at) IS NULL THEN TRUE
            ELSE COALESCE(post_cp.run_log_ended_at, post_cp.run_log_started_at)
                 > COALESCE(pre_cp.run_log_ended_at, pre_cp.run_log_started_at)
        END AS run_detected,
        UPPER(COALESCE(post_cp.run_log_status, '')) IN ('SUCCESS', 'COMPLETED', 'OK', 'DONE') AS run_success,
        COUNT(tc.table_name) FILTER (WHERE tc.changed_flag) AS changed_table_count,
        COUNT(tc.table_name) AS watched_table_count,
        COALESCE(string_agg(tc.table_name, ', ' ORDER BY tc.table_name) FILTER (WHERE tc.changed_flag), '') AS changed_table_list
    FROM post_checkpoint post_cp
    LEFT JOIN pre_checkpoint pre_cp ON TRUE
    LEFT JOIN table_comparison tc ON TRUE
    GROUP BY
        pre_cp.checkpoint_id,
        post_cp.checkpoint_id,
        pre_cp.run_log_started_at,
        pre_cp.run_log_ended_at,
        pre_cp.run_log_status,
        post_cp.run_log_started_at,
        post_cp.run_log_ended_at,
        post_cp.run_log_status
),
applied AS (
    UPDATE control.cron_validation_checkpoint post_row
    SET
        validation_status = CASE
            WHEN summary.pre_checkpoint_id IS NULL THEN 'FAILURE'
            WHEN NOT summary.run_detected THEN 'FAILURE'
            WHEN NOT summary.run_success THEN 'FAILURE'
            WHEN summary.changed_table_count > 0 THEN 'SUCCESS'
            ELSE 'WARNING'
        END,
        validation_message = CASE
            WHEN summary.pre_checkpoint_id IS NULL THEN 'No PRE snapshot found for this job.'
            WHEN NOT summary.run_detected THEN 'No newer run-log entry was detected after the PRE snapshot.'
            WHEN NOT summary.run_success THEN 'A newer run-log entry exists, but status is not SUCCESS/COMPLETED.'
            WHEN summary.changed_table_count > 0 THEN 'Cron job appears to have run successfully and changed the expected dataset state.'
            ELSE 'Cron job appears successful, but no watched-table changes were detected.'
        END,
        notes = COALESCE(post_row.notes, '{}'::jsonb) || jsonb_build_object(
            'pre_checkpoint_id', summary.pre_checkpoint_id,
            'run_detected', summary.run_detected,
            'run_success', summary.run_success,
            'changed_table_count', summary.changed_table_count,
            'changed_table_list', summary.changed_table_list
        )
    FROM summary
    WHERE post_row.checkpoint_id = :post_checkpoint_id
    RETURNING post_row.checkpoint_id
)
SELECT checkpoint_id
FROM applied;

\qecho
\qecho POST SNAPSHOT SUMMARY
SELECT
    checkpoint_id,
    job_name,
    snapshot_phase,
    captured_at,
    run_log_table,
    run_log_started_at,
    run_log_ended_at,
    run_log_status,
    validation_status,
    validation_message
FROM control.cron_validation_checkpoint
WHERE checkpoint_id = :post_checkpoint_id;

\qecho
\qecho RUN LOG COMPARISON
WITH latest_pre AS (
    SELECT checkpoint_id
    FROM control.cron_validation_checkpoint
    WHERE job_name = :'job_name'
      AND snapshot_phase = 'PRE'
      AND checkpoint_id < :post_checkpoint_id
    ORDER BY captured_at DESC
    LIMIT 1
),
pre_checkpoint AS (
    SELECT *
    FROM control.cron_validation_checkpoint
    WHERE checkpoint_id = (SELECT checkpoint_id FROM latest_pre)
),
post_checkpoint AS (
    SELECT *
    FROM control.cron_validation_checkpoint
    WHERE checkpoint_id = :post_checkpoint_id
)
SELECT
    pre_cp.checkpoint_id AS pre_checkpoint_id,
    pre_cp.run_log_started_at AS pre_run_log_started_at,
    pre_cp.run_log_ended_at AS pre_run_log_ended_at,
    pre_cp.run_log_status AS pre_run_log_status,
    post_cp.checkpoint_id AS post_checkpoint_id,
    post_cp.run_log_started_at AS post_run_log_started_at,
    post_cp.run_log_ended_at AS post_run_log_ended_at,
    post_cp.run_log_status AS post_run_log_status
FROM post_checkpoint post_cp
LEFT JOIN pre_checkpoint pre_cp ON TRUE;

\qecho
\qecho WATCHED TABLE COMPARISON
WITH latest_pre AS (
    SELECT checkpoint_id
    FROM control.cron_validation_checkpoint
    WHERE job_name = :'job_name'
      AND snapshot_phase = 'PRE'
      AND checkpoint_id < :post_checkpoint_id
    ORDER BY captured_at DESC
    LIMIT 1
),
pre_snapshots AS (
    SELECT *
    FROM control.cron_validation_table_snapshot
    WHERE checkpoint_id = (SELECT checkpoint_id FROM latest_pre)
),
post_snapshots AS (
    SELECT *
    FROM control.cron_validation_table_snapshot
    WHERE checkpoint_id = :post_checkpoint_id
)
SELECT
    COALESCE(pre.table_name, post.table_name) AS table_name,
    pre.table_exists AS pre_exists,
    post.table_exists AS post_exists,
    pre.row_count AS pre_row_count,
    post.row_count AS post_row_count,
    COALESCE(post.marker_column, pre.marker_column) AS marker_column,
    pre.marker_value AS pre_marker_value,
    post.marker_value AS post_marker_value,
    CASE
        WHEN pre.table_exists IS DISTINCT FROM post.table_exists
          OR pre.row_count IS DISTINCT FROM post.row_count
          OR COALESCE(pre.marker_value, '') IS DISTINCT FROM COALESCE(post.marker_value, '')
        THEN 'CHANGED'
        ELSE 'UNCHANGED'
    END AS table_status
FROM pre_snapshots pre
FULL OUTER JOIN post_snapshots post USING (table_name)
ORDER BY table_name;

\qecho
\qecho FINAL VALIDATION STATUS
SELECT
    validation_status,
    validation_message,
    notes
FROM control.cron_validation_checkpoint
WHERE checkpoint_id = :post_checkpoint_id;

\qecho
SELECT to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS TZ') AS finished_at
\gset

\qecho
\qecho POST-CRON VALIDATION COMPLETE
\qecho Post Checkpoint ID : :post_checkpoint_id
\qecho Completed At       : :finished_at
\qecho ============================================

\o
\echo Wrote post-cron validation log to :log_file
