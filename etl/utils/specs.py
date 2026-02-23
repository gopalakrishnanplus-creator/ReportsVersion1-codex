"""Canonical table specs from the production engineering document."""

SOURCE_TABLE_SPECS = {
    "mysql_server_1": {
        "campaign_fieldrep": [
            "id", "full_name", "phone_number", "brand_supplied_field_rep_id", "is_active",
            "password_hash", "created_at", "updated_at", "brand_id", "user_id", "state",
        ]
    },
    "mysql_server_2": {
        "collateral_management_campaigncollateral": [
            "id", "start_date", "end_date", "created_at", "updated_at", "campaign_id", "collateral_id",
        ],
        "collateral_management_collateral": [
            "id", "type", "title", "file", "vimeo_url", "content_id", "upload_date", "is_active",
            "created_at", "updated_at", "banner_1", "banner_2", "campaign_id", "created_by_id",
            "description", "purpose", "doctor_name", "webinar_date", "webinar_description", "webinar_title", "webinar_url",
        ],
        "doctor_viewer_doctor": ["id", "name", "phone", "rep_id", "source"],
        "sharing_management_collateraltransaction": [
            "id", "transaction_id", "brand_campaign_id", "field_rep_id", "field_rep_unique_id", "doctor_name", "doctor_number",
            "doctor_unique_id", "collateral_id", "transaction_date", "has_viewed", "downloaded_pdf", "pdf_completed",
            "video_view_lt_50", "video_view_gt_50", "video_completed", "pdf_total_pages", "last_video_percentage", "pdf_last_page",
            "doctor_viewer_engagement_id", "share_management_engagement_id", "video_tracking_last_event_id", "created_at", "updated_at",
            "sent_at", "viewed_at", "first_viewed_at", "viewed_last_page_at", "video_lt_50_at", "video_gt_50_at", "video_100_at",
            "last_viewed_at", "dv_engagement_id", "field_rep_email", "share_channel", "sm_engagement_id", "video_watch_percentage",
        ],
    },
}

AUDIT_COLUMNS = [
    "_ingestion_run_id", "_ingested_at", "_source_server", "_source_table", "_extract_started_at", "_extract_ended_at",
    "_record_hash", "_is_deleted", "_dq_status", "_dq_errors",
]
