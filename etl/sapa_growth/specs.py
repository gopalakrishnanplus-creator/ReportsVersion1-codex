from __future__ import annotations

from dataclasses import dataclass


RAW_MYSQL_SCHEMA = "raw_sapa_mysql"
RAW_API_SCHEMA = "raw_sapa_api"
BRONZE_SCHEMA = "bronze_sapa"
SILVER_SCHEMA = "silver_sapa"
GOLD_SCHEMA = "gold_sapa"
GOLD_STAGE_SCHEMA = "gold_sapa_stage"

RAW_AUDIT_COLUMNS = [
    "_ingestion_run_id",
    "_ingested_at",
    "_source_system",
    "_source_table",
    "_extract_started_at",
    "_extract_ended_at",
    "_record_hash",
    "_dq_status",
    "_dq_errors",
]


@dataclass(frozen=True)
class SourceTableSpec:
    source_table: str
    raw_table: str
    columns: list[str]
    key_columns: list[str]
    watermark_field: str | None = None
    lookback_days: int = 30


MYSQL_TABLE_SPECS: dict[str, SourceTableSpec] = {
    "campaign_doctor": SourceTableSpec(
        source_table="campaign_doctor",
        raw_table="campaign_doctor_raw",
        columns=["id", "doctor_id", "full_name", "email", "phone", "city", "state", "created_at"],
        key_columns=["id"],
        watermark_field="created_at",
    ),
    "redflags_doctor": SourceTableSpec(
        source_table="redflags_doctor",
        raw_table="redflags_doctor_raw",
        columns=[
            "doctor_id",
            "first_name",
            "last_name",
            "email",
            "whatsapp_no",
            "clinic_name",
            "clinic_phone",
            "created_at",
            "imc_registration_number",
            "clinic_appointment_number",
            "clinic_address",
            "postal_code",
            "state",
            "district",
            "receptionist_whatsapp_number",
            "photo",
            "field_rep_id",
            "recruited_via",
            "clinic_password_hash",
            "clinic_password_set_at",
            "clinic_user1_name",
            "clinic_user1_email",
            "clinic_user1_password_hash",
            "clinic_user2_name",
            "clinic_user2_email",
            "clinic_user2_password_hash",
            "partner_id",
            "gender",
        ],
        key_columns=["doctor_id"],
        watermark_field="created_at",
    ),
    "redflags_patientsubmission": SourceTableSpec(
        source_table="redflags_patientsubmission",
        raw_table="redflags_patientsubmission_raw",
        columns=["record_id", "language_code", "submitted_at", "patient_id", "doctor_id", "form_id", "overall_flag_code"],
        key_columns=["record_id"],
        watermark_field="submitted_at",
    ),
    "gnd_gndpatientsubmission": SourceTableSpec(
        source_table="gnd_gndpatientsubmission",
        raw_table="gnd_gndpatientsubmission_raw",
        columns=["id", "patient_id", "language_code", "submitted_at", "doctor_id", "form_id", "overall_flag_code"],
        key_columns=["id"],
        watermark_field="submitted_at",
    ),
    "redflags_submissionredflag": SourceTableSpec(
        source_table="redflags_submissionredflag",
        raw_table="redflags_submissionredflag_raw",
        columns=["id", "red_flag_id", "submission_id"],
        key_columns=["id"],
        watermark_field=None,
    ),
    "redflags_redflag": SourceTableSpec(
        source_table="redflags_redflag",
        raw_table="redflags_redflag_raw",
        columns=["red_flag_id", "severity", "default_patient_response", "doctor_at_a_glance", "doctor_video_url"],
        key_columns=["red_flag_id"],
        watermark_field=None,
    ),
    "redflags_patientvideo": SourceTableSpec(
        source_table="redflags_patientvideo",
        raw_table="redflags_patientvideo_raw",
        columns=["id", "language_code", "patient_video_url", "red_flag_id"],
        key_columns=["id"],
        watermark_field=None,
    ),
    "gnd_gndsubmissionredflag": SourceTableSpec(
        source_table="gnd_gndsubmissionredflag",
        raw_table="gnd_gndsubmissionredflag_raw",
        columns=["id", "red_flag_id", "submission_id"],
        key_columns=["id"],
        watermark_field=None,
    ),
    "gnd_gndredflag": SourceTableSpec(
        source_table="gnd_gndredflag",
        raw_table="gnd_gndredflag_raw",
        columns=["red_flag_id", "severity", "default_patient_response", "doctor_at_a_glance", "doctor_video_url"],
        key_columns=["red_flag_id"],
        watermark_field=None,
    ),
    "gnd_gndpatientvideo": SourceTableSpec(
        source_table="gnd_gndpatientvideo",
        raw_table="gnd_gndpatientvideo_raw",
        columns=["id", "language_code", "patient_video_url", "red_flag_id"],
        key_columns=["id"],
        watermark_field=None,
    ),
    "redflags_followupreminder": SourceTableSpec(
        source_table="redflags_followupreminder",
        raw_table="redflags_followupreminder_raw",
        columns=[
            "id",
            "created_at",
            "updated_at",
            "patient_id",
            "patient_name",
            "patient_whatsapp",
            "followup_date1",
            "followup_date2",
            "followup_date3",
            "frequency_unit",
            "frequency",
            "first_followup_date",
            "num_followups",
            "doctor_id",
        ],
        key_columns=["id"],
        watermark_field="updated_at",
    ),
    "redflags_metricevent": SourceTableSpec(
        source_table="redflags_metricevent",
        raw_table="redflags_metricevent_raw",
        columns=[
            "id",
            "event_type",
            "action_key",
            "share_code",
            "form_id",
            "language_code",
            "video_url",
            "meta",
            "ts",
            "doctor_id",
            "patient_id",
            "red_flag_id",
            "overall_flag_code",
        ],
        key_columns=["id"],
        watermark_field="ts",
    ),
    "campaign_clinic_outcome_master": SourceTableSpec(
        source_table="campaign_clinic_outcome_master",
        raw_table="campaign_clinic_outcome_master_raw",
        columns=[
            "doctor_id",
            "total_form_fills",
            "current_month_form_fills",
            "previous_month_form_fills",
            "total_red_flags",
            "current_month_red_flags",
            "previous_month_red_flags",
            "current_month_start",
            "created_at",
            "updated_at",
        ],
        key_columns=["doctor_id"],
        watermark_field="updated_at",
    ),
}

API_TABLE_SPECS = {
    "wp_webinar_registrations": {
        "raw_table": "wp_webinar_registrations_raw",
        "columns": [
            "registration_id",
            "event_id",
            "event_title",
            "start_date",
            "end_date",
            "timezone",
            "email",
            "first_name",
            "last_name",
            "phone",
            "registration_created_at",
            "payload_json",
        ],
        "key_columns": ["registration_id", "event_id", "email", "phone"],
    },
    "wp_course_summary": {
        "raw_table": "wp_course_summary_raw",
        "columns": [
            "course_id",
            "total_enrolled",
            "completed",
            "in_progress",
            "not_started",
            "course_audience",
            "payload_json",
        ],
        "key_columns": ["course_id", "course_audience"],
    },
    "wp_course_breakdown": {
        "raw_table": "wp_course_breakdown_raw",
        "columns": [
            "course_id",
            "course_audience",
            "user_id",
            "display_name",
            "user_email",
            "first_name",
            "last_name",
            "progress_status",
            "enrolled_at",
            "started_at",
            "completed_at",
            "phone",
            "payload_json",
        ],
        "key_columns": ["course_id", "user_id", "user_email", "phone"],
    },
}
