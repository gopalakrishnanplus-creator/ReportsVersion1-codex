from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


MASTER_DB = "rfa_master"
INCLINIC_DB = "inclinic"
PE_DB = "pe_portal"
RAW_V2_MASTER_SCHEMA = "raw_v2_master"
RAW_V2_INCLINIC_SCHEMA = "raw_v2_inclinic"
RAW_V2_PE_PORTAL_SCHEMA = "raw_v2_pe_portal"
UUID_NAMESPACE = uuid.UUID("8a2f1b96-bf9c-47e3-a2c6-3c4eb266a2e2")
RUN_ID = f"v1_to_v2_{datetime.now(timezone.utc):%Y%m%d%H%M%S}"

def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"null", "none"} else text


def truthy(value: Any) -> bool:
    return clean(value).lower() in {"1", "true", "t", "yes", "y", "on"}


def phone(value: Any) -> str:
    digits = re.sub(r"\D+", "", clean(value))
    if len(digits) > 10:
        return digits[-10:]
    return digits


def norm_key(value: Any) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "", clean(value)).lower()


def stable_uuid(*parts: Any) -> str:
    return uuid.uuid5(UUID_NAMESPACE, ":".join(clean(part) for part in parts)).hex


def md5(*parts: Any) -> str:
    return hashlib.md5(":".join(clean(part) for part in parts).encode("utf-8")).hexdigest()


def now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def latest_rows(rows: list[dict[str, str]], pk: str = "id") -> list[dict[str, str]]:
    best: dict[str, dict[str, str]] = {}
    for row in rows:
        if clean(row.get("_is_deleted")).lower() in {"1", "true", "t", "yes", "y"}:
            continue
        key = clean(row.get(pk))
        if not key:
            continue
        old = best.get(key)
        if old is None or clean(row.get("_ingested_at")) >= clean(old.get("_ingested_at")):
            best[key] = row
    return list(best.values())


def source_database_name(system: str, requested_database: str, row: dict[str, Any]) -> str:
    return clean(
        row.get("_source_database")
        or row.get("_source_server")
        or row.get("_source_system")
        or requested_database
        or system
    )


def source_table_name(table: str, row: dict[str, Any]) -> str:
    return clean(row.get("_source_table") or table)


def source_common(system: str, database: str, table: str, row: dict[str, Any], basis: str, status: str = "verified") -> dict[str, Any]:
    raw_payload = json.dumps(row, ensure_ascii=True, sort_keys=True, default=str)
    return {
        "source_system": system,
        "source_database": source_database_name(system, database, row),
        "source_table": source_table_name(table, row),
        "source_pk_column": "id",
        "source_pk_value": clean(row.get("id")),
        "source_created_at": clean(row.get("created_at") or row.get("date_joined") or row.get("share_timestamp")),
        "source_updated_at": clean(row.get("updated_at")),
        "migration_batch_id": RUN_ID,
        "migrated_at": now_text(),
        "verification_status": status,
        "verification_basis": basis,
        "is_current": "1",
        "valid_from": clean(row.get("created_at") or row.get("date_joined") or row.get("share_timestamp")),
        "valid_to": "",
        "raw_payload_json": raw_payload,
    }


def build_master_v2(data: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, Any]]]:
    campaigns = latest_rows(data.get("raw_server1.campaign_campaign", []))
    field_reps = latest_rows(data.get("raw_server1.campaign_fieldrep", []))
    assignments = latest_rows(data.get("raw_server1.campaign_campaignfieldrep", []))
    auth_users = latest_rows(data.get("raw_server1.auth_user", []))
    brands = latest_rows(data.get("raw_sapa_mysql.campaign_brand_raw", []))
    doctors = latest_rows(data.get("raw_sapa_mysql.campaign_doctor_raw", []))
    enrollments = data.get("raw_sapa_mysql.campaign_doctorcampaignenrollment_raw", [])
    doctor_viewer = latest_rows(data.get("raw_server2.doctor_viewer_doctor", []))
    local_users = latest_rows(data.get("raw_server2.user_management_user", []))

    auth_by_id = {clean(row.get("id")): row for row in auth_users}
    fr_by_id = {clean(row.get("id")): row for row in field_reps}
    fr_by_brand = {clean(row.get("brand_supplied_field_rep_id")): row for row in field_reps if clean(row.get("brand_supplied_field_rep_id"))}
    fr_by_auth_email = {
        clean(auth_by_id.get(clean(row.get("user_id")), {}).get("email")).lower(): row
        for row in field_reps
        if clean(auth_by_id.get(clean(row.get("user_id")), {}).get("email"))
    }
    local_user_by_id = {clean(row.get("id")): row for row in local_users if clean(row.get("id"))}
    assignments_by_rep: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in assignments:
        assignments_by_rep[clean(row.get("field_rep_id"))].append(row)

    brand_rows = []
    for row in brands:
        brand_id = clean(row.get("id"))
        if not brand_id:
            continue
        brand_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_brand", row, "campaign_brand"),
                "brand_uuid": stable_uuid("brand", brand_id),
                "id": brand_id,
                "legacy_brand_id": brand_id,
                "name": clean(row.get("name")),
                "company_name": clean(row.get("name")),
                "display_name": clean(row.get("name")) or brand_id,
                "company_name_normalized": clean(row.get("name")).lower(),
                "status": "active",
            }
        )

    campaign_rows = []
    for row in campaigns:
        campaign_id = clean(row.get("id"))
        if not campaign_id:
            continue
        brand_id = clean(row.get("brand_id"))
        campaign_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_campaign", row, "campaign_campaign"),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)),
                "id": campaign_id,
                "legacy_campaign_id": campaign_id,
                "legacy_campaign_id_normalized": norm_key(campaign_id),
                "name": clean(row.get("name")),
                "brand_id": brand_id,
                "brand_uuid": stable_uuid("brand", brand_id) if brand_id else "",
                "start_date": clean(row.get("start_date")),
                "end_date": clean(row.get("end_date")),
                "created_at": clean(row.get("created_at")),
                "updated_at": clean(row.get("updated_at")),
                "system_rfa": "1" if truthy(row.get("system_rfa")) else "0",
                "system_pe": "1" if truthy(row.get("system_pe")) else "0",
                "system_ic": "1" if truthy(row.get("system_ic")) else "0",
                "status": clean(row.get("status")),
                "num_doctors_supported": clean(row.get("num_doctors_supported")),
                "brand_manager_login_link": clean(row.get("brand_manager_login_link")),
                "banner_small_url": clean(row.get("banner_small_url")),
                "banner_large_url": clean(row.get("banner_large_url")),
                "banner_target_url": clean(row.get("banner_target_url")),
            }
        )

    field_rep_rows = []
    for row in field_reps:
        rep_id = clean(row.get("id"))
        if not rep_id:
            continue
        auth = auth_by_id.get(clean(row.get("user_id")), {})
        field_rep_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_fieldrep", row, "campaign_fieldrep"),
                "field_rep_uuid": stable_uuid("field_rep", rep_id),
                "id": rep_id,
                "current_campaign_fieldrep_id": rep_id,
                "full_name": clean(row.get("full_name")),
                "display_name": clean(row.get("full_name")) or rep_id,
                "phone_number": clean(row.get("phone_number")),
                "primary_phone_raw": clean(row.get("phone_number")),
                "primary_phone_normalized": phone(row.get("phone_number")),
                "primary_email": clean(auth.get("email")),
                "brand_supplied_field_rep_id": clean(row.get("brand_supplied_field_rep_id")),
                "current_brand_supplied_field_rep_id": clean(row.get("brand_supplied_field_rep_id")),
                "is_active": "1" if truthy(row.get("is_active")) else "0",
                "password_hash": clean(row.get("password_hash")),
                "created_at": clean(row.get("created_at")),
                "updated_at": clean(row.get("updated_at")),
                "brand_id": clean(row.get("brand_id")),
                "brand_uuid": stable_uuid("brand", row.get("brand_id")) if clean(row.get("brand_id")) else "",
                "user_id": clean(row.get("user_id")),
                "state": clean(row.get("state")),
                "status": "active" if truthy(row.get("is_active")) else "inactive",
            }
        )

    assignment_rows = []
    for row in assignments:
        assignment_id = clean(row.get("id")) or md5(row.get("campaign_id"), row.get("field_rep_id"))
        campaign_id = clean(row.get("campaign_id"))
        rep_id = clean(row.get("field_rep_id"))
        fr = fr_by_id.get(rep_id, {})
        assignment_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_campaignfieldrep", row, "campaign_campaignfieldrep"),
                "campaign_field_rep_assignment_uuid": stable_uuid("campaign_field_rep_assignment", assignment_id),
                "id": assignment_id,
                "legacy_campaign_fieldrep_id": assignment_id,
                "campaign_id": campaign_id,
                "legacy_campaign_id": campaign_id,
                "legacy_campaign_id_normalized": norm_key(campaign_id),
                "field_rep_id": rep_id,
                "campaign_fieldrep_id": rep_id,
                "brand_supplied_field_rep_id": clean(fr.get("brand_supplied_field_rep_id")),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)),
                "field_rep_uuid": stable_uuid("field_rep", rep_id),
                "created_at": clean(row.get("created_at")),
                "assigned_at": clean(row.get("created_at")),
                "assigned_from": clean(row.get("created_at")),
                "assigned_to": "",
                "assignment_status": "active",
                "state": clean(row.get("state")),
                "is_authoritative": "1",
            }
        )

    doctor_rows = []
    for row in doctors:
        doctor_id = clean(row.get("id"))
        if not doctor_id:
            continue
        doctor_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_doctor", row, "campaign_doctor"),
                "doctor_uuid": stable_uuid("master_doctor", doctor_id),
                "id": doctor_id,
                "doctor_id": clean(row.get("doctor_id")) or doctor_id,
                "legacy_doctor_id": doctor_id,
                "full_name": clean(row.get("full_name")),
                "email": clean(row.get("email")),
                "phone": clean(row.get("phone")),
                "phone_normalized": phone(row.get("phone")),
                "city": clean(row.get("city")),
                "state": clean(row.get("state")),
                "created_at": clean(row.get("created_at")),
            }
        )

    enrollment_rows = []
    for row in enrollments:
        campaign_id = clean(row.get("campaign_id"))
        doctor_id = clean(row.get("doctor_id"))
        if not campaign_id or not doctor_id:
            continue
        enrollment_rows.append(
            {
                **source_common("rfa_master", MASTER_DB, "campaign_doctorcampaignenrollment", row, "campaign_doctorcampaignenrollment"),
                "doctor_campaign_enrollment_uuid": stable_uuid("doctor_campaign_enrollment", campaign_id, doctor_id),
                "campaign_id": campaign_id,
                "legacy_campaign_id": campaign_id,
                "legacy_campaign_id_normalized": norm_key(campaign_id),
                "doctor_id": doctor_id,
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)),
                "doctor_uuid": stable_uuid("master_doctor", doctor_id),
                "registered_at": clean(row.get("registered_at")),
                "created_at": clean(row.get("created_at")),
                "updated_at": clean(row.get("updated_at")),
                "registered_by_id": clean(row.get("registered_by_id")),
                "registered_by_field_rep_uuid": stable_uuid("field_rep", row.get("registered_by_id")) if clean(row.get("registered_by_id")) else "",
                "whitelabel_enabled": "1" if truthy(row.get("whitelabel_enabled")) else "0",
                "whitelabel_subdomain": clean(row.get("whitelabel_subdomain")),
            }
        )

    roster_rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    roster_exception_rows: list[dict[str, Any]] = []
    for doctor in doctor_viewer:
        phone_norm = phone(doctor.get("phone"))
        if not phone_norm:
            continue
        doctor_rep_id = clean(doctor.get("rep_id"))
        local_user = local_user_by_id.get(doctor_rep_id, {})
        local_field_id = clean(local_user.get("field_id"))
        local_email = clean(local_user.get("email")).lower()
        fr = fr_by_brand.get(local_field_id) or fr_by_auth_email.get(local_email)
        if not fr:
            roster_exception_rows.append(
                {
                    "exception_id": str(len(roster_exception_rows) + 1),
                    "migration_batch_id": RUN_ID,
                    "system_name": "inclinic",
                    "database_name": INCLINIC_DB,
                    "source_table": "doctor_viewer_doctor",
                    "source_pk_column": "id",
                    "source_pk_value": clean(doctor.get("id")),
                    "entity_type": "doctor_field_rep_roster",
                    "issue_code": "DOCTOR_VIEWER_FIELD_REP_IDENTITY_UNRESOLVED",
                    "issue_details": json.dumps(
                        {
                            "doctor_viewer_rep_id": doctor_rep_id,
                            "user_management_field_id": local_field_id,
                            "user_management_email": local_email,
                        },
                        sort_keys=True,
                    ),
                    "raw_payload_json": json.dumps(doctor, sort_keys=True),
                    "resolution_status": "open",
                }
            )
            continue
        rep_id = clean(fr.get("id"))
        rep_assignments = assignments_by_rep.get(rep_id, [])
        if not rep_assignments:
            roster_exception_rows.append(
                {
                    "exception_id": str(len(roster_exception_rows) + 1),
                    "migration_batch_id": RUN_ID,
                    "system_name": "inclinic",
                    "database_name": INCLINIC_DB,
                    "source_table": "doctor_viewer_doctor",
                    "source_pk_column": "id",
                    "source_pk_value": clean(doctor.get("id")),
                    "entity_type": "doctor_field_rep_roster",
                    "issue_code": "DOCTOR_VIEWER_REP_HAS_NO_CAMPAIGN_ASSIGNMENT",
                    "issue_details": json.dumps(
                        {
                            "doctor_viewer_rep_id": doctor_rep_id,
                            "campaign_fieldrep_id": rep_id,
                            "brand_supplied_field_rep_id": clean(fr.get("brand_supplied_field_rep_id")),
                        },
                        sort_keys=True,
                    ),
                    "raw_payload_json": json.dumps(doctor, sort_keys=True),
                    "resolution_status": "open",
                }
            )
            continue
        for assignment in rep_assignments:
            campaign_id = clean(assignment.get("campaign_id"))
            key = (campaign_id, rep_id, phone_norm)
            roster_rows_by_key[key] = {
                **source_common("inclinic", INCLINIC_DB, "doctor_viewer_doctor", doctor, "doctor_viewer_rep_id_to_user_management_field_id"),
                "doctor_field_rep_roster_bridge_uuid": stable_uuid("doctor_field_rep_roster_bridge", campaign_id, rep_id, phone_norm),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)),
                "legacy_campaign_id": campaign_id,
                "legacy_campaign_id_normalized": norm_key(campaign_id),
                "field_rep_uuid": stable_uuid("field_rep", rep_id),
                "campaign_fieldrep_id": rep_id,
                "brand_supplied_field_rep_id": clean(fr.get("brand_supplied_field_rep_id")),
                "doctor_uuid": stable_uuid("doctor_phone", phone_norm),
                "doctor_name_raw": clean(doctor.get("name")),
                "doctor_name_normalized": clean(doctor.get("name")).lower(),
                "doctor_phone_raw": clean(doctor.get("phone")),
                "doctor_phone_normalized": phone_norm,
                "assignment_status": "active",
                "match_status": "matched",
                "match_basis": "doctor_viewer_rep_id_to_user_management_id_to_field_id_to_brand_supplied_field_rep_id",
                "old_doctor_viewer_id": clean(doctor.get("id")),
                "old_doctor_viewer_rep_id": doctor_rep_id,
                "old_user_management_user_id": clean(local_user.get("id")),
                "old_user_management_field_id": local_field_id,
                "old_user_management_email": clean(local_user.get("email")),
            }
    roster_rows = list(roster_rows_by_key.values())

    return {
        "brand_v2": brand_rows,
        "campaign_v2": campaign_rows,
        "field_rep_v2": field_rep_rows,
        "campaign_field_rep_assignment_v2": assignment_rows,
        "doctor_v2": doctor_rows,
        "doctor_campaign_enrollment_v2": enrollment_rows,
        "doctor_field_rep_roster_bridge_v2": roster_rows,
        "doctor_field_rep_roster_exception_v2": roster_exception_rows,
        "campaign_fieldrep": field_reps,
        "auth_user": auth_users,
        "campaign_campaign": campaigns,
        "campaign_campaignfieldrep": assignments,
    }


def build_inclinic_v2(data: dict[str, list[dict[str, str]]], master_v2: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    field_reps = latest_rows(data.get("raw_server1.campaign_fieldrep", []))
    auth_users = latest_rows(data.get("raw_server1.auth_user", []))
    master_assignments = latest_rows(data.get("raw_server1.campaign_campaignfieldrep", []))
    local_campaigns = latest_rows(data.get("raw_server2.campaign_management_campaign", []))
    local_users = latest_rows(data.get("raw_server2.user_management_user", []))
    share_logs = latest_rows(data.get("raw_server2.sharing_management_sharelog", []))
    transactions = latest_rows(data.get("raw_server2.sharing_management_collateraltransaction", []))
    collaterals = latest_rows(data.get("raw_server2.collateral_management_collateral", []))
    campaign_collaterals = latest_rows(data.get("raw_server2.collateral_management_campaigncollateral", []))

    fr_by_id = {clean(row.get("id")): row for row in field_reps}
    fr_by_brand = {clean(row.get("brand_supplied_field_rep_id")): row for row in field_reps if clean(row.get("brand_supplied_field_rep_id"))}
    fr_by_user_id = {clean(row.get("user_id")): row for row in field_reps if clean(row.get("user_id"))}
    auth_by_id = {clean(row.get("id")): row for row in auth_users}
    fr_by_auth_email = {
        clean(auth_by_id.get(clean(row.get("user_id")), {}).get("email")).lower(): row
        for row in field_reps
        if clean(auth_by_id.get(clean(row.get("user_id")), {}).get("email"))
    }
    local_campaign_by_id = {clean(row.get("id")): row for row in local_campaigns}
    share_by_id = {clean(row.get("id")): row for row in share_logs}

    assigned_reps_by_campaign: dict[str, set[str]] = defaultdict(set)
    for assignment in master_assignments:
        campaign_id = clean(assignment.get("campaign_id"))
        rep_id = clean(assignment.get("field_rep_id"))
        if campaign_id and rep_id:
            assigned_reps_by_campaign[campaign_id].add(rep_id)

    local_user_resolved_by_id: dict[str, dict[str, str]] = {}
    local_user_resolved_by_email: dict[str, dict[str, str]] = {}
    local_user_resolved_by_field_id: dict[str, dict[str, str]] = {}
    for user in local_users:
        field_id = clean(user.get("field_id"))
        email = clean(user.get("email")).lower()
        resolved = fr_by_brand.get(field_id) or fr_by_auth_email.get(email)
        if not resolved:
            continue
        user_id = clean(user.get("id"))
        if user_id:
            local_user_resolved_by_id[user_id] = resolved
        if email:
            local_user_resolved_by_email[email] = resolved
        if field_id:
            local_user_resolved_by_field_id[field_id] = resolved

    def resolve_field_rep_identity(
        *,
        source_field_rep_id: Any = "",
        brand_supplied_id: Any = "",
        email: Any = "",
        campaign_id: Any = "",
        linked_source_field_rep_id: Any = "",
        linked_email: Any = "",
    ) -> dict[str, Any]:
        raw_source_id = clean(source_field_rep_id)
        raw_brand_id = clean(brand_supplied_id)
        raw_email = clean(email).lower()
        raw_campaign_id = clean(campaign_id)
        raw_linked_id = clean(linked_source_field_rep_id)
        raw_linked_email = clean(linked_email).lower()
        candidates: list[dict[str, Any]] = []

        def add_candidate(rep: dict[str, str] | None, basis: str, value: str, score: int) -> None:
            rep_id = clean((rep or {}).get("id"))
            if not rep_id:
                return
            scoped_score = score
            if raw_campaign_id and rep_id in assigned_reps_by_campaign.get(raw_campaign_id, set()):
                scoped_score += 5
            candidates.append(
                {
                    "rep_id": rep_id,
                    "basis": basis,
                    "value": value,
                    "score": scoped_score,
                    "brand_supplied_field_rep_id": clean((rep or {}).get("brand_supplied_field_rep_id")),
                    "full_name": clean((rep or {}).get("full_name")),
                }
            )

        if raw_brand_id:
            add_candidate(fr_by_brand.get(raw_brand_id), "brand_supplied_field_rep_id", raw_brand_id, 100)
            add_candidate(local_user_resolved_by_field_id.get(raw_brand_id), "local_user_field_id", raw_brand_id, 90)
        for candidate_email, basis in ((raw_linked_email, "linked_share_field_rep_email"), (raw_email, "field_rep_email")):
            if candidate_email:
                add_candidate(fr_by_auth_email.get(candidate_email), basis, candidate_email, 95)
                add_candidate(local_user_resolved_by_email.get(candidate_email), f"{basis}_local_user", candidate_email, 92)
        for candidate_id, basis_prefix in ((raw_linked_id, "linked_share_field_rep_id"), (raw_source_id, "source_field_rep_id")):
            if not candidate_id:
                continue
            add_candidate(fr_by_user_id.get(candidate_id), f"{basis_prefix}_matches_campaign_fieldrep_user_id", candidate_id, 90)
            add_candidate(local_user_resolved_by_id.get(candidate_id), f"{basis_prefix}_matches_user_management_user_id", candidate_id, 85)
            add_candidate(fr_by_id.get(candidate_id), f"{basis_prefix}_matches_campaign_fieldrep_id", candidate_id, 60)

        candidates = [candidate for candidate in candidates if candidate["rep_id"]]
        if not candidates:
            return {
                "rep_id": "",
                "status": "missing",
                "basis": "no_field_rep_identity_match",
                "candidate_json": "[]",
            }

        candidates.sort(key=lambda item: (-int(item["score"]), item["rep_id"], item["basis"]))
        top_score = int(candidates[0]["score"])
        top_candidates = [candidate for candidate in candidates if int(candidate["score"]) == top_score]
        top_rep_ids = sorted({candidate["rep_id"] for candidate in top_candidates})
        if len(top_rep_ids) != 1:
            return {
                "rep_id": "",
                "status": "ambiguous",
                "basis": "multiple_equal_field_rep_identity_matches",
                "candidate_json": json.dumps(candidates, sort_keys=True),
            }

        resolved_rep_id = top_rep_ids[0]
        high_trust = [
            candidate
            for candidate in candidates
            if candidate["basis"] in {
                "brand_supplied_field_rep_id",
                "linked_share_field_rep_email",
                "field_rep_email",
                "linked_share_field_rep_email_local_user",
                "field_rep_email_local_user",
            }
        ]
        high_trust_rep_ids = sorted({candidate["rep_id"] for candidate in high_trust})
        has_high_trust_conflict = len(high_trust_rep_ids) > 1
        all_rep_ids = sorted({candidate["rep_id"] for candidate in candidates})
        if has_high_trust_conflict:
            status = "conflict"
            basis = "conflicting_high_trust_field_rep_identity"
        elif len(all_rep_ids) == 1:
            status = "consistent"
            basis = candidates[0]["basis"]
        else:
            status = "resolved"
            basis = candidates[0]["basis"]
        return {
            "rep_id": resolved_rep_id if status != "conflict" else "",
            "status": status,
            "basis": basis,
            "candidate_json": json.dumps(candidates, sort_keys=True),
        }

    identity_rows = []
    for fr in field_reps:
        rep_id = clean(fr.get("id"))
        if not rep_id:
            continue
        base = {
            **source_common("inclinic", INCLINIC_DB, "campaign_fieldrep", fr, "campaign_fieldrep"),
            "inclinic_field_rep_identity_id": stable_uuid("identity", "campaign_fieldrep", rep_id),
            "field_rep_uuid": stable_uuid("field_rep", rep_id),
            "campaign_fieldrep_id": rep_id,
            "brand_supplied_field_rep_id": clean(fr.get("brand_supplied_field_rep_id")),
            "campaign_fieldrep_full_name": clean(fr.get("full_name")),
            "campaign_fieldrep_phone_number": clean(fr.get("phone_number")),
            "campaign_fieldrep_is_active": "1" if truthy(fr.get("is_active")) else "0",
            "campaign_fieldrep_password_hash": clean(fr.get("password_hash")),
            "campaign_fieldrep_created_at": clean(fr.get("created_at")),
            "campaign_fieldrep_updated_at": clean(fr.get("updated_at")),
            "campaign_fieldrep_brand_id": clean(fr.get("brand_id")),
            "campaign_fieldrep_user_id": clean(fr.get("user_id")),
            "campaign_fieldrep_state": clean(fr.get("state")),
            "source_table": "campaign_fieldrep",
            "source_column": "id",
            "source_value": rep_id,
            "source_value_normalized": rep_id.lower(),
            "match_basis": "campaign_fieldrep",
            "phone_normalized": phone(fr.get("phone_number")),
        }
        identity_rows.append(base)

    for user in local_users:
        field_id = clean(user.get("field_id"))
        email = clean(user.get("email")).lower()
        resolved = fr_by_brand.get(field_id) or fr_by_auth_email.get(email)
        rep_id = clean((resolved or {}).get("id"))
        for source_column, value in (("id", user.get("id")), ("field_id", field_id), ("email", email)):
            if not clean(value):
                continue
            identity_rows.append(
                {
                    **source_common("inclinic", INCLINIC_DB, "user_management_user", user, "user_management_user", "resolved" if resolved else "unresolved"),
                    "inclinic_field_rep_identity_id": stable_uuid("identity", "user_management_user", source_column, value),
                    "field_rep_uuid": stable_uuid("field_rep", rep_id) if rep_id else "",
                    "campaign_fieldrep_id": rep_id,
                    "brand_supplied_field_rep_id": clean((resolved or {}).get("brand_supplied_field_rep_id")),
                    "source_table": "user_management_user",
                    "source_column": source_column,
                    "source_value": clean(value),
                    "source_value_normalized": clean(value).lower(),
                    "email_normalized": email,
                    "phone_normalized": phone(user.get("phone_number")),
                    "match_basis": "user_management_user_to_campaign_fieldrep" if resolved else "user_management_user_unresolved",
                    "campaign_fieldrep_full_name": clean((resolved or {}).get("full_name")),
                    "user_management_user_id": clean(user.get("id")),
                    "user_management_email": clean(user.get("email")),
                    "user_management_field_id": field_id,
                }
            )

    for auth in auth_users:
        resolved = fr_by_user_id.get(clean(auth.get("id")))
        rep_id = clean((resolved or {}).get("id"))
        for source_column, value in (("id", auth.get("id")), ("email", auth.get("email"))):
            if not clean(value):
                continue
            identity_rows.append(
                {
                    **source_common("inclinic", INCLINIC_DB, "auth_user", auth, "auth_user", "resolved" if resolved else "unresolved"),
                    "inclinic_field_rep_identity_id": stable_uuid("identity", "auth_user", source_column, value),
                    "field_rep_uuid": stable_uuid("field_rep", rep_id) if rep_id else "",
                    "campaign_fieldrep_id": rep_id,
                    "brand_supplied_field_rep_id": clean((resolved or {}).get("brand_supplied_field_rep_id")),
                    "source_table": "auth_user",
                    "source_column": source_column,
                    "source_value": clean(value),
                    "source_value_normalized": clean(value).lower(),
                    "email_normalized": clean(auth.get("email")).lower(),
                    "match_basis": "auth_user_to_campaign_fieldrep" if resolved else "auth_user_unresolved",
                    "campaign_fieldrep_full_name": clean((resolved or {}).get("full_name")),
                    "auth_user_id": clean(auth.get("id")),
                    "auth_user_email": clean(auth.get("email")),
                }
            )

    local_campaign_rows = []
    for row in local_campaigns:
        local_campaign_id = clean(row.get("id"))
        brand_campaign_id = clean(row.get("brand_campaign_id"))
        if not local_campaign_id and not brand_campaign_id:
            continue
        local_campaign_rows.append(
            {
                **source_common("inclinic", INCLINIC_DB, "campaign_management_campaign", row, "campaign_management_campaign"),
                "inclinic_campaign_uuid": stable_uuid("inclinic_campaign", local_campaign_id or brand_campaign_id),
                "id": local_campaign_id,
                "local_campaign_id": local_campaign_id,
                "legacy_campaign_id": brand_campaign_id,
                "legacy_campaign_id_normalized": norm_key(brand_campaign_id),
                "brand_campaign_id": brand_campaign_id,
                "campaign_uuid": stable_uuid("campaign", norm_key(brand_campaign_id)) if brand_campaign_id else "",
                "name": clean(row.get("name")),
                "brand_name": clean(row.get("brand_name")),
                "company_name": clean(row.get("company_name")),
                "company_logo": clean(row.get("company_logo")),
                "brand_logo": clean(row.get("brand_logo")),
                "start_date": clean(row.get("start_date")),
                "end_date": clean(row.get("end_date")),
                "status": clean(row.get("status")),
                "num_doctors": clean(row.get("num_doctors")),
                "created_at": clean(row.get("created_at")),
                "updated_at": clean(row.get("updated_at")),
            }
        )

    assignment_rows = []
    for row in master_assignments:
        campaign_id = clean(row.get("campaign_id"))
        rep_id = clean(row.get("field_rep_id"))
        fr = fr_by_id.get(rep_id, {})
        assignment_rows.append(
            {
                **source_common("inclinic", INCLINIC_DB, "campaign_campaignfieldrep", row, "campaign_campaignfieldrep"),
                "assignment_uuid": stable_uuid("inclinic_assignment", row.get("id"), campaign_id, rep_id),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)),
                "legacy_campaign_id": campaign_id,
                "legacy_campaign_id_normalized": norm_key(campaign_id),
                "field_rep_uuid": stable_uuid("field_rep", rep_id),
                "campaign_fieldrep_id": rep_id,
                "brand_supplied_field_rep_id": clean(fr.get("brand_supplied_field_rep_id")),
                "assigned_at": clean(row.get("created_at")),
                "assigned_from": clean(row.get("created_at")),
                "assigned_to": "",
                "assignment_status": "active",
                "is_authoritative": "1",
                "old_state": clean(row.get("state")),
                "old_id": clean(row.get("id")),
                "old_field_rep_id": rep_id,
                "old_created_at": clean(row.get("created_at")),
                "old_campaign_id": campaign_id,
            }
        )

    collateral_rows = []
    for row in collaterals:
        local_campaign = local_campaign_by_id.get(clean(row.get("campaign_id")), {})
        brand_campaign_id = clean(local_campaign.get("brand_campaign_id"))
        collateral_id = clean(row.get("id"))
        collateral_rows.append(
            {
                **source_common("inclinic", INCLINIC_DB, "collateral_management_collateral", row, "collateral_management_collateral"),
                "collateral_uuid": stable_uuid("collateral", collateral_id),
                "campaign_uuid": stable_uuid("campaign", norm_key(brand_campaign_id)) if brand_campaign_id else "",
                "content_type_normalized": clean(row.get("type")).lower(),
                "status": "active" if truthy(row.get("is_active")) else "inactive",
                "old_id": collateral_id,
                "old_type": clean(row.get("type")),
                "old_title": clean(row.get("title")),
                "old_file": clean(row.get("file")),
                "old_vimeo_url": clean(row.get("vimeo_url")),
                "old_content_id": clean(row.get("content_id")),
                "old_upload_date": clean(row.get("upload_date")),
                "old_is_active": "1" if truthy(row.get("is_active")) else "0",
                "old_created_at": clean(row.get("created_at")),
                "old_updated_at": clean(row.get("updated_at")),
                "old_campaign_id": clean(row.get("campaign_id")),
                "old_created_by_id": clean(row.get("created_by_id")),
                "old_description": clean(row.get("description")),
                "old_purpose": clean(row.get("purpose")),
                "old_doctor_name": clean(row.get("doctor_name")),
                "old_webinar_date": clean(row.get("webinar_date")),
                "old_webinar_description": clean(row.get("webinar_description")),
                "old_webinar_title": clean(row.get("webinar_title")),
                "old_webinar_url": clean(row.get("webinar_url")),
            }
        )

    campaign_collateral_rows = []
    for row in campaign_collaterals:
        local_campaign = local_campaign_by_id.get(clean(row.get("campaign_id")), {})
        brand_campaign_id = clean(local_campaign.get("brand_campaign_id")) or clean(row.get("campaign_id"))
        collateral_id = clean(row.get("collateral_id"))
        campaign_collateral_rows.append(
            {
                **source_common("inclinic", INCLINIC_DB, "collateral_management_campaigncollateral", row, "campaign_collateral"),
                "campaign_collateral_uuid": stable_uuid("campaign_collateral", row.get("id"), brand_campaign_id, collateral_id),
                "campaign_uuid": stable_uuid("campaign", norm_key(brand_campaign_id)),
                "legacy_campaign_id": brand_campaign_id,
                "collateral_uuid": stable_uuid("collateral", collateral_id),
                "old_id": clean(row.get("id")),
                "old_start_date": clean(row.get("start_date")),
                "old_end_date": clean(row.get("end_date")),
                "old_created_at": clean(row.get("created_at")),
                "old_updated_at": clean(row.get("updated_at")),
                "old_campaign_id": clean(row.get("campaign_id")),
                "old_collateral_id": collateral_id,
            }
        )

    exception_rows = []
    share_rows = []
    for row in share_logs:
        email = clean(row.get("field_rep_email")).lower()
        source_rep_id = clean(row.get("field_rep_id"))
        email_match = ""
        campaign_id = clean(row.get("brand_campaign_id"))
        resolution = resolve_field_rep_identity(
            source_field_rep_id=source_rep_id,
            email=email,
            campaign_id=campaign_id,
        )
        rep_id = clean(resolution.get("rep_id"))
        fr = fr_by_id.get(rep_id)
        auth = auth_by_id.get(clean((fr or {}).get("user_id")), {})
        if email and auth:
            email_match = "1" if email == clean(auth.get("email")).lower() else "0"
        if resolution["status"] in {"missing", "ambiguous", "conflict"}:
            exception_rows.append(
                {
                    "exception_id": str(len(exception_rows) + 1),
                    "migration_batch_id": RUN_ID,
                    "system_name": "inclinic",
                    "database_name": INCLINIC_DB,
                    "source_table": "sharing_management_sharelog",
                    "source_pk_column": "id",
                    "source_pk_value": clean(row.get("id")),
                    "entity_type": "share_event",
                    "issue_code": f"SHARE_FIELD_REP_IDENTITY_{resolution['status'].upper()}",
                    "issue_details": resolution["candidate_json"],
                    "raw_payload_json": json.dumps(row, sort_keys=True),
                    "resolution_status": "open",
                }
            )
        doctor_phone = phone(row.get("doctor_identifier"))
        share_rows.append(
            {
                **source_common(
                    "inclinic",
                    INCLINIC_DB,
                    "sharing_management_sharelog",
                    row,
                    clean(resolution.get("basis")),
                    "verified" if rep_id else "unresolved",
                ),
                "share_event_uuid": stable_uuid("share_event", row.get("id")),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)) if campaign_id else "",
                "legacy_campaign_id": campaign_id,
                "collateral_uuid": stable_uuid("collateral", row.get("collateral_id")) if clean(row.get("collateral_id")) else "",
                "doctor_uuid": stable_uuid("doctor_phone", doctor_phone) if doctor_phone else "",
                "inclinic_doctor_uuid": "",
                "doctor_phone_normalized": doctor_phone,
                "shared_by_field_rep_uuid": stable_uuid("field_rep", rep_id) if fr else "",
                "campaign_fieldrep_id": rep_id,
                "source_field_rep_id": source_rep_id,
                "field_rep_email_normalized": email,
                "field_rep_email_matches_campaign_fieldrep": email_match,
                "field_rep_identifier_consistency_status": clean(resolution.get("status")),
                "field_rep_resolution_basis": clean(resolution.get("basis")),
                "field_rep_resolution_candidates_json": clean(resolution.get("candidate_json")),
                "share_channel_normalized": clean(row.get("share_channel")).lower(),
                "shared_at": clean(row.get("share_timestamp")),
                "old_id": clean(row.get("id")),
                "old_share_channel": clean(row.get("share_channel")),
                "old_share_timestamp": clean(row.get("share_timestamp")),
                "old_message_text": clean(row.get("message_text")),
                "old_created_at": clean(row.get("created_at")),
                "old_updated_at": clean(row.get("updated_at")),
                "old_short_link_id": clean(row.get("short_link_id")),
                "old_collateral_id": clean(row.get("collateral_id")),
                "old_doctor_identifier": clean(row.get("doctor_identifier")),
                "old_brand_campaign_id": campaign_id,
                "old_field_rep_email": clean(row.get("field_rep_email")),
                "old_field_rep_id": source_rep_id,
            }
        )

    transaction_rows = []
    for row in transactions:
        source_rep_id = clean(row.get("field_rep_id"))
        brand_rep_id = clean(row.get("field_rep_unique_id"))
        linked_share = share_by_id.get(clean(row.get("sm_engagement_id"))) or share_by_id.get(clean(row.get("share_management_engagement_id"))) or {}
        linked_email = clean(linked_share.get("field_rep_email")).lower()
        linked_source_rep_id = clean(linked_share.get("field_rep_id"))
        campaign_id = clean(row.get("brand_campaign_id"))
        resolution = resolve_field_rep_identity(
            source_field_rep_id=source_rep_id,
            brand_supplied_id=brand_rep_id,
            email=row.get("field_rep_email"),
            campaign_id=campaign_id,
            linked_source_field_rep_id=linked_source_rep_id,
            linked_email=linked_email,
        )
        rep_id = clean(resolution.get("rep_id"))
        fr = fr_by_id.get(rep_id)
        brand_fr = fr_by_brand.get(brand_rep_id) if brand_rep_id else None
        status = clean(resolution.get("status"))
        basis = clean(resolution.get("basis"))
        resolved_uuid = stable_uuid("field_rep", rep_id) if rep_id else ""
        if status in {"missing", "ambiguous", "conflict"}:
            exception_rows.append(
                {
                    "exception_id": str(len(exception_rows) + 1),
                    "migration_batch_id": RUN_ID,
                    "system_name": "inclinic",
                    "database_name": INCLINIC_DB,
                    "source_table": "sharing_management_collateraltransaction",
                    "source_pk_column": "id",
                    "source_pk_value": clean(row.get("id")),
                    "entity_type": "collateral_transaction",
                    "issue_code": f"TRANSACTION_FIELD_REP_IDENTITY_{status.upper()}",
                    "issue_details": clean(resolution.get("candidate_json")),
                    "raw_payload_json": json.dumps(row, sort_keys=True),
                    "resolution_status": "open",
                }
            )
        doctor_phone = phone(row.get("doctor_number"))
        tx_id = clean(row.get("id"))
        transaction_rows.append(
            {
                **source_common("inclinic", INCLINIC_DB, "sharing_management_collateraltransaction", row, basis, "verified" if resolved_uuid else "unresolved"),
                "transaction_uuid": stable_uuid("collateral_transaction", tx_id),
                "campaign_uuid": stable_uuid("campaign", norm_key(campaign_id)) if campaign_id else "",
                "legacy_campaign_id": campaign_id,
                "collateral_uuid": stable_uuid("collateral", row.get("collateral_id")) if clean(row.get("collateral_id")) else "",
                "doctor_uuid": stable_uuid("doctor_phone", doctor_phone) if doctor_phone else "",
                "inclinic_doctor_uuid": "",
                "doctor_phone_normalized": doctor_phone,
                "field_rep_uuid_from_campaign_fieldrep_id": stable_uuid("field_rep", rep_id) if fr else "",
                "field_rep_uuid_from_brand_supplied_id": stable_uuid("field_rep", brand_fr.get("id")) if brand_fr else "",
                "resolved_field_rep_uuid": resolved_uuid,
                "campaign_fieldrep_id": rep_id,
                "source_field_rep_id": source_rep_id,
                "brand_supplied_field_rep_id": brand_rep_id,
                "linked_share_field_rep_id": linked_source_rep_id,
                "linked_share_field_rep_email": linked_email,
                "field_rep_email_normalized": clean(row.get("field_rep_email")).lower() or linked_email,
                "field_rep_identifier_consistency_status": status,
                "field_rep_resolution_basis": basis,
                "field_rep_resolution_candidates_json": clean(resolution.get("candidate_json")),
                "activity_summary_status": "viewed" if truthy(row.get("has_viewed")) or clean(row.get("viewed_at")) or clean(row.get("first_viewed_at")) else "sent",
                **{f"old_{key}": clean(value) for key, value in row.items() if not key.startswith("_")},
            }
        )

    return {
        "inclinic_field_rep_identity_v2": identity_rows,
        "inclinic_campaign_v2": local_campaign_rows,
        "inclinic_campaign_field_rep_assignment_v2": assignment_rows,
        "inclinic_assigned_doctor_roster_v2": master_v2["doctor_field_rep_roster_bridge_v2"],
        "inclinic_collateral_v2": collateral_rows,
        "inclinic_campaign_collateral_v2": campaign_collateral_rows,
        "inclinic_share_event_v2": share_rows,
        "inclinic_collateral_transaction_v2": transaction_rows,
        "migration_exception_v2": exception_rows,
        "campaign_management_campaign": local_campaigns,
        "campaign_management_campaignassignment": latest_rows(data.get("raw_server2.campaign_management_campaignassignment", [])),
        "admin_dashboard_fieldrepcampaign": latest_rows(data.get("raw_server2.admin_dashboard_fieldrepcampaign", [])),
        "doctor_viewer_doctor": latest_rows(data.get("raw_server2.doctor_viewer_doctor", [])),
        "sharing_management_sharelog": share_logs,
        "sharing_management_collateraltransaction": transactions,
        "collateral_management_collateral": collaterals,
        "collateral_management_campaigncollateral": campaign_collaterals,
        "user_management_user": local_users,
    }


def build_pe_portal_v2(data: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, Any]]]:
    mappings = {
        "raw_pe_portal.sharing_doctorsharesummary_raw": "pe_doctor_share_summary_v2",
        "raw_pe_portal.sharing_shareactivity_raw": "pe_share_event_v2",
        "raw_pe_portal.sharing_shareplaybackevent_raw": "pe_playback_event_v2",
        "raw_pe_portal.sharing_sharebannerclickevent_raw": "pe_banner_click_event_v2",
        "raw_pe_portal.publisher_campaign_raw": "pe_campaign_v2",
    }
    out: dict[str, list[dict[str, Any]]] = {}
    for source_name, table_name in mappings.items():
        out[table_name] = latest_rows(data.get(source_name, []), "public_id" if "shareactivity" in source_name else "id")
    if not out["pe_campaign_v2"]:
        out["pe_campaign_v2"] = [
            {
                "campaign_id": clean(row.get("id")),
                "new_video_cluster_name": clean(row.get("name")),
                "selection_json": "{}",
                "doctors_supported": clean(row.get("num_doctors_supported")),
                "banner_small": clean(row.get("banner_small_url")),
                "banner_large": clean(row.get("banner_large_url")),
                "banner_target_url": clean(row.get("banner_target_url")),
                "start_date": clean(row.get("start_date")),
                "end_date": clean(row.get("end_date")),
                "video_cluster_id": "",
                "publisher_sub": "",
                "publisher_username": "",
                "publisher_roles": "",
                "email_registration": "",
                "wa_addition": "",
                "created_at": clean(row.get("created_at")),
                "updated_at": clean(row.get("updated_at")),
                "_source_table": "pe_campaign_v2",
            }
            for row in latest_rows(data.get("raw_server1.campaign_campaign", []))
            if clean(row.get("id")) and truthy(row.get("system_pe"))
        ]
    for source_name in (
        "raw_pe_portal.catalog_therapyarea_raw",
        "raw_pe_portal.catalog_triggercluster_raw",
        "raw_pe_portal.catalog_trigger_raw",
        "raw_pe_portal.catalog_video_raw",
        "raw_pe_portal.catalog_videolanguage_raw",
        "raw_pe_portal.catalog_videocluster_raw",
        "raw_pe_portal.catalog_videoclusterlanguage_raw",
        "raw_pe_portal.catalog_videoclustervideo_raw",
        "raw_pe_portal.catalog_videotriggermap_raw",
    ):
        table_name = source_name.split(".", 1)[1].removesuffix("_raw")
        out[table_name] = latest_rows(data.get(source_name, []))
    return out
