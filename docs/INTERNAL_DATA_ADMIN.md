# Internal Data Admin

The internal PostgreSQL dashboard is intentionally hidden from normal navigation.

Local URL:

```text
http://127.0.0.1:8000/_internal/data-admin/
```

Local credentials are read from `.env`:

```text
INTERNAL_DATA_ADMIN_USERNAME=internal_admin
INTERNAL_DATA_ADMIN_PASSWORD=ChangeMeLocalOnly!
```

The dashboard exposes reporting schemas only: `raw_*`, `bronze*`, `silver*`, `gold_*`, `control`, and `ops`. Django auth/session/admin tables are excluded.

System grouping:

- Inclinic: `raw_server1`, `raw_server2`, `bronze`, `silver`, `gold_global`, and `gold_campaign_*`.
- SAPA / RFA: `raw_sapa_mysql`, `raw_sapa_api`, `bronze_sapa`, `silver_sapa`, `gold_sapa`, and `gold_sapa_stage`.
- Patient Education: `raw_pe_master`, `raw_pe_portal`, `bronze_pe`, `silver_pe`, `gold_pe_global`, and `gold_pe_campaign_*`.
- Shared Ops / Control: `control` and `ops`.

Cleanup rule of thumb:

You usually do not need to delete from every table. Start with source or RAW records for the relevant system, then rerun that system's ETL so BRONZE, SILVER, and GOLD are rebuilt consistently. Direct deletion from derived tables is mostly for clearing stale or corrupt report output before a full rebuild.

Bulk delete:

Open a table, select multiple rows, choose "Review Selected Delete", inspect dependency warnings, then confirm with the exact phrase shown on screen. Bulk delete is table-scoped, transaction-wrapped, and audited one row at a time.

Hierarchy cleanup planner:

Use the planner when clutter belongs to one campaign, source entity, or campaign GOLD schema and should be cleared consistently across layers.

```text
http://127.0.0.1:8000/_internal/data-admin/cleanup/
```

Select the system, choose the earliest layer to clear, and enter the campaign/entity key. The planner previews every matching table before deletion.

- RAW means RAW, BRONZE, SILVER, and GOLD are included.
- BRONZE means BRONZE, SILVER, and GOLD are included.
- SILVER means SILVER and GOLD are included.
- GOLD means only GOLD/report output is included.

Deletes execute downstream first: `GOLD -> SILVER -> BRONZE -> RAW`. This avoids manually deleting from every table and reduces dependency mistakes. If RAW/source rows remain and the ETL is rerun, derived rows can be recreated; use RAW cleanup for permanent source removal.

Batch campaign cleanup:

Use batch cleanup when campaign IDs span more than one reporting system, or when you want to clean many campaigns in one reviewed plan.

Modes:

- Delete listed campaigns: enter campaign IDs to delete; all other campaign records are kept.
- Keep listed campaigns, delete the rest: enter campaign IDs that must remain; other campaign-scoped rows in selected systems are planned for deletion.

Batch cleanup can include Inclinic, SAPA/RFA, and Patient Education together. It only targets rows with recognized campaign identity columns, plus non-kept campaign GOLD schemas. Always review the preview table before execution, especially in keep-list mode.

Safety behavior:

- Mutations require login and CSRF protection.
- Create, update, and delete actions run inside database transactions.
- Every mutation writes an audit row to `ops.internal_dashboard_audit`.
- Row links use signed locators so table and row identity cannot be tampered with casually.
- Delete requires a typed confirmation phrase and a reason.
- Bulk delete requires a typed confirmation phrase and a reason.
- Hierarchy cleanup requires a typed confirmation phrase and a reason.
- Batch cleanup requires a typed confirmation phrase and a reason.
- Delete is blocked when foreign key or known reporting dependencies are detected.
