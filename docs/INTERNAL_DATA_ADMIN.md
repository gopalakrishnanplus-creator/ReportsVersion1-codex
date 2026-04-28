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

The dashboard exposes reporting schemas only: `raw_*`, `bronze`, `silver*`, `gold_*`, `control`, and `ops`. Django auth/session/admin tables are excluded.

Safety behavior:

- Mutations require login and CSRF protection.
- Create, update, and delete actions run inside database transactions.
- Every mutation writes an audit row to `ops.internal_dashboard_audit`.
- Row links use signed locators so table and row identity cannot be tampered with casually.
- Delete requires a typed confirmation phrase and a reason.
- Delete is blocked when foreign key or known reporting dependencies are detected.
