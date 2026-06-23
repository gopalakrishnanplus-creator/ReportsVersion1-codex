#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=${PROJECT_DIR:-/var/www/ReportsVersion1}
VENV_DIR=${VENV_DIR:-/var/www/venv}
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"
DJANGO_SETTINGS_MODULE=${DJANGO_SETTINGS_MODULE:-config.settings.prod}
ENV_FILE=${ENV_FILE:-/var/www/secrets/.env}
GUNICORN_SERVICE=${GUNICORN_SERVICE:-gunicorn}

DEPLOY_STATE_DIR=${DEPLOY_STATE_DIR:-/var/www/reports_deploy_state}
REPORTING_V1_TO_V2_MARKER=${REPORTING_V1_TO_V2_MARKER:-$DEPLOY_STATE_DIR/reporting_v1_to_v2_migration.done}
BACKUP_DIR=${BACKUP_DIR:-/var/www/reports_db_backups}

RUN_ETL_ON_DEPLOY=${RUN_ETL_ON_DEPLOY:-1}
RUN_ETL_CONTINUE_ON_ERROR=${RUN_ETL_CONTINUE_ON_ERROR:-0}

RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY=${RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY:-0}
RUN_REPORTING_V1_TO_V2_MIGRATION_CONTINUE_ON_ERROR=${RUN_REPORTING_V1_TO_V2_MIGRATION_CONTINUE_ON_ERROR:-0}
FORCE_REPORTING_V1_TO_V2_MIGRATION=${FORCE_REPORTING_V1_TO_V2_MIGRATION:-0}
SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY=${SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY:-1}

RUN_SAPA_GROWTH_ETL_ON_DEPLOY=${RUN_SAPA_GROWTH_ETL_ON_DEPLOY:-0}
RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR=${RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR:-0}

RUN_PE_REPORTS_ETL_ON_DEPLOY=${RUN_PE_REPORTS_ETL_ON_DEPLOY:-0}
RUN_PE_REPORTS_ETL_CONTINUE_ON_ERROR=${RUN_PE_REPORTS_ETL_CONTINUE_ON_ERROR:-0}

BACKUP_REPORTING_DB_ON_DEPLOY=${BACKUP_REPORTING_DB_ON_DEPLOY:-1}
ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP=${ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP:-0}
DEPLOY_HEARTBEAT_SECONDS=${DEPLOY_HEARTBEAT_SECONDS:-60}

run_with_heartbeat() {
  local label="$1"
  shift
  local status_file
  status_file="$(mktemp)"

  (
    set +e
    "$@"
    echo "$?" > "$status_file"
  ) &
  local cmd_pid=$!

  while [ ! -s "$status_file" ]; do
    sleep "$DEPLOY_HEARTBEAT_SECONDS"
    if [ -s "$status_file" ]; then
      break
    fi
    if ! kill -0 "$cmd_pid" 2>/dev/null; then
      wait "$cmd_pid" || true
      rm -f "$status_file"
      return 1
    fi
    echo "[deploy] Still running: $label ($(date -Is))"
  done

  wait "$cmd_pid" || true
  local status
  status="$(cat "$status_file")"
  rm -f "$status_file"
  return "$status"
}

cd "$PROJECT_DIR"

echo "[deploy] Ensuring venv exists..."
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

echo "[deploy] Installing requirements..."
"$PIP" install --upgrade pip
"$PIP" install -r requirements.txt

echo "[deploy] Resolving environment file..."
if [ -f "$ENV_FILE" ]; then
  ACTIVE_ENV_FILE="$ENV_FILE"
elif [ -f "$PROJECT_DIR/.env.prod" ]; then
  ACTIVE_ENV_FILE="$PROJECT_DIR/.env.prod"
elif [ -f "$PROJECT_DIR/.env" ]; then
  ACTIVE_ENV_FILE="$PROJECT_DIR/.env"
else
  echo "[deploy] ERROR: no env file found. Checked: $ENV_FILE, $PROJECT_DIR/.env.prod, $PROJECT_DIR/.env"
  exit 1
fi

echo "[deploy] Loading environment variables from $ACTIVE_ENV_FILE"
set -a
# shellcheck disable=SC1090
source "$ACTIVE_ENV_FILE"
set +a

export DJANGO_SETTINGS_MODULE
export ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP

mkdir -p "$DEPLOY_STATE_DIR"
mkdir -p "$BACKUP_DIR"

echo "[deploy] Runtime summary:"
echo "         DJANGO_SETTINGS_MODULE=$DJANGO_SETTINGS_MODULE"
echo "         POSTGRES_HOST=${POSTGRES_HOST:-${DB_HOST:-localhost}}"
echo "         POSTGRES_PORT=${POSTGRES_PORT:-${DB_PORT:-5432}}"
echo "         POSTGRES_DB=${POSTGRES_DB:-${DB_NAME:-}}"
echo "         INCLINIC_REPORTING_SOURCE_MODE=${INCLINIC_REPORTING_SOURCE_MODE:-v2}"
echo "         INCLINIC_REPORTING_REFRESH_RAW_V2_FROM_SOURCE=${INCLINIC_REPORTING_REFRESH_RAW_V2_FROM_SOURCE:-1}"
echo "         INCLINIC_REPORTING_ENABLE_LEGACY_V2_FALLBACKS=${INCLINIC_REPORTING_ENABLE_LEGACY_V2_FALLBACKS:-0}"
echo "         SAPA_ENABLE_LEGACY_V2_FALLBACKS=${SAPA_ENABLE_LEGACY_V2_FALLBACKS:-0}"
echo "         PE_REPORTS_ENABLE_LEGACY_V2_FALLBACKS=${PE_REPORTS_ENABLE_LEGACY_V2_FALLBACKS:-0}"
echo "         RUN_ETL_ON_DEPLOY=$RUN_ETL_ON_DEPLOY"
echo "         RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY=$RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY"
echo "         SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY=$SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY"
echo "         RUN_SAPA_GROWTH_ETL_ON_DEPLOY=$RUN_SAPA_GROWTH_ETL_ON_DEPLOY"
echo "         RUN_PE_REPORTS_ETL_ON_DEPLOY=$RUN_PE_REPORTS_ETL_ON_DEPLOY"
echo "         ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP=$ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP"

if [ "$ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP" != "0" ]; then
  echo "[deploy] ERROR: ENABLE_SOURCE_TRANSFER_DELETE_CLEANUP must remain 0 during deployment."
  echo "[deploy] Refusing deploy because source-delete cleanup is enabled."
  exit 1
fi

if [ "$BACKUP_REPORTING_DB_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Creating PostgreSQL backup before migrations/ETL..."
  if ! command -v pg_dump >/dev/null 2>&1; then
    echo "[deploy] ERROR: pg_dump is not installed. Install postgresql-client or set BACKUP_REPORTING_DB_ON_DEPLOY=0 only if a separate RDS snapshot exists."
    exit 1
  fi

  BACKUP_FILE="$BACKUP_DIR/reports_$(date +%Y%m%d_%H%M%S).dump"
  PGPASSWORD="${POSTGRES_PASSWORD:-${DB_PASSWORD:-}}" pg_dump \
    --host="${POSTGRES_HOST:-${DB_HOST:-localhost}}" \
    --port="${POSTGRES_PORT:-${DB_PORT:-5432}}" \
    --username="${POSTGRES_USER:-${DB_USER:-postgres}}" \
    --dbname="${POSTGRES_DB:-${DB_NAME:-reports}}" \
    --format=custom \
    --no-owner \
    --file="$BACKUP_FILE"

  echo "[deploy] PostgreSQL backup created: $BACKUP_FILE"
else
  echo "[deploy] Skipping PostgreSQL backup (BACKUP_REPORTING_DB_ON_DEPLOY=$BACKUP_REPORTING_DB_ON_DEPLOY)"
fi

echo "[deploy] Running Django migrations with $DJANGO_SETTINGS_MODULE..."
"$PYTHON" manage.py migrate --noinput --fake-initial

if [ "$RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY" = "1" ]; then
  if [ -f "$REPORTING_V1_TO_V2_MARKER" ] && [ "$FORCE_REPORTING_V1_TO_V2_MIGRATION" != "1" ]; then
    echo "[deploy] Reporting V1 -> V2 migration already completed. Marker exists: $REPORTING_V1_TO_V2_MARKER"
    echo "[deploy] Skipping migration. Set FORCE_REPORTING_V1_TO_V2_MIGRATION=1 only if you intentionally want to rerun it."
  else
    echo "[deploy] Running one-time reporting V1 -> V2 migration/backfill..."
    if "$PYTHON" manage.py migrate_v1_to_v2 --skip-reporting-rebuild; then
      date -Is > "$REPORTING_V1_TO_V2_MARKER"
      echo "[deploy] Reporting V1 -> V2 migration completed. Marker written: $REPORTING_V1_TO_V2_MARKER"
    else
      if [ "$RUN_REPORTING_V1_TO_V2_MIGRATION_CONTINUE_ON_ERROR" = "1" ]; then
        echo "[deploy] WARNING: migrate_v1_to_v2 failed, continuing because RUN_REPORTING_V1_TO_V2_MIGRATION_CONTINUE_ON_ERROR=1"
      else
        echo "[deploy] ERROR: migrate_v1_to_v2 failed."
        exit 1
      fi
    fi
  fi
else
  echo "[deploy] Skipping reporting V1 -> V2 migration (RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY=$RUN_REPORTING_V1_TO_V2_MIGRATION_ON_DEPLOY)"
fi

if [ "$SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Seeding reviewed InClinic reporting corrections..."
  "$PYTHON" manage.py seed_inclinic_reporting_corrections --preset apex-83ce-week5
else
  echo "[deploy] Skipping InClinic reporting correction seed (SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY=$SEED_INCLINIC_REPORTING_CORRECTIONS_ON_DEPLOY)"
fi

if [ "$RUN_ETL_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Running InClinic reporting ETL..."
  if ! run_with_heartbeat "InClinic reporting ETL" "$PYTHON" manage.py run_etl; then
    if [ "$RUN_ETL_CONTINUE_ON_ERROR" = "1" ]; then
      echo "[deploy] WARNING: run_etl failed, continuing because RUN_ETL_CONTINUE_ON_ERROR=1"
    else
      echo "[deploy] ERROR: run_etl failed."
      exit 1
    fi
  fi
else
  echo "[deploy] Skipping InClinic ETL (RUN_ETL_ON_DEPLOY=$RUN_ETL_ON_DEPLOY)"
fi

if [ "$RUN_SAPA_GROWTH_ETL_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Running SAPA Growth ETL..."
  if ! run_with_heartbeat "SAPA Growth ETL" "$PYTHON" manage.py run_sapa_growth_etl; then
    if [ "$RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR" = "1" ]; then
      echo "[deploy] WARNING: run_sapa_growth_etl failed, continuing because RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR=1"
    else
      echo "[deploy] ERROR: run_sapa_growth_etl failed."
      exit 1
    fi
  fi
else
  echo "[deploy] Skipping SAPA Growth ETL (RUN_SAPA_GROWTH_ETL_ON_DEPLOY=$RUN_SAPA_GROWTH_ETL_ON_DEPLOY)"
fi

if [ "$RUN_PE_REPORTS_ETL_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Running PE Reports ETL..."
  if ! run_with_heartbeat "PE Reports ETL" "$PYTHON" manage.py run_pe_reports_etl; then
    if [ "$RUN_PE_REPORTS_ETL_CONTINUE_ON_ERROR" = "1" ]; then
      echo "[deploy] WARNING: run_pe_reports_etl failed, continuing because RUN_PE_REPORTS_ETL_CONTINUE_ON_ERROR=1"
    else
      echo "[deploy] ERROR: run_pe_reports_etl failed."
      exit 1
    fi
  fi
else
  echo "[deploy] Skipping PE Reports ETL (RUN_PE_REPORTS_ETL_ON_DEPLOY=$RUN_PE_REPORTS_ETL_ON_DEPLOY)"
fi

echo "[deploy] Collecting static files..."
"$PYTHON" manage.py collectstatic --noinput

echo "[deploy] Restarting gunicorn service: $GUNICORN_SERVICE"
sudo systemctl restart "$GUNICORN_SERVICE"

echo "[deploy] Deployment complete."
