#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "[setup] .env not found. Please create it (or copy from template) before running setup."
  exit 1
fi

# Load environment variables from .env
set -a
# shellcheck disable=SC1091
source .env
set +a

echo "[setup] Creating virtual environment..."
python3 -m venv .venv

# shellcheck disable=SC1091
source .venv/bin/activate

echo "[setup] Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

if command -v docker >/dev/null 2>&1; then
  if ! docker ps --format '{{.Names}}' | rg -x 'reports-postgres' >/dev/null 2>&1; then
    if docker ps -a --format '{{.Names}}' | rg -x 'reports-postgres' >/dev/null 2>&1; then
      echo "[setup] Starting existing Docker container: reports-postgres"
      docker start reports-postgres >/dev/null
    else
      echo "[setup] Creating PostgreSQL Docker container: reports-postgres"
      docker run --name reports-postgres \
        -e POSTGRES_USER="${POSTGRES_USER}" \
        -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
        -e POSTGRES_DB="${POSTGRES_DB}" \
        -p "${POSTGRES_PORT}:5432" \
        -d postgres:16 >/dev/null
    fi
  else
    echo "[setup] Docker container reports-postgres is already running"
  fi
else
  echo "[setup] Docker not found; expecting a local PostgreSQL server at ${POSTGRES_HOST}:${POSTGRES_PORT}."
fi

echo "[setup] Running Django system checks..."
python manage.py check

echo "[setup] Running ETL..."
python manage.py run_etl

echo "[setup] Setup complete. Start app with:"
echo "  source .venv/bin/activate"
echo "  python manage.py runserver"
echo "[setup] URLs:"
echo "  Dashboard: http://127.0.0.1:8000/"
echo "  Campaign view: http://127.0.0.1:8000/campaign/<brand_campaign_id>/"
echo "  Admin: http://127.0.0.1:8000/admin/"
