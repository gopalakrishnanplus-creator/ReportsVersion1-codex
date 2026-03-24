#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=${PROJECT_DIR:-/var/www/ReportsVersion1}
VENV_DIR=${VENV_DIR:-/var/www/venv}
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"
DJANGO_SETTINGS_MODULE=${DJANGO_SETTINGS_MODULE:-config.settings.prod}
ENV_FILE=${ENV_FILE:-/var/www/secrets/.env}
RUN_ETL_ON_DEPLOY=${RUN_ETL_ON_DEPLOY:-1}
RUN_ETL_CONTINUE_ON_ERROR=${RUN_ETL_CONTINUE_ON_ERROR:-1}
RUN_SAPA_GROWTH_ETL_ON_DEPLOY=${RUN_SAPA_GROWTH_ETL_ON_DEPLOY:-1}
RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR=${RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR:-1}
GUNICORN_SERVICE=${GUNICORN_SERVICE:-gunicorn}


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

echo "[deploy] Runtime summary:"
echo "         DJANGO_SETTINGS_MODULE=$DJANGO_SETTINGS_MODULE"
echo "         POSTGRES_HOST=${POSTGRES_HOST:-${DB_HOST:-localhost}}"
echo "         POSTGRES_PORT=${POSTGRES_PORT:-${DB_PORT:-5432}}"
echo "         RUN_ETL_ON_DEPLOY=$RUN_ETL_ON_DEPLOY"
echo "         RUN_SAPA_GROWTH_ETL_ON_DEPLOY=$RUN_SAPA_GROWTH_ETL_ON_DEPLOY"

echo "[deploy] Running Django migrations with $DJANGO_SETTINGS_MODULE..."
"$PYTHON" manage.py migrate --noinput --fake-initial

if [ "$RUN_ETL_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Running ETL..."
  if ! "$PYTHON" manage.py run_etl; then
    if [ "$RUN_ETL_CONTINUE_ON_ERROR" = "1" ]; then
      echo "[deploy] WARNING: run_etl failed, continuing because RUN_ETL_CONTINUE_ON_ERROR=1"
    else
      echo "[deploy] ERROR: run_etl failed. Set RUN_ETL_CONTINUE_ON_ERROR=1 to continue deployment anyway."
      exit 1
    fi
  fi
else
  echo "[deploy] Skipping ETL (RUN_ETL_ON_DEPLOY=$RUN_ETL_ON_DEPLOY)"
fi

if [ "$RUN_SAPA_GROWTH_ETL_ON_DEPLOY" = "1" ]; then
  echo "[deploy] Running SAPA Growth ETL..."
  if ! "$PYTHON" manage.py run_sapa_growth_etl; then
    if [ "$RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR" = "1" ]; then
      echo "[deploy] WARNING: run_sapa_growth_etl failed, continuing because RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR=1"
    else
      echo "[deploy] ERROR: run_sapa_growth_etl failed. Set RUN_SAPA_GROWTH_ETL_CONTINUE_ON_ERROR=1 to continue deployment anyway."
      exit 1
    fi
  fi
else
  echo "[deploy] Skipping SAPA Growth ETL (RUN_SAPA_GROWTH_ETL_ON_DEPLOY=$RUN_SAPA_GROWTH_ETL_ON_DEPLOY)"
fi

echo "[deploy] Collecting static files..."
"$PYTHON" manage.py collectstatic --noinput

echo "[deploy] Restarting gunicorn service: $GUNICORN_SERVICE"
sudo systemctl restart "$GUNICORN_SERVICE"

echo "[deploy] Deployment complete."
