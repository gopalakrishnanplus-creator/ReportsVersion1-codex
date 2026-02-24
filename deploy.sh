#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=${PROJECT_DIR:-/var/www/ReportsVersion1}
VENV_DIR=${VENV_DIR:-/var/www/venv}
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"
DJANGO_SETTINGS_MODULE=${DJANGO_SETTINGS_MODULE:-config.settings.prod}
ENV_FILE=${ENV_FILE:-/var/www/secrets/.env}

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

echo "[deploy] Running Django migrations with $DJANGO_SETTINGS_MODULE..."
"$PYTHON" manage.py migrate --noinput --fake-initial

echo "[deploy] Collecting static files..."
"$PYTHON" manage.py collectstatic --noinput

echo "[deploy] Restarting gunicorn service..."
sudo systemctl restart gunicorn

echo "[deploy] Deployment complete."
