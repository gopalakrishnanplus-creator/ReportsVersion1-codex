#!/usr/bin/env bash
set -e

PROJECT_DIR=/var/www/ReportsVersion1
VENV_DIR=/var/www/venv
PYTHON=$VENV_DIR/bin/python
PIP=$VENV_DIR/bin/pip

cd "$PROJECT_DIR"

echo "[deploy] Ensuring venv exists..."
if [ ! -d "$VENV_DIR" ]; then
  python3 -m venv "$VENV_DIR"
fi

echo "[deploy] Installing requirements..."
$PIP install --upgrade pip
$PIP install -r requirements.txt

echo "[deploy] Ensuring production env is preserved..."
if [ -f "$PROJECT_DIR/.env.prod" ]; then
  cp -f "$PROJECT_DIR/.env.prod" "$PROJECT_DIR/.env"
else
  if [ ! -f "$PROJECT_DIR/.env" ] && [ -f "$PROJECT_DIR/.env.example" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
  fi
fi

echo "[deploy] Loading environment variables..."
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$PROJECT_DIR/.env"
  set +a
fi

echo "[deploy] Running Django migrations..."
$PYTHON manage.py migrate --noinput --fake-initial

echo "[deploy] Deployment complete (install + migrate only)."

echo "[deploy] Running Django migrations..."
$PYTHON manage.py migrate --noinput --fake-initial

echo "[deploy] Restarting gunicorn service..."
sudo systemctl restart gunicorn

echo "[deploy] Deployment complete."
