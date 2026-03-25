from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[2]
BASE_DIR = Path(BASE_DIR)


def _load_dotenv() -> None:
    """Load environment variables from candidate dotenv files.

    Precedence (first existing file wins):
    1) DJANGO_ENV_FILE (explicit path override)
    2) /var/www/secrets/.env (EC2 deployment default)
    3) <repo>/.env (local development fallback)
    """
    explicit = os.getenv("DJANGO_ENV_FILE")
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.append(Path("/var/www/secrets/.env"))
    candidates.append(Path(BASE_DIR) / ".env")

    env_path = next((c for c in candidates if c.exists()), None)
    if env_path is None:
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if not key:
            continue
        # Keep explicit process env precedence when already set.
        os.environ.setdefault(key, value)


def _env(*names: str, default: str = "") -> str:
    """Return the first non-empty env value from a list of variable names."""
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value
    return default


def _env_int(*names: str, default: int) -> int:
    value = _env(*names, default=str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_bool(*names: str, default: bool = False) -> bool:
    value = _env(*names, default="1" if default else "0").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


_load_dotenv()
SECRET_KEY = _env("DJANGO_SECRET_KEY", default="dev-secret-key")
DEBUG = _env("DJANGO_DEBUG", default="0") == "1"


def _csv_env(*names: str, default: list[str] | None = None) -> list[str]:
    raw = _env(*names, default="")
    if not raw:
        return list(default or [])
    return [item.strip() for item in raw.split(",") if item.strip()]


ALLOWED_HOSTS = _csv_env(
    "DJANGO_ALLOWED_HOSTS",
    default=["reports.inditech.co.in", "localhost", "127.0.0.1"],
)
CSRF_TRUSTED_ORIGINS = _csv_env(
    "DJANGO_CSRF_TRUSTED_ORIGINS",
    default=[
        "https://reports.inditech.co.in",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "etl",
    "dashboard",
    "sapa_growth",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _env("POSTGRES_DB", "DB_NAME", "PGDATABASE", default="reports"),
        "USER": _env("POSTGRES_USER", "DB_USER", "PGUSER", default="postgres"),
        "PASSWORD": _env("POSTGRES_PASSWORD", "DB_PASSWORD", "PGPASSWORD", default="postgres"),
        "HOST": _env("POSTGRES_HOST", "DB_HOST", "PGHOST", default="localhost"),
        "PORT": _env("POSTGRES_PORT", "DB_PORT", "PGPORT", default="5432"),
    }
}

MYSQL_SERVER_1 = {
    "HOST": _env("MYSQL_SERVER1_HOST", "MYSQL1_HOST", default="localhost"),
    "PORT": _env_int("MYSQL_SERVER1_PORT", "MYSQL1_PORT", default=3306),
    "USER": _env("MYSQL_SERVER1_USER", "MYSQL1_USER", default="root"),
    "PASSWORD": _env("MYSQL_SERVER1_PASSWORD", "MYSQL1_PASSWORD", default=""),
    "DATABASE": _env("MYSQL_SERVER1_DB", "MYSQL1_DB", default=""),
    "CONNECT_TIMEOUT": _env_int("MYSQL_SERVER1_CONNECT_TIMEOUT", default=10),
    "READ_TIMEOUT": _env_int("MYSQL_SERVER1_READ_TIMEOUT", default=60),
    "WRITE_TIMEOUT": _env_int("MYSQL_SERVER1_WRITE_TIMEOUT", default=60),
    "SSL_MODE": _env("MYSQL_SERVER1_SSL_MODE", default=""),
    "SSL_CA": _env("MYSQL_SERVER1_SSL_CA", default=""),
}

MYSQL_SERVER_2 = {
    "HOST": _env("MYSQL_SERVER2_HOST", "MYSQL2_HOST", default="localhost"),
    "PORT": _env_int("MYSQL_SERVER2_PORT", "MYSQL2_PORT", default=3306),
    "USER": _env("MYSQL_SERVER2_USER", "MYSQL2_USER", default="root"),
    "PASSWORD": _env("MYSQL_SERVER2_PASSWORD", "MYSQL2_PASSWORD", default=""),
    "DATABASE": _env("MYSQL_SERVER2_DB", "MYSQL2_DB", default=""),
    "CONNECT_TIMEOUT": _env_int("MYSQL_SERVER2_CONNECT_TIMEOUT", default=10),
    "READ_TIMEOUT": _env_int("MYSQL_SERVER2_READ_TIMEOUT", default=60),
    "WRITE_TIMEOUT": _env_int("MYSQL_SERVER2_WRITE_TIMEOUT", default=60),
    "SSL_MODE": _env("MYSQL_SERVER2_SSL_MODE", default=""),
    "SSL_CA": _env("MYSQL_SERVER2_SSL_CA", default=""),
}

SAPA_MYSQL = {
    "HOST": _env(
        "SAPA_MYSQL_HOST",
        "MYSQL_SERVER1_HOST",
        "MYSQL1_HOST",
        "MYSQL_SERVER2_HOST",
        "MYSQL2_HOST",
        default="localhost",
    ),
    "PORT": _env_int(
        "SAPA_MYSQL_PORT",
        "MYSQL_SERVER1_PORT",
        "MYSQL1_PORT",
        "MYSQL_SERVER2_PORT",
        "MYSQL2_PORT",
        default=3306,
    ),
    "USER": _env(
        "SAPA_MYSQL_USER",
        "MYSQL_SERVER1_USER",
        "MYSQL1_USER",
        "MYSQL_SERVER2_USER",
        "MYSQL2_USER",
        default="root",
    ),
    "PASSWORD": _env(
        "SAPA_MYSQL_PASSWORD",
        "MYSQL_SERVER1_PASSWORD",
        "MYSQL1_PASSWORD",
        "MYSQL_SERVER2_PASSWORD",
        "MYSQL2_PASSWORD",
        default="",
    ),
    "DATABASE": _env(
        "SAPA_MYSQL_DB",
        "MYSQL_SERVER1_DB",
        "MYSQL1_DB",
        "MYSQL_SERVER2_DB",
        "MYSQL2_DB",
        default="",
    ),
    "CONNECT_TIMEOUT": _env_int("SAPA_MYSQL_CONNECT_TIMEOUT", default=10),
    "READ_TIMEOUT": _env_int("SAPA_MYSQL_READ_TIMEOUT", default=60),
    "WRITE_TIMEOUT": _env_int("SAPA_MYSQL_WRITE_TIMEOUT", default=60),
    "SSL_MODE": _env("SAPA_MYSQL_SSL_MODE", default=""),
    "SSL_CA": _env("SAPA_MYSQL_SSL_CA", default=""),
}

SAPA_SOURCE_EXTRACTOR_BACKEND = _env("SAPA_SOURCE_EXTRACTOR_BACKEND", default="mysql").strip().lower()
SAPA_SOURCE_FIXTURE_DIR = _env(
    "SAPA_SOURCE_FIXTURE_DIR",
    default=str(Path.home() / "Desktop" / "Archive"),
)
SAPA_VIDEO_METADATA_TIMEOUT = _env_int("SAPA_VIDEO_METADATA_TIMEOUT", default=8)

SAPA_WORDPRESS = {
    "BASE_URL": _env("SAPA_WORDPRESS_URL", "WORDPRESS_URL", default="https://esapa.one/"),
    "API_SECRET": _env("SAPA_WORDPRESS_SECRET", "WORDPRESS_API_SECRET", "API_SECRET", default=""),
    "TIMEOUT": _env_int("SAPA_WORDPRESS_TIMEOUT", "WORDPRESS_TIMEOUT", default=30),
    "RETRY_COUNT": _env_int("SAPA_WORDPRESS_RETRY_COUNT", default=2),
    "BACKEND": _env("SAPA_WORDPRESS_BACKEND", default="http").strip().lower(),
    "FIXTURE_DIR": _env("SAPA_WORDPRESS_FIXTURE_DIR", default=""),
    "DOCTOR_COURSE_ID": _env_int("SAPA_DOCTOR_COURSE_ID", default=8693),
    "PARAMEDIC_COURSE_ID": _env_int("SAPA_PARAMEDIC_COURSE_ID", default=9204),
    "WEBINAR_TITLE_FILTER": _env(
        "SAPA_WEBINAR_TITLE_FILTER",
        default="SAPA Growth Clinics - Introduction & Q&A",
    ),
    "STALE_MIN_RATIO": float(_env("SAPA_WORDPRESS_STALE_MIN_RATIO", default="0.2")),
}

SAPA_ETL = {
    "DEFAULT_LOOKBACK_DAYS": _env_int("SAPA_ETL_LOOKBACK_DAYS", default=30),
    "STATUS_RECOMPUTE_DAYS": _env_int("SAPA_STATUS_RECOMPUTE_DAYS", default=45),
    "PIPELINE_LOCK_KEY": _env_int("SAPA_ETL_PIPELINE_LOCK_KEY", default=991842),
}

SAPA_DASHBOARD = {
    "TIMEZONE": _env("SAPA_DASHBOARD_TIMEZONE", default="Asia/Kolkata"),
    "CERTIFICATION_ENABLED": _env_bool("SAPA_CERTIFICATION_ENABLED", default=False),
    "CONDITION_RANKING_ENABLED": _env_bool("SAPA_CONDITION_RANKING_ENABLED", default=False),
}

SOURCE_EXTRACTOR_BACKEND = _env("SOURCE_EXTRACTOR_BACKEND", default="mysql").strip().lower()
SQLITE_SOURCE_1 = {
    "PATH": _env("SQLITE_SOURCE1_PATH", default=str(BASE_DIR / "var" / "source_server1.sqlite3")),
}
SQLITE_SOURCE_2 = {
    "PATH": _env("SQLITE_SOURCE2_PATH", default=str(BASE_DIR / "var" / "source_server2.sqlite3")),
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Use cookie-based sessions so local report authentication works even when
# Django auth/session migrations are not applied yet.
SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"
