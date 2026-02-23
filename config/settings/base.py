from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[2]


def _load_dotenv() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        os.environ.setdefault(key, value)


_load_dotenv()
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-secret-key")
DEBUG = os.getenv("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "etl",
    "dashboard",
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
        "NAME": os.getenv("POSTGRES_DB", "reports"),
        "USER": os.getenv("POSTGRES_USER", "postgres"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "PORT": os.getenv("POSTGRES_PORT", "5432"),
    }
}

MYSQL_SERVER_1 = {
    "HOST": os.getenv("MYSQL_SERVER1_HOST", "localhost"),
    "PORT": int(os.getenv("MYSQL_SERVER1_PORT", "3306")),
    "USER": os.getenv("MYSQL_SERVER1_USER", "root"),
    "PASSWORD": os.getenv("MYSQL_SERVER1_PASSWORD", ""),
    "DATABASE": os.getenv("MYSQL_SERVER1_DB", ""),
    "CONNECT_TIMEOUT": int(os.getenv("MYSQL_SERVER1_CONNECT_TIMEOUT", "10")),
    "READ_TIMEOUT": int(os.getenv("MYSQL_SERVER1_READ_TIMEOUT", "60")),
    "WRITE_TIMEOUT": int(os.getenv("MYSQL_SERVER1_WRITE_TIMEOUT", "60")),
    "SSL_MODE": os.getenv("MYSQL_SERVER1_SSL_MODE", ""),
    "SSL_CA": os.getenv("MYSQL_SERVER1_SSL_CA", ""),
}

MYSQL_SERVER_2 = {
    "HOST": os.getenv("MYSQL_SERVER2_HOST", "localhost"),
    "PORT": int(os.getenv("MYSQL_SERVER2_PORT", "3306")),
    "USER": os.getenv("MYSQL_SERVER2_USER", "root"),
    "PASSWORD": os.getenv("MYSQL_SERVER2_PASSWORD", ""),
    "DATABASE": os.getenv("MYSQL_SERVER2_DB", ""),
    "CONNECT_TIMEOUT": int(os.getenv("MYSQL_SERVER2_CONNECT_TIMEOUT", "10")),
    "READ_TIMEOUT": int(os.getenv("MYSQL_SERVER2_READ_TIMEOUT", "60")),
    "WRITE_TIMEOUT": int(os.getenv("MYSQL_SERVER2_WRITE_TIMEOUT", "60")),
    "SSL_MODE": os.getenv("MYSQL_SERVER2_SSL_MODE", ""),
    "SSL_CA": os.getenv("MYSQL_SERVER2_SSL_CA", ""),
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
