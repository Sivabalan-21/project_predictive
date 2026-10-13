"""Application-level settings read from the environment (never from files committed to git).

Environment variables
---------------------
APP_ENV        development (default) | test | production.  Anything else is rejected (a typo such as ``prod`` must
               not silently disable the production safeguards).
ENABLE_DOCS    true/false. Serves /docs, /redoc and /openapi.json. Default: on, except when APP_ENV=production.
JWT_SECRET     HS256 signing secret (API only). Required, >= 32 characters. In production a placeholder value
               (``change-me``, ``example``, ...) is rejected. Generate one with:
               ``python -c "import secrets; print(secrets.token_urlsafe(48))"``
LOG_FORMAT     json | text. Default: json in production, text otherwise.

In production the database password (``DATABASE_URL`` / ``POSTGRES_PASSWORD``) must not be a placeholder either.
Secret VALUES are never included in error messages or logs.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy.engine import make_url

from db.config import ConfigurationError

log = logging.getLogger("core.settings")

APP_ENVS = ("development", "test", "production")
MIN_SECRET_LENGTH = 32

_PLACEHOLDER_RE = re.compile(
    r"(change[-_ ]?me|replace[-_ ]?me|placeholder|your[-_ ]?(secret|password|key)|example|todo|fixme|xxxx|dummy|"
    r"insert[-_ ]?here|set[-_ ]?me)", re.IGNORECASE)
_WEAK_EXACT = frozenset({"password", "passw0rd", "secret", "postgres", "admin", "root", "test", "default", "changeit",
                         "letmein", "qwerty", "123456", "12345678", "1234567890"})


def looks_like_placeholder(value: str | None) -> bool:
    """True for empty values, well-known weak passwords and obvious template text (``change-me``, ``example`` ...)."""
    if value is None or not value.strip():
        return True
    v = value.strip()
    return v.lower() in _WEAK_EXACT or bool(_PLACEHOLDER_RE.search(v))


def _bool_env(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    raise ConfigurationError(f"{name} must be one of true/false/1/0/yes/no/on/off")


@dataclass(frozen=True)
class AppSettings:
    env: str = "development"
    enable_docs: bool = True
    jwt_secret: str | None = None
    log_format: str = "text"
    login_rate_limit: int = 10
    login_rate_window_seconds: int = 60

    def __repr__(self) -> str:            # never print the secret, even by accident (logs, tracebacks, debuggers)
        return (f"AppSettings(env={self.env!r}, enable_docs={self.enable_docs}, "
                f"jwt_secret={'<set>' if self.jwt_secret else None}, log_format={self.log_format!r})")

    @property
    def is_production(self) -> bool:
        return self.env == "production"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, *, require_jwt: bool = True) -> "AppSettings":
        """Validate and return the settings. Raises ConfigurationError (the process must refuse to start)."""
        env = os.environ if environ is None else environ
        app_env = (env.get("APP_ENV") or "development").strip().lower()
        if app_env not in APP_ENVS:
            raise ConfigurationError(f"APP_ENV must be one of {', '.join(APP_ENVS)} (got {app_env!r})")
        production = app_env == "production"

        enable_docs = _bool_env(env, "ENABLE_DOCS", default=not production)

        log_format = (env.get("LOG_FORMAT") or ("json" if production else "text")).strip().lower()
        if log_format not in ("json", "text"):
            raise ConfigurationError("LOG_FORMAT must be 'json' or 'text'")

        secret = (env.get("JWT_SECRET") or "").strip() or None
        if require_jwt:
            if secret is None:
                raise ConfigurationError("JWT_SECRET is not set (generate one: python -c \"import secrets; "
                                         "print(secrets.token_urlsafe(48))\")")
            if len(secret) < MIN_SECRET_LENGTH:
                raise ConfigurationError(f"JWT_SECRET must be at least {MIN_SECRET_LENGTH} characters long")
            if len(set(secret)) < 8:
                raise ConfigurationError("JWT_SECRET has too little variety to be a secret")
            if looks_like_placeholder(secret):
                if production:
                    raise ConfigurationError("JWT_SECRET looks like a placeholder; set a real random secret "
                                             "(refused because APP_ENV=production)")
                log.warning("JWT_SECRET looks like a placeholder - acceptable for local development only")

        _check_database_password(env, production)
        login_limit = _positive_int(env, "LOGIN_RATE_LIMIT", 10)
        login_window = _positive_int(env, "LOGIN_RATE_WINDOW_SECONDS", 60)
        return cls(env=app_env, enable_docs=enable_docs, jwt_secret=secret, log_format=log_format,
                   login_rate_limit=login_limit, login_rate_window_seconds=login_window)


def _check_database_password(env: Mapping[str, str], production: bool) -> None:
    """Reject placeholder database passwords in production (warn elsewhere). Values are never echoed."""
    candidates: list[tuple[str, str | None]] = [("POSTGRES_PASSWORD", env.get("POSTGRES_PASSWORD"))]
    url = (env.get("DATABASE_URL") or "").strip()
    if url:
        try:
            candidates.append(("DATABASE_URL", make_url(url).password))
        except Exception:  # noqa: BLE001 - an invalid URL is reported by DatabaseSettings with a clearer message
            pass
    for name, value in candidates:
        if value is None or value == "":
            continue                     # unset / password-less URL (e.g. certificate auth): nothing to judge
        if looks_like_placeholder(value):
            if production:
                raise ConfigurationError(f"the password in {name} looks like a placeholder; set a real secret "
                                         "(refused because APP_ENV=production)")
            log.warning("the password in %s looks like a placeholder - acceptable for local development only", name)


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(env.get(name, str(default)))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value
