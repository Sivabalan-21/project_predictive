"""Database configuration from the environment. No credentials are hard-coded anywhere."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy.engine import make_url


class ConfigurationError(ValueError):
    """Fatal configuration problem (the process should fail clearly instead of guessing)."""


@dataclass(frozen=True)
class DatabaseSettings:
    """``DATABASE_URL`` must be a PostgreSQL URL, e.g. ``postgresql+psycopg://user:pass@host:5432/db``."""
    url: str
    pool_size: int = 5
    echo: bool = False

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "DatabaseSettings":
        env = os.environ if environ is None else environ
        url = (env.get("DATABASE_URL") or "").strip()
        if not url:
            raise ConfigurationError("DATABASE_URL is not set (see .env.example)")
        try:
            backend = make_url(url).get_backend_name()
        except Exception as exc:  # noqa: BLE001 - re-raised as a configuration error
            raise ConfigurationError("DATABASE_URL is not a valid SQLAlchemy URL") from exc
        if backend != "postgresql":
            raise ConfigurationError(f"DATABASE_URL must be a PostgreSQL URL (got backend {backend!r}); SQLite is not supported")
        try:
            pool = int(env.get("DATABASE_POOL_SIZE", "5"))
        except ValueError as exc:
            raise ConfigurationError("DATABASE_POOL_SIZE must be an integer") from exc
        return cls(url=url, pool_size=pool, echo=env.get("DATABASE_ECHO", "").lower() in ("1", "true"))

    def safe_url(self) -> str:
        """URL with the password masked - the only form that may be logged."""
        return make_url(self.url).render_as_string(hide_password=True)
