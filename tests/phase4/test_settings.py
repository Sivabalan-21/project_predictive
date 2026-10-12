import pytest

from core.settings import AppSettings, looks_like_placeholder
from db.config import ConfigurationError

GOOD = "k3J9xQ2mVb7LpR4tYw8ZnA5cHd6FsEu1GoIq0XyTjBvC"      # random-looking, 44 chars
BASE = {"JWT_SECRET": GOOD}


def env(**kw):
    return {**BASE, **kw}


# ----------------------------------------------------------------------------- APP_ENV / ENABLE_DOCS
def test_defaults_are_development_with_docs_on_and_text_logs():
    s = AppSettings.from_env(env())
    assert (s.env, s.enable_docs, s.log_format, s.is_production) == ("development", True, "text", False)


def test_production_defaults_docs_off_and_json_logs():
    s = AppSettings.from_env(env(APP_ENV="production"))
    assert s.is_production and s.enable_docs is False and s.log_format == "json"


@pytest.mark.parametrize("value", ["prod", "staging", "Production1", "dev "])
def test_unknown_app_env_is_rejected_not_silently_treated_as_development(value):
    with pytest.raises(ConfigurationError, match="APP_ENV"):
        AppSettings.from_env(env(APP_ENV=value))


@pytest.mark.parametrize("raw,expected", [("true", True), ("1", True), ("YES", True), ("false", False), ("0", False), ("off", False)])
def test_enable_docs_switch_overrides_the_environment_default(raw, expected):
    assert AppSettings.from_env(env(ENABLE_DOCS=raw)).enable_docs is expected
    assert AppSettings.from_env(env(APP_ENV="production", ENABLE_DOCS=raw)).enable_docs is expected


def test_enable_docs_garbage_is_rejected():
    with pytest.raises(ConfigurationError, match="ENABLE_DOCS"):
        AppSettings.from_env(env(ENABLE_DOCS="maybe"))


def test_log_format_validated():
    with pytest.raises(ConfigurationError, match="LOG_FORMAT"):
        AppSettings.from_env(env(LOG_FORMAT="xml"))
    assert AppSettings.from_env(env(LOG_FORMAT="JSON")).log_format == "json"


# ----------------------------------------------------------------------------- JWT secret
def test_jwt_secret_is_required():
    with pytest.raises(ConfigurationError, match="JWT_SECRET is not set"):
        AppSettings.from_env({})
    with pytest.raises(ConfigurationError, match="JWT_SECRET is not set"):
        AppSettings.from_env({"JWT_SECRET": "   "})


def test_jwt_secret_may_be_skipped_for_processes_that_do_not_sign_tokens():
    assert AppSettings.from_env({}, require_jwt=False).jwt_secret is None


def test_jwt_secret_too_short_or_low_variety_is_rejected_in_every_environment():
    for app_env in ("development", "test", "production"):
        with pytest.raises(ConfigurationError, match="at least 32"):
            AppSettings.from_env({"APP_ENV": app_env, "JWT_SECRET": "short-secret"})
    with pytest.raises(ConfigurationError, match="variety"):
        AppSettings.from_env({"JWT_SECRET": "a" * 40})


PLACEHOLDER_SECRETS = ["change-me-generate-with-secrets-token-urlsafe-48", "REPLACE_ME_REPLACE_ME_REPLACE_ME_1234",
                       "your-secret-here-your-secret-here-0123", "example-example-example-example-1234"]


@pytest.mark.parametrize("secret", PLACEHOLDER_SECRETS)
def test_placeholder_jwt_secret_rejected_in_production(secret):
    with pytest.raises(ConfigurationError, match="placeholder") as ei:
        AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": secret})
    assert secret not in str(ei.value)                      # the secret value is never echoed


@pytest.mark.parametrize("secret", PLACEHOLDER_SECRETS)
def test_placeholder_jwt_secret_only_warns_in_development(secret, caplog):
    caplog.set_level("WARNING")
    assert AppSettings.from_env({"APP_ENV": "development", "JWT_SECRET": secret}).jwt_secret == secret
    assert "placeholder" in caplog.text and secret not in caplog.text


def test_strong_secret_accepted_in_production():
    assert AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": GOOD}).jwt_secret == GOOD


# ----------------------------------------------------------------------------- database password
@pytest.mark.parametrize("url", ["postgresql+psycopg://postgres:change-me@db:5432/x", "postgresql+psycopg://u:password@db/x",
                                 "postgresql+psycopg://u:CHANGEME@db/x", "postgresql+psycopg://u:postgres@db/x"])
def test_placeholder_database_password_rejected_in_production(url):
    with pytest.raises(ConfigurationError, match="DATABASE_URL"):
        AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": GOOD, "DATABASE_URL": url})
    AppSettings.from_env({"APP_ENV": "development", "JWT_SECRET": GOOD, "DATABASE_URL": url})   # dev: allowed


def test_placeholder_postgres_password_rejected_in_production():
    with pytest.raises(ConfigurationError, match="POSTGRES_PASSWORD"):
        AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": GOOD, "POSTGRES_PASSWORD": "change-me"})


def test_strong_database_password_and_password_less_url_pass_in_production():
    AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": GOOD, "POSTGRES_PASSWORD": "q8Zr1VxLm3Tn7KbW",
                         "DATABASE_URL": "postgresql+psycopg://u:q8Zr1VxLm3Tn7KbW@db/x"})
    AppSettings.from_env({"APP_ENV": "production", "JWT_SECRET": GOOD, "DATABASE_URL": "postgresql+psycopg://u@db/x"})


def test_repr_never_contains_the_secret():
    assert GOOD not in repr(AppSettings.from_env(env())) and "<set>" in repr(AppSettings.from_env(env()))


@pytest.mark.parametrize("value,expected", [("change-me", True), ("ChangeMe123", True), ("", True), (None, True), ("password", True),
                                            ("q8Zr1VxLm3Tn7KbW", False), ("tr0ub4dor&3-horse-staple", False)])
def test_looks_like_placeholder(value, expected):
    assert looks_like_placeholder(value) is expected
