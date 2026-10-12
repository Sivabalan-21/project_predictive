import json
import logging

import pytest

from streaming.logging_setup import (REDACTED, RedactingFilter, configure_logging, redact_text, redact_value,
                                     request_id_var)

JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJlLXZhbHVl"
HASH = "$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ$aGFzaGhhc2hoYXNo"


@pytest.mark.parametrize("raw,secret", [
    (f"Authorization: Bearer {JWT}", JWT),
    (f"token {JWT} leaked", JWT),
    ("password=hunter2hunter2 user=bob", "hunter2hunter2"),
    ('{"password": "s3cr3t-value", "x": 1}', "s3cr3t-value"),
    ("JWT_SECRET=abcdefghijklmnopqrstuvwxyz0123456789", "abcdefghijklmnopqrstuvwxyz0123456789"),
    ("connect postgresql+psycopg://admin:Sup3rS3cret@db:5432/x failed", "Sup3rS3cret"),
    (f"stored hash {HASH}", HASH),
    ("api_key: abcd1234efgh5678", "abcd1234efgh5678"),
])
def test_redact_text_removes_secrets(raw, secret):
    out = redact_text(raw)
    assert secret not in out and "REDACTED" in out


def test_redact_text_leaves_ordinary_messages_alone():
    msg = "persisted machine_id=M001 event_id=abc status=NORMAL risk_score=0.123 passed=3 tokens_per_s=5"
    assert redact_text(msg).startswith("persisted machine_id=M001 event_id=abc status=NORMAL risk_score=0.123 passed=3")


def test_redact_value_masks_sensitive_keys_and_recurses():
    out = redact_value(None, {"user": "bob", "password": "x", "nested": {"Authorization": "Bearer abcdefgh", "ok": 1},
                              "items": [f"Bearer {JWT}", "fine"]})
    assert out["password"] == REDACTED and out["nested"]["Authorization"] == REDACTED and out["nested"]["ok"] == 1
    assert out["user"] == "bob" and JWT not in json.dumps(out)


def _capture(capsys, fmt, emit):
    configure_logging("INFO", fmt)
    emit()
    return capsys.readouterr().err.strip().splitlines()


def test_json_logs_are_valid_json_with_request_id_and_extras(capsys):
    def emit():
        tok = request_id_var.set("req-12345678")
        try:
            logging.getLogger("t").info("hello %s", "world", extra={"duration_ms": 1.5, "password": "p@ss"})
        finally:
            request_id_var.reset(tok)
    (line,) = _capture(capsys, "json", emit)
    rec = json.loads(line)
    assert rec["message"] == "hello world" and rec["level"] == "INFO" and rec["logger"] == "t"
    assert rec["request_id"] == "req-12345678" and rec["duration_ms"] == 1.5 and rec["password"] == REDACTED
    assert rec["ts"].endswith("Z")


def test_json_logs_redact_message_args_and_exceptions(capsys):
    def emit():
        try:
            raise RuntimeError(f"boom with password=hunter2hunter2 and {JWT}")
        except RuntimeError:
            logging.getLogger("t").exception("failed Authorization: Bearer %s", JWT)
    (line,) = _capture(capsys, "json", emit)
    assert JWT not in line and "hunter2hunter2" not in line
    rec = json.loads(line)
    assert rec["exc_type"] == "RuntimeError" and "Traceback" in rec["exception"]


def test_text_format_is_redacted_and_keeps_the_phase2_layout(capsys):
    def emit():
        logging.getLogger("t").warning("db url postgresql://u:pw123456@h/db")
    (line,) = _capture(capsys, "text", emit)
    assert "pw123456" not in line and "| WARNING | t | db url postgresql://u:" in line


def test_request_id_absent_outside_a_request(capsys):
    (line,) = _capture(capsys, "json", lambda: logging.getLogger("t").info("x"))
    assert "request_id" not in json.loads(line)


def test_format_defaults_follow_app_env(monkeypatch, capsys):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.setenv("APP_ENV", "production")
    (line,) = _capture(capsys, None, lambda: logging.getLogger("t").info("x"))
    assert json.loads(line)["message"] == "x"
    monkeypatch.setenv("APP_ENV", "development")
    (line,) = _capture(capsys, None, lambda: logging.getLogger("t").info("x"))
    assert "| INFO    | t | x" in line


def test_bad_format_string_in_log_call_does_not_break_logging():
    rec = logging.LogRecord("t", logging.INFO, "f", 1, "value %d", ("not-a-number",), None)
    assert RedactingFilter().filter(rec) is True
