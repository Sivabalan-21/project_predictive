"""Logging for all processes (API, consumers, CLIs): human-readable text or structured JSON, with secret redaction.

* ``LOG_FORMAT=json|text`` (default: json when APP_ENV=production, text otherwise).
* Every record passes through :class:`RedactingFilter` BEFORE it is formatted, so bearer tokens, JWTs, password hashes,
  ``password=...``/``token=...`` pairs and credentials inside URLs never reach the output - for both formats.
* The current HTTP request id (set by ``api.middleware``) is attached to every record emitted while handling the request.

Redaction is a safety net (pattern based), not a licence to log secrets: code must still avoid logging them.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"   # text format (unchanged since Phase 2)
REDACTED = "[REDACTED]"

#: id of the HTTP request being handled (None outside a request, e.g. in the consumer).
request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)

# --- redaction ----------------------------------------------------------------------------------------------------
_KEY = r"(?:password|passwd|pwd|secret|token|api[-_]?key|authorization|credential|cookie|private[-_]?key|jwt)"
_SENSITIVE_KEY_RE = re.compile(_KEY, re.IGNORECASE)
_TEXT_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{6,}"), r"\1 " + REDACTED),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*"), "[REDACTED_JWT]"),
    (re.compile(r"\$argon2[a-z0-9]*\$[^\s\"']+"), "[REDACTED_HASH]"),
    (re.compile(r"(://[^/\s:@]+:)[^@\s/]+(@)"), r"\1" + REDACTED + r"\2"),                   # scheme://user:PASS@host
    (re.compile(r"(?i)\b([\w.-]*" + _KEY + r"[\w.-]*)([\"']?\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&\"'}\]]+)"),
     r"\1\2" + REDACTED),                                                                    # password=..., "token": "..."
]


def redact_text(text: str) -> str:
    for pattern, repl in _TEXT_RULES:
        text = pattern.sub(repl, text)
    return text


def redact_value(key: str | None, value: Any) -> Any:
    """Redact a structured value: sensitive keys are masked wholesale, strings are pattern-scrubbed, containers recurse."""
    if key is not None and _SENSITIVE_KEY_RE.search(key):
        return REDACTED
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {str(k): redact_value(str(k), v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [redact_value(None, v) for v in value]
    return value


class RedactingFilter(logging.Filter):
    """Resolves the message, scrubs it (and exception text / extras) and attaches the request id."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - a bad %-format must never break logging
            message = str(record.msg)
        record.msg, record.args = redact_text(message), None
        if getattr(record, "request_id", None) is None:
            record.request_id = request_id_var.get()
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redact_text(record.exc_text)
        for name in _extra_names(record):
            setattr(record, name, redact_value(name, getattr(record, name)))
        return True


_STANDARD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "request_id", "taskName"}


def _extra_names(record: logging.LogRecord) -> list[str]:
    return [n for n in record.__dict__ if n not in _STANDARD]


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ts, level, logger, message, request_id, any ``extra=`` fields, exception."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname, "logger": record.name, "message": record.getMessage()}
        rid = getattr(record, "request_id", None)
        if rid:
            payload["request_id"] = rid
        for name in _extra_names(record):
            payload[name] = getattr(record, name)
        if record.exc_info or record.exc_text:
            if record.exc_info and record.exc_info[0] is not None:
                payload["exc_type"] = record.exc_info[0].__name__
            payload["exception"] = record.exc_text or self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__(LOG_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        rid = getattr(record, "request_id", None)
        return f"{line} [request_id={rid}]" if rid else line


def resolve_format(fmt: str | None = None) -> str:
    fmt = (fmt or os.environ.get("LOG_FORMAT") or "").strip().lower()
    if not fmt:
        fmt = "json" if (os.environ.get("APP_ENV") or "").strip().lower() == "production" else "text"
    if fmt not in ("json", "text"):
        raise ValueError("LOG_FORMAT must be 'json' or 'text'")
    return fmt


def configure_logging(level: str = "INFO", fmt: str | None = None) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(RedactingFilter())
    handler.setFormatter(JsonFormatter() if resolve_format(fmt) == "json" else TextFormatter())
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level.upper())
    logging.getLogger("kafka").setLevel(logging.WARNING)   # librdkafka internals, routed via the logger= option
    # uvicorn installs its own handlers; route its records through ours (the API logs requests itself -> no duplicate).
    for name in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(name)
        lg.handlers, lg.propagate = [], True
    access = logging.getLogger("uvicorn.access")
    access.handlers, access.propagate, access.disabled = [], False, True
