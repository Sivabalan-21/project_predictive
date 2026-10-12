"""FastAPI application factory.  uvicorn api.main:create_app --factory --port 8000

Security model (Phase 4): every /api/v1 endpoint except ``POST /api/v1/auth/login`` needs a bearer token
(viewer: all GETs, operator: + resolve alerts, admin: reserved). ``/health``, ``/ready`` and ``/metrics`` are public.
"""
from __future__ import annotations

import logging
import os
from typing import Callable

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.auth import require_viewer
from api.middleware import RequestContextMiddleware
from api.routes import alerts, auth, health, machines, metrics as metrics_routes, predictions
from api.schemas import ErrorBody
from core.settings import AppSettings
from db.config import DatabaseSettings
from db.session import make_engine, make_session_factory
from ml.config import KafkaSettings
from observability.metrics import ApiMetrics
from services.errors import ConflictError, NotFoundError
from streaming.logging_setup import configure_logging

log = logging.getLogger("api")

_HTTP_ERROR_CODES = {400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found",
                     405: "method_not_allowed", 413: "payload_too_large"}


def _kafka_check(settings: KafkaSettings) -> Callable[[], bool]:
    def check() -> bool:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": settings.bootstrap_servers}).list_topics(timeout=3)
        return True
    return check


def _error(status: int, error: str, detail: str, **extra) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail, **extra})


def create_app(session_factory: sessionmaker[Session] | None = None,
               kafka_check: Callable[[], bool] | None | str = "default",
               settings: AppSettings | None = None) -> FastAPI:
    """``kafka_check``: callable -> bool; None disables the Kafka readiness check; "default" builds one from env.
    ``settings``: validated AppSettings; default reads (and validates) APP_ENV / ENABLE_DOCS / JWT_SECRET from the
    environment and raises ConfigurationError (the process refuses to start) when they are unsafe or missing."""
    settings = settings or AppSettings.from_env()
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"), settings.log_format)
    docs_on = settings.enable_docs
    app = FastAPI(title="Predictive Maintenance API", version="4.0.0",
                  docs_url="/docs" if docs_on else None, redoc_url="/redoc" if docs_on else None,
                  openapi_url="/openapi.json" if docs_on else None,
                  responses={422: {"model": ErrorBody, "description": "Validation error"}})
    app.state.settings = settings
    if session_factory is None:
        db_settings = DatabaseSettings.from_env()               # fails clearly if DATABASE_URL is missing
        log.info("database %s", db_settings.safe_url())
        session_factory = make_session_factory(make_engine(db_settings))
    app.state.session_factory = session_factory
    if kafka_check == "default":
        enabled = os.environ.get("READINESS_CHECK_KAFKA", "true").lower() not in ("0", "false", "no")
        kafka_check = _kafka_check(KafkaSettings.from_env()) if enabled else None
    app.state.kafka_check = kafka_check
    app.state.metrics = ApiMetrics()                            # per-app registry (never the global default)
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR") or int(os.environ.get("WEB_CONCURRENCY", "1") or 1) > 1:
        log.warning("metrics are per-process and this API is single-worker by design; with several workers /metrics is "
                    "inconsistent - see docs/observability.md")

    app.add_middleware(RequestContextMiddleware, metrics=app.state.metrics)

    app.include_router(health.router)
    app.include_router(metrics_routes.router)
    app.include_router(auth.router)
    protected = dict(dependencies=[Depends(require_viewer)],
                     responses={401: {"model": ErrorBody}, 403: {"model": ErrorBody}})
    for module in (predictions, machines, alerts):
        app.include_router(module.router, **protected)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        # Never echo the submitted input (exc.errors()[i]["input"]): it may contain secrets.
        errors = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"], "type": e["type"]}
                  for e in exc.errors()]
        return _error(422, "validation_error", "request validation failed", errors=errors)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException):
        response = _error(exc.status_code, _HTTP_ERROR_CODES.get(exc.status_code, "http_error"), str(exc.detail))
        for k, v in (exc.headers or {}).items():
            response.headers[k] = v
        return response

    @app.exception_handler(NotFoundError)
    async def _not_found(_: Request, exc: NotFoundError):
        return _error(404, "not_found", str(exc))

    @app.exception_handler(ConflictError)
    async def _conflict(_: Request, exc: ConflictError):
        return _error(409, "conflict", str(exc))

    @app.exception_handler(OperationalError)
    async def _db_unavailable(_: Request, exc: OperationalError):
        log.error("database unavailable: %s", exc.__class__.__name__)
        return _error(503, "service_unavailable", "database unavailable")

    @app.exception_handler(SQLAlchemyError)
    async def _db_error(_: Request, exc: SQLAlchemyError):
        log.exception("database error")
        return _error(503 if isinstance(exc, DBAPIError) and exc.connection_invalidated else 500, "database_error",
                      "database error")

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception):
        rid = request.scope.get("state", {}).get("request_id")
        log.error("unhandled exception", exc_info=exc, extra={"request_id": rid})
        response = _error(500, "internal_error", "internal server error")      # no internals leaked to the client
        if rid:
            response.headers["X-Request-ID"] = rid
        return response

    return app
