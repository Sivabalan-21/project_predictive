"""FastAPI application factory.  uvicorn api.main:create_app --factory --port 8000"""
from __future__ import annotations

import logging
import os
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from api.routes import alerts, health, machines, predictions
from db.config import DatabaseSettings
from db.session import make_engine, make_session_factory
from ml.config import KafkaSettings
from services.errors import ConflictError, NotFoundError
from streaming.logging_setup import configure_logging

log = logging.getLogger("api")


def _kafka_check(settings: KafkaSettings) -> Callable[[], bool]:
    def check() -> bool:
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers": settings.bootstrap_servers}).list_topics(timeout=3)
        return True
    return check


def _error(status: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail})


def create_app(session_factory: sessionmaker[Session] | None = None,
               kafka_check: Callable[[], bool] | None | str = "default") -> FastAPI:
    """``kafka_check``: callable -> bool; None disables the Kafka readiness check; "default" builds one from env."""
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    app = FastAPI(title="Predictive Maintenance API", version="3.0.0")
    if session_factory is None:
        settings = DatabaseSettings.from_env()                  # fails clearly if DATABASE_URL is missing
        log.info("database %s", settings.safe_url())
        session_factory = make_session_factory(make_engine(settings))
    app.state.session_factory = session_factory
    if kafka_check == "default":
        enabled = os.environ.get("READINESS_CHECK_KAFKA", "true").lower() not in ("0", "false", "no")
        kafka_check = _kafka_check(KafkaSettings.from_env()) if enabled else None
    app.state.kafka_check = kafka_check

    app.include_router(health.router)
    for module in (predictions, machines, alerts):
        app.include_router(module.router)

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

    return app
