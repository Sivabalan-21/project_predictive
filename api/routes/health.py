from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from api.schemas import HealthOut, ReadyOut

router = APIRouter(tags=["health"])
log = logging.getLogger("api.health")


@router.get("/health", response_model=HealthOut)
def health() -> HealthOut:
    """Liveness: the process is up. Checks no dependencies."""
    return HealthOut(status="ok")


@router.get("/ready", response_model=ReadyOut, responses={503: {"model": ReadyOut}})
def ready(request: Request):
    """Readiness: PostgreSQL (required) and Kafka (unless disabled). 503 when a dependency is unavailable."""
    state = request.app.state
    db = "ok"
    try:
        with state.session_factory() as session:
            session.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - any failure means "not ready"; logged, not swallowed
        log.warning("readiness: database check failed: %s", exc.__class__.__name__)
        db = "unavailable"
    if state.kafka_check is None:
        kafka = "skipped"
    else:
        try:
            kafka = "ok" if state.kafka_check() else "unavailable"
        except Exception as exc:  # noqa: BLE001
            log.warning("readiness: kafka check failed: %s", exc.__class__.__name__)
            kafka = "unavailable"
    ok = db == "ok" and kafka in ("ok", "skipped")
    body = ReadyOut(status="ready" if ok else "not_ready", database=db, kafka=kafka)
    return body if ok else JSONResponse(status_code=503, content=body.model_dump())
