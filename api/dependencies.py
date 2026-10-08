from __future__ import annotations

from typing import Iterator

from fastapi import Depends, Query, Request
from sqlalchemy.orm import Session

from services.alert_service import AlertService
from services.prediction_query_service import PredictionQueryService


def get_session(request: Request) -> Iterator[Session]:
    with request.app.state.session_factory() as session:
        yield session


class PageParams:
    def __init__(self, page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200)):
        self.page, self.page_size = page, page_size


def get_prediction_service() -> PredictionQueryService:
    return PredictionQueryService()


def get_alert_service() -> AlertService:
    return AlertService()


SessionDep = Depends(get_session)
