from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.dependencies import PageParams, get_alert_service, get_prediction_service, get_session
from api.schemas import AlertOut, ErrorBody, Page, Pagination, PredictionOut
from services.alert_service import AlertService
from services.prediction_query_service import PredictionQueryService

router = APIRouter(prefix="/api/v1/machines", tags=["machines"])
MACHINE_ID = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


@router.get("/{machine_id}/predictions", response_model=Page[PredictionOut], responses={404: {"model": ErrorBody}})
def machine_predictions(machine_id: str, p: PageParams = Depends(), session: Session = Depends(get_session),
                        svc: PredictionQueryService = Depends(get_prediction_service)):
    rows, total = svc.list_for_machine(session, machine_id, page=p.page, page_size=p.page_size)
    return Page[PredictionOut](data=rows, pagination=Pagination(page=p.page, page_size=p.page_size, total=total))


@router.get("/{machine_id}/alerts", response_model=Page[AlertOut], responses={404: {"model": ErrorBody}})
def machine_alerts(machine_id: str, status: Literal["ACTIVE", "RESOLVED"] | None = Query(None), p: PageParams = Depends(),
                   session: Session = Depends(get_session), preds: PredictionQueryService = Depends(get_prediction_service),
                   svc: AlertService = Depends(get_alert_service)):
    preds.require_machine(session, machine_id)
    rows, total = svc.list(session, machine_id=machine_id, status=status, page=p.page, page_size=p.page_size)
    return Page[AlertOut](data=rows, pagination=Pagination(page=p.page, page_size=p.page_size, total=total))
