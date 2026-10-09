from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.dependencies import PageParams, get_alert_service, get_session
from api.schemas import AlertOut, ErrorBody, Item, Page, Pagination
from services.alert_service import AlertService

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


def _page(rows, total, p: PageParams) -> Page[AlertOut]:
    return Page[AlertOut](data=rows, pagination=Pagination(page=p.page, page_size=p.page_size, total=total))


@router.get("", response_model=Page[AlertOut])
def list_alerts(status: Literal["ACTIVE", "RESOLVED"] | None = Query(None), p: PageParams = Depends(),
                session: Session = Depends(get_session), svc: AlertService = Depends(get_alert_service)):
    rows, total = svc.list(session, status=status, page=p.page, page_size=p.page_size)
    return _page(rows, total, p)


@router.get("/active", response_model=Page[AlertOut])
def list_active_alerts(p: PageParams = Depends(), session: Session = Depends(get_session),
                       svc: AlertService = Depends(get_alert_service)):
    rows, total = svc.list(session, status="ACTIVE", page=p.page, page_size=p.page_size)
    return _page(rows, total, p)


@router.post("/{alert_id}/resolve", response_model=Item[AlertOut],
             responses={404: {"model": ErrorBody}, 409: {"model": ErrorBody}})
def resolve_alert(alert_id: int, session: Session = Depends(get_session), svc: AlertService = Depends(get_alert_service)):
    return Item[AlertOut](data=svc.resolve(session, alert_id))
