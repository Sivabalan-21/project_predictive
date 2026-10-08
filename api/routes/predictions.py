from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.dependencies import PageParams, get_prediction_service, get_session
from api.schemas import ErrorBody, Item, Page, Pagination, PredictionOut
from services.prediction_query_service import PredictionQueryService

router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("", response_model=Page[PredictionOut])
def list_predictions(p: PageParams = Depends(), session: Session = Depends(get_session),
                     svc: PredictionQueryService = Depends(get_prediction_service)):
    rows, total = svc.list(session, page=p.page, page_size=p.page_size)
    return Page[PredictionOut](data=rows, pagination=Pagination(page=p.page, page_size=p.page_size, total=total))


@router.get("/{prediction_id}", response_model=Item[PredictionOut], responses={404: {"model": ErrorBody}})
def get_prediction(prediction_id: int, session: Session = Depends(get_session),
                   svc: PredictionQueryService = Depends(get_prediction_service)):
    return Item[PredictionOut](data=svc.get(session, prediction_id))
