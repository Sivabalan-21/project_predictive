from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


class PredictionOut(BaseModel):
    """``risk_score`` = mean(RF, XGBoost) failure probability. It is NOT a calibrated probability or confidence."""
    model_config = ConfigDict(from_attributes=True)
    id: int
    sensor_reading_id: int
    machine_id: str
    timestamp: datetime
    prediction: Literal["NORMAL", "FAILURE"]
    risk_score: float
    rf_probability: float
    xgb_probability: float
    anomaly_score: float
    fault_type: str | None
    model_version: str
    created_at: datetime


class AlertOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    machine_id: str
    sensor_reading_id: int
    prediction_id: int
    fault_type: str
    risk_score: float
    status: Literal["ACTIVE", "RESOLVED"]
    created_at: datetime
    resolved_at: datetime | None


class Pagination(BaseModel):
    page: int
    page_size: int
    total: int


class Page(BaseModel, Generic[T]):
    data: list[T]
    pagination: Pagination


class Item(BaseModel, Generic[T]):
    data: T


class ErrorBody(BaseModel):
    error: str
    detail: str


class HealthOut(BaseModel):
    status: str


class ReadyOut(BaseModel):
    status: str
    database: str
    kafka: str
