"""Event schemas shared by the Kafka producer and consumer.

* SensorEvent      - topic ``machine-sensor-data``  (one sensor reading)
* PredictionEvent  - topic ``machine-predictions``  (result of the PredictionService)

Sensor value ranges are NOT redefined here: they come from ``ml.preprocessing.SANITY_BOUNDS``
(the same plausibility limits the PredictionService enforces), so a message that passes this
schema can never be rejected later by the ML layer for range reasons.

Invalid messages are rejected with EventValidationError instead of entering the pipeline.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, ValidationInfo, field_serializer, field_validator

from ml.preprocessing import RAW_FEATURES, SANITY_BOUNDS

# strict=True: "300.5" (a string) or true/false are rejected instead of silently coerced.
StrictNumber = Annotated[float, Field(strict=True, allow_inf_nan=False)]
MACHINE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


class EventValidationError(ValueError):
    """A Kafka message failed schema validation. ``problems`` lists each field problem."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("; ".join(problems))


def _format_errors(exc: ValidationError) -> list[str]:
    return [f"{'.'.join(str(p) for p in e['loc']) or 'event'}: {e['msg']}" for e in exc.errors()]


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"not an ISO-8601 timestamp: {value!r}") from exc
    else:
        raise ValueError("timestamp must be an ISO-8601 string")
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone (use UTC, e.g. 2026-10-06T11:30:01Z)")
    parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=parsed.microsecond // 1000 * 1000)   # wire format keeps milliseconds


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class SensorEvent(BaseModel):
    """One machine sensor reading as it travels through Kafka."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    machine_id: str = Field(pattern=MACHINE_ID_PATTERN)
    timestamp: datetime
    air_temperature: StrictNumber        # K
    process_temperature: StrictNumber    # K
    rotational_speed: StrictNumber       # rpm
    torque: StrictNumber                 # Nm
    tool_wear: StrictNumber              # min
    product_type: Literal["L", "M", "H"] | None = None   # optional; only refines the OSF rule

    @field_validator("timestamp", mode="before")
    @classmethod
    def _timestamp(cls, value: Any) -> datetime:
        return _parse_timestamp(value)

    @field_validator(*RAW_FEATURES)
    @classmethod
    def _range(cls, value: float, info: ValidationInfo) -> float:
        low, high = SANITY_BOUNDS[info.field_name]
        if not low <= value <= high:
            raise ValueError(f"{value} outside plausible range [{low}, {high}]")
        return value

    @field_serializer("timestamp")
    def _ser_timestamp(self, value: datetime) -> str:
        return _iso_z(value)

    # ---- wire helpers
    def to_json_bytes(self) -> bytes:
        return self.model_dump_json(exclude_none=True).encode("utf-8")

    @classmethod
    def from_payload(cls, payload: bytes | str) -> "SensorEvent":
        """Parse + validate a raw Kafka message value. Raises EventValidationError."""
        try:
            return cls.model_validate_json(payload)
        except ValidationError as exc:
            raise EventValidationError(_format_errors(exc)) from exc

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SensorEvent":
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            raise EventValidationError(_format_errors(exc)) from exc

    def to_reading(self) -> dict[str, Any]:
        """Input mapping for ml.prediction.PredictionService.predict()."""
        reading: dict[str, Any] = {name: getattr(self, name) for name in RAW_FEATURES}
        if self.product_type:
            reading["product_type"] = self.product_type
        return reading


class PredictionEvent(BaseModel):
    """Result of running one SensorEvent through the PredictionService.

    ``risk_score`` is the model-derived score defined in Phase 1 (mean of the RF and XGBoost failure
    probabilities). It is NOT a calibrated probability or confidence.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    machine_id: str = Field(pattern=MACHINE_ID_PATTERN)
    timestamp: datetime                                   # timestamp of the sensor reading
    status: Literal["NORMAL", "FAILURE"]
    risk_score: float = Field(ge=0.0, le=1.0)
    fault_type: str | None = None                         # e.g. "OSF" or "HDF+PWF"; None when NORMAL
    model_method: str                                     # decision method, e.g. rf_only_tuned_threshold
    fault_indicators: list[str] = Field(default_factory=list)   # rule conditions that hold (any status)
    rf_prediction: int
    xgb_prediction: int
    isolation_prediction: int
    model_version: str
    processed_at: datetime                                # when the consumer produced this result

    @field_validator("timestamp", "processed_at", mode="before")
    @classmethod
    def _ts(cls, value: Any) -> datetime:
        return _parse_timestamp(value)

    @field_serializer("timestamp", "processed_at")
    def _ser_ts(self, value: datetime) -> str:
        return _iso_z(value)

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_payload(cls, payload: bytes | str) -> "PredictionEvent":
        try:
            return cls.model_validate_json(payload)
        except ValidationError as exc:
            raise EventValidationError(_format_errors(exc)) from exc

    @classmethod
    def from_result(cls, event: SensorEvent, result: Any, processed_at: datetime) -> "PredictionEvent":
        """Build from a SensorEvent and an ml.prediction.PredictionResult (no ML logic here)."""
        return cls(
            machine_id=event.machine_id, timestamp=event.timestamp, status=result.final_status,
            risk_score=result.risk_score, fault_type=result.fault_type, model_method=result.decision_method,
            fault_indicators=list(result.fault_indicators), rf_prediction=result.rf_prediction,
            xgb_prediction=result.xgb_prediction, isolation_prediction=result.isolation_prediction,
            model_version=result.model_version, processed_at=processed_at,
        )
