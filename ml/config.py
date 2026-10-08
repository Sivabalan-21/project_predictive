"""Central configuration: paths, seeds and training constants.

All paths are absolute and derived from the project root, so scripts work from any
working directory.
"""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATASET_PATH = PROJECT_ROOT / "data" / "ai4i2020.csv"
ARTIFACTS_DIR = PROJECT_ROOT / "ml" / "artifacts"
REPORTS_DIR = PROJECT_ROOT / "reports"
METRICS_PATH = REPORTS_DIR / "model_metrics.json"

SEED = 42
TEST_SIZE = 0.20
CV_FOLDS = 5
BOOTSTRAP_RESAMPLES = 1000

# Artifact file names (inside ARTIFACTS_DIR)
SCALER_FILE = "scaler.joblib"
RF_FILE = "random_forest.joblib"
XGB_FILE = "xgboost.joblib"
ISO_FILE = "isolation_forest.joblib"
MANIFEST_FILE = "manifest.json"


# ---------------------------------------------------------------------------- Kafka (Phase 2)
# Read from environment variables at call time (never hardcoded, no credentials stored here).
import os  # noqa: E402
from dataclasses import dataclass  # noqa: E402
from typing import Mapping  # noqa: E402

DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092"
DEFAULT_SENSOR_TOPIC = "machine-sensor-data"
DEFAULT_PREDICTION_TOPIC = "machine-predictions"


@dataclass(frozen=True)
class KafkaSettings:
    """Kafka connection and topic settings.

    Environment variables:
      KAFKA_BOOTSTRAP_SERVERS   broker list              (default localhost:9092)
      KAFKA_SENSOR_TOPIC        raw sensor events        (default machine-sensor-data)
      KAFKA_PREDICTION_TOPIC    prediction events        (default machine-predictions)
      KAFKA_CONSUMER_GROUP      consumer group id        (default prediction-service)
      KAFKA_AUTO_OFFSET_RESET   earliest | latest        (default earliest)
      KAFKA_TOPIC_PARTITIONS    partitions when a topic is auto-created (default 3)
    """
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP
    sensor_topic: str = DEFAULT_SENSOR_TOPIC
    prediction_topic: str = DEFAULT_PREDICTION_TOPIC
    consumer_group: str = "prediction-service"
    auto_offset_reset: str = "earliest"
    topic_partitions: int = 3

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "KafkaSettings":
        env = os.environ if environ is None else environ
        settings = cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers).strip(),
            sensor_topic=env.get("KAFKA_SENSOR_TOPIC", cls.sensor_topic).strip(),
            prediction_topic=env.get("KAFKA_PREDICTION_TOPIC", cls.prediction_topic).strip(),
            consumer_group=env.get("KAFKA_CONSUMER_GROUP", cls.consumer_group).strip(),
            auto_offset_reset=env.get("KAFKA_AUTO_OFFSET_RESET", cls.auto_offset_reset).strip().lower(),
            topic_partitions=_int_env(env, "KAFKA_TOPIC_PARTITIONS", cls.topic_partitions),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        for name in ("bootstrap_servers", "sensor_topic", "prediction_topic", "consumer_group"):
            if not getattr(self, name):
                raise ValueError(f"Kafka setting {name!r} must not be empty")
        if self.sensor_topic == self.prediction_topic:
            raise ValueError("KAFKA_SENSOR_TOPIC and KAFKA_PREDICTION_TOPIC must be different topics")
        if self.auto_offset_reset not in ("earliest", "latest"):
            raise ValueError("KAFKA_AUTO_OFFSET_RESET must be 'earliest' or 'latest'")
        if self.topic_partitions < 1:
            raise ValueError("KAFKA_TOPIC_PARTITIONS must be >= 1")


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
