import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def dataset():
    from ml.training.load_data import load_raw_dataset
    return load_raw_dataset()


@pytest.fixture(scope="session")
def service():
    from ml.prediction import get_prediction_service
    return get_prediction_service()


NORMAL = {"air_temperature": 300.5, "process_temperature": 310.8, "rotational_speed": 1512,
          "torque": 39.8, "tool_wear": 104}
# Overstrain: tool_wear x torque = 17,500 (> 13,000, the highest AI4I limit) and wear > 200 min
OVERSTRAIN = {"air_temperature": 300.0, "process_temperature": 310.0, "rotational_speed": 1300,
              "torque": 70.0, "tool_wear": 250}
