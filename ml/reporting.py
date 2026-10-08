"""Read reports/model_metrics.json for the UIs. No metric is ever hardcoded in a UI."""
from __future__ import annotations

import json
import logging
from typing import Any

from ml import config

log = logging.getLogger(__name__)
MISSING_HINT = "reports/model_metrics.json not found. Generate it with:  python -m ml.training.train"


def load_metrics() -> dict[str, Any] | None:
    """Return the parsed report, or None if it has not been generated / is unreadable."""
    try:
        return json.loads(config.METRICS_PATH.read_text())
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        log.exception("model_metrics.json is not valid JSON")
        return None


def pct(value: float | None, digits: int = 1) -> str:
    return "n/a" if value is None else f"{100 * value:.{digits}f}%"


def headline(report: dict[str, Any]) -> dict[str, Any]:
    """Small summary used on home/dashboard cards."""
    sel = report["test"]["selected"]
    return {"method": sel["name"], "precision": sel["precision"], "recall": sel["recall"], "f1": sel["f1"],
            "f1_ci": sel["bootstrap_95ci"]["f1"], "rows": report["dataset"]["rows_after_cleaning"],
            "test_failures": report["split"]["test_failures"]}
