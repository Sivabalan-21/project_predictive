"""Load trained artifacts with integrity and version checks."""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import sklearn
import xgboost

from ml import config
from ml.ensemble import EnsembleSpec

log = logging.getLogger(__name__)


class ModelLoadError(RuntimeError):
    """Artifacts are missing, corrupted, or incompatible."""


@dataclass(frozen=True)
class ModelBundle:
    scaler: Any
    rf: Any
    xgb: Any
    iso: Any
    spec: EnsembleSpec
    feature_set: str
    features: tuple[str, ...]
    manifest: dict

    @property
    def model_version(self) -> str:
        """Short id that changes whenever any artifact changes."""
        digest = hashlib.sha256(json.dumps(self.manifest["artifact_sha256"], sort_keys=True).encode())
        return digest.hexdigest()[:12]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_bundle(artifacts_dir: Path = config.ARTIFACTS_DIR) -> ModelBundle:
    """Load models. Raises ModelLoadError instead of failing silently.

    NOTE: joblib/pickle can execute code on load. Hashes recorded at training time are
    verified first, but only load artifacts you produced yourself.
    """
    manifest_path = artifacts_dir / config.MANIFEST_FILE
    if not manifest_path.exists():
        raise ModelLoadError(f"{manifest_path} not found. Train first:  python -m ml.training.train")
    manifest = json.loads(manifest_path.read_text())

    for fname, expected in manifest["artifact_sha256"].items():
        path = artifacts_dir / fname
        if not path.exists():
            raise ModelLoadError(f"missing artifact: {path}")
        if _sha256(path) != expected:
            raise ModelLoadError(f"artifact {fname} does not match the hash in the manifest (modified or corrupted)")

    trained = manifest["library_versions"]
    for lib, installed in (("scikit-learn", sklearn.__version__), ("xgboost", xgboost.__version__)):
        if trained.get(lib) != installed:
            log.warning("%s version mismatch: trained with %s, running %s. Retrain if predictions look off.",
                        lib, trained.get(lib), installed)

    try:
        bundle = ModelBundle(
            scaler=joblib.load(artifacts_dir / config.SCALER_FILE),
            rf=joblib.load(artifacts_dir / config.RF_FILE),
            xgb=joblib.load(artifacts_dir / config.XGB_FILE),
            iso=joblib.load(artifacts_dir / config.ISO_FILE),
            spec=EnsembleSpec.from_dict(manifest["decision"]),
            feature_set=manifest["feature_set"],
            features=tuple(manifest["features"]),
            manifest=manifest,
        )
    except Exception as exc:  # noqa: BLE001 - re-raised with context
        raise ModelLoadError(f"failed to deserialize artifacts: {exc}") from exc
    log.info("Loaded model bundle %s (decision=%s, features=%s)", bundle.model_version, bundle.spec.name,
             bundle.feature_set)
    return bundle
