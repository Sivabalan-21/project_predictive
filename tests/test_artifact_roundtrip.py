"""Artifact portability + save -> reload round-trip regression tests.

Guards the bug "xgboost._c_api.XGBoostError: input stream corrupted": a pickled XGBoost model
embeds a C++ std::mt19937 text dump (`rng_state`) whose format differs between libstdc++ (Linux)
and MSVC STL (Windows), so Linux-trained pickles fail to load on Windows.
"""
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

import joblib
import numpy as np
import pytest
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

from ml import config, persistence

ROOT = Path(__file__).resolve().parent.parent
ARTIFACT_FILES = [config.SCALER_FILE, config.RF_FILE, config.XGB_FILE, config.ISO_FILE]


@pytest.fixture(scope="module")
def manifest():
    return json.loads((config.ARTIFACTS_DIR / config.MANIFEST_FILE).read_text())


def _tiny_models():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(300, 8))
    y = (X[:, 0] + 0.5 * X[:, 1] + rng.normal(scale=0.3, size=300) > 1.0).astype(int)
    scaler = StandardScaler().fit(X)
    Xs = scaler.transform(X)
    # subsample/colsample < 1 is what makes XGBoost store RNG state, exactly like the real model
    xgb = XGBClassifier(n_estimators=20, max_depth=3, subsample=0.8, colsample_bytree=0.8,
                        random_state=42, n_jobs=1, verbosity=0).fit(Xs, y)
    rf = RandomForestClassifier(n_estimators=10, random_state=42).fit(Xs, y)
    return scaler, rf, xgb, Xs, X


# ------------------------------------------------------------------ committed artifacts
def test_manifest_hashes_match_files(manifest):
    assert sorted(manifest["artifact_sha256"]) == sorted(ARTIFACT_FILES)
    for fname, expected in manifest["artifact_sha256"].items():
        assert hashlib.sha256((config.ARTIFACTS_DIR / fname).read_bytes()).hexdigest() == expected


def test_manifest_records_environment_and_validation(manifest):
    assert {"python", "scikit-learn", "xgboost", "imbalanced-learn", "numpy", "pandas", "joblib"} <= set(manifest["library_versions"])
    assert manifest["environment"]["os"] and manifest["environment"]["machine"]
    assert manifest["xgboost_serialization"] == persistence.XGBOOST_SERIALIZATION
    v = manifest["validation"]
    assert v["in_process_reload"] and v["fresh_process_reload"]
    assert sorted(v["artifacts_verified"]) == sorted(ARTIFACT_FILES)


@pytest.mark.parametrize("fname", ARTIFACT_FILES)
def test_no_platform_dependent_state_in_artifacts(fname):
    assert persistence.find_non_portable_markers(config.ARTIFACTS_DIR / fname) == []


def test_all_artifacts_load_in_clean_process():
    code = ("import joblib, pathlib; p = pathlib.Path('ml/artifacts'); "
            f"[print(f, '=>', type(joblib.load(p / f)).__name__) for f in {ARTIFACT_FILES!r}]")
    proc = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    for fname, cls in zip(ARTIFACT_FILES, ["StandardScaler", "RandomForestClassifier", "XGBClassifier", "IsolationForest"]):
        assert f"{fname} => {cls}" in proc.stdout


def test_requirements_pin_the_versions_recorded_in_manifest(manifest):
    pins = dict(re.findall(r"^([A-Za-z0-9_.\-]+)==([^\s#]+)", (ROOT / "requirements.txt").read_text(), re.M))
    for lib in ("scikit-learn", "xgboost", "imbalanced-learn", "numpy", "pandas", "joblib"):
        assert pins[lib] == manifest["library_versions"][lib], lib


# ------------------------------------------------------------------ the mechanism itself
def test_plain_pickle_of_xgboost_embeds_rng_state_canary(tmp_path):
    """Documents the root cause. Skips (rather than fails) if a future XGBoost stops embedding it."""
    *_, xgb, Xs, _ = _tiny_models()
    joblib.dump(xgb, tmp_path / "plain.joblib", compress=3)
    if persistence.find_non_portable_markers(tmp_path / "plain.joblib") == []:
        pytest.skip("this XGBoost version no longer embeds rng_state in pickles")
    persistence.dump_artifact(xgb, tmp_path / "portable.joblib")
    assert persistence.find_non_portable_markers(tmp_path / "portable.joblib") == []


def test_dump_artifact_roundtrip_is_exact_and_restores_pickling(tmp_path):
    import copyreg
    import xgboost
    before = copyreg.dispatch_table.get(xgboost.Booster)
    *_, xgb, Xs, _ = _tiny_models()
    persistence.dump_artifact(xgb, tmp_path / "x.joblib")
    assert copyreg.dispatch_table.get(xgboost.Booster) is before   # global pickling hook not leaked
    reloaded = joblib.load(tmp_path / "x.joblib")
    assert type(reloaded) is XGBClassifier
    assert np.array_equal(reloaded.predict_proba(Xs), xgb.predict_proba(Xs))
    assert reloaded.get_params()["colsample_bytree"] == 0.8


def test_save_and_validate_passes_including_clean_process(tmp_path):
    scaler, rf, xgb, Xs, X = _tiny_models()
    record = persistence.save_and_validate({"scaler.joblib": scaler, "random_forest.joblib": rf, "xgboost.joblib": xgb},
                                           tmp_path, probe_scaled=Xs, probe_raw=X)
    assert record["fresh_process_reload"] and record["in_process_reload"]
    assert set(record["sha256"]) == {"scaler.joblib", "random_forest.joblib", "xgboost.joblib"}


def test_save_and_validate_rejects_artifact_that_does_not_reproduce(tmp_path, monkeypatch):
    scaler, rf, xgb, Xs, X = _tiny_models()
    other = XGBClassifier(n_estimators=5, random_state=1, n_jobs=1, verbosity=0).fit(Xs, (X[:, 2] > 0).astype(int))
    monkeypatch.setattr(persistence.joblib, "load", lambda path: other)
    with pytest.raises(persistence.ArtifactValidationError, match="differ"):
        persistence.save_and_validate({"xgboost.joblib": xgb}, tmp_path, probe_scaled=Xs, probe_raw=X, fresh_process=False)


def test_save_and_validate_rejects_unloadable_artifact(tmp_path, monkeypatch):
    scaler, rf, xgb, Xs, X = _tiny_models()

    def boom(path):
        raise RuntimeError("input stream corrupted")
    monkeypatch.setattr(persistence.joblib, "load", boom)
    with pytest.raises(persistence.ArtifactValidationError, match="failed to reload"):
        persistence.save_and_validate({"xgboost.joblib": xgb}, tmp_path, probe_scaled=Xs, probe_raw=X, fresh_process=False)
