import math

import numpy as np
import pandas as pd
import pytest

from ml.preprocessing import (FEATURE_SETS, RAW_FEATURES, InvalidReadingError, add_engineered_features,
                              build_feature_frame, validate_frame, validate_product_type, validate_values)
from ml.training.load_data import load_raw_dataset
from ml.training.preprocess import prepare_training_data, split_dataset, transform_eval_data

GOOD = {"air_temperature": 300.0, "process_temperature": 310.0, "rotational_speed": 1500,
        "torque": 40.0, "tool_wear": 100}


# ---------------------------------------------------------------- validation
def test_valid_reading_passes_and_is_float():
    out = validate_values(GOOD)
    assert set(out) == set(RAW_FEATURES) and all(isinstance(v, float) for v in out.values())


@pytest.mark.parametrize("field,value", [
    ("air_temperature", float("nan")), ("torque", float("inf")), ("tool_wear", -1),
    ("rotational_speed", 50_000), ("air_temperature", "hot"), ("torque", True), ("tool_wear", None),
])
def test_invalid_values_rejected(field, value):
    with pytest.raises(InvalidReadingError, match=field):
        validate_values({**GOOD, field: value})


def test_missing_field_rejected_and_all_problems_reported():
    bad = {k: v for k, v in GOOD.items() if k not in ("torque", "tool_wear")}
    with pytest.raises(InvalidReadingError) as err:
        validate_values(bad)
    assert "torque" in str(err.value) and "tool_wear" in str(err.value)


def test_product_type_validation():
    assert validate_product_type(" m ") == "M"
    assert validate_product_type(None) is None
    with pytest.raises(InvalidReadingError):
        validate_product_type("X")


def test_validate_frame_flags_nan_and_range():
    df = pd.DataFrame([GOOD, {**GOOD, "torque": np.nan}])
    with pytest.raises(InvalidReadingError, match="torque"):
        validate_frame(df)
    with pytest.raises(InvalidReadingError, match="missing columns"):
        validate_frame(df.drop(columns=["torque"]))


# ---------------------------------------------------------------- features
def test_engineered_features_values():
    out = add_engineered_features(pd.DataFrame([GOOD]))
    assert out.loc[0, "temp_diff"] == pytest.approx(10.0)
    assert out.loc[0, "power_w"] == pytest.approx(40 * 1500 * 2 * math.pi / 60)  # ~6283.19 W
    assert out.loc[0, "wear_torque"] == pytest.approx(4000.0)


def test_feature_frame_column_order_and_unknown_set():
    df = pd.DataFrame([GOOD])
    for name, cols in FEATURE_SETS.items():
        assert list(build_feature_frame(df, name).columns) == cols
    with pytest.raises(ValueError):
        build_feature_frame(df, "nope")
    with pytest.raises(InvalidReadingError):
        build_feature_frame(df.drop(columns=["torque"]), "raw")


# ---------------------------------------------------------------- loading
def test_load_real_dataset(dataset):
    df, report = dataset
    assert len(df) == 10_000 and report.failure_rows == int(df["machine_failure"].sum())
    assert {"product_type", "machine_failure", *RAW_FEATURES} <= set(df.columns)
    # known AI4I label inconsistencies are REPORTED, not silently altered
    assert report.label_inconsistencies["failure_without_any_fault_flag"] == 9
    assert report.label_inconsistencies["fault_flag_without_machine_failure"] == 18
    assert len(report.sha256) == 64


def _write_csv(path, rows):
    cols = ["Type", "Air temperature", "Process temperature", "Rotational speed", "Torque", "Tool wear",
            "Machine failure", "TWF", "HDF", "PWF", "OSF", "RNF"]
    pd.DataFrame(rows, columns=cols).to_csv(path, index=False)


def test_loader_drops_duplicates_and_missing(tmp_path):
    row = ["L", 300.0, 310.0, 1500, 40.0, 10, 0, 0, 0, 0, 0, 0]
    rows = [row, row, ["M", 301.0, 311.0, 1400, 42.0, 20, 0, 0, 0, 0, 0, 0],
            ["H", np.nan, 311.0, 1400, 42.0, 20, 0, 0, 0, 0, 0, 0]]
    p = tmp_path / "d.csv"
    _write_csv(p, rows)
    df, rep = load_raw_dataset(p)
    assert len(df) == 2 and rep.duplicate_rows_dropped == 1 and rep.missing_rows_dropped == 1


def test_loader_rejects_implausible_values_and_missing_columns(tmp_path):
    p = tmp_path / "bad.csv"
    _write_csv(p, [["L", 3000.0, 310.0, 1500, 40.0, 10, 0, 0, 0, 0, 0, 0]])  # 3000 K air temperature
    with pytest.raises(InvalidReadingError, match="air_temperature"):
        load_raw_dataset(p)
    pd.DataFrame({"Type": ["L"]}).to_csv(p, index=False)
    with pytest.raises(InvalidReadingError, match="missing columns"):
        load_raw_dataset(p)
    with pytest.raises(FileNotFoundError):
        load_raw_dataset(tmp_path / "nope.csv")


# ---------------------------------------------------------------- leakage guards
@pytest.fixture(scope="module")
def split(dataset):
    df, _ = dataset
    return split_dataset(df.assign(_id=np.arange(len(df))))


def test_split_is_disjoint_stratified_and_deterministic(dataset, split):
    df, _ = dataset
    assert len(split.train) == 8000 and len(split.test) == 2000
    assert set(split.train["_id"]).isdisjoint(split.test["_id"])
    assert split.train["machine_failure"].mean() == pytest.approx(df["machine_failure"].mean(), abs=0.002)
    assert split.test["machine_failure"].mean() == pytest.approx(df["machine_failure"].mean(), abs=0.002)
    again = split_dataset(df.assign(_id=np.arange(len(df))))
    assert split.test["_id"].tolist() == again.test["_id"].tolist()


def test_scaler_is_fitted_on_training_rows_only(dataset, split):
    df, _ = dataset
    prep = prepare_training_data(split.train, "engineered")
    train_mean = build_feature_frame(split.train, "engineered").mean().to_numpy()
    full_mean = build_feature_frame(df, "engineered").mean().to_numpy()
    assert np.allclose(prep.scaler.mean_, train_mean)
    assert not np.allclose(prep.scaler.mean_, full_mean, rtol=0, atol=1e-9)  # would match if fitted on all data
    before = prep.scaler.mean_.copy()
    transform_eval_data(split.test, prep.scaler, "engineered")
    assert np.array_equal(prep.scaler.mean_, before)  # transforming test data never refits


def test_smote_touches_training_data_only(split):
    prep = prepare_training_data(split.train, "engineered")
    n = len(split.train)
    assert len(prep.y_train_smote) > n                       # synthetic rows were added ...
    assert (prep.y_train_smote == 0).sum() == (prep.y_train_smote == 1).sum()  # ... to balance classes
    assert np.array_equal(prep.X_train_smote[:n], prep.X_train)  # originals preserved first
    assert len(split.test) == 2000 and split.y_test.sum() == 68   # test set never resampled
    assert prep.y_train.sum() == split.y_train.sum()


def test_preparation_is_deterministic(split):
    a, b = prepare_training_data(split.train, "raw"), prepare_training_data(split.train, "raw")
    assert np.array_equal(a.X_train_smote, b.X_train_smote)
