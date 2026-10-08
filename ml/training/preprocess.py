"""Leak-free data preparation: split -> scaler on TRAIN only -> SMOTE on TRAIN only."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from ml.config import SEED, TEST_SIZE
from ml.preprocessing import TARGET_COLUMN, build_feature_frame


@dataclass
class Split:
    train: pd.DataFrame   # full rows (sensors, product_type, labels), index reset
    test: pd.DataFrame

    @property
    def y_train(self) -> np.ndarray:
        return self.train[TARGET_COLUMN].to_numpy()

    @property
    def y_test(self) -> np.ndarray:
        return self.test[TARGET_COLUMN].to_numpy()


def split_dataset(df: pd.DataFrame, test_size: float = TEST_SIZE, seed: int = SEED) -> Split:
    """Stratified split of the RAW (unscaled, un-resampled) rows."""
    train, test = train_test_split(df, test_size=test_size, random_state=seed, stratify=df[TARGET_COLUMN])
    return Split(train.reset_index(drop=True), test.reset_index(drop=True))


@dataclass
class PreparedFold:
    """Scaled data for one training run. ``scaler`` was fitted on ``train`` rows only."""
    scaler: StandardScaler
    feature_names: list[str]
    X_train: np.ndarray            # scaled, original class balance
    y_train: np.ndarray
    X_train_smote: np.ndarray      # scaled, then SMOTE-resampled (training rows only)
    y_train_smote: np.ndarray


def prepare_training_data(train: pd.DataFrame, feature_set: str, seed: int = SEED) -> PreparedFold:
    """Fit scaler on ``train``, then SMOTE the scaled training rows. Never sees test data."""
    features = build_feature_frame(train, feature_set)
    scaler = StandardScaler().fit(features)
    X = scaler.transform(features)
    y = train[TARGET_COLUMN].to_numpy()
    X_res, y_res = SMOTE(random_state=seed).fit_resample(X, y)
    return PreparedFold(scaler, list(features.columns), X, y, X_res, y_res)


def transform_eval_data(frame: pd.DataFrame, scaler: StandardScaler, feature_set: str) -> np.ndarray:
    """Apply an already-fitted scaler to held-out rows (no refitting, no resampling)."""
    return scaler.transform(build_feature_frame(frame, feature_set))
