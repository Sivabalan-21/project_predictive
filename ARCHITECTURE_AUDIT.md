# ARCHITECTURE_AUDIT.md

Repo audited: `Sivabalan-21/project_predictive` (1 commit, `main`). Everything below was read from the cloned code and checked by running it, unless marked **(unverified)**.

## 1. Current architecture

```
data/ai4i2020.csv
   -> src/preprocess.py  (scale ALL data -> X_scaled.csv)
   -> src/smote.py       (SMOTE on ALL data -> X_balanced.csv)
   -> src/train.py       (split AFTER SMOTE -> RF, XGB, IsoForest .pkl)
   -> src/evaluate.py    (metrics + PNGs into data/)

Two separate UIs, both with their own copy of the prediction code:
   app/ (Streamlit, 4 pages)  ->  src/database.py (SQLite)
   flask_app/ (Flask + Jinja) ->  src/database.py (SQLite)
```

Facts: 10,000 rows, 3.39% failures, 0 duplicates, 5 features, models load fine, scaler stores feature names.

## 2. Problems found

### Critical (results are wrong or misleading)

| # | Problem | Evidence |
|---|---------|----------|
| 1 | **Data leakage: scaler fitted on the full dataset** before the split. | `preprocess.py` fits `StandardScaler` on all 10,000 rows. |
| 2 | **Data leakage: SMOTE applied before train/test split.** The test set contains synthetic points interpolated from training rows, so reported scores are inflated. | `smote.py` runs on all data; `train.py` splits `X_balanced.csv` afterwards. `X_test.csv` has 3,865 rows, far more than the real ~2,000 held-out. |
| 3 | **Hardcoded metrics in the UI** (98.58%, 99.43%, ...) typed into `4_model_report.py` and `main.py`, not read from any report file. They are also the leaked numbers from #2. | `4_model_report.py`, `main.py` |
| 4 | **Isolation Forest is near-random but has an equal vote.** Its own metrics in the UI: F1 8.63%, ROC AUC 52.73%. In a 2-of-3 vote it can still flip the outcome. | `4_model_report.py` |
| 5 | **PWF rule uses the wrong quantity.** Code tests `torque * rpm` against 3500-9000. Real power is `torque * rpm * 2*pi/60` (watts). In the actual PWF rows, `torque*rpm` ranges 10,967 to 99,980, so the rule is out of range for every real power failure. | Computed from `ai4i2020.csv` |
| 6 | **Fault classification is an if/elif chain on guessed thresholds** (`tool_wear > 200`, `torque > 40 and tool_wear > 150`, ...) and falls through to "RNF" by default. It does not follow the AI4I failure definitions, and it ignores the `Type` column that the OSF definition depends on (dropped in `preprocess.py`). Treat all of these as engineering assumptions, not validated. | `utils.py`, `app.py`, `2_predict.py` |
| 7 | **"Best model" chosen from accuracy/F1 on the leaked test set**, with a static banner claiming XGBoost. | `4_model_report.py` |

### Bugs

| # | Problem | Evidence |
|---|---------|----------|
| 8 | **`utils.predict()` is broken.** It expects keys like `'Air temperature [K]'`, but the scaler was fitted on `'Air temperature'`. Running it raises `ValueError: feature names should match`. Nothing imports it, so it is dead code. | Reproduced |
| 9 | **Prediction logic is copy-pasted 3 times** (`utils.py`, `flask_app/app.py`, `2_predict.py`) with different outputs (emoji vs plain status strings, confidence in two of them). | Code diff |
| 10 | **Status stored as emoji strings** (`"🔴 FAILURE"` in Streamlit, `"FAILURE"` in Flask), so filters in the history page break on mixed rows. | `2_predict.py` vs `app.py` |
| 11 | **Confidence shown = raw `predict_proba` of one model**, labelled "confidence". It is a model probability, not a calibrated confidence. Isolation Forest shows the label "Anomaly score" with no number. | `2_predict.py` |
| 12 | **Relative paths** (`models/...`, `data/...`) in `src/*.py` only work when run from `predictive_maintenance/`. Apps use `sys.path.insert` hacks. | all of `src/` |
| 13 | **Scripts run on import** (no `main()`), so `evaluate.py` and friends cannot be imported or tested. | `src/*.py` |
| 14 | **`4_model_report.py` radar chart fill colour built by chained `.replace()`** on hex strings. Fragile. | line ~130 |
| 15 | **`create_tables()` called on every page render.** | pages 1-3 |
| 16 | **Flask `debug=True`** and no input validation on `request.form` / JSON (`float()` on missing keys gives a 500). | `flask_app/app.py` |
| 17 | **README-level claim mismatch**: the home page says the system "monitors vibration and temperature sensor data". The dataset has no vibration feature. | `app/main.py` |

### Hygiene

| # | Problem |
|---|---------|
| 18 | **Generated artifacts committed**: `X_scaled.csv`, `X_balanced.csv`, `X_test*.csv`, `y_*.csv`, `scaler.pkl`, PNGs. Only the raw dataset is a legitimate input. (`models/*.pkl` ~8.9 MB also committed; fine for now, better via a reproducible training step or Git LFS.) |
| 19 | `.gitignore` is minimal: no `*.pkl` policy, no `data/` policy, no `.env` template. It also has a BOM at the start of the file, which can break the first pattern. |
| 20 | `requirements.txt` has no versions. Pickles depend on the exact scikit-learn / xgboost version, so this is a real reproducibility risk. |
| 21 | `psycopg2-binary` and `python-dotenv` are listed but unused (SQLite only). No `.env.example`. |
| 22 | No tests, no logging (`print` with emoji), no README. |
| 23 | Streamlit CSS is duplicated in every page. |

## 3. Worth keeping

- **Dataset choice and 5-feature set**: fine and matches the target spec.
- **Majority-vote concept**: keep as one option, but evaluate against probability averaging (see section 5).
- **Fault code names** (TWF/HDF/PWF/OSF/RNF) and the `FAULT_MAP` dict.
- **Database column layout** of `predictions`: close to the target schema, just needs `machine_id`, `sensor_reading_id`, `risk_score`.
- **Streamlit/Flask UI designs**: useful as a visual reference for the React dashboard. Not production code.
- **`class_weight='balanced'` on RF**: good instinct, and a better imbalance tool than SMOTE-then-split here.

## 4. To refactor

| Component | Action |
|-----------|--------|
| `preprocess.py`, `smote.py`, `train.py`, `evaluate.py`, `load_data.py` | Merge into one reproducible pipeline under `ml/training/` with `main()` entry points. Split first, then fit scaler on train only, then SMOTE on train only (or drop SMOTE and rely on class weights, compare both). |
| Three copies of `predict` | Replace with one `ml/prediction/prediction_service.py`. Streamlit, Flask and the future Kafka consumer all call it. |
| Fault rules | Move to `ml/fault_classification.py`; one table of thresholds, each labelled **dataset definition / statistical / assumption**; re-derive PWF in watts. |
| `database.py` | Replace with SQLAlchemy models + Alembic against PostgreSQL. |
| Metrics | Write `reports/model_metrics.json` from the training run; UI reads that file, never typed numbers. |
| Paths | One `config.py` using `pathlib` relative to the project root. |

## 5. Proposed architecture

As in your spec (simulator -> Kafka -> consumer -> prediction service -> PostgreSQL / alert engine -> FastAPI -> WebSocket -> React), with these audit-driven decisions:

1. **One prediction service** is the only place models are loaded and called.
2. **Ensemble decision method: to be decided from evaluation, not assumed.** Candidates: (a) 2-of-3 vote as today, (b) vote of RF+XGB only with Isolation Forest as an anomaly flag, (c) mean of RF and XGB probabilities with a tuned threshold. Pick by recall/precision/F1 on a clean held-out test set. Given Isolation Forest scores ~8% F1 and AUC ~0.53 on the existing split, expect it to be demoted from "voter" to "anomaly signal" unless retraining changes that. **(unverified until retrained)**
3. **`risk_score`**: define it as the mean failure probability of RF and XGB (a model output, not a calibrated confidence), and document that in `docs/ml_pipeline.md`.
4. **Fault classification** stays rule-based and separate from the binary models, since the models are trained on `Machine failure` only (`y_multi.csv` is produced but never used). Optional later: a multi-label model trained on `y_multi`.
5. **Simulator** should draw from AI4I-derived ranges (observed: air 295.3-304.5 K, process 305.7-313.8 K, rpm 1168-2886, torque 3.8-76.6 Nm, tool wear 0-253 min) and inject failures using the corrected fault definitions, so the same rules produce the labels.

## 6. Migration plan

**Phase 1 (this one)**
1. Branch `refactor/ml-pipeline`; commit the current state first.
2. Add `ml/` package: `config.py`, `training/{load_data,preprocess,train,evaluate}.py`, `prediction/{model_loader,prediction_service}.py`, `fault_classification.py`.
3. Retrain with leak-free order; write `reports/model_metrics.json`, confusion matrix, feature importance.
4. Pin `requirements.txt` to the versions used for training.
5. Point Streamlit and Flask at the prediction service (so nothing regresses while later phases land).
6. Tests: preprocessing, prediction, fault classification.
7. Untrack generated CSVs; fix `.gitignore`.

**Phases 2-7**: as in your spec; do not start Phase 2 until Phase 1 tests pass and the retrained metrics are recorded.

## 7. Open questions for you

- OK to **retrain and replace** the committed `.pkl` models? The old ones were trained on the leaked pipeline, so their metrics shouldn't be reused in the viva.
- Is the dissertation report quoting the current 98.58% figures? If so they'll need updating after the clean run.

---

## 8. Phase 1 resolution status

| Audit # | Status | How |
|---|---|---|
| 1, 2 Scaler / SMOTE leakage | Fixed | `ml/training/preprocess.py`: split first; scaler and SMOTE fitted on training rows only (also inside every CV fold). Guarded by `tests/test_preprocessing.py` (mutation-checked). |
| 3 Hardcoded UI metrics | Fixed | Both UIs read `reports/model_metrics.json` via `ml/reporting.py`. A test asserts the old figures never appear. |
| 4 Isolation Forest vote | Resolved by evidence | Decision method chosen by cross-validation; IF kept as an extra output (see `docs/ml_pipeline.md`). |
| 5 PWF wrong quantity | Fixed | Power in watts (`torque * rpm * 2*pi/60`); rule reproduces all 95 PWF labels, 0 false positives. |
| 6 Fault rules | Fixed | `ml/fault_classification.py`, AI4I definitions, every rule tagged with its basis, product type used for OSF. Separate from the failure models. |
| 7 "Best model" by accuracy | Fixed | Selection by out-of-fold F1; CI and ties stated. |
| 8, 9, 10 Broken / duplicated predict, emoji status | Fixed | One `PredictionService`; plain `NORMAL`/`FAILURE`. |
| 11 "Confidence" | Fixed | Replaced by `risk_score` (mean RF/XGB probability), documented as NOT calibrated. |
| 12, 13 Relative paths, scripts running on import | Fixed | `ml/config.py` absolute paths; every script has `main()`. |
| 14-17 UI bugs / Flask debug / vibration claim | Fixed | See commit log. |
| 18-20 Generated files, `.gitignore`, unpinned deps | Fixed | Intermediates untracked, BOM-free `.gitignore`, pinned `requirements.txt`. |
| 21 Unused deps | Fixed | `psycopg2-binary`, `python-dotenv`, `ucimlrepo`, `seaborn` removed (psycopg2 returns in Phase 3). |
| 22 No tests / logging | Partly | 80 tests; `ml/` uses `logging`. Structured logging for the full platform comes with the backend phases. |
| 23 Duplicated Streamlit CSS | Open | Legacy UI; superseded by the React dashboard (Phase 5). |
| `src/database.py` SQLite | Open (by design) | Replaced by PostgreSQL + Alembic in Phase 3. |
