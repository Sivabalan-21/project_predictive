# Predictive Maintenance Platform (Phases 1-2: ML core + Kafka streaming)

Machine failure prediction on the AI4I 2020 dataset (UCI, CC BY 4.0 per the UCI listing; verify before redistributing).
Phase 1 delivers a leak-free training pipeline, one prediction service, rule-based fault classification and the
legacy Streamlit/Flask demos on top of it. Phase 2 adds a simulated sensor feed streamed through Kafka into the PredictionService. PostgreSQL, FastAPI/WebSocket, React and Docker are later phases.

See `ARCHITECTURE_AUDIT.md` (what was wrong, what changed) and `docs/ml_pipeline.md` (method, decision, caveats).

## Setup  (PowerShell, from the project root)

Run:
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
```

## Train (regenerates models, reports/model_metrics.json and the plots)

Run:
```powershell
python -m ml.training.train
```

## Test

Run:
```powershell
python -m pytest tests -q
```

## Apps

Run (pick one):
```powershell
streamlit run app/main.py
python flask_app/app.py
```

## Use the prediction service from Python

Edit/create a file such as `try_predict.py`:
```python
from ml.prediction import get_prediction_service

result = get_prediction_service().predict({
    "air_temperature": 300.5, "process_temperature": 310.8,
    "rotational_speed": 1512, "torque": 39.8, "tool_wear": 104,
})
print(result.final_status, result.risk_score, result.fault_type)
```
Run: `python try_predict.py`

## Phase 2 architecture: real-time streaming (simulated data)

```
Sensor simulator -> Kafka producer -> topic machine-sensor-data -> Kafka consumer -> PredictionService -> topic machine-predictions
```
The sensor data is **simulated** (synthetic, modelled on AI4I 2020), not real industrial data. The consumer contains no ML code: it calls the
Phase 1 `PredictionService`. Full details, schemas, example messages, troubleshooting and limitations: `docs/streaming.md`.

You need a Kafka-compatible broker on `localhost:9092` (see `docs/streaming.md`; Docker Compose arrives in Phase 6).

Run (terminal 1, consumer first):
```powershell
python -m streaming.consumer
```
Run (terminal 2, producer + simulator):
```powershell
python -m streaming.producer --interval 1 --machines 5 --failure-probability 0.02 --seed 12
```
Run (optional, simulator alone, no Kafka needed):
```powershell
python -m simulation.sensor_simulator --interval 0.5 --seed 12
```
Configuration (environment variables, see `.env.example`): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SENSOR_TOPIC`, `KAFKA_PREDICTION_TOPIC`.

Tests: `python -m pytest tests -q` runs without any broker. Real-broker tests: set `RUN_KAFKA_INTEGRATION=1`, then `python -m pytest tests/test_kafka_integration.py -v`.

## Phase 3: PostgreSQL + Kafka + FastAPI backend
Production-style pipeline: sensor events -> Kafka -> consumer -> `PredictionService` -> PostgreSQL (+ `predictions`/`alerts` topics) -> FastAPI.
Quick start: `copy .env.example .env`, `docker compose up -d`, `alembic upgrade head`, `python -m streaming.db_consumer`,
`uvicorn api.main:create_app --factory`. Full documentation, failure semantics and limitations: [docs/architecture.md](docs/architecture.md).
Streamlit/Flask apps and the Phase 2 consumer keep working unchanged.
