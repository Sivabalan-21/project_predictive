# Running with Docker (Phase 4)

> **Verification status.** The Dockerfile and Compose file were validated *statically* (YAML parse, Compose-spec JSON-schema check, the exact
> `pip install` command and the image's file set exercised in a clean virtualenv, and the unit tests in `tests/phase4/test_deploy_files.py`).
> `docker build` and `docker compose up` were **not run** in the environment where this was written. Treat the first run as a test and report problems.

## Two ways to run
```bash
cp .env.example .env            # edit POSTGRES_PASSWORD (URL-safe: openssl rand -hex 24) and JWT_SECRET

# 1. DEFAULT (unchanged from Phase 3): only PostgreSQL + Kafka run in Docker; API/consumer run from your virtualenv
docker compose up -d
alembic upgrade head
python -m auth.cli create-user --username alice --role operator
python -m streaming.db_consumer          # terminal 1
uvicorn api.main:create_app --factory    # terminal 2

# 2. Everything in containers: profile "app" adds  migrate (one-shot) -> api + consumer
docker compose --profile app up -d --build
docker compose --profile app ps
printf '%s' "$PW" | docker compose --profile app run --rm -T api python -m auth.cli create-user --username alice --role operator --password-stdin
python -m streaming.producer --max-readings 50      # from the host: Kafka is published on 127.0.0.1:9092
curl -s localhost:8000/ready
```
Stop with `docker compose --profile app stop` / `down`. **Do not add `-v`** unless you want to delete the database and Kafka volumes (`pgdata`, `kafkadata`, ...); nothing here deletes them.

## What the application image does
* `Dockerfile`: `python:3.12-slim-bookworm`, dependencies from `requirements-service.txt` constrained by the pins in `requirements.txt` (no training/UI packages),
  **non-root** user `app` (uid/gid 10001, no shell, no home), application files owned by root and not writable, no secrets in the image or build context (`.dockerignore` excludes `.env*`).
* One image, three commands: `migrate` (`alembic upgrade head`), `api` (uvicorn, port 8000), `consumer` (`python -m streaming.db_consumer`).
* Hardening in Compose: `read_only` root filesystem with a `/tmp` tmpfs, `cap_drop: ALL`, `no-new-privileges`, `init: true`.
  If a read-only filesystem ever breaks something in your environment, remove `read_only`/`tmpfs` from `x-app-hardening` and report it.

## Start order, health checks, restarts
| Service | Waits for | Health check | Restart |
|---|---|---|---|
| `migrate` | postgres healthy | - (exits 0 when done) | `no` |
| `api` | postgres healthy, kafka healthy, migrate completed | `GET /health` (liveness, no dependencies) | `unless-stopped` |
| `consumer` | postgres, kafka healthy, kafka-init and migrate completed | `GET 127.0.0.1:9101/healthz` (poll-loop heartbeat) | `unless-stopped` |

`GET /ready` (API) is the readiness probe (PostgreSQL + Kafka). The consumer **exits with code 5 on purpose** when it cannot persist a message or confirm a dead-letter publish;
`restart: unless-stopped` then retries the same message with Docker's restart back-off. A crash loop therefore means the database or Kafka is unavailable - look at the logs, not at the restart count.

## Shutdown
`docker compose stop` sends SIGTERM (via tini as PID 1). The API drains in-flight requests (<= 20 s, `stop_grace_period: 30s`). The consumer finishes the message it is handling, closes the Kafka
client and exits (`stop_grace_period: 45s`). An exit status of 143 (128 + SIGTERM) after `docker compose stop` is normal: uvicorn drains and then re-raises the signal. A message still in flight when Docker sends SIGKILL is safe: its offset was not committed, so it is redelivered and the database treats it as a duplicate
(a downstream event for it may be lost, see [kafka.md](kafka.md)).

## Ports and exposure
| Port | Default publish | Change with |
|---|---|---|
| PostgreSQL 5432 -> host **5433** | `127.0.0.1` | `POSTGRES_BIND_ADDR`, `POSTGRES_HOST_PORT` |
| Kafka 9092 | `127.0.0.1` | `KAFKA_BIND_ADDR` (+ `KAFKA_EXTERNAL_HOST`, the advertised address remote clients must resolve) |
| API 8000 | `127.0.0.1` | `API_BIND_ADDR`, `API_PORT` |
| Consumer 9101 | **not published** (loopback inside the container) | `CONSUMER_METRICS_ADDR`; to let Prometheus on the compose network scrape it set `0.0.0.0` and still do not publish it |

PostgreSQL and Kafka run **without TLS or authentication** here. Binding them to a non-loopback interface exposes unauthenticated services; do that only on a trusted, firewalled network.
The API speaks plain HTTP: put a TLS-terminating reverse proxy in front of it (tokens and passwords are otherwise sent in clear text) and rate-limit `/api/v1/auth/login` there.

## Production checklist (what the shipped Compose file does **not** do for you)
* `APP_ENV=production` (docs off by default, JSON logs, placeholder secrets refused) and real, random `JWT_SECRET` / `POSTGRES_PASSWORD` from a secret manager, not a `.env` file on disk.
* Managed or replicated PostgreSQL and Kafka (here: one broker, replication factor 1, no backups).
* TLS in front of the API, network policy for 5433/9092/9101, log shipping, alerting on the metrics listed in [observability.md](observability.md).
* Image scanning / pinned digests; the base image is referenced by tag only.
