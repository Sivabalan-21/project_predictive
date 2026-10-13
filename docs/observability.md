# Logging, request ids and metrics (Phase 4)

## Logging
* `LOG_FORMAT=json|text` (default: **json** when `APP_ENV=production`, **text** otherwise; `docker-compose.yml` sets json). `LOG_LEVEL` as before.
* JSON logs are one object per line: `ts` (UTC, ms), `level`, `logger`, `message`, `request_id` (inside an HTTP request), any `extra=` fields, and for
  exceptions `exc_type` + `exception`.
* **Redaction** runs on every record before formatting, in both formats: bearer tokens, JWTs, Argon2 hashes, `password=`/`token=`/`secret=`/`api_key=`/
  `authorization=` pairs (also JSON `"password": "..."`), credentials inside URLs (`scheme://user:PASS@host`), and values of `extra=` fields whose key looks sensitive.
  It is a **safety net based on patterns**; code still must not log secrets (the login handler, for example, never logs the attempted username).
* The API access log (`api.access`) records method, path (**without query string**), route template, status and duration - never headers or bodies.
* Request ids: every response carries `X-Request-ID`. A client-supplied id is reused only if it matches `[A-Za-z0-9._-]{8,64}` (anything else is replaced by a random id), and it is attached to every log line produced while handling that request.

## Metrics
Library: `prometheus_client`. Each process owns its **own** registry (never the global default), so `create_app()` can be called repeatedly (tests).
Label values come from small closed sets; there are **no** machine ids, usernames, request ids, raw paths or error texts in labels.

### API - `GET /metrics` (public, like `/health`; contains no user data)
| Metric | Labels | Meaning |
|---|---|---|
| `http_requests_total` | `method`, `route`, `status` | `route` is the matched **template** (`/api/v1/predictions/{prediction_id}`); unknown paths are `unmatched`; unusual methods are `OTHER` |
| `http_request_duration_seconds` | `method`, `route` | latency histogram |
| `auth_login_attempts_total` | `result` = `success`/`failure` | |
| `process_*` | - | CPU, memory, file descriptors of the API process |

### Consumer - separate endpoint, **localhost-bound**
`python -m streaming.db_consumer` starts a small HTTP server on `CONSUMER_METRICS_ADDR` (default **127.0.0.1**) : `CONSUMER_METRICS_PORT` (default 9101; `0` disables).
`/metrics` = Prometheus text, `/healthz` = 200 while the poll loop has iterated within the last 60 s, otherwise 503 (used by the container health check).
Binding to a non-loopback address is possible but is logged as a warning; the endpoint has no authentication.

| Metric | Labels | Meaning |
|---|---|---|
| `consumer_messages_total` | `outcome` = `persisted`/`duplicate`/`dead_lettered`/`halted` | messages fully handled |
| `consumer_message_processing_seconds` | - | histogram: validate, predict, persist, publish, commit |
| `consumer_dlq_messages_total` | `reason` = `invalid_schema`/`prediction_failed` | confirmed dead-letter publishes |
| `consumer_dlq_publish_retries_total`, `consumer_dlq_publish_failures_total` | - | DLQ retries; DLQ failures after all retries (the consumer then halts) |
| `consumer_db_retries_total` | - | transient database errors retried |
| `consumer_alerts_created_total`, `consumer_offsets_committed_total` | - | |
| `consumer_last_heartbeat_timestamp_seconds` | - | unix time of the last poll-loop iteration |

Suggested alerts: `increase(consumer_dlq_messages_total[15m]) > 0`,
`increase(consumer_dlq_publish_failures_total[5m]) > 0`, `time() - consumer_last_heartbeat_timestamp_seconds > 120`.

### Outbox dispatcher
`streaming.outbox_dispatcher` uses its own process registry. `outbox_events_pending` and `outbox_oldest_pending_age_seconds` show the backlog;
`outbox_publications_total{kind,result}`, `outbox_retries_total`, and `outbox_permanent_failures_total{kind}` show relay outcomes. Alert on any
permanent failure and on an oldest pending age above the delivery SLO; neither metric proves downstream business processing.

## Single-worker assumption (and what multiprocess deployment would need)
The API is meant to run as **one uvicorn worker** (the shipped Dockerfile/Compose do), and the consumer as **one process per container**. In-memory registries are
then exact. With `--workers N` (or gunicorn) every worker has its own counters and each scrape reaches a random worker, so counters appear to jump and `rate()` is wrong. The
API logs a warning if `WEB_CONCURRENCY > 1` or `PROMETHEUS_MULTIPROC_DIR` is set. To run several API workers you would have to:

1. set `PROMETHEUS_MULTIPROC_DIR` to an empty, writable directory (a tmpfs under the read-only root filesystem), cleaned at every start, **before** the workers import `prometheus_client`;
2. make `/metrics` build a *fresh* `CollectorRegistry` per scrape and attach `prometheus_client.multiprocess.MultiProcessCollector` to it, instead of exposing the per-app registry (and drop `ProcessCollector`, which does not work in multiprocess mode);
3. give any `Gauge` a `multiprocess_mode` (the API currently has none; the consumer heartbeat gauge is single-process);
4. call `multiprocess.mark_process_dead(pid)` when a worker exits (gunicorn `child_exit` hook; plain `uvicorn --workers` has no such hook, so use gunicorn with the uvicorn worker class);
5. stop relying on process-local state elsewhere (there is none today besides the metrics).

Several **consumer** replicas are fine: each exposes its own endpoint and Prometheus scrapes every replica (they are different targets).
