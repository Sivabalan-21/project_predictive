# Phase 5 Operations Runbook

## Event lifecycle

`streaming.db_consumer` validates a sensor message and commits the sensor reading, prediction, any newly active alert, and one or two `outbox_events` rows in one PostgreSQL transaction. Only after that durable commit does it commit the input Kafka offset. `streaming.outbox_dispatcher` leases pending rows with PostgreSQL `FOR UPDATE SKIP LOCKED`, publishes each to its recorded existing topic with `acks=all`, then marks it `DELIVERED` in a separate short transaction.

This is at-least-once publication. If Kafka accepts a record and the dispatcher crashes before `DELIVERED` is recorded, its lease expires and the record is published again. Every relay record has a stable `event_id` header, plus `event_type` and `schema_version`; downstream consumers with side effects must deduplicate that id. Kafka producer idempotence does not make PostgreSQL and Kafka one atomic transaction.

## Start, upgrade, and stop

1. Set strong, non-placeholder `POSTGRES_PASSWORD`, `JWT_SECRET`, and production TLS/SASL settings in the deployment secret store. Do not put them in Git.
2. Run `alembic upgrade head`. Revision `0003` only creates `outbox_events`; it does not alter or delete existing business data.
3. Start `docker compose --profile app up -d --build`. The profile starts `consumer` and `outbox-dispatcher` after `migrate` succeeds.
4. Stop with `docker compose --profile app down`. Back up PostgreSQL before a migration; downgrade `0003` only if any queued outbox data may be discarded intentionally.

## Outbox recovery

Inspect backlog and failures with a read-only query:

```sql
SELECT status, count(*) FROM outbox_events GROUP BY status;
SELECT id, event_type, topic, attempt_count, last_error, created_at
FROM outbox_events WHERE status = 'FAILED' ORDER BY created_at;
```

`PENDING` rows retry after `OUTBOX_POLL_INTERVAL_SECONDS` (default 1) and `OUTBOX_MAX_ATTEMPTS` bounds attempts (default 20). `FAILED` is deliberate and observable: repair broker/topic/credentials or malformed payload conditions, validate the target, then have an authorized database operator set only the affected row to `PENDING`, clear `last_error`, and reset `available_at`. Do not replay by inserting a new row with a new ID; that defeats downstream duplicate protection.

## Consumer, DLQ, and replay

Input offsets are committed after the database-plus-outbox transaction, after a duplicate is safely recognized, or after confirmed DLQ delivery. Database and DLQ failures halt the consumer without an offset commit. Invalid and model-failed messages go to `sensor-readings.dlq`; inspect source headers, correct the cause, validate the original bytes against the current sensor schema, and republish to `sensor-readings`. Source reading uniqueness (`event_id` when present, otherwise machine/timestamp) prevents duplicate predictions and alerts.

Do not replay arbitrary DLQ payloads through an unprotected endpoint: this project has no HTTP replay API. Restrict manual replay to authorized operators and retain source coordinates for audit. A failed offset commit may redeliver an already processed event; this is expected.

## Metrics and alerts

Scrape the consumer `/metrics` endpoint only from its intended network. The dispatcher exposes `outbox_events_pending`, `outbox_oldest_pending_age_seconds`, `outbox_publications_total`, `outbox_retries_total`, and `outbox_permanent_failures_total` through its process registry when configured for scraping. Alert on any permanent failure, a growing pending backlog, oldest age exceeding the normal delivery SLO (for example 5 minutes), DLQ growth, or a stalled consumer heartbeat. Thresholds require tuning to expected traffic and maintenance windows; they do not prove end-to-end Kafka consumption.

## Deployment limits

The shipped Compose stack is development-oriented: Kafka uses PLAINTEXT and PostgreSQL has no configured TLS verification. Both bind loopback by default, but production must supply Kafka TLS/SASL and PostgreSQL TLS with certificate verification through infrastructure-specific client configuration before exposing them outside a trusted network. The API login throttle is process-local (`LOGIN_RATE_LIMIT=10`, `LOGIN_RATE_WINDOW_SECONDS=60` by default); use a shared reverse-proxy/WAF limiter for replicated production API instances.
