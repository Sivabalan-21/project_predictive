# Kafka topics, delivery behaviour and known limitations (Phase 3 + 4)

This document covers the **persisting consumer** (`python -m streaming.db_consumer`). The Phase 2 demo consumer
(`python -m streaming.consumer`) is documented in [streaming.md](streaming.md) and is **unchanged**: it still logs and skips invalid
messages (no dead-letter topic, no database).

## Topics

| Topic (default) | Env variable | Content | Key | Written by |
|---|---|---|---|---|
| `sensor-readings` | `KAFKA_SENSOR_TOPIC` | one `SensorEvent` | `machine_id` | producer / simulator |
| `predictions` | `KAFKA_PREDICTION_TOPIC` | `PredictionEvent` for a newly persisted reading | `machine_id` | db_consumer |
| `alerts` | `KAFKA_ALERT_TOPIC` | `AlertEvent` when a NEW active alert is created | `machine_id` | db_consumer |
| `sensor-readings.dlq` | `KAFKA_DLQ_TOPIC` | the original, unmodifiable bytes of a message that can never be processed | original key | db_consumer |

All four names must be different (`KafkaSettings.validate`). The DLQ may never equal the input topic: that would loop forever.
Topics are created by `kafka-init` in `docker-compose.yml` and by the consumer at start-up (`ensure_topics`; 3 partitions, replication
factor 1 - single-broker local defaults).

## What happens to each message

```
poll -> validate -> [ DB transaction: reading + prediction + alert + outbox intents ] COMMIT -> commit offset
                                                                         -> dispatcher publishes predictions/alerts
            |                      |
            |                      +-- DB unavailable ... retried, then HALT (offset not committed)
            +-- invalid / un-predictable -> publish ORIGINAL bytes to the DLQ (confirmed) -> commit offset
                                               +-- DLQ publish fails after retries ... HALT (offset not committed)
```

| Situation | Behaviour | Offset |
|---|---|---|
| Valid, new | reading, prediction/alert, and durable outbox intent committed together | committed after that transaction |
| Valid, duplicate (same `machine_id`+`timestamp`, or `event_id`) | nothing written, nothing re-published | committed |
| Invalid JSON / schema / out-of-range value / undecodable bytes / tombstone | **dead-lettered** (`dlq.reason=invalid_schema`) | committed **only after** the DLQ publish is confirmed |
| `PredictionService` raises on a schema-valid event | transaction rolled back, **dead-lettered** (`dlq.reason=prediction_failed`) | committed **only after** the DLQ publish is confirmed |
| Database connection error | retried with exponential backoff (`--db-retries`, default 3); then the consumer **stops** (exit 5) | **not** committed; redelivered after restart |
| Non-retryable database error (integrity/data) | consumer stops (exit 5) - never skipped silently | **not** committed |
| DLQ publish not confirmed after `--dlq-retries` (default 3) retries | consumer **stops** (exit 5) | **not** committed |

Exit codes: `2` bad configuration, `3` Kafka unreachable at start-up, `4` model artifacts missing, `5` halted (database or DLQ could not
take a message; the container restarts and the same message is retried).

## Dead-letter messages

* **Value and key**: byte-for-byte the original message (the consumer never re-serialises it).
* **Headers** (UTF-8 strings):

| Header | Meaning |
|---|---|
| `dlq.reason` | `invalid_schema` or `prediction_failed` (a closed set) |
| `dlq.error` | human-readable cause, secrets redacted, at most 500 characters |
| `dlq.source.topic` / `dlq.source.partition` / `dlq.source.offset` | where the message came from |
| `dlq.consumer_group` | the consumer group that rejected it |
| `dlq.failed_at` | UTC timestamp (ISO-8601, `Z`) |

* **Confirmed, bounded retry**: a DLQ publish counts only when the broker's delivery report says so. A failed or unconfirmed attempt is
  retried after 0.5 s, 1 s, 2 s, ... (`--dlq-retries`). If it still fails the consumer halts **without** committing, so a poison message
  is never dropped silently - the price is that the stream is blocked until the DLQ works again.
* **Duplicates in the DLQ are possible**: if the offset commit fails after a confirmed DLQ publish (or a delivery report is lost after
  the broker accepted the message) the message is redelivered and dead-lettered again. Consumers of the DLQ must tolerate that; the
  source coordinates in the headers identify repeats.
* **Replaying** is manual: fix the cause (or the producer), then re-publish the DLQ value to the input topic. There is no automatic replay.
* Retention/size of the DLQ topic is a broker setting; nothing prunes it here.

## Known limitations (read before relying on this in production)

* **At-least-once, not exactly-once.** The transaction also creates `outbox_events`; `streaming.outbox_dispatcher` records `DELIVERED`
  only after a confirmed Kafka delivery. If it crashes after Kafka accepts the record but before that database update, the expired lease
  is retried and the output can be delivered twice. It cannot make PostgreSQL and Kafka a single atomic commit. Downstream consumers
  must deduplicate the stable `event_id` Kafka header where they have side effects.
* A halted consumer blocks its partition(s) until the database/DLQ recovers (by design: no message is skipped).
* Retries are in-process and short (seconds). They are not a substitute for broker/database availability.
* Single-node local Kafka (replication factor 1), no TLS, no authentication in the shipped Compose setup.
* The producer does not assign `event_id`s; idempotency relies on `(machine_id, timestamp)`.
* The Phase 2 consumer has no DLQ.

## Operations
See [operations-runbook.md](operations-runbook.md) for backlog inspection, failed-event recovery, and safe DLQ replay.
