# Predictive Maintenance — Live Pipeline Verification

**Date:** 14 October 2026  
**Environment:** Local Docker Compose + Python  
**Result:** PASS

## 1. Components Verified

- Sensor data producer
- Kafka message broker
- Kafka consumer
- Machine-learning prediction service
- PostgreSQL persistence
- Alert generation and deduplication
- Kafka outbox event dispatcher
- Authenticated REST API

## 2. Producer Results

| Metric | Result |
|---|---:|
| Messages enqueued | 60 |
| Messages delivered | 60 |
| Delivery failures | 0 |
| Invalid messages | 0 |

## 3. Failure Detection

| Field | Verified Result |
|---|---|
| Machine ID | MACHINE-003 |
| Fault type | OSF |
| Sensor reading ID | 17 |
| Prediction ID | 17 |
| Alert ID | 2 |
| Risk score | 0.68549 |
| Alert status | ACTIVE |

The simulated overstrain scenario produced a failure prediction.
The consumer persisted the prediction and created an alert in PostgreSQL.

## 4. Integration Checks

- Confirmed that subsequent readings did not create duplicate
  active alerts for the same machine and fault.
- Confirmed that the outbox dispatcher delivered an alert event
  to Kafka.
- Confirmed that the authenticated API returned the active alert.
- Previously executed Kafka integration tests passed: 3 of 3.

## 5. API Verification

Endpoint:

GET /api/v1/alerts/active

The API returned alert ID 2 for MACHINE-003, with fault type OSF
and status ACTIVE.

## 6. Conclusion

The local simulated sensor-to-alert pipeline passed the tested
end-to-end verification flow.

This test demonstrates integration behavior, not real-world
predictive accuracy. Additional evaluation on representative
historical data is required before making accuracy or production
readiness claims.
