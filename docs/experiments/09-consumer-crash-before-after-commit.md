# Experiment 09 — Consumer Crash Before vs After Commit

## Goal
Prove how manual Kafka offset commits behave when a consumer crashes before or after committing a processed record, and connect the result to at-least-once delivery and duplicate-safe application design.

## Setup
- Topic: `oceanpulse.telemetry.raw.v1`
- Consumer group: `oceanpulse-crash-test-v1`
- Consumer: `services/consumer_crash_test.py`
- `enable.auto.commit=false`
- Manual synchronous commit after processing

The crash-test consumer supports three modes:
- `none` — process then commit
- `before_commit` — process then crash before commit
- `after_commit` — process, commit, then crash

## Phase 1 — Crash before commit

Command:

```bash
CRASH_MODE=before_commit \
KAFKA_CONSUMER_GROUP=oceanpulse-crash-test-v1 \
python -m services.consumer_crash_test
```

Observed record:

```text
PROCESSED P5 O11879 H429 ME01 COOLING_WATER_TEMP 66.92 C
CRASHING BEFORE COMMIT: P5 O11879
```

Running the same command again delivered the same record again:

```text
PROCESSED P5 O11879 H429 ME01 COOLING_WATER_TEMP 66.92 C
CRASHING BEFORE COMMIT: P5 O11879
```

### What this proves

The application processed the record, but Kafka never received a successful offset commit for it. After restart, the same record was eligible for delivery again.

```text
Kafka sends P5 O11879
        |
        v
consumer processes it
        |
        v
CRASH before commit
        |
        v
Kafka still has no committed progress past O11879
        |
        v
restart
        |
        v
P5 O11879 can be delivered again
```

This is the failure window that creates duplicate processing under an at-least-once design.

## Phase 2 — Crash after commit

Command:

```bash
CRASH_MODE=after_commit \
KAFKA_CONSUMER_GROUP=oceanpulse-crash-test-v1 \
python -m services.consumer_crash_test
```

Observed:

```text
PROCESSED P5 O11879 H429 ME01 COOLING_WATER_TEMP 66.92 C
COMMITTED next offset after P5 O11879
CRASHING AFTER COMMIT: P5 O11879
```

A later restart processed:

```text
PROCESSED P5 O11881 DubaiSky ME02 EXHAUST_TEMP_CYL_1 335.72 C
COMMITTED next offset after P5 O11881
CRASHING AFTER COMMIT: P5 O11881
```

The important fact is not that the next observed record was exactly O11880. Kafka offsets are partition-local and polling across multiple assigned partitions does not create one global ordered stream. The important fact is that O11879 was not replayed after it had been committed.

## Phase 3 — Inspect committed progress

Command:

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-crash-test-v1
```

Observed:

```text
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
5          11882           11954           72
```

The last committed processed record was P5 O11881. Kafka stored the next position as 11882.

```text
processed offset = 11881
committed next position = 11882
```

## Core lesson

```text
PROCESS != COMMIT
```

A consumer can finish application work and still crash before Kafka sees the commit.

### Safer order

```text
read
  -> validate
  -> process
  -> durable DB/storage write
  -> commit Kafka offset
```

### Dangerous order

```text
commit Kafka offset
  -> crash
  -> DB/storage write never happens
```

That can create application-level data loss because Kafka may resume after work that was never durably completed.

## Duplicate-safe production design

For OceanPulse/DRUMS, every event should carry a stable unique `event_id`.

The downstream store should make that identity unique or otherwise make the write idempotent.

Conceptually:

```text
Kafka may redeliver event_id=abc123
            |
            v
DB sees abc123 already exists
            |
            v
no duplicate business effect
            |
            v
commit Kafka offset
```

Producer idempotence prevents duplicate appends caused by producer retries. It does not remove the need for idempotent consumer-side effects.

## Delivery semantics summary

- **At-most-once:** progress can be committed before processing; duplicate risk is lower, but data can be lost.
- **At-least-once:** process first, commit after success; data loss risk is lower, but duplicate processing is possible.
- **Exactly-once end-to-end:** requires coordinated transactional/idempotent design; `enable.idempotence=true` alone is not enough.

## Interview takeaway

> In an at-least-once Kafka consumer, I process the record before committing its offset. If the application crashes after the business side effect but before the commit, Kafka can redeliver the same record after restart. Therefore the downstream operation must be idempotent, usually using a stable event ID or business key. In my lab I processed P5 O11879 twice when crashing before commit, then proved that a committed P5 O11881 advanced the stored next position to 11882.

## Status
✅ Complete
