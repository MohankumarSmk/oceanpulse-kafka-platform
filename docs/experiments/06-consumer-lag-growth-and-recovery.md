# Experiment 06 — Consumer Lag Growth and Recovery

## Goal
Prove that Kafka retains records while a consumer group is stopped, that consumer lag grows as producers keep writing, and that the consumer can later drain the backlog from its committed offsets.

## Concepts
- `CURRENT-OFFSET` = the consumer group's committed progress for a partition.
- `LOG-END-OFFSET` = the next offset at the end of that partition.
- `LAG = LOG-END-OFFSET - CURRENT-OFFSET`.
- Producer progress and consumer progress are independent.

## Setup
- Topic: `oceanpulse.telemetry.raw.v1`
- Consumer group: `oceanpulse-terminal-v1`
- Kafka: single-broker KRaft development cluster
- Producer: `python -m services.producer`
- Consumer: `python -m services.consumer`

## Phase 1 — Grow the lag

### Step 1 — Keep the consumer stopped
Do not run `make consumer`.

### Step 2 — Start the producer
```bash
make producer
```

Allow it to publish records, then stop it with `Ctrl+C`.

### Step 3 — Inspect the consumer group
```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

## Observed result after the producer ran while the consumer was stopped

| Partition | Current Offset | Log End Offset | Lag |
|---:|---:|---:|---:|
| 0 | 588 | 597 | 9 |
| 1 | 1264 | 1279 | 15 |
| 3 | 580 | 595 | 15 |
| 4 | 564 | 577 | 13 |
| 5 | 1738 | 1771 | 33 |

**Total observed lag: 85 records.**

Kafka also reported:

```text
Consumer group 'oceanpulse-terminal-v1' has no active members.
```

That is expected because the consumer process was stopped.

## Why the lag grew
The consumer group had already committed offsets. While the consumer was stopped, those committed positions stayed fixed. The producer continued appending records, increasing each active partition's `LOG-END-OFFSET`.

Therefore:

```text
consumer stopped
      ↓
CURRENT-OFFSET stays fixed

producer running
      ↓
LOG-END-OFFSET increases

LAG = LOG-END-OFFSET - CURRENT-OFFSET
      ↓
lag grows
```

## What this proves
1. Kafka retains records even when a consumer is offline.
2. Consumer progress is independent from producer progress.
3. Consumer lag is backlog, not data loss.
4. A consumer can later resume from the group's committed offsets.

## Phase 2 — Drain the lag
This phase is intentionally completed after Phase 1 so the recovery can be observed separately.

### Step 4 — Start the consumer
```bash
make consumer
```

The consumer should resume from the group's committed offsets and process the backlog.

### Step 5 — Let it catch up, then stop it
Use `Ctrl+C` after the backlog has drained.

### Step 6 — Inspect lag again
```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

### Expected recovery result
The lag for the active partitions should move back toward `0` if the consumer processes all retained records successfully.

## Production lesson
Consumer lag is one of the most important Kafka health signals. A growing lag can mean:
- the consumer is stopped,
- processing is slower than production,
- a downstream dependency is slow,
- a rebalance or failure is delaying work,
- or one partition is becoming a hotspot.

A production system should monitor lag per partition and per consumer group rather than only checking whether the consumer process is alive.

## Interview takeaway
A concise explanation:

> Kafka consumers track progress with committed offsets. If producers keep appending while the consumer is stopped, the log-end offsets move forward while committed offsets stay fixed, so lag grows. When the consumer restarts with the same group, it resumes from the committed offsets and drains the backlog.
