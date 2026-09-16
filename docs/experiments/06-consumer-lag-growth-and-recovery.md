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

## Phase 2 — Drain the lag

### Step 4 — Start the consumer
```bash
make consumer
```

The consumer resumed from the group's committed offsets and processed the retained backlog.

### Actual replayed ranges

| Partition | Replayed offsets | Records |
|---:|---|---:|
| 0 | 588–596 | 9 |
| 1 | 1264–1278 | 15 |
| 3 | 580–594 | 15 |
| 4 | 564–576 | 13 |
| 5 | 1738–1770 | 33 |

**Total replayed backlog: 85 records.**

This exactly matched the lag measured before restart.

### Step 5 — Stop the consumer after catch-up
Use `Ctrl+C` once the backlog has drained.

### Step 6 — Verify lag again
```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

## Final observed result

| Partition | Current Offset | Log End Offset | Lag |
|---:|---:|---:|---:|
| 0 | 597 | 597 | 0 |
| 1 | 1279 | 1279 | 0 |
| 3 | 595 | 595 | 0 |
| 4 | 577 | 577 | 0 |
| 5 | 1771 | 1771 | 0 |

Kafka again reported no active members because the consumer had already been stopped before this inspection.

## What this proves
1. Kafka retained the producer backlog while the consumer was offline.
2. The same consumer group resumed from its committed offsets rather than replaying everything from the beginning.
3. The measured backlog of 85 records was exactly the backlog later consumed.
4. Once those records were processed and committed, lag returned to zero.
5. Consumer lag is measurable backlog, not evidence of data loss by itself.

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

> Kafka consumers track progress with committed offsets. If producers keep appending while the consumer is stopped, the log-end offsets move forward while committed offsets stay fixed, so lag grows. When the consumer restarts with the same group, it resumes from the committed offsets and drains the backlog. In this lab, lag grew to 85 records and returned to zero after restart.
