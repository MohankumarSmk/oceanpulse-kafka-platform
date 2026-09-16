# Experiment 07 — Two Consumers and Rebalancing

## Goal
Prove how Kafka distributes partitions across two consumers in the same consumer group, then observe what happens when one consumer leaves.

## Setup
- Topic: `oceanpulse.telemetry.raw.v1`
- Partitions: 6
- Consumer group: `oceanpulse-terminal-v1`
- Consumer A: first `make consumer` process
- Consumer B: second `make consumer` process

Both consumers use the same `group.id`, so Kafka treats them as members of one consumer group.

## Phase 1 — Start two consumers

Terminal 1:
```bash
make consumer
```

Terminal 2:
```bash
make consumer
```

Inspect the group:

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

## Observed assignment with two consumers

| Consumer ID | Assigned partitions |
|---|---|
| `rdkafka-f30d844c-fd30-4e5a-81ae-b805a9c019f9` | 3, 4, 5 |
| `rdkafka-df51cff6-57f3-4575-aedd-be9a1d2826df` | 0, 1, 2 |

This run produced an even 3/3 partition split.

| Partition | Current Offset | Log End Offset | Lag |
|---:|---:|---:|---:|
| 0 | 746 | 746 | 0 |
| 1 | 1599 | 1599 | 0 |
| 2 | - | 0 | - |
| 3 | 753 | 753 | 0 |
| 4 | 748 | 748 | 0 |
| 5 | 2240 | 2240 | 0 |

## Important observation about partition 2
Partition 2 had no records (`LOG-END-OFFSET = 0`) and no committed offset yet, but Kafka still assigned it to a consumer.

Kafka assigns **partitions**, not only partitions that currently contain records.

## Phase 2 — Stop one consumer and observe rebalance

The consumer owning partitions 3, 4 and 5 was stopped with `Ctrl+C`.

The group was inspected again:

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

## Final observed assignment after Consumer B stopped

Only one consumer remained:

`rdkafka-f30d844c-fd30-4e5a-81ae-b805a9c019f9`

Kafka reassigned all six partitions to that remaining consumer:

| Partition | Current Offset | Log End Offset | Lag |
|---:|---:|---:|---:|
| 0 | 761 | 761 | 0 |
| 1 | 1630 | 1630 | 0 |
| 2 | - | 0 | - |
| 3 | 770 | 770 | 0 |
| 4 | 769 | 769 | 0 |
| 5 | 2299 | 2299 | 0 |

## Rebalance sequence

```text
Before:
Consumer A → P0 P1 P2
Consumer B → P3 P4 P5

Consumer B stops
        ↓
consumer-group membership changes
        ↓
Kafka rebalances assignments
        ↓
Consumer A → P0 P1 P2 P3 P4 P5
```

## What this proves
1. Starting another consumer process with the same `group.id` adds another member to the same consumer group.
2. Kafka redistributes partitions when group membership changes.
3. A partition is assigned to only one active consumer within a group at a time.
4. With six partitions and two consumers, Kafka distributed three partitions to each consumer in this run.
5. Empty partitions still participate in consumer-group assignment.
6. When one consumer leaves, Kafka rebalances and transfers its partitions to the remaining consumer.
7. The application did not need to manually reassign partitions; Kafka handled the new ownership automatically.

## Production lesson
Consumer groups provide horizontal scaling and failover. When a consumer instance joins or leaves, Kafka coordinates a rebalance so partitions continue to have an active owner. Rebalances can temporarily interrupt processing, so production consumers should be designed for safe retries, idempotent downstream processing, and correct offset handling.

## Interview takeaway
> A consumer group consists of multiple consumer instances sharing the same group ID. Kafka assigns each partition to only one consumer in that group at a time. When a consumer joins or leaves, Kafka rebalances partition ownership across the active members. In this lab, two consumers split six partitions 3/3; after one consumer stopped, the remaining consumer automatically received all six partitions.
