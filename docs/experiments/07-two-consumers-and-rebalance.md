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

## Step 1 — Start two consumers

Terminal 1:
```bash
make consumer
```

Terminal 2:
```bash
make consumer
```

## Step 2 — Inspect the group

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

## Observed assignment

| Consumer ID | Assigned partitions |
|---|---|
| `rdkafka-f30d844c-fd30-4e5a-81ae-b805a9c019f9` | 3, 4, 5 |
| `rdkafka-df51cff6-57f3-4575-aedd-be9a1d2826df` | 0, 1, 2 |

This run produced an even 3/3 partition split.

The populated partitions were fully caught up at inspection time:

| Partition | Current Offset | Log End Offset | Lag |
|---:|---:|---:|---:|
| 0 | 746 | 746 | 0 |
| 1 | 1599 | 1599 | 0 |
| 2 | - | 0 | - |
| 3 | 753 | 753 | 0 |
| 4 | 748 | 748 | 0 |
| 5 | 2240 | 2240 | 0 |

## Important observation about partition 2
Partition 2 has no records (`LOG-END-OFFSET = 0`) and no committed offset yet, but Kafka still assigned it to a consumer.

Kafka assigns **partitions**, not only partitions that currently contain records.

## What this proves
1. Starting another consumer process with the same `group.id` adds another member to the same consumer group.
2. Kafka redistributes partitions when group membership changes.
3. A partition is assigned to only one active consumer within a group at a time.
4. With six partitions and two consumers, Kafka can distribute three partitions to each consumer.
5. Empty partitions still participate in consumer-group assignment.

## Mental model

```text
6 partitions
      ↓
consumer group
      ↓
Consumer A      Consumer B
P0 P1 P2        P3 P4 P5
```

The exact consumer-to-partition mapping may change during later rebalances.

## Phase 2 — Consumer leaves and Kafka rebalances

### Next step
Stop the consumer currently handling partitions 3, 4 and 5 with `Ctrl+C`.

Then inspect the group again:

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1
```

### Expected result
The remaining consumer should receive the partitions previously owned by the stopped consumer, demonstrating a consumer-group rebalance and failover of partition ownership.

## Interview takeaway
> A consumer group consists of multiple consumer instances sharing the same group ID. Kafka assigns each partition to only one consumer in that group at a time. When a consumer joins or leaves, Kafka rebalances partition ownership across the active members.
