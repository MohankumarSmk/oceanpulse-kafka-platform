# Experiment 08 — More Consumers Than Partitions

## Goal
Prove what happens when a Kafka consumer group contains more consumers than the topic has partitions, and verify that an idle consumer can become active after an active consumer leaves.

## Setup
- Topic: `oceanpulse.telemetry.raw.v1`
- Partitions: 6
- Consumer group: `oceanpulse-terminal-v1`
- Multiple `make consumer` processes, all using the same `group.id`

## Phase 1 — More consumers than partitions

The group was inspected with:

```bash
docker exec oceanpulse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group oceanpulse-terminal-v1 \
  --members \
  --verbose
```

### Observed result
Eight consumers were present in the group while the topic had only six partitions.

Six consumers each owned one partition:

| Partition | Consumer |
|---:|---|
| 0 | `rdkafka-052c57f7-3fad-404b-b5ee-d4efc468318e` |
| 1 | `rdkafka-23a59156-ff1d-4a8b-bd49-4031020fbba6` |
| 2 | `rdkafka-4683ea94-dd37-4c79-8ddd-af278958255e` |
| 3 | `rdkafka-6e8c0fd8-48f3-452c-8643-362e98a9dc2f` |
| 4 | `rdkafka-7a87beea-3437-4db5-8370-7fa6eee5b867` |
| 5 | `rdkafka-7de6bb78-83bd-4d6d-b9cc-dff14ae8229f` |

Two consumers had zero partitions:

- `rdkafka-cdfe8f64-8fdd-43e1-a5b4-c0d4e16cf532`
- `rdkafka-ba8ae891-f7ef-42fe-b28e-bb1106980d00`

This proves that for one topic inside one consumer group, useful active parallelism is limited by the number of partitions.

With 6 partitions and 8 consumers:

```text
6 consumers active
2 consumers idle
```

## Phase 2 — Stop an idle consumer

One idle consumer was stopped.

The group then contained seven members:
- six active consumers, each with one partition
- one idle consumer with zero partitions

The partition assignments did not need to change because the removed consumer owned no partition.

## Phase 3 — Stop an active consumer

An active consumer was then stopped.

Before the rebalance, relevant assignments included:

```text
P4 -> rdkafka-7a87...
P5 -> rdkafka-7de6...
rdkafka-ba8a... -> idle
```

After the active member left, Kafka rebalanced the group.

### Observed final assignment

| Partition | Consumer |
|---:|---|
| 0 | `rdkafka-052c57f7-3fad-404b-b5ee-d4efc468318e` |
| 1 | `rdkafka-23a59156-ff1d-4a8b-bd49-4031020fbba6` |
| 2 | `rdkafka-4683ea94-dd37-4c79-8ddd-af278958255e` |
| 3 | `rdkafka-6e8c0fd8-48f3-452c-8643-362e98a9dc2f` |
| 4 | `rdkafka-7de6bb78-83bd-4d6d-b9cc-dff14ae8229f` |
| 5 | `rdkafka-ba8ae891-f7ef-42fe-b28e-bb1106980d00` |

The previously idle consumer `rdkafka-ba8a...` became active and received partition 5.

Also, `rdkafka-7de6...` moved from partition 5 to partition 4. This shows that a rebalance can redistribute more than only the partition whose previous owner left.

## What this proves

1. A partition can be actively owned by only one consumer within the same consumer group.
2. The maximum useful active consumer count for one topic is bounded by the number of partitions.
3. Extra consumers remain members of the group but receive zero partitions.
4. Removing an idle consumer may require no partition ownership change.
5. Removing an active consumer triggers a rebalance.
6. A previously idle consumer can automatically become active when a partition becomes available.
7. Kafka may redistribute multiple assignments during a rebalance according to the active assignment strategy.

## Mental model

```text
6 partitions + 8 consumers

P0 -> C1
P1 -> C2
P2 -> C3
P3 -> C4
P4 -> C5
P5 -> C6
      C7 idle
      C8 idle

active consumer leaves
        |
        v
    REBALANCE
        |
        v
previously idle consumer receives work
```

## Production lesson
Partition count is a capacity-planning decision. Adding consumer instances beyond the available partitions does not increase processing parallelism for that topic. Spare consumers can, however, provide failover capacity when an active member disappears.

## Interview takeaway
> In a Kafka consumer group, partition count determines the maximum useful parallelism for a topic. If there are more consumers than partitions, the extra consumers remain idle. When an active consumer leaves, Kafka rebalances the group and a previously idle consumer can take ownership of a partition. The rebalance may move other partitions as well depending on the assignment strategy.

## Status
✅ Complete
