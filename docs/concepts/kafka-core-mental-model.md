# Kafka Core Mental Model — OceanPulse

This page is the short memory map for the Kafka concepts proven so far.

## 1. The whole system in one picture

```text
VESSEL / PRODUCER
      |
      | event + key
      v
+-----------------------+
|       KAFKA TOPIC     |
|                       |
| P0  P1  P2  P3  P4 P5|
+-----------------------+
      |
      | consumer group owns partitions
      v
CONSUMER
      |
      | process
      v
DB / OBJECT STORAGE
      |
      | only after durable success
      v
COMMIT KAFKA OFFSET
```

Memory sentence:

> Producers append events. Partitions preserve local order. Consumer groups divide partitions. Committed offsets remember progress.

## 2. Five objects to never confuse

### Topic
A named stream, for example:

`oceanpulse.telemetry.raw.v1`

### Partition
An ordered log inside a topic.

Ordering guarantee:

```text
inside one partition: YES
across all partitions: NO
```

### Key
A value used by the producer partitioner to consistently route related records.

OceanPulse key:

```text
vessel_id:engine_id
```

Same key normally stays on the same partition while partitioning conditions stay compatible.

### Offset
The position of a record inside one partition.

```text
P0: O0 O1 O2 ...
P1: O0 O1 O2 ...
```

There is no global topic offset.

### Consumer group
A team of consumers sharing work.

Inside one group:

```text
one partition -> at most one active consumer
```

Different groups are independent and can read the same topic.

## 3. Consumer scaling

For 6 partitions:

```text
1 consumer  -> can own all 6
2 consumers -> about 3 each
6 consumers -> at most 1 each
8 consumers -> 6 active, 2 idle
```

Memory sentence:

> Partitions set the ceiling for useful consumer parallelism.

When group membership changes, Kafka rebalances partition ownership.

## 4. Lag

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

- `CURRENT-OFFSET` = committed next position for the group.
- `LOG-END-OFFSET` = next position at the end of the partition.

OceanPulse proved an 85-record backlog could grow while the consumer was stopped and then drain back to zero.

Memory sentence:

> Lag is backlog, not automatically data loss.

## 5. Commit and crash

The most important consumer reliability picture:

```text
Kafka record
    |
    v
PROCESS
    |
    v
DURABLE WRITE
    |
    v
COMMIT OFFSET
```

Crash before commit:

```text
process -> side effect -> CRASH
                     X no commit

restart -> same record may return
```

Result: **duplicate processing is possible**.

Crash after commit:

```text
process -> durable write -> commit -> CRASH
```

Result: Kafka resumes from the committed next position.

Memory sentence:

> Process first, commit after durable success, and make processing idempotent.

## 6. Idempotency

A duplicate delivery should not create a duplicate business effect.

Use a stable identity such as:

```text
event_id = abc123
```

Then make the downstream write unique/idempotent.

```text
first abc123  -> write
second abc123 -> already processed -> harmless
```

Do not confuse:

- **producer idempotence** — protects against duplicate appends from producer retries.
- **consumer idempotency** — protects downstream effects from redelivery.
- **end-to-end exactly-once** — requires additional coordination.

## 7. `earliest` vs `latest`

```text
Does a valid committed offset exist?
        |
       YES
        v
resume from commit

        NO
        v
auto.offset.reset
   |             |
earliest       latest
oldest         current end
retained       wait for new
record         records
```

Memory sentence:

> Commit wins. Reset policy is only the fallback.

## 8. Producer durability

```text
Replication Factor = desired number of copies
ISR                = copies currently caught up
min.insync.replicas= minimum healthy copies needed for acks=all
acks=all           = wait for required ISR before ACK
idempotence        = suppress duplicate producer retries
```

Production-style target:

```text
RF = 3
min.insync.replicas = 2
acks = all
```

The current OceanPulse lab is still one broker and RF=1, so the next major phase is to prove these concepts with a three-broker cluster.

## 9. Hot partition

A key strategy can create skew.

```text
P0  ####
P1  #####
P2  ####
P3  #####
P4  ####
P5  ############################  HOT
```

Adding more consumers does not split one partition across multiple consumers in the same group.

Memory sentence:

> Bad key distribution can defeat horizontal scaling.

## 10. Reliable OceanPulse consumer target

```text
Kafka message
  -> validate headers/payload
  -> decrypt if encrypted
  -> decompress if gzip
  -> validate event/schema
  -> idempotent durable archive/storage write
  -> DB transaction / derived write
  -> commit Kafka offset
```

If the ship is offline for days or weeks, the edge side will eventually buffer locally and replay later. That belongs to the next resilience phase.

## 11. The five reliability layers

1. **Producer** — ACKs, retries, idempotence.
2. **Broker** — replication, ISR, leader election, min ISR.
3. **Consumer** — groups, offsets, commits, lag, replay.
4. **Application** — idempotent writes, transactions, deduplication, DLQ.
5. **Edge** — offline buffering, reconnect, replay, encryption.

A reliable streaming system needs all five.

## 12. Interview answers worth memorizing

**Why can Kafka consumers create duplicates?**

> If processing succeeds but the application crashes before committing the offset, Kafka can redeliver the same record. I make downstream processing idempotent and commit only after durable success.

**What limits consumer parallelism?**

> Within one consumer group, useful parallelism is bounded by partition count because each partition can have at most one active consumer in that group.

**What does `earliest` mean?**

> It is the oldest retained offset used when the group has no valid committed offset. It does not override an existing commit.

**Is `acks=all` enough for durability?**

> No. Durability also depends on replication factor, ISR health, and `min.insync.replicas`. With RF=1, `acks=all` still protects only one physical copy.

## Current checkpoint

Completed:
- E06 lag growth/recovery
- E07 two consumers/rebalance
- E08 more consumers than partitions
- E09 crash before/after commit
- E10 earliest vs latest

Next:
- E11 hot partition
- multi-broker replication and failover
