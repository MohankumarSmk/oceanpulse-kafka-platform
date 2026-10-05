# Experiment 10 — `earliest` vs `latest`

## Goal
Prove what `auto.offset.reset=earliest` and `auto.offset.reset=latest` do for a new consumer group, and clarify when the setting is ignored.

## Critical rule

`auto.offset.reset` matters only when Kafka has no valid committed offset for that consumer group and partition.

```text
Valid committed offset exists?
        |
       YES
        |
        v
resume from committed position
(auto.offset.reset is not used)

        NO
        |
        v
use auto.offset.reset
        |
   +----+----+
   |         |
earliest   latest
```

## Phase 1 — New group with `earliest`

Command:

```bash
KAFKA_CONSUMER_GROUP=oceanpulse-earliest-test-v1 \
python -m services.consumer
```

The standard OceanPulse consumer uses:

```python
"auto.offset.reset": "earliest",
```

Because the group was new and had no committed offsets, Kafka immediately replayed retained historical records.

Examples observed:

```text
P5 O11879 ...
P4 O3969  ...
P3 O3923  ...
P1 O8029  ...
P0 O3876  ...
```

### Important detail

`earliest` means the earliest **retained** offset still available in the partition. It does not necessarily mean offset 0.

Kafka retention may already have removed older records.

## Phase 2 — New group with `latest`

A copy of the consumer was created with:

```python
"auto.offset.reset": "latest",
```

Then a brand-new group was started:

```bash
KAFKA_CONSUMER_GROUP=oceanpulse-latest-test-v1 \
python -m services.consumer_latest
```

Observed initially:

```text
OceanPulse consumer group=oceanpulse-latest-test-v1
Press Ctrl+C to stop.
```

No historical records were printed. The consumer waited at the current end of the partitions.

A producer was then started:

```bash
make producer
```

New records were acknowledged, including:

```text
P1 O8090
P3 O3956
P5 O11954
P0 O3911
P1 O8091
```

The `latest` consumer then began receiving newly produced events.

## What this proves

```text
NEW GROUP + earliest
-> replay retained history

NEW GROUP + latest
-> skip retained history
-> wait for new records
```

## Offsets are per partition

There is no global Kafka offset.

Examples from the same topic:

```text
P0 -> O3876...
P1 -> O8029...
P3 -> O3923...
P4 -> O3969...
P5 -> O11879...
```

Each partition owns its own independent offset sequence.

## Production examples

Use `earliest` when a brand-new service should process available history, such as:
- rebuilding a derived database,
- backfilling analytics,
- replaying retained telemetry,
- creating a new downstream sink that needs historical data.

Use `latest` when a brand-new service only cares about events arriving after it starts, such as:
- a temporary live dashboard,
- a live-only development monitor,
- certain transient notification or debugging consumers.

Do not rely on `auto.offset.reset` as a replay control after a group already has valid committed offsets. For a deliberate replay, use a new group ID or an explicit offset-reset procedure.

## Interview trap

Question:

> If my consumer config says `earliest`, will it always start from the beginning?

Answer:

> No. Kafka first checks for a valid committed offset for the group. If one exists, the consumer resumes from that offset. `auto.offset.reset` is used only when no valid committed offset exists. `earliest` means the oldest retained record, not necessarily offset 0.

## Status
✅ Complete
