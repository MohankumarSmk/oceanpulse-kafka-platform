# Architecture v0.1

```text
Telemetry Simulator
       |
       | keyed by vessel_id:engine_id
       v
+-----------------------------+
| Apache Kafka 4.3.1          |
| KRaft                       |
| 1 broker for Level 0        |
|                             |
| oceanpulse.telemetry.raw.v1 |
| 6 partitions                |
+-------------+---------------+
              |
        +-----+------+
        |            |
        v            v
Terminal        Web consumer
consumer        group
        |            |
        |            v
        |       FastAPI/WebSocket
        |            |
        |            v
        |      Browser Control Room
        |
        v
Offsets + manual commits
```

## Why only one broker today?

Level 0 is about event flow, partitions and offsets. Multiple brokers introduce replication,
ISR and leader election before those foundations are understood.

A three-broker KRaft cluster is a planned milestone. We will use it to demonstrate:

- replication factor 3
- ISR changes
- leader election
- broker failure
- acknowledged-write survival
- recovery

## Why six partitions?

Six partitions are unnecessary for the first few events, and that is intentional. They give us
room to experiment later with keys, distribution and consumer-group parallelism without
recreating the topic immediately.

## Why two consumer groups?

The terminal consumer and the website intentionally use different consumer groups.

That proves an important Kafka property: different consumer groups can independently read the
same topic. The web UI does not "steal" records from the terminal consumer.
