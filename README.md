# OceanPulse

**A browser-built, production-style Apache Kafka learning lab and real-time maritime streaming platform.**

OceanPulse is both a Kafka learning system and a portfolio project. Every important concept is learned through a repeatable loop:

`Learn -> Predict -> Build -> Observe -> Break -> Fix -> Document -> Commit`

## Current checkpoint

Completed and proven hands-on:

- Kafka 4.3.1 in KRaft mode
- 6-partition telemetry topic
- keyed producer using `vessel_id:engine_id`
- `acks=all` and producer idempotence
- manual-commit consumer
- consumer lag growth and recovery
- consumer groups and rebalancing
- more consumers than partitions
- crash before commit vs crash after commit
- at-least-once duplicate-processing window
- `earliest` vs `latest`
- committed-offset precedence

**Next experiment:** E11 — hot partition and key skew.

## Core mental model

```text
VESSEL / PRODUCER
      |
      | event + key
      v
+-----------------------+
|       KAFKA TOPIC     |
| P0 P1 P2 P3 P4 P5    |
+-----------------------+
      |
      | consumer group owns partitions
      v
CONSUMER
      |
      | process
      v
DURABLE DB / STORAGE
      |
      | after success
      v
COMMIT KAFKA OFFSET
```

The four sentences to remember:

1. **Producers append events.**
2. **Partitions preserve order locally, not globally.**
3. **Consumer groups divide partitions.**
4. **Committed offsets remember consumer progress.**

See the full memory map: [Kafka Core Mental Model](docs/concepts/kafka-core-mental-model.md).

## Reliability rule

For an at-least-once consumer:

```text
read -> validate -> process -> durable write -> commit Kafka offset
```

If the application crashes after the durable write but before the offset commit, Kafka may redeliver the record. Therefore downstream processing must be idempotent, typically using a stable `event_id` or business key.

Do **not** confuse producer idempotence with end-to-end exactly-once processing.

## Browser-only development

This repository is configured for **GitHub Codespaces**. You can edit files, use a terminal, run Docker, execute Kafka clients, open the web application through a forwarded port, and commit/push without installing the project locally.

## Start the lab

```bash
cp .env.example .env
make up
make topic
make describe
```

Terminal 2:

```bash
make consumer
```

Terminal 3:

```bash
make web
```

Terminal 4:

```bash
make producer
```

The producer prints acknowledgements such as:

```text
ACK vessel=Selena:ME01 partition=4 offset=18
```

The consumer prints Kafka metadata such as:

```text
P4 O18 Selena ME01 MAIN_ENGINE_RPM 712.4 RPM
```

## Experiments

- [E06 — Consumer Lag Growth and Recovery](docs/experiments/06-consumer-lag-growth-and-recovery.md)
- [E07 — Two Consumers and Rebalancing](docs/experiments/07-two-consumers-and-rebalance.md)
- [E08 — More Consumers Than Partitions](docs/experiments/08-more-consumers-than-partitions.md)
- [E09 — Consumer Crash Before vs After Commit](docs/experiments/09-consumer-crash-before-after-commit.md)
- [E10 — earliest vs latest](docs/experiments/10-earliest-vs-latest.md)
- E11 — Hot Partition — next

## Delivery-semantics proof

E09 directly demonstrated the duplicate window:

```text
process P5 O11879
        |
        v
crash BEFORE commit
        |
        v
restart
        |
        v
P5 O11879 delivered again
```

Then after committing P5 O11881, Kafka stored the next position as `11882`.

## Offset reset proof

E10 demonstrated:

```text
NEW GROUP + earliest
-> oldest retained records

NEW GROUP + latest
-> current end
-> wait for new records

VALID COMMITTED OFFSET
-> resume from commit
-> reset policy is not used
```

## Roadmap

- v0.1 — first event, topic, partitions, offsets
- v0.2 — partitioning and key experiments
- v0.3 — consumer groups, lag, rebalancing, commits, replay semantics
- **E11 — hot partition / key skew**
- v0.4 — three-broker KRaft cluster, RF=3, ISR and failover
- v0.5 — offline vessel + SQLite backlog
- v0.6 — duplicate handling and idempotent processing
- v0.7 — validation, retry and DLQ
- v0.8 — gzip + AES-256-GCM
- v0.9 — Schema Registry and schema evolution
- v1.0 — interactive fault-injection OceanPulse Control Room

## Production target

The eventual OceanPulse/DRUMS-style consumer flow is:

```text
Kafka message
  -> validate headers/payload
  -> decrypt
  -> decompress
  -> validate event/schema
  -> idempotent durable archive/storage write
  -> DB transaction / derived write
  -> commit Kafka offset
```

The edge side will later support days or weeks of vessel disconnection through local SQLite buffering and controlled replay.

## Documentation

- [Learning contract](docs/00-learning-contract.md)
- [Architecture v0.1](docs/01-architecture-v0.1.md)
- [Kafka Core Mental Model](docs/concepts/kafka-core-mental-model.md)

## Portfolio rule

This repository uses generated telemetry only. Do not commit proprietary vessel data, internal credentials, production hostnames, encryption keys, or company-specific source code.
