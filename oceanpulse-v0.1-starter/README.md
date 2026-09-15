# OceanPulse

**A browser-built, production-style Apache Kafka learning lab and real-time maritime streaming platform.**

OceanPulse is designed to answer two questions at the same time:

1. **Do I understand Kafka?**
2. **Can I build and demonstrate a real streaming system?**

The project evolves from one telemetry event through Kafka into a fault-injection control room
with offline edge buffering, replay, encryption, schema evolution, DLQs, stream processing,
multi-broker failure recovery, and real-time web observability.

## v0.1 — First Event

```text
Simulated Vessel
      |
      v
Python Producer
      |
      | key = vessel_id:engine_id
      v
Apache Kafka 4.3.1 / KRaft
      |
      +---------> Terminal Consumer
      |
      +---------> Web Consumer -> FastAPI -> WebSocket -> Custom Control Room
```

No Grafana is required. The user-facing observability layer is an OceanPulse web application.

## Browser-only development

This repository is configured for **GitHub Codespaces**. You can edit files, use a terminal,
run Docker, execute Kafka clients, open the website through a forwarded port, and commit/push
changes without installing the project on a local computer.

## Start v0.1

```bash
cp .env.example .env
make up
make topic
make describe
```

Open terminal 2:

```bash
make consumer
```

Open terminal 3:

```bash
make web
```

Open terminal 4:

```bash
make producer
```

Codespaces should offer to open forwarded port `8000`. That page is the OceanPulse Control Room.

## What to observe

The producer prints Kafka acknowledgements such as:

```text
ACK vessel=Selena:ME01 partition=4 offset=18
```

The terminal consumer shows the same Kafka metadata:

```text
P4 O18 Selena ME01 MAIN_ENGINE_RPM 712.4 RPM
```

The custom webpage receives a copy through an **independent consumer group** and shows live
events in the browser.

## First experiment

Before running the producer, predict:

> If several records use exactly the same key `Selena:ME01`, should they keep landing on the
> same partition?

Do not memorize the answer. Run the producer, observe the partition number, and record the result.

## Learning documentation

- [`docs/00-learning-contract.md`](docs/00-learning-contract.md)
- [`docs/01-architecture-v0.1.md`](docs/01-architecture-v0.1.md)

## Roadmap

- v0.1 — first event, topic, partitions, offsets
- v0.2 — partitioning and key experiments
- v0.3 — consumer groups and rebalancing
- v0.4 — three-broker KRaft cluster and failover
- v0.5 — offline vessel + SQLite backlog
- v0.6 — duplicate handling and delivery semantics
- v0.7 — validation, retry and DLQ
- v0.8 — gzip + AES-256-GCM
- v0.9 — Schema Registry and schema evolution
- v1.0 — interactive fault-injection OceanPulse Control Room

## Portfolio rule

This repository uses generated telemetry only. Do not commit proprietary vessel data, internal
credentials, production hostnames, encryption keys, or company-specific source code.
