# OceanPulse Learning Contract

OceanPulse is both a portfolio project and a Kafka training lab.

For every Kafka capability we follow the same cycle:

1. **Learn** — understand the concept in plain English.
2. **Predict** — say what we expect Kafka to do before running it.
3. **Build** — implement the smallest useful version.
4. **Observe** — inspect partitions, offsets, consumer groups, logs and the web UI.
5. **Break** — intentionally trigger a failure or edge case.
6. **Fix** — change configuration or design and prove the recovery.
7. **Document** — capture the result in this repository.
8. **Commit** — create a focused Git commit for the milestone.

## Rules

- We do not add a feature until its purpose is understood.
- We do not hide Kafka behavior behind a framework before observing the raw behavior.
- Every major concept gets a reproducible experiment.
- The final control room is custom-built; Grafana is not required.
- Secrets never go into Git.
- Real company data is never committed to this public portfolio repository.

## Level 0 objective

Understand and demonstrate:

- event
- producer
- broker
- topic
- partition
- offset
- consumer
- consumer group
- message key
- acknowledgement

The v0.1 milestone is complete when a simulated engine event travels:

`Python producer -> Kafka -> terminal consumer + OceanPulse web control room`
