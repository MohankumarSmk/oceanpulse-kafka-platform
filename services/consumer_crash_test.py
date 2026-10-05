import json
import os

from confluent_kafka import Consumer, KafkaException
from services.common import BOOTSTRAP, TOPIC

GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "oceanpulse-crash-test-v1")
CRASH_MODE = os.getenv("CRASH_MODE", "none")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])

print(f"OceanPulse crash-test consumer group={GROUP}")
print(f"CRASH_MODE={CRASH_MODE}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        event = json.loads(msg.value())

        print(
            f"PROCESSED P{msg.partition()} O{msg.offset()} "
            f"{event['vessel_id']} {event['engine_id']} "
            f"{event['channel']} {event['value']} {event['unit']}",
            flush=True,
        )

        if CRASH_MODE == "before_commit":
            print(
                f"CRASHING BEFORE COMMIT: P{msg.partition()} O{msg.offset()}",
                flush=True,
            )
            os._exit(1)

        consumer.commit(message=msg, asynchronous=False)

        print(
            f"COMMITTED next offset after P{msg.partition()} O{msg.offset()}",
            flush=True,
        )

        if CRASH_MODE == "after_commit":
            print(
                f"CRASHING AFTER COMMIT: P{msg.partition()} O{msg.offset()}",
                flush=True,
            )
            os._exit(1)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer stopped cleanly.")
