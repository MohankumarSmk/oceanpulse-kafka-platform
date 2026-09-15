import json
import os
from confluent_kafka import Consumer, KafkaException
from services.common import BOOTSTRAP, TOPIC

GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "oceanpulse-terminal-v1")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])

print(f"OceanPulse consumer group={GROUP}")
print("Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        event = json.loads(msg.value())
        print(
            f"P{msg.partition()} O{msg.offset():<6} "
            f"{event['vessel_id']:<10} {event['engine_id']} "
            f"{event['channel']:<22} {event['value']} {event['unit']}"
        )

        # Manual commit is intentional: later we will crash this consumer
        # before/after commits to understand at-least-once delivery.
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer stopped cleanly.")
