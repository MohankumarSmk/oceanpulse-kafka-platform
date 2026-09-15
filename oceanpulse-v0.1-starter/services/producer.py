import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from services.common import BOOTSTRAP, TOPIC

VESSELS = [v.strip() for v in os.getenv(
    "OCEANPULSE_VESSELS", "DubaiSky,Selena,Queenie,H429"
).split(",") if v.strip()]
INTERVAL = float(os.getenv("OCEANPULSE_INTERVAL_SECONDS", "1"))

CHANNELS = [
    ("MAIN_ENGINE_RPM", "RPM", 620, 820),
    ("LO_INLET_PRESSURE", "bar", 2.1, 5.2),
    ("COOLING_WATER_TEMP", "C", 65, 92),
    ("EXHAUST_TEMP_CYL_1", "C", 280, 440),
    ("ENGINE_LOAD", "%", 25, 95),
]

running = True

def stop(*_):
    global running
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "client.id": "oceanpulse-edge-simulator",
})

sequence = 0

def delivered(err, msg):
    if err:
        print(f"DELIVERY FAILED: {err}")
    else:
        print(
            f"ACK vessel={msg.key().decode()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )

print(f"OceanPulse producer -> {BOOTSTRAP} / {TOPIC}")
print("Press Ctrl+C to stop.")

while running:
    sequence += 1
    vessel = random.choice(VESSELS)
    engine = random.choice(["ME01", "ME02"])
    channel, unit, low, high = random.choice(CHANNELS)
    event = {
        "event_id": str(uuid.uuid4()),
        "vessel_id": vessel,
        "engine_id": engine,
        "channel": channel,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "value": round(random.uniform(low, high), 2),
        "unit": unit,
        "sequence_no": sequence,
    }

    # Keeping the same vessel+engine on the same partition gives us
    # per-engine ordering, which we will study in Level 1.
    key = f"{vessel}:{engine}"

    producer.produce(
        TOPIC,
        key=key.encode(),
        value=json.dumps(event).encode(),
        callback=delivered,
    )
    producer.poll(0)
    time.sleep(INTERVAL)

producer.flush(10)
print("Producer stopped cleanly.")
