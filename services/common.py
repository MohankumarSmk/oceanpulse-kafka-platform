import os

def env(name: str, default: str) -> str:
    return os.getenv(name, default)

BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = env("KAFKA_TOPIC", "oceanpulse.telemetry.raw.v1")
