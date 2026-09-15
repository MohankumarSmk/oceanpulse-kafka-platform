.PHONY: up down topic describe producer consumer web status clean

up:
	docker compose up -d

down:
	docker compose down

topic:
	docker exec oceanpulse-kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 \
	  --create --if-not-exists \
	  --topic oceanpulse.telemetry.raw.v1 \
	  --partitions 6 \
	  --replication-factor 1

describe:
	docker exec oceanpulse-kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 \
	  --describe --topic oceanpulse.telemetry.raw.v1

producer:
	python services/producer.py

consumer:
	python services/consumer.py

web:
	uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload

status:
	docker compose ps

clean:
	docker compose down -v --remove-orphans
