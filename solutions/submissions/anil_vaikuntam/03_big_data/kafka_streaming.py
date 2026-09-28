"""
=============================================================
StackUp Engineering Academy — Data Engineering Assessment
Solution File: kafka_streaming.py
Pillar: Big Data Processing (Task 3.2)
Author: Anil Vaikuntam
=============================================================

SCENARIO
--------
Real-time Kafka producer/consumer simulating the platform event stream,
with severity-based forwarding of Critical escalations to a second topic.

HOW TO RUN
----------
  Kafka must be up: docker compose up -d
  python solutions/submissions/anil_vaikuntam/03_big_data/kafka_streaming.py --mode both
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_FILE = os.path.join(BASE_DIR, "datasets", "events_stream", "events_2025_01.jsonl")
# outputs/results/anil_vaikuntam/... — the submission-folder convention, so
# multiple trainees' outputs in the shared repo don't collide.
RESULTS_DIR = os.path.join(BASE_DIR, "outputs", "results", "anil_vaikuntam", "03_big_data", "kafka")
os.makedirs(RESULTS_DIR, exist_ok=True)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS = "presight.project.events"
TOPIC_ESCALATIONS = "presight.escalations.critical"


# ==============================================================================
# SETUP — topics
# ==============================================================================

def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="presight-admin")
    topics = [
        NewTopic(name=TOPIC_EVENTS, num_partitions=3, replication_factor=1),
        NewTopic(name=TOPIC_ESCALATIONS, num_partitions=1, replication_factor=1),
    ]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        logger.info("Created topics: %s, %s", TOPIC_EVENTS, TOPIC_ESCALATIONS)
    except TopicAlreadyExistsError:
        logger.info("Topics already exist — skipping creation (idempotent re-run)")
    finally:
        admin.close()


# ==============================================================================
# PRODUCER
# ==============================================================================

def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        request_timeout_ms=30000,
    )


def run_producer(producer, events_file: str, delay_seconds: float = 0.05):
    """Streams events one at a time, keyed by event_type so the default
    partitioner routes same-type events to the same partition."""
    logger.info("Starting producer — streaming events from: %s", events_file)
    sent = 0
    with open(events_file, "r") as f:
        for line in f:
            event = json.loads(line)
            event["produced_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(TOPIC_EVENTS, key=event["event_type"], value=event)
            sent += 1
            if sent % 100 == 0:
                logger.info("Produced %d messages (last: %s / %s)", sent, event["event_id"], event["event_type"])
            time.sleep(delay_seconds)
    producer.flush()
    producer.close()
    logger.info("Producer done. Total sent: %d", sent)
    return sent


# ==============================================================================
# CONSUMER
# ==============================================================================

def build_consumer(topic: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="presight-assessment-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )


def run_consumer(consumer):
    logger.info("Starting consumer — listening on: %s", TOPIC_EVENTS)

    escalation_producer = build_producer()
    summary = {}
    consumed = 0
    forwarded = 0
    start = time.time()

    for message in consumer:
        event = message.value
        event_type = event.get("event_type")
        consumed += 1
        summary[event_type] = summary.get(event_type, 0) + 1

        if consumed % 100 == 0:
            logger.info("Consumed %d messages (last: %s / %s / project=%s)",
                        consumed, event.get("event_id"), event_type, event.get("project_id"))

        if event_type == "escalation_raised" and event.get("payload", {}).get("severity") == "Critical":
            # Pass raw str/dict — escalation_producer already has
            # key_serializer/value_serializer configured.
            escalation_producer.send(TOPIC_ESCALATIONS, key=event["event_id"], value=event)
            forwarded += 1

    escalation_producer.flush()
    escalation_producer.close()
    elapsed = time.time() - start
    throughput = consumed / elapsed if elapsed > 0 else 0

    logger.info("Consumer done. Total consumed: %d | Critical escalations forwarded: %d", consumed, forwarded)

    summary_path = os.path.join(RESULTS_DIR, "summary.json")
    payload = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "topic": TOPIC_EVENTS,
        "total_messages_consumed": consumed,
        "event_counts": summary,
        "critical_escalations_forwarded": forwarded,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_messages_per_sec": round(throughput, 2),
    }
    with open(summary_path, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info("Summary written to: %s", summary_path)

    return payload


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kafka assessment — producer/consumer")
    parser.add_argument("--mode", choices=["producer", "consumer", "both"], default="both")
    args = parser.parse_args()

    create_topics()

    if args.mode == "producer":
        run_producer(build_producer(), EVENTS_FILE)
    elif args.mode == "consumer":
        run_consumer(build_consumer(TOPIC_EVENTS))
    elif args.mode == "both":
        run_producer(build_producer(), EVENTS_FILE)
        summary = run_consumer(build_consumer(TOPIC_EVENTS))
        print("\nEvent summary:", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
