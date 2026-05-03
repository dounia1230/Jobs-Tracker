"""
============================================================
Moroccan Job Market Analyzer — Kafka Consumer
============================================================
Continuously consumes job events from Kafka topic 'jobs-raw'
and writes them to MinIO in micro-batches for near real-time
dashboard updates.

Run inside Docker: python kafka_consumer.py
============================================================
"""

import json
import logging
import os
import io
import signal
import sys
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "jobs-raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

BATCH_SIZE = 50          # Write to MinIO every N messages
BATCH_TIMEOUT_SEC = 60   # Or every 60 seconds, whichever comes first

# Graceful shutdown flag
_shutdown = False

def handle_shutdown(sig, frame):
    global _shutdown
    logger.info("Shutdown signal received — flushing final batch...")
    _shutdown = True

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def get_minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def flush_batch(client: Minio, batch: list[dict]) -> None:
    """Write a micro-batch of jobs to MinIO bronze layer."""
    if not batch:
        return

    # Group by source for organized storage
    by_source = defaultdict(list)
    for job in batch:
        source = job.get("source", "unknown")
        by_source[source].append(job)

    now = datetime.utcnow()
    for source, jobs in by_source.items():
        content = json.dumps(jobs, ensure_ascii=False).encode("utf-8")
        object_path = (
            f"{source}/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{source}_kafka_{now.strftime('%H%M%S_%f')}.json"
        )
        try:
            if not client.bucket_exists("bronze"):
                client.make_bucket("bronze")
            client.put_object(
                "bronze", object_path,
                io.BytesIO(content), len(content),
                content_type="application/json"
            )
            logger.info(f"Flushed {len(jobs)} jobs from '{source}' to s3://bronze/{object_path}")
        except Exception as e:
            logger.error(f"Error flushing batch to MinIO: {e}")


def run_consumer():
    """Main Kafka consumer loop."""
    logger.info(f"Starting Kafka consumer on {KAFKA_BROKER}, topic={KAFKA_TOPIC}")

    minio_client = get_minio()
    batch: list[dict] = []
    last_flush = datetime.utcnow()
    total_consumed = 0

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="job_market_consumer",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,   # 5s timeout between polls
        )
        logger.info("Kafka consumer connected ✅")
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    while not _shutdown:
        try:
            for message in consumer:
                job = message.value
                batch.append(job)
                total_consumed += 1

                elapsed = (datetime.utcnow() - last_flush).total_seconds()
                if len(batch) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT_SEC:
                    flush_batch(minio_client, batch)
                    batch.clear()
                    last_flush = datetime.utcnow()

                if _shutdown:
                    break

        except Exception as e:
            logger.error(f"Consumer error: {e}")

    # Final flush on shutdown
    if batch:
        flush_batch(minio_client, batch)
    logger.info(f"Consumer stopped. Total messages consumed: {total_consumed}")
    consumer.close()


if __name__ == "__main__":
    run_consumer()
