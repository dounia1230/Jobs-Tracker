"""
============================================================
Moroccan Job Market Analyzer — Base Scraper Class
============================================================
Abstract base class for all scrapers. Provides:
  - Common data structure (JobPosting TypedDict)
  - MinIO upload functionality
  - Kafka producer integration
  - Shared utility methods
============================================================
"""

import json
import logging
import os
import io
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

from minio import Minio
from minio.error import S3Error
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


@dataclass
class JobPosting:
    """Canonical schema for a raw job posting (Bronze layer)."""
    uid: str
    title: str
    company: Optional[str]
    city: Optional[str]
    sector: Optional[str]
    contract_type: Optional[str]
    salary: Optional[str]
    date_posted: Optional[str]
    url: str
    description: Optional[str]
    source: str
    scraped_at: str
    pipeline_run_id: Optional[str] = None
    experience_level: Optional[str] = None
    education_level: Optional[str] = None
    deadline: Optional[str] = None
    is_remote: Optional[bool] = None
    language: Optional[str] = None


class BaseScraper(ABC):
    """
    Abstract base class for all Moroccan job market scrapers.

    Concrete subclasses must implement:
      - SOURCE: str — name of the data source
      - scrape() -> list[dict] — main scraping logic

    Provides:
      - save_to_minio() — uploads raw data to Bronze layer
      - send_to_kafka() — streams each job to Kafka
      - run() — calls scrape + save + stream in sequence
    """

    SOURCE: str = "unknown"

    def __init__(self):
        self.minio_client = self._init_minio()
        self.kafka_producer = self._init_kafka()

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Scrape jobs from the source. Returns list of raw dicts."""
        pass

    def run(self, pipeline_run_id: Optional[str] = None) -> list[dict]:
        """
        Full pipeline:
        1. Scrape jobs from source
        2. Save batch to MinIO bronze layer
        3. Stream each job to Kafka

        Returns the list of scraped jobs.
        """
        logger.info(f"[{self.SOURCE}] Starting run (pipeline_run_id={pipeline_run_id})")

        # ── Step 1: Scrape ─────────────────────────────────────
        jobs = self.scrape()
        if not jobs:
            logger.warning(f"[{self.SOURCE}] No jobs scraped, nothing to save")
            return []

        # Inject pipeline_run_id into each record
        if pipeline_run_id:
            for job in jobs:
                job["pipeline_run_id"] = pipeline_run_id

        # ── Step 2: Save to MinIO Bronze ──────────────────────
        object_path = self._build_object_path()
        self.save_to_minio(jobs, "bronze", object_path)

        # ── Step 3: Stream to Kafka ───────────────────────────
        sent_count = 0
        for job in jobs:
            success = self.send_to_kafka(job)
            if success:
                sent_count += 1

        logger.info(
            f"[{self.SOURCE}] Run complete: {len(jobs)} jobs scraped, "
            f"{sent_count} sent to Kafka, saved to MinIO at {object_path}"
        )
        return jobs

    def save_to_minio(self, jobs: list[dict], bucket: str, object_path: str) -> bool:
        """
        Saves a list of job dicts as a JSON file to MinIO.

        Args:
            jobs: List of raw job posting dicts
            bucket: MinIO bucket name (e.g., 'bronze')
            object_path: Path within bucket (e.g., 'rekrute/2025/01/01/rekrute_20250101_120000.json')

        Returns:
            True if upload succeeded, False otherwise
        """
        if self.minio_client is None:
            logger.error(f"[{self.SOURCE}] MinIO client not initialized, skipping save")
            return False

        try:
            # Serialize to JSON bytes
            content = json.dumps(jobs, ensure_ascii=False, indent=2).encode("utf-8")
            content_stream = io.BytesIO(content)
            content_length = len(content)

            # Ensure bucket exists
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"[{self.SOURCE}] Created bucket: {bucket}")

            # Upload
            self.minio_client.put_object(
                bucket_name=bucket,
                object_name=object_path,
                data=content_stream,
                length=content_length,
                content_type="application/json",
                metadata={
                    "source": self.SOURCE,
                    "job_count": str(len(jobs)),
                    "scraped_at": datetime.utcnow().isoformat(),
                }
            )

            logger.info(f"[{self.SOURCE}] Saved {len(jobs)} jobs to s3://{bucket}/{object_path}")
            return True

        except S3Error as e:
            logger.error(f"[{self.SOURCE}] MinIO S3Error: {e}")
            return False
        except Exception as e:
            logger.error(f"[{self.SOURCE}] Unexpected error saving to MinIO: {e}")
            return False

    def send_to_kafka(self, job: dict, topic: str = "jobs-raw") -> bool:
        """
        Sends a single job posting to Kafka.

        Args:
            job: Job posting dict
            topic: Kafka topic name

        Returns:
            True if send succeeded, False otherwise
        """
        if self.kafka_producer is None:
            return False  # Kafka unavailable, skip silently

        try:
            message = json.dumps(job, ensure_ascii=False).encode("utf-8")
            future = self.kafka_producer.send(
                topic,
                value=message,
                key=job.get("uid", "").encode("utf-8")
            )
            # Non-blocking: don't wait for ACK in hot path
            return True

        except KafkaError as e:
            logger.warning(f"[{self.SOURCE}] Kafka send error: {e}")
            return False
        except Exception as e:
            logger.warning(f"[{self.SOURCE}] Error sending to Kafka: {e}")
            return False

    def flush_kafka(self):
        """Flush all pending Kafka messages. Call at end of run."""
        if self.kafka_producer:
            try:
                self.kafka_producer.flush(timeout=10)
            except Exception as e:
                logger.warning(f"[{self.SOURCE}] Error flushing Kafka: {e}")

    def _build_object_path(self) -> str:
        """
        Builds the MinIO object path following naming convention:
        {source}/{YYYY}/{MM}/{DD}/{source}_{YYYYMMDD_HHMMSS}.json
        """
        now = datetime.utcnow()
        filename = f"{self.SOURCE}_{now.strftime('%Y%m%d_%H%M%S')}.json"
        return f"{self.SOURCE}/{now.year}/{now.month:02d}/{now.day:02d}/{filename}"

    def _init_minio(self) -> Optional[Minio]:
        """Initialize MinIO client from environment variables."""
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

        try:
            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False  # HTTP inside Docker network
            )
            # Verify connection by listing buckets
            _ = list(client.list_buckets())
            logger.info(f"[{self.SOURCE}] MinIO connected at {endpoint}")
            return client

        except Exception as e:
            logger.warning(f"[{self.SOURCE}] MinIO not available ({e}), will save locally only")
            return None

    def _init_kafka(self) -> Optional[KafkaProducer]:
        """Initialize Kafka producer from environment variables."""
        broker = os.getenv("KAFKA_BROKER", "localhost:9092")

        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                retries=3,
                request_timeout_ms=10000,
                acks=1,
            )
            logger.info(f"[{self.SOURCE}] Kafka producer connected to {broker}")
            return producer

        except Exception as e:
            logger.warning(f"[{self.SOURCE}] Kafka not available ({e}), streaming disabled")
            return None
