import os
import json
import time
import logging
import psycopg2
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class PostgresConnector:
    """Handles Postgres connection and queries."""

    def __init__(self):
        self.dsn = (
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')} "
            f"host={os.getenv('POSTGRES_HOST')} "
            f"port={os.getenv('POSTGRES_PORT')}"
        )

    def fetch_pending_events(self, batch_size):
        """Fetch pending rows from outbox_events."""
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, topic, key, payload
                    FROM outbox_events
                    WHERE status='pending'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                """, (batch_size,))
                return cur.fetchall()

    def mark_as_sent(self, ids):
        """Update events as sent."""
        if not ids:
            return
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE outbox_events
                       SET status='sent', sent_at=now()
                     WHERE id = ANY(%s)
                """, (ids,))
                conn.commit()
                logger.info(f"Marked {len(ids)} rows as sent in Postgres")


class KafkaProducerWrapper:
    """Kafka producer wrapper for message delivery."""

    def __init__(self):
        self.producer = Producer({'bootstrap.servers': os.getenv("KAFKA_BROKER")})
        self.successful_ids = []
        self.failed_ids = []

    def delivery_report(self, oid, err, msg):
        """Delivery callback for Kafka."""
        if err:
            logger.error(f"Failed to deliver row id={oid}, topic={msg.topic()}: {err}")
            self.failed_ids.append(oid)
        else:
            logger.info(f"Successfully delivered row id={oid} to {msg.topic()} [{msg.partition()}]")
            self.successful_ids.append(oid)

    def produce(self, oid, topic, key, payload):
        try:
            logger.info(f"Producing row id={oid} to topic={topic}")
            self.producer.produce(
                topic=topic,
                key=key,
                value=json.dumps(payload, default=str),
                callback=lambda err, msg, oid=oid: self.delivery_report(oid, err, msg)
            )
        except Exception as e:
            logger.error(f"Error producing row id={oid}: {e}")
            self.failed_ids.append(oid)

    def flush(self):
        self.producer.flush()
        logger.info("All messages flushed to Kafka")
        return self.successful_ids, self.failed_ids


class Ingestor:
    """Main ingestion service."""

    def __init__(self):
        self.db = PostgresConnector()
        self.kafka = KafkaProducerWrapper()
        self.batch_size = int(os.getenv("BATCH_SIZE", 100))
        self.sleep_interval = int(os.getenv("SLEEP_INTERVAL", 5))

    def run_once(self):
        logger.info("Starting batch fetch from outbox_events")
        try:
            rows = self.db.fetch_pending_events(self.batch_size)

            if not rows:
                logger.info("No pending rows found")
                return 0

            logger.info(f"Fetched {len(rows)} rows to produce to Kafka")

            for oid, topic, key, payload in rows:
                self.kafka.produce(oid, topic, key, payload)

            successful, failed = self.kafka.flush()

            # ✅ Only mark successfully delivered rows
            if successful:
                self.db.mark_as_sent(successful)

            if failed:
                logger.warning(f"{len(failed)} rows failed delivery and will remain pending")

            return len(successful)

        except Exception as e:
            logger.exception(f"Error in run_once: {e}")
            return 0

    def run_forever(self):
        logger.info("Starting ingestion loop")
        while True:
            n = self.run_once()
            if n == 0:
                logger.info(f"No rows processed, sleeping for {self.sleep_interval} seconds")
                time.sleep(self.sleep_interval)
