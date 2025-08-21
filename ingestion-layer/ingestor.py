import json
import time
import logging
import psycopg2
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Example constants
PG_DSN = "dbname=streaming_db user=analytics_user password=myStrongPassword123 host=postgresql port=5432"
BATCH = 100
SLEEP = 5

# Kafka producer config
p = Producer({'bootstrap.servers': 'kafka:29092'})

def delivery(err, msg):
    if err:
        logging.error(f"Failed to deliver message {msg.key()}: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def run_once():
    logging.info("Starting batch fetch from outbox_events")
    try:
        with psycopg2.connect(PG_DSN, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, topic, key, payload
                    FROM outbox_events
                    WHERE status='pending'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                """, (BATCH,))
                rows = cur.fetchall()

                if not rows:
                    logging.info("No pending rows found, rolling back")
                    conn.rollback()
                    return 0

                logging.info(f"Fetched {len(rows)} rows to produce to Kafka")

                # Produce all rows
                for oid, topic, key, payload in rows:
                    try:
                        logging.info(f"Producing row id={oid} to topic={topic}")
                        p.produce(topic=topic, key=key, value=json.dumps(payload, default=str), on_delivery=delivery)
                    except Exception as e:
                        logging.error(f"Error producing row id={oid}: {e}")

                p.flush()
                logging.info("All messages flushed to Kafka")

                # Mark as sent
                ids = [r[0] for r in rows]
                cur.execute("""
                    UPDATE outbox_events
                       SET status='sent', sent_at=now()
                     WHERE id = ANY(%s)
                """, (ids,))
                conn.commit()
                logging.info(f"Marked {len(ids)} rows as sent in Postgres")

                return len(rows)
    except Exception as e:
        logging.exception(f"Error in run_once: {e}")
        return 0

if __name__ == "__main__":
    logging.info("Starting ingestion loop")
    while True:
        n = run_once()
        if n == 0:
            logging.info(f"No rows processed, sleeping for {SLEEP} seconds")
            time.sleep(SLEEP)
