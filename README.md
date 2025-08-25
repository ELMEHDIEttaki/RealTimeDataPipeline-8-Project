⚡ Real-Time Engagement Analytics Platform

A distributed data pipeline for real-time engagement analytics, designed to process high-throughput events (plays, pauses, clicks, etc.) and serve both low-latency APIs and long-term analytics queries.

Built with PostgreSQL, Kafka, Spark Structured Streaming, Cassandra, and Redis — all containerized via Docker.

🌟 Design Decisions and Reasoning
Why Real-Time Analytics?

Real-time analytics enables businesses to make immediate decisions based on user engagement data (e.g., boosting content recommendations, detecting drop-offs).

This project was designed to handle high-throughput event streams while maintaining scalability and fault tolerance.

🏗️ Architecture
![Architecture](architecture-8-UseCase.drawio.svg)
🔑 Key Design Choices

PostgreSQL as Source of Truth

Ideal for structured metadata & transactional integrity.

Used as the central store for content & engagement events.

Kafka for Event Streaming

Handles durability, scalability, and backpressure.

Decouples producers and consumers for flexibility.

Spark Structured Streaming for Processing

Efficient at handling large-scale real-time computations.

Provides checkpointing and fault tolerance.

Cassandra for Long-Term Storage

Distributed, highly available, write-optimized.

Chosen instead of BigQuery (due to GCP quota limits).

Redis for Low-Latency Serving

In-memory caching for instant API responses.

Stores event-level engagement data keyed by event_id.

🧱 Layered Architecture
1️⃣ Data Source Layer

📌 Purpose: Holds raw metadata and events.

🛠️ Tools:

PostgreSQL (Dockerized)

📂 Tables:

content: Metadata (type, length, publish date, etc.)

engagement_events: Raw user interactions (play, pause, etc.)

outbox_events: Event sourcing table → ingested into Kafka

⚙️ Flow:

Bootstrap schema via setup.sql.

Insert data manually (for tests) or through a small Python producer (ingestorProducer).

2️⃣ Ingestion Layer

📌 Purpose: Pushes new events to Kafka.

🛠️ Tools:

Python Producer (Dockerized)

Kafka Topics (engagement_events)

⚙️ Flow:

New rows in outbox_events are published as JSON to Kafka.

3️⃣ Message Broker Layer

📌 Purpose: Decouples producers from stream processing.

🛠️ Tools:

Kafka (KRaft mode) for pub/sub messaging.

Kafdrop UI for inspecting topics/messages.

⚙️ Flow:

Producers → Kafka → Consumers (Spark, etc.)

4️⃣ Stream Processing Layer

📌 Purpose: Enriches, transforms, and routes engagement data.

🛠️ Tools:

Apache Spark (Standalone cluster)

PySpark job (EngagementProcessor)

⚙️ Flow:

Consume from engagement_events (Kafka).

Enrich events with PostgreSQL content table.

Compute:

engagement_seconds = duration_ms / 1000

engagement_pct = engagement_seconds / length_seconds

Write to Cassandra & Redis.

5️⃣ Serving Layer

📌 Purpose: Provides APIs and queries on processed data.

🛠️ Tools:

Cassandra for historical analytics.

Redis for cached lookups.

APIs (TO DO) → External consumers via REST/GraphQL.

⚙️ Flow:

Spark → Cassandra (engagement_metrics table).

Spark → Redis (engagement_events:event:{id}).

📊 Metrics Computed

Engagement Seconds: Total time spent engaging with content.

Engagement Percentage: Ratio of engagement time to total content length.

🛡️ Fault Tolerance & Scalability

✅ Kafka → Durable event log.

✅ Spark Structured Streaming → Checkpoint recovery.

✅ Cassandra → Horizontally scalable writes.

✅ Redis → Low-latency reads.

🚀 Execution Methodology
✅ Prerequisites

Docker & Docker Compose

Python 3.12+

.env file for configuration (see below)

⚙️ Setup
git clone https://github.com/ELMEHDIEttaki/real-time-engagement.git
cd real-time-engagement

# Run everything
docker compose up --build

🧪 Usage Example
1. Insert Content Metadata
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'daily-news-ep1', 'Daily News – Episode 1', 'podcast', 1800, NOW() - INTERVAL '2 days'),
  ('22222222-2222-2222-2222-222222222222', 'tech-weekly', 'Tech Weekly Newsletter', 'newsletter', NULL, NOW() - INTERVAL '1 day'),
  ('33333333-3333-3333-3333-333333333333', 'ai-documentary', 'AI Documentary 2025', 'video', 5400, NOW() - INTERVAL '3 hours');

2. Insert Engagement Events
INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'play',   NOW() - INTERVAL '1 hour', 30000, 'ios', '{"bitrate": "128kbps"}'),
  ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'pause',  NOW() - INTERVAL '59 minutes', NULL, 'ios', '{"reason": "incoming call"}'),
  ('11111111-1111-1111-1111-111111111111', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'finish', NOW() - INTERVAL '30 minutes', 1800000, 'web-safari', '{"quality": "HD"}'),
  ('33333333-3333-3333-3333-333333333333', 'cccccccc-cccc-cccc-cccc-cccccccccccc', 'click',  NOW() - INTERVAL '10 minutes', NULL, 'android', '{"button": "subscribe"}');


🔄 A trigger will insert into outbox_events → ingestor picks it up → sends to Kafka.

🔍 Verification
1. Spark Logs
docker compose logs -f processing

2. Cassandra
docker exec -it Container_Cassandra cqlsh -u cassandra -p cassandra
USE content_analytics;
SELECT * FROM engagement_metrics;

3. Redis

Web UI → http://localhost:8001

CLI:

# List keys
KEYS engagement_events:*

# Inspect one
HGETALL engagement_events:event:1
