## 🌟 Design Decisions and Reasoning

### Why Real-Time Analytics?
Real-time analytics enables businesses to make immediate decisions based on user engagement data. This project was designed to handle high-throughput event streams while maintaining scalability and fault tolerance.
## **Architecture**
![Architecture](architecture-8-UseCase.drawio.svg)
### Key Design Choices:
1. **PostgreSQL for Source of Truth**:
  - Relational databases are ideal for structured data and transactional integrity.
  - PostgreSQL was chosen for its reliability and compatibility with Docker.

2. **Kafka for Event Streaming**:
  - Kafka provides durability, scalability, and backpressure handling.
  - It decouples producers and consumers, ensuring flexibility in downstream processing.

3. **Spark Structured Streaming for Processing**:
  - Spark was selected for its ability to handle large-scale data processing.
  - Structured Streaming simplifies real-time computations with built-in fault tolerance.

4. **Cassandra for Long-Term Storage Since I reached out Project Limit in my GCP Console Account to use BigQuery**:
  - Cassandra offers high availability and scalability for analytics data.
  - It is optimized for write-heavy workloads, making it ideal for storing engagement metrics.

5. **Redis for Low-Latency Serving**:
  - Redis was chosen for its in-memory data store capabilities.
  - It ensures fast access to frequently queried data, improving API response times.

---


## 🧱 Layers Explained

### 1. **Data Source Layer**

#### 📌 Purpose:
This layer holds the source of truth for raw data — including content metadata and engagement events.

#### 🛠️ Tools:
- **PostgreSQL**: Hosted in Docker.
- **Tables**:
  - `content`: Holds metadata about content (type, length, etc.)
  - `engagement_events`: Raw user interactions with content.
  - `outbox_events`: Tracks events to be published to Kafka.

#### ⚙️ How It Works:
- We run a SQL script (`setup.sql`) to bootstrap the schema.
- Engagement events are sent via a small Python API producer (`ingestorProducer`) to Kafka.

---

### 2. **Ingestion Layer**

#### 📌 Purpose:
Ingests raw events from PostgreSQL (via outbox) and pushes them to Kafka topics.

#### 🛠️ Tools:
- **Python Producer** (Dockerized): Sends engagement events to Kafka.
- **Kafka Topics**:
  - `engagement_events`

#### ⚙️ How It Works:
- Python reads rows from PostgreSQL and pushes JSON payloads to Kafka.
- Kafka handles buffering, durability, and backpressure.

---

### 3. **Message Broker Layer**

#### 📌 Purpose:
Provides decoupling and durability for event transport.

#### 🛠️ Tools:
- **Kafka (KRaft mode)**: Broker for pub/sub messaging.
- **Kafdrop**: Web UI for inspecting Kafka topics/messages.

#### ⚙️ How It Works:
- Engagement events are produced into the `engagement_events` topic.
- Spark Structured Streaming consumes the topic downstream.

---

### 4. **Stream Processing Layer**

#### 📌 Purpose:
Consumes Kafka messages, enriches data with content metadata, calculates engagement metrics, and writes results to Cassandra and Redis.

#### 🛠️ Tools:
- **Apache Spark** (Standalone cluster with master + workers)
- **Python (Spark job)**: Using `pyspark`.

#### ⚙️ How It Works:
- Reads `engagement_events` from Kafka using Spark Structured Streaming.
- Enriches events by joining with the `content` table in PostgreSQL.
- Computes:
  - `engagement_seconds = duration_ms / 1000`
  - `engagement_pct = engagement_seconds / length_seconds`
- Writes enriched data to:
  - **Cassandra** for long-term serving.
  - **Redis** for caching.

---

### 5. **Serving Layer**

#### 📌 Purpose:
Makes processed data available to downstream systems and APIs.

#### 🛠️ Tools:
- **Cassandra**: Primary analytics store (historical queries).
- **Redis**: Low-latency caching layer for serving APIs.
- **External APIs**: for the exetrnal system via REST APIs. (TO DO)

#### ⚙️ How It Works:
- Spark writes enriched metrics into `content_analytics.engagement_metrics` (Cassandra).
- Redis caches event-level data keyed by `event_id`.
- APIs or BI tools can consume data from either source.

---


## 📊 Analytics Metrics

### Engagement Metrics Calculated:
- **Engagement Seconds**: Total time spent engaging with content.
- **Engagement Percentage**: Ratio of engagement time to content length.

### Data Flow:
1. Events are ingested from PostgreSQL into Kafka.
2. Spark enriches and computes metrics.
3. Metrics are stored in Cassandra for historical queries.
4. Redis caches data for low-latency API responses.

---

## 🛡️ Fault Tolerance and Scalability

### Fault Tolerance:
- Kafka ensures durability of events.
- Spark Structured Streaming provides checkpointing for recovery.

### Scalability:
- Kafka partitions allow horizontal scaling of producers and consumers.
- Cassandra's distributed architecture supports high write throughput.

---
---


## 🏗️ Execution Methodology


## 🛠️ Getting Started (Run Locally)

### ✅ Prerequisites

- Docker & Docker Compose
- Python 3.12+

Run the docker compose :

``` docker compose build up ```
# wait docker setup all service ....

### Lets' simulate an cas d'usage:
open postgresql console from a posgresql_container via:
``` docker exec -it ontainer_PostgreSQL psql -U analytics_user -d streaming_db ```
# you will see an interface to log via password
password: myStrongPassword123

Now you will enter to postgresql SQL instance

To check if table are created or not run:
``` \dt ```
You will see List of created realations as shown below:

Schema |       Name        | Type  |     Owner      
--------+-------------------+-------+----------------
 public | content           | table | analytics_user
 public | engagement_events | table | analytics_user
 public | outbox_events     | table | analytics_user

then run the first insert data to content table:


-- =====================================
-- CONTENT TABLE TEST DATA
-- =====================================

```
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'daily-news-ep1', 'Daily News – Episode 1', 'podcast', 1800, NOW() - INTERVAL '2 days'),
    ('22222222-2222-2222-2222-222222222222', 'tech-weekly', 'Tech Weekly Newsletter', 'newsletter', NULL, NOW() - INTERVAL '1 day'),
    ('33333333-3333-3333-3333-333333333333', 'ai-documentary', 'AI Documentary 2025', 'video', 5400, NOW() - INTERVAL '3 hours');

```

Insert also data to :
-- =====================================
-- ENGAGEMENT_EVENTS TABLE TEST DATA
-- =====================================

``` INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'play',   NOW() - INTERVAL '1 hour', 30000, 'ios', '{"bitrate": "128kbps"}'),
    ('11111111-1111-1111-1111-111111111111', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'pause',  NOW() - INTERVAL '59 minutes', NULL, 'ios', '{"reason": "incoming call"}'),
    ('11111111-1111-1111-1111-111111111111', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'finish', NOW() - INTERVAL '30 minutes', 1800000, 'web-safari', '{"quality": "HD"}'),
    ('33333333-3333-3333-3333-333333333333', 'cccccccc-cccc-cccc-cccc-cccccccccccc', 'click',  NOW() - INTERVAL '10 minutes', NULL, 'android', '{"button": "subscribe"}');
```

When you insert the data to engagement_events table A Trriger will invoked to set related data fields to outbox_events table to ingest it to kafka from the ingestor layer

Go to spark job log to see what happen via:

```docker compose logs -f processing```

Then go to cassandra service to check the transformed data serving :

``` docker exec -it Container_Cassandra cqlsh -u cassandra -p cassandra ```
Then
execute cassandra queries below

``` DESCRIBE KEYSPACES; ```
you will see content_analytics keyspace

To enter into content_analytics keyspace : ``` USE content_analytics;```

Execute: ``` select * from content_analytics.engagement_metrics ; ```

For Redis go to browser and Tape: localhost:8001

## 1. Check if keys exist:
Enter to CLI console and run: KEYS engagement_events:*
OUTPUT:
```
1) "engagement_events:event:2"
2) "engagement_events:event:3"
3) "engagement_events:event:1"
4) "engagement_events:event:4"

```
## 2. Inspect one record
HGETALL engagement_events:event:1
OUPTUT: 
```
1) "user_id"
2) "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
3) "event_time_str"
4) "2025-08-25 16:13:40.436907"
5) "engagement_seconds_str"
6) "30.0"
7) "duration_ms_str"
8) "30000"
9) "event_id"
10) "1"
11) "content_id"
12) "11111111-1111-1111-1111-111111111111"
```

TO DO:
We stilling develop the BACKFILL Mechanisme && REST API for the ThirParty Systems

---

## 🚀 Step-by-Step Setup

### 1. **Clone the Repository**

```bash
git clone https://github.com/ELMEHDIEttaki/real-time-engagement.git
cd real-time-engagement

docker compose build up