-- Keyspace for analytics
CREATE KEYSPACE IF NOT EXISTS content_analytics
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Switch to the keyspace
USE content_analytics;

-- Table for engagement analytics
CREATE TABLE IF NOT EXISTS engagement_metrics (
    event_id UUID,
    content_id UUID,
    user_id UUID,
    event_time TIMESTAMP,
    duration_ms BIGINT,
    content_type TEXT,
    length_seconds INT,
    engagement_seconds DOUBLE,
    engagement_pct DECIMAL,
    PRIMARY KEY ((content_id), event_time, user_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- Optional: Index for fast queries by user_id
CREATE INDEX IF NOT EXISTS idx_user_id ON engagement_metrics (user_id);

-- Optional: Index for content_type queries
CREATE INDEX IF NOT EXISTS idx_content_type ON engagement_metrics (content_type);
