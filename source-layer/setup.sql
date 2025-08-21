-- Content catalogue ----------------------------------------------
CREATE TABLE content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER, 
    publish_ts      TIMESTAMPTZ NOT NULL
);

-- Raw engagement telemetry ---------------------------------------
CREATE TABLE engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,      -- nullable for events without duration
    device       TEXT,         -- e.g. "ios", "web‑safari"
    raw_payload  JSONB         -- anything extra the client sends
);


-- outbox: one row per event to publish
CREATE TABLE IF NOT EXISTS outbox_events (
  id           BIGSERIAL PRIMARY KEY,
  aggregate_id BIGINT NOT NULL,           -- engagement_events.id
  topic        TEXT NOT NULL,             -- 'engagement_events'
  key          TEXT NOT NULL,             -- event key (stringified id)
  payload      JSONB NOT NULL,            -- full event as JSON
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at      TIMESTAMPTZ,               -- set by relay
  status       TEXT NOT NULL DEFAULT 'pending'  -- pending|sent|error
);

-- trigger: on INSERT to engagement_events, enqueue to outbox
CREATE OR REPLACE FUNCTION enqueue_outbox() RETURNS trigger AS $$
BEGIN
  INSERT INTO outbox_events (aggregate_id, topic, key, payload)
  VALUES (NEW.id,
          'engagement_events',
          NEW.id::text,
          to_jsonb(NEW));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_engagement_outbox ON engagement_events;
CREATE TRIGGER tr_engagement_outbox
AFTER INSERT ON engagement_events
FOR EACH ROW EXECUTE FUNCTION enqueue_outbox();

-- helpful indexes
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, id);
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON engagement_events(event_ts);