CREATE TABLE IF NOT EXISTS events_raw (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service VARCHAR(255) NOT NULL,
    metric VARCHAR(255) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    payload JSONB
);

CREATE INDEX idx_events_raw_service_metric ON events_raw (service, metric, timestamp DESC);
