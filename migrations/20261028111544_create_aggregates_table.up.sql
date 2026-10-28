CREATE TABLE IF NOT EXISTS aggregates (
    window_start TIMESTAMPTZ NOT NULL,
    service VARCHAR(255) NOT NULL,
    metric VARCHAR(255) NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    sum DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    min DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    max DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p99 DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    PRIMARY KEY (window_start, service, metric)
);

CREATE INDEX idx_aggregates_service_metric_window ON aggregates (service, metric, window_start DESC);
