CREATE TABLE IF NOT EXISTS telemetry_events (
    id              VARCHAR(64) PRIMARY KEY,
    source          VARCHAR(255) NOT NULL,
    type            VARCHAR(64)  NOT NULL,
    subtype         VARCHAR(255),
    timestamp       TIMESTAMPTZ  NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    correlation_id  VARCHAR(255),
    payload         JSONB        DEFAULT '{}',
    tags            JSONB        DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_telemetry_source   ON telemetry_events (source);
CREATE INDEX IF NOT EXISTS idx_telemetry_type     ON telemetry_events (type);
CREATE INDEX IF NOT EXISTS idx_telemetry_ts       ON telemetry_events (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_corr     ON telemetry_events (correlation_id);

CREATE TABLE IF NOT EXISTS metric_points (
    time            TIMESTAMPTZ  NOT NULL,
    id              VARCHAR(64)  NOT NULL,
    service         VARCHAR(255) NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    tags            JSONB        DEFAULT '{}',
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('metric_points', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_metric_service ON metric_points (service);
CREATE INDEX IF NOT EXISTS idx_metric_name    ON metric_points (metric_name);
CREATE INDEX IF NOT EXISTS idx_metric_time    ON metric_points (time DESC);
