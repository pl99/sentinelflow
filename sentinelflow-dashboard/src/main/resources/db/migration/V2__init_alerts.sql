CREATE TABLE IF NOT EXISTS alerts (
    id              VARCHAR(64) PRIMARY KEY,
    title           VARCHAR(512) NOT NULL,
    severity        VARCHAR(32)  NOT NULL DEFAULT 'WARNING',
    status          VARCHAR(32)  NOT NULL DEFAULT 'OPEN',
    source          VARCHAR(255) NOT NULL,
    description     TEXT,
    recommendation  TEXT,
    anomaly_id      VARCHAR(64),
    detected_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_status     ON alerts (status);
CREATE INDEX IF NOT EXISTS idx_alerts_severity   ON alerts (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_detected   ON alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_source     ON alerts (source);
