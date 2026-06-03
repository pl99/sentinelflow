DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_alerts_anomaly_id') THEN
    DELETE FROM alerts;
    ALTER TABLE alerts ADD CONSTRAINT uq_alerts_anomaly_id UNIQUE (anomaly_id);
  END IF;
END $$;