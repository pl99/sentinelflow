#!/bin/bash
set -e

FLINK_JOBMANAGER="${FLINK_JOBMANAGER_URL:-flink-jobmanager}"
FLINK_PORT="${FLINK_PORT:-8081}"
FLINK_BIN="${FLINK_HOME}/bin/flink"

echo "Waiting for Flink JobManager at ${FLINK_JOBMANAGER}:${FLINK_PORT}..."
until curl -sf "http://${FLINK_JOBMANAGER}:${FLINK_PORT}/taskmanagers" > /dev/null 2>&1; do
  sleep 3
done
echo "JobManager is ready."

echo "Submitting Processor Job..."
"${FLINK_BIN}" run -d -m "${FLINK_JOBMANAGER}:${FLINK_PORT}" \
  /jobs/sentinelflow-processor.jar "kafka:9094"

echo "Submitting Detector Job..."
"${FLINK_BIN}" run -d -m "${FLINK_JOBMANAGER}:${FLINK_PORT}" \
  /jobs/sentinelflow-detector.jar "kafka:9094"

echo "Both Flink jobs submitted successfully."
echo "Monitoring Flink UI at http://${FLINK_JOBMANAGER}:${FLINK_PORT}"
echo "Container will stay alive for health checks."

# Keep container alive for docker health checks
tail -f /dev/null
