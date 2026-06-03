#!/bin/sh
set -e

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9094}"
RETRIES=30
SLEEP=2

echo "Waiting for Kafka at $BOOTSTRAP_SERVER..."
i=0
while [ $i -lt $RETRIES ]; do
  if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi
  i=$((i + 1))
  echo "Kafka not ready yet (attempt $i/$RETRIES), sleeping ${SLEEP}s..."
  sleep $SLEEP
done
if [ $i -eq $RETRIES ]; then
  echo "ERROR: Kafka did not become ready after $RETRIES attempts."
  exit 1
fi

TOPICS="raw-events:3 enriched-events:3 anomaly-events:2 llm-insights:2 alerts:1"

ALL_OK=true
for entry in $TOPICS; do
  topic="${entry%:*}"
  partitions="${entry#*:}"
  echo "Creating topic $topic (partitions=$partitions)..."
  if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1; then
    echo "  OK: $topic"
  else
    echo "  FAILED: $topic"
    ALL_OK=false
  fi
done

echo "---"
if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list; then
  echo "---"
fi

if [ "$ALL_OK" = true ]; then
  echo "All topics created successfully."
else
  echo "WARNING: Some topics may have failed."
fi
