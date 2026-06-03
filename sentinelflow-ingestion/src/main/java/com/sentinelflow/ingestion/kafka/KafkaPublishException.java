package com.sentinelflow.ingestion.kafka;

/**
 * Thrown when {@link TelemetryProducer} fails to publish an event to Kafka
 * within the configured timeout, indicating the event was not durably stored.
 * <p>
 * The caller (ingestion controller) should respond with {@code 503 Service Unavailable}
 * so the client knows to retry.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}