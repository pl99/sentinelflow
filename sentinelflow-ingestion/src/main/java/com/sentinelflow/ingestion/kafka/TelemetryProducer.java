package com.sentinelflow.ingestion.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);

    static final Duration SEND_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;

    public TelemetryProducer(KafkaTemplate<String, TelemetryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Send an event to Kafka and block up to {@link #SEND_TIMEOUT} for the broker
     * acknowledgment.  A return value of {@code true} means the broker has confirmed
     * receipt; {@code false} or an exception means the event was not persisted.
     * <p>
     * This guarantees the controller responds 202 <em>only</em> after Kafka
     * confirms durability — eliminating the former fire-and-forget hole.
     */
    public RecordMetadata send(TelemetryEvent event) {
        String key = event.source() + ":" + event.type();
        CompletableFuture<SendResult<String, TelemetryEvent>> future =
                kafkaTemplate.send(KafkaTopics.RAW_EVENTS, key, event);
        try {
            SendResult<String, TelemetryEvent> result = future.get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            RecordMetadata meta = result.getRecordMetadata();
            log.debug("Sent event {} to topic {} partition {} offset {}",
                    event.id(), meta.topic(), meta.partition(), meta.offset());
            return meta;
        } catch (Exception e) {
            log.error("Failed to send event {} to Kafka within {}: {}",
                    event.id(), SEND_TIMEOUT, e.getMessage(), e);
            throw new KafkaPublishException("Kafka send failed for event " + event.id(), e);
        }
    }
}
