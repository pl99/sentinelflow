package com.sentinelflow.ingestion.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.TelemetryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;

    public TelemetryProducer(KafkaTemplate<String, TelemetryEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(TelemetryEvent event) {
        String key = event.source() + ":" + event.type();
        kafkaTemplate.send(KafkaTopics.RAW_EVENTS, key, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send event {} to Kafka: {}", event.id(), ex.getMessage(), ex);
                    } else {
                        log.debug("Sent event {} to topic {}, partition {}",
                                event.id(), KafkaTopics.RAW_EVENTS, result.getRecordMetadata().partition());
                    }
                });
    }
}
