package com.sentinelflow.storage.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.TelemetryEvent;
import com.sentinelflow.storage.service.StorageWriterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final StorageWriterService writerService;

    private final List<TelemetryEvent> batch = new ArrayList<>();
    private final List<Acknowledgment> acks = new ArrayList<>();
    private static final int BATCH_SIZE = 50;

    public EventConsumer(StorageWriterService writerService) {
        this.writerService = writerService;
    }

    @KafkaListener(
            topics = KafkaTopics.RAW_EVENTS,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(TelemetryEvent event, Acknowledgment ack) {
        synchronized (batch) {
            batch.add(event);
            acks.add(ack);
            if (batch.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    @Scheduled(fixedDelayString = "${sentinelflow.storage.flush-interval-ms:5000}")
    public void scheduledFlush() {
        synchronized (batch) {
            if (!batch.isEmpty()) {
                flush();
            }
        }
    }

    private void flush() {
        if (batch.isEmpty()) return;
        var copy = List.copyOf(batch);
        var ackCopy = List.copyOf(acks);
        batch.clear();
        acks.clear();
        try {
            writerService.writeBatch(copy);
            ackCopy.forEach(Acknowledgment::acknowledge);
            log.info("Flushed {} events to storage", copy.size());
        } catch (Exception e) {
            log.error("Failed to write batch of {} events: {}", copy.size(), e.getMessage(), e);
            // Batch cleared, messages NOT acknowledged.
            // Kafka will redeliver them on consumer restart or rebalance (at-least-once).
        }
    }
}
