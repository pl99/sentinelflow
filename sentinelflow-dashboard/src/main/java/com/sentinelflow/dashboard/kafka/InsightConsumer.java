package com.sentinelflow.dashboard.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.BroadcastAlertEvent;
import com.sentinelflow.common.event.LlmInsight;
import com.sentinelflow.dashboard.entity.Alert;
import com.sentinelflow.dashboard.service.AlertManagementService;
import com.sentinelflow.dashboard.websocket.AlertWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class InsightConsumer {

    private static final Logger log = LoggerFactory.getLogger(InsightConsumer.class);

    private final AlertManagementService alertService;
    private final AlertWebSocketHandler webSocketHandler;
    private final KafkaTemplate<String, BroadcastAlertEvent> broadcastKafkaTemplate;

    public InsightConsumer(AlertManagementService alertService,
                           AlertWebSocketHandler webSocketHandler,
                           KafkaTemplate<String, BroadcastAlertEvent> broadcastKafkaTemplate) {
        this.alertService = alertService;
        this.webSocketHandler = webSocketHandler;
        this.broadcastKafkaTemplate = broadcastKafkaTemplate;
    }

    @KafkaListener(
            topics = KafkaTopics.LLM_INSIGHTS,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(LlmInsight insight, Acknowledgment ack) {
        log.info("Processing insight for anomaly {} (severity={})", insight.anomalyId(), insight.severity());

        String title = String.format("Anomaly %s: %s", insight.anomalyId(), insight.incidentClassification());
        String description = insight.interpretation() != null ? insight.interpretation() : "No interpretation";
        String recommendation = insight.recommendations() != null
                ? String.join("; ", insight.recommendations())
                : "";
        String service = insight.metadata() != null
                ? insight.metadata().getOrDefault("service", "unknown").toString()
                : "unknown";

        try {
            Alert alert = alertService.createAlert(title, insight.severity(), service, description, recommendation, insight.anomalyId());

            webSocketHandler.broadcastAlert(Map.of(
                    "type", "NEW_ALERT",
                    "id", alert.getId(),
                    "title", alert.getTitle(),
                    "severity", alert.getSeverity(),
                    "status", alert.getStatus(),
                    "source", alert.getSource(),
                    "detectedAt", alert.getDetectedAt().toString()
            ));

            broadcastKafkaTemplate.send(KafkaTopics.ALERTS_BROADCAST,
                    new BroadcastAlertEvent(
                            alert.getId(),
                            alert.getSeverity(),
                            alert.getSource(),
                            alert.getDetectedAt()
                    ));

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process insight for anomaly {}: {}", insight.anomalyId(), e.getMessage(), e);
        }
    }
}
