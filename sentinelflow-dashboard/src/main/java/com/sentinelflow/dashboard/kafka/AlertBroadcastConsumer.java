package com.sentinelflow.dashboard.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.BroadcastAlertEvent;
import com.sentinelflow.dashboard.websocket.AlertWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumes {@link BroadcastAlertEvent}s from the {@code alerts-broadcast}
 * topic and pushes them to <strong>all</strong> locally connected WebSocket
 * clients.
 * <p>
 * Each dashboard instance uses a <em>unique</em> consumer group ID so every
 * instance receives every broadcast event — this guarantees that no WebSocket
 * client misses an alert regardless of which instance originally processed
 * the underlying {@code LlmInsight}.
 */
@Component
public class AlertBroadcastConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertBroadcastConsumer.class);

    private final AlertWebSocketHandler webSocketHandler;

    public AlertBroadcastConsumer(AlertWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    @KafkaListener(
            topics = KafkaTopics.ALERTS_BROADCAST,
            groupId = "${sentinelflow.kafka.broadcast.group-id}",
            containerFactory = "broadcastContainerFactory"
    )
    public void consume(BroadcastAlertEvent event, Acknowledgment ack) {
        log.debug("Broadcasting alert {} (severity={}) to local WebSocket clients", event.id(), event.severity());

        webSocketHandler.broadcastAlert(Map.of(
                "type", "NEW_ALERT",
                "id", event.id(),
                "severity", event.severity(),
                "source", event.source(),
                "detectedAt", event.detectedAt().toString()
        ));

        ack.acknowledge();
    }
}