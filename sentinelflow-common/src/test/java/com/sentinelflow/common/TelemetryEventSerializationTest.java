package com.sentinelflow.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinelflow.common.config.KafkaConfig;
import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TelemetryEventSerializationTest {

    private final ObjectMapper mapper = KafkaConfig.objectMapper();

    @Test
    void shouldSerializeAndDeserializeTelemetryEvent() throws Exception {
        var event = new TelemetryEvent(
                "evt-1", "order-service", "metric", "latency",
                Map.of("value", 150), Instant.now(), "corr-1", Map.of("env", "prod")
        );

        String json = mapper.writeValueAsString(event);
        TelemetryEvent deserialized = mapper.readValue(json, TelemetryEvent.class);

        assertThat(deserialized.id()).isEqualTo(event.id());
        assertThat(deserialized.source()).isEqualTo(event.source());
        assertThat(deserialized.type()).isEqualTo(event.type());
        assertThat(deserialized.payload()).containsEntry("value", 150);
    }

    @Test
    void shouldSerializeAndDeserializeAnomalyEvent() throws Exception {
        var event = new AnomalyEvent(
                "anom-1", "order-service", "p99-latency", 3.5,
                "CRITICAL", "Latency spike detected", Instant.now(),
                Map.of("currentValue", 500), "corr-1"
        );

        String json = mapper.writeValueAsString(event);
        AnomalyEvent deserialized = mapper.readValue(json, AnomalyEvent.class);

        assertThat(deserialized.id()).isEqualTo(event.id());
        assertThat(deserialized.severity()).isEqualTo("CRITICAL");
        assertThat(deserialized.score()).isEqualTo(3.5);
    }

    @Test
    void shouldRejectTelemetryEventWithBlankSource() {
        assertThatThrownBy(() -> new TelemetryEvent("id", "", "type", null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectAnomalyEventWithBlankService() {
        assertThatThrownBy(() -> new AnomalyEvent("id", "", "metric", 0, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
