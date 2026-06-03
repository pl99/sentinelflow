package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

public record TelemetryEvent(
        String id,
        String source,
        String type,
        String subtype,
        Map<String, Object> payload,
        Instant timestamp,
        String correlationId,
        Map<String, String> tags
) implements Serializable {
    public TelemetryEvent {
        if (source == null || source.isBlank()) throw new IllegalArgumentException("source must not be blank");
        if (type == null || type.isBlank()) throw new IllegalArgumentException("type must not be blank");
        if (timestamp == null) timestamp = Instant.now();
    }
}
