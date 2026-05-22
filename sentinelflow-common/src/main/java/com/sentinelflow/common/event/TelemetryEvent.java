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
        if (source == null) source = "unknown";
        if (type == null) type = "unknown";
        if (timestamp == null) timestamp = Instant.now();
    }
}
