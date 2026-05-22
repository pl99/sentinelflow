package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

public record AnomalyEvent(
        String id,
        String service,
        String metric,
        double score,
        String severity,
        String description,
        Instant detectedAt,
        Map<String, Object> details,
        String correlationId
) implements Serializable {
    public AnomalyEvent {
        if (service == null || service.isBlank()) throw new IllegalArgumentException("service must not be blank");
        if (metric == null || metric.isBlank()) throw new IllegalArgumentException("metric must not be blank");
        if (severity == null) severity = "WARNING";
        if (detectedAt == null) detectedAt = Instant.now();
    }
}
