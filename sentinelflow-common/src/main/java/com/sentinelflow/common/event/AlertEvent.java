package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;

public record AlertEvent(
        String id,
        String title,
        String severity,
        String status,
        String source,
        String description,
        String recommendation,
        Instant detectedAt,
        Instant resolvedAt
) implements Serializable {
    public AlertEvent {
        if (title == null || title.isBlank()) throw new IllegalArgumentException("title must not be blank");
        if (severity == null) severity = "WARNING";
        if (status == null) status = "OPEN";
        if (detectedAt == null) detectedAt = Instant.now();
    }
}
