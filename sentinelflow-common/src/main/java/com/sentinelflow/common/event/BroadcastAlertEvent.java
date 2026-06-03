package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;

/**
 * Lightweight event published to the {@code alerts-broadcast} topic
 * so that every dashboard instance can notify its locally connected
 * WebSocket clients about a new or updated alert.
 * <p>
 * This is NOT the full {@code Alert} entity — it carries only the
 * fields required for the real-time WebSocket push.
 *
 * @param id         alert unique identifier
 * @param severity   CRITICAL / WARNING
 * @param source     service name
 * @param detectedAt when the alert was created
 */
public record BroadcastAlertEvent(
        String id,
        String severity,
        String source,
        Instant detectedAt
) implements Serializable {
    public BroadcastAlertEvent {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("id must not be blank");
        if (severity == null || severity.isBlank()) throw new IllegalArgumentException("severity must not be blank");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("source must not be blank");
        if (detectedAt == null) throw new IllegalArgumentException("detectedAt must not be null");
    }
}