package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record LlmInsight(
        String id,
        String anomalyId,
        String interpretation,
        String incidentClassification,
        List<String> probableCauses,
        List<String> recommendations,
        String rawLlmResponse,
        Instant analyzedAt,
        Map<String, Object> metadata
) implements Serializable {
    public LlmInsight {
        if (anomalyId == null || anomalyId.isBlank()) throw new IllegalArgumentException("anomalyId must not be blank");
        if (analyzedAt == null) analyzedAt = Instant.now();
    }
}
