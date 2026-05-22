package com.sentinelflow.storage.dto;

import java.time.Instant;
import java.util.Map;

public record EventQueryResponse(
        String id,
        String source,
        String type,
        String subtype,
        Instant timestamp,
        String correlationId,
        Map<String, Object> payload,
        Map<String, String> tags
) {}
