package com.sentinelflow.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

import java.time.Instant;
import java.util.Map;

public record IngestEventRequest(
        @NotBlank String source,
        @NotBlank String type,
        String subtype,
        Map<String, Object> payload,
        String correlationId,
        Map<String, String> tags,
        Instant timestamp
) {}
