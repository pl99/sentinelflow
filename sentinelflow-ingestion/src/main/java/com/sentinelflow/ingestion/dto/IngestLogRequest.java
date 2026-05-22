package com.sentinelflow.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

import java.time.Instant;
import java.util.Map;

public record IngestLogRequest(
        @NotBlank String source,
        @NotBlank String level,
        @NotBlank String message,
        String service,
        String correlationId,
        Map<String, String> tags,
        Instant timestamp
) {}
