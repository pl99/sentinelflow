package com.sentinelflow.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

public record IngestMetricRequest(
        @NotBlank String source,
        @NotBlank String name,
        @NotNull Double value,
        Map<String, String> tags,
        Instant timestamp
) {}
