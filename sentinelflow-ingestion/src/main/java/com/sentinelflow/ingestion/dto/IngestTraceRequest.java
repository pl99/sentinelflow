package com.sentinelflow.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record IngestTraceRequest(
        @NotBlank String traceId,
        @NotBlank String spanId,
        String parentSpanId,
        @NotBlank String service,
        @NotBlank String operation,
        @NotNull Long durationMs,
        Instant startTime,
        Instant timestamp
) {}
