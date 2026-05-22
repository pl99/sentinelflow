package com.sentinelflow.storage.dto;

public record SummaryResponse(
        long totalEvents,
        long totalMetrics,
        long eventsLastHour,
        long metricsLastHour
) {}
