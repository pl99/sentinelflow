package com.sentinelflow.storage.dto;

import java.time.Instant;

public record AggregatedMetric(
        Instant bucket,
        String service,
        String metricName,
        double avg,
        double min,
        double max,
        double p95,
        double p99,
        long count
) {}
