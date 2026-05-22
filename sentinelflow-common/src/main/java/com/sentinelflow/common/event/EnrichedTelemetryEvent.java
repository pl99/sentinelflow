package com.sentinelflow.common.event;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record EnrichedTelemetryEvent(
        TelemetryEvent original,
        List<String> correlatedEventIds,
        List<String> servicesInChain,
        Instant windowStart,
        Instant windowEnd,
        Map<String, Object> enrichmentMetadata
) implements Serializable {}
