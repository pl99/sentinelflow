package com.sentinelflow.simulator.service;

import java.time.Instant;
import java.util.Map;

public interface EventProcessor<T> {

    String source();

    String metricType();

    Map<String, Object> toEventPayload(T record);

    Map<String, String> extractTags(T record);

    Instant extractTimestamp(T record);
}
