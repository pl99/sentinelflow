package com.sentinelflow.storage.dto;

import java.util.List;

public record TimeSeriesResponse(
        String service,
        String metricName,
        String bucket,
        List<AggregatedMetric> dataPoints
) {}
