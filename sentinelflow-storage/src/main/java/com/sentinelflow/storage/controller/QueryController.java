package com.sentinelflow.storage.controller;

import com.sentinelflow.storage.dto.AggregatedMetric;
import com.sentinelflow.storage.dto.EventQueryResponse;
import com.sentinelflow.storage.dto.SummaryResponse;
import com.sentinelflow.storage.service.TimeSeriesQueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/query")
public class QueryController {

    private final TimeSeriesQueryService queryService;

    public QueryController(TimeSeriesQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/metrics")
    public List<AggregatedMetric> queryMetrics(
            @RequestParam(required = false) String service,
            @RequestParam(required = false) String metricName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end,
            @RequestParam(defaultValue = "1 minute") String bucket,
            @RequestParam(defaultValue = "100") int limit) {
        return queryService.queryMetrics(service, metricName, start, end, bucket, limit);
    }

    @GetMapping("/events")
    public List<EventQueryResponse> queryEvents(
            @RequestParam(required = false) String source,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String subtype,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant start,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant end,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "100") int limit) {
        return queryService.queryEvents(source, type, subtype, start, end, search, limit);
    }

    @GetMapping("/summary")
    public SummaryResponse summary() {
        return queryService.summary();
    }
}
