package com.sentinelflow.ingestion.controller;

import com.sentinelflow.ingestion.dto.IngestEventRequest;
import com.sentinelflow.ingestion.dto.IngestLogRequest;
import com.sentinelflow.ingestion.dto.IngestMetricRequest;
import com.sentinelflow.ingestion.dto.IngestTraceRequest;
import com.sentinelflow.ingestion.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingest")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> ingestEvent(@Valid @RequestBody IngestEventRequest request) {
        String id = ingestionService.ingestEvent(request);
        return Map.of("accepted", true, "id", id);
    }

    @PostMapping(value = "/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> ingestBatch(@Valid @RequestBody List<IngestEventRequest> requests) {
        var ids = requests.stream()
                .map(ingestionService::ingestEvent)
                .toList();
        return Map.of("accepted", true, "count", ids.size(), "ids", ids);
    }

    @PostMapping(value = "/logs", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> ingestLog(@Valid @RequestBody IngestLogRequest request) {
        String id = ingestionService.ingestLog(request);
        return Map.of("accepted", true, "id", id);
    }

    @PostMapping(value = "/metrics", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> ingestMetric(@Valid @RequestBody IngestMetricRequest request) {
        String id = ingestionService.ingestMetric(request);
        return Map.of("accepted", true, "id", id);
    }

    @PostMapping(value = "/traces", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> ingestTrace(@Valid @RequestBody IngestTraceRequest request) {
        String id = ingestionService.ingestTrace(request);
        return Map.of("accepted", true, "id", id);
    }
}
