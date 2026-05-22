package com.sentinelflow.ingestion.service;

import com.sentinelflow.common.event.TelemetryEvent;
import com.sentinelflow.ingestion.dto.IngestEventRequest;
import com.sentinelflow.ingestion.dto.IngestLogRequest;
import com.sentinelflow.ingestion.dto.IngestMetricRequest;
import com.sentinelflow.ingestion.dto.IngestTraceRequest;
import com.sentinelflow.ingestion.kafka.TelemetryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final TelemetryProducer producer;

    public IngestionService(TelemetryProducer producer) {
        this.producer = producer;
    }

    public String ingestEvent(IngestEventRequest request) {
        var event = new TelemetryEvent(
                UUID.randomUUID().toString(),
                request.source(),
                request.type(),
                request.subtype(),
                request.payload() != null ? request.payload() : Map.of(),
                request.timestamp() != null ? request.timestamp() : Instant.now(),
                request.correlationId(),
                request.tags()
        );
        producer.send(event);
        log.info("Ingested event {} from {}", event.id(), request.source());
        return event.id();
    }

    public String ingestLog(IngestLogRequest request) {
        var payload = Map.<String, Object>of(
                "level", request.level(),
                "message", request.message()
        );
        var event = new TelemetryEvent(
                UUID.randomUUID().toString(),
                request.source(),
                "log",
                request.level().toLowerCase(),
                payload,
                request.timestamp() != null ? request.timestamp() : Instant.now(),
                request.correlationId(),
                request.tags()
        );
        producer.send(event);
        log.info("Ingested log {} from {}", event.id(), request.source());
        return event.id();
    }

    public String ingestMetric(IngestMetricRequest request) {
        var payload = Map.<String, Object>of(
                "metric", request.name(),
                "value", request.value()
        );
        var event = new TelemetryEvent(
                UUID.randomUUID().toString(),
                request.source(),
                "metric",
                request.name(),
                payload,
                request.timestamp() != null ? request.timestamp() : Instant.now(),
                null,
                request.tags()
        );
        producer.send(event);
        log.info("Ingested metric {} = {} from {}", request.name(), request.value(), request.source());
        return event.id();
    }

    public String ingestTrace(IngestTraceRequest request) {
        var payload = Map.<String, Object>of(
                "traceId", request.traceId(),
                "spanId", request.spanId(),
                "parentSpanId", request.parentSpanId() != null ? request.parentSpanId() : "",
                "operation", request.operation(),
                "durationMs", request.durationMs(),
                "startTime", request.startTime() != null ? request.startTime().toString() : ""
        );
        var tags = Map.of("service", request.service());
        var event = new TelemetryEvent(
                UUID.randomUUID().toString(),
                request.service(),
                "trace",
                request.operation(),
                payload,
                request.timestamp() != null ? request.timestamp() : Instant.now(),
                request.traceId(),
                tags
        );
        producer.send(event);
        log.info("Ingested trace {} span {} from {}", request.traceId(), request.spanId(), request.service());
        return event.id();
    }
}
