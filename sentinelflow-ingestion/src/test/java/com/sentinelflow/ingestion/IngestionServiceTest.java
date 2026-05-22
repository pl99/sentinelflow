package com.sentinelflow.ingestion;

import com.sentinelflow.ingestion.dto.IngestEventRequest;
import com.sentinelflow.ingestion.dto.IngestLogRequest;
import com.sentinelflow.ingestion.dto.IngestMetricRequest;
import com.sentinelflow.ingestion.dto.IngestTraceRequest;
import com.sentinelflow.ingestion.kafka.TelemetryProducer;
import com.sentinelflow.ingestion.service.IngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class IngestionServiceTest {

    @Mock
    private TelemetryProducer producer;

    @Captor
    private ArgumentCaptor<com.sentinelflow.common.event.TelemetryEvent> eventCaptor;

    private IngestionService service;

    @BeforeEach
    void setUp() {
        service = new IngestionService(producer);
    }

    @Test
    void shouldProduceEvent() {
        var request = new IngestEventRequest("svc", "metric", "latency",
                Map.of("value", 100), "corr-1", Map.of("env", "test"), null);

        String id = service.ingestEvent(request);

        assertThat(id).isNotBlank();
        verify(producer).send(eventCaptor.capture());
        assertThat(eventCaptor.getValue().source()).isEqualTo("svc");
        assertThat(eventCaptor.getValue().type()).isEqualTo("metric");
        assertThat(eventCaptor.getValue().correlationId()).isEqualTo("corr-1");
    }

    @Test
    void shouldProduceLog() {
        var request = new IngestLogRequest("svc", "ERROR", "timeout", null, null, null, null);

        String id = service.ingestLog(request);

        assertThat(id).isNotBlank();
        verify(producer).send(eventCaptor.capture());
        assertThat(eventCaptor.getValue().source()).isEqualTo("svc");
        assertThat(eventCaptor.getValue().type()).isEqualTo("log");
        assertThat(eventCaptor.getValue().payload()).containsEntry("level", "ERROR");
    }

    @Test
    void shouldProduceMetric() {
        var request = new IngestMetricRequest("svc", "cpu.usage", 85.5, null, null);

        String id = service.ingestMetric(request);

        assertThat(id).isNotBlank();
        verify(producer).send(eventCaptor.capture());
        assertThat(eventCaptor.getValue().type()).isEqualTo("metric");
        assertThat(eventCaptor.getValue().payload()).containsEntry("metric", "cpu.usage");
    }

    @Test
    void shouldProduceTrace() {
        var request = new IngestTraceRequest("tr-1", "sp-1", null, "svc", "GET /api", 200L, null, null);

        String id = service.ingestTrace(request);

        assertThat(id).isNotBlank();
        verify(producer).send(eventCaptor.capture());
        assertThat(eventCaptor.getValue().type()).isEqualTo("trace");
        assertThat(eventCaptor.getValue().payload()).containsEntry("traceId", "tr-1");
    }
}
