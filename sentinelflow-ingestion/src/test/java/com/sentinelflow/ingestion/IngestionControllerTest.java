package com.sentinelflow.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinelflow.ingestion.controller.IngestionController;
import com.sentinelflow.ingestion.dto.IngestEventRequest;
import com.sentinelflow.ingestion.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(IngestionController.class)
class IngestionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private IngestionService ingestionService;

    @Test
    void shouldAcceptValidEvent() throws Exception {
        when(ingestionService.ingestEvent(any())).thenReturn("evt-1");

        var body = new IngestEventRequest("order-service", "metric", "latency",
                Map.of("value", 150), "corr-1", Map.of("env", "prod"), null);

        mockMvc.perform(post("/api/v1/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.id").value("evt-1"));
    }

    @Test
    void shouldRejectEventWithoutSource() throws Exception {
        var body = Map.of("type", "metric");

        mockMvc.perform(post("/api/v1/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectEventWithoutType() throws Exception {
        var body = Map.of("source", "order-service");

        mockMvc.perform(post("/api/v1/ingest")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldAcceptBatchOfEvents() throws Exception {
        when(ingestionService.ingestEvent(any())).thenReturn("evt-1", "evt-2");

        var body = java.util.List.of(
                new IngestEventRequest("svc-a", "metric", null, null, null, null, null),
                new IngestEventRequest("svc-b", "log", null, null, null, null, null)
        );

        mockMvc.perform(post("/api/v1/ingest/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.count").value(2));
    }

    @Test
    void shouldAcceptMetric() throws Exception {
        when(ingestionService.ingestMetric(any())).thenReturn("m-1");

        var body = Map.of("source", "order-service", "name", "latency", "value", 150.0);

        mockMvc.perform(post("/api/v1/ingest/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted());
    }

    @Test
    void shouldRejectMetricWithoutValue() throws Exception {
        var body = Map.of("source", "order-service", "name", "latency");

        mockMvc.perform(post("/api/v1/ingest/metrics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldAcceptLog() throws Exception {
        when(ingestionService.ingestLog(any())).thenReturn("l-1");

        var body = Map.of("source", "order-service", "level", "ERROR", "message", "timeout");

        mockMvc.perform(post("/api/v1/ingest/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted());
    }

    @Test
    void shouldRejectLogWithoutMessage() throws Exception {
        var body = Map.of("source", "order-service", "level", "ERROR");

        mockMvc.perform(post("/api/v1/ingest/logs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldAcceptTrace() throws Exception {
        when(ingestionService.ingestTrace(any())).thenReturn("t-1");

        var body = Map.of(
                "traceId", "tr-1", "spanId", "sp-1",
                "service", "order-service", "operation", "GET /orders",
                "durationMs", 150
        );

        mockMvc.perform(post("/api/v1/ingest/traces")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted());
    }
}
