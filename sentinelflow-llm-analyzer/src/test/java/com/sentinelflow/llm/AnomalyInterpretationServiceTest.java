package com.sentinelflow.llm;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.llm.client.OllamaClient;
import com.sentinelflow.llm.service.AnomalyInterpretationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AnomalyInterpretationServiceTest {

    @Mock
    private OllamaClient ollamaClient;

    private AnomalyInterpretationService service;

    @BeforeEach
    void setUp() {
        service = new AnomalyInterpretationService(ollamaClient, "en");
    }

    @Test
    void shouldParseInsightFromLlmResponse() {
        when(ollamaClient.generate(anyString())).thenReturn("""
                INTERPRETATION: Latency spike detected in order-service
                CLASSIFICATION: LatencyIssue
                PROBABLE_CAUSES: Database connection pool exhausted
                RECOMMENDATIONS: Check PgBouncer connections
                """);

        var anomaly = new AnomalyEvent("a1", "order-service", "p99-latency", 4.2,
                "WARNING", "Latency spike", Instant.now(), Map.of(), "corr-1");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a1");
        assertThat(insight.incidentClassification()).isEqualTo("LatencyIssue");
        assertThat(insight.interpretation()).contains("Latency spike");
        assertThat(insight.recommendations()).isNotEmpty();
    }

    @Test
    void shouldReturnFallbackWhenLlmUnavailable() {
        when(ollamaClient.generate(anyString())).thenReturn(null);

        var anomaly = new AnomalyEvent("a2", "svc", "cpu", 6.0,
                "CRITICAL", "High CPU", Instant.now(), Map.of(), "corr-2");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a2");
        assertThat(insight.interpretation()).isEqualTo("LLM service unavailable");
    }

    @Test
    void shouldHandleEmptyResponse() {
        when(ollamaClient.generate(anyString())).thenReturn("");

        var anomaly = new AnomalyEvent("a3", "svc", "mem", 1.0,
                "WARNING", "Memory high", Instant.now(), Map.of(), "corr-3");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a3");
        assertThat(insight.interpretation()).isEqualTo("LLM analysis unavailable");
    }
}
