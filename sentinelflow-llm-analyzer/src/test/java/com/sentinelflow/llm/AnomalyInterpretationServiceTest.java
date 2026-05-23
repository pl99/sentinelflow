package com.sentinelflow.llm;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.llm.service.AnomalyInterpretationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;

import java.util.List;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AnomalyInterpretationServiceTest {

    @Mock
    private ChatModel chatModel;

    private AnomalyInterpretationService service;

    @BeforeEach
    void setUp() {
        service = new AnomalyInterpretationService(chatModel, "en");
    }

    @Test
    void shouldParseInsightFromLlmResponse() {
        var responseJson = """
                {
                    "interpretation": "Latency spike detected in order-service",
                    "classification": "LatencyIssue",
                    "probableCauses": ["Database connection pool exhausted", "Slow query detected"],
                    "recommendations": ["Check PgBouncer connections", "Review slow query log"]
                }
                """;

        var chatResponse = new ChatResponse(
                List.of(new Generation(new AssistantMessage(responseJson))));
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse);

        var anomaly = new AnomalyEvent("a1", "order-service", "p99-latency", 4.2,
                "WARNING", "Latency spike", Instant.now(), Map.of(), "corr-1");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a1");
        assertThat(insight.incidentClassification()).isEqualTo("LatencyIssue");
        assertThat(insight.interpretation()).contains("Latency spike");
        assertThat(insight.recommendations()).contains("Check PgBouncer connections");
        assertThat(insight.probableCauses()).hasSize(2);
    }

    @Test
    void shouldReturnFallbackWhenLlmUnavailable() {
        when(chatModel.call(any(Prompt.class))).thenThrow(new RuntimeException("Connection refused"));

        var anomaly = new AnomalyEvent("a2", "svc", "cpu", 6.0,
                "CRITICAL", "High CPU", Instant.now(), Map.of(), "corr-2");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a2");
        assertThat(insight.interpretation()).isEqualTo("LLM service unavailable");
    }

    @Test
    void shouldHandleEmptyResponse() {
        var chatResponse = new ChatResponse(
                List.of(new Generation(new AssistantMessage(""))));
        when(chatModel.call(any(Prompt.class))).thenReturn(chatResponse);

        var anomaly = new AnomalyEvent("a3", "svc", "mem", 1.0,
                "WARNING", "Memory high", Instant.now(), Map.of(), "corr-3");

        var insight = service.analyze(anomaly);

        assertThat(insight.anomalyId()).isEqualTo("a3");
        assertThat(insight.interpretation()).isEqualTo("LLM service unavailable");
    }
}
