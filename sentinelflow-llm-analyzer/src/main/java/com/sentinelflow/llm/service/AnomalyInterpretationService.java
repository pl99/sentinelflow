package com.sentinelflow.llm.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.LlmInsight;
import com.sentinelflow.llm.dto.AnomalyAnalysis;
import com.sentinelflow.llm.prompt.PromptTemplates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.ollama.api.OllamaChatOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class AnomalyInterpretationService {

    private static final Logger log = LoggerFactory.getLogger(AnomalyInterpretationService.class);

    private final ChatModel chatModel;
    private final ObjectMapper objectMapper;
    private final String language;

    public AnomalyInterpretationService(ChatModel chatModel,
                                        @Value("${sentinelflow.llm.language}") String language) {
        this.chatModel = chatModel;
        this.objectMapper = new ObjectMapper();
        this.language = PromptTemplates.resolveLanguage(language);
    }

    public LlmInsight analyze(AnomalyEvent anomaly) {
        String promptText = PromptTemplates.anomalyInterpretation(
                language,
                anomaly.service(),
                anomaly.metric(),
                anomaly.score(),
                anomaly.severity(),
                anomaly.description(),
                anomaly.details() != null ? anomaly.details().toString() : ""
        );

        var options = OllamaChatOptions.builder()
                .format("json")
                .build();

        var prompt = new Prompt(new UserMessage(promptText), options);

        try {
            log.debug("Calling Ollama for anomaly {}/{}", anomaly.service(), anomaly.metric());
            var response = chatModel.call(prompt);
            String rawJson = response.getResult().getOutput().getText();
            log.debug("Ollama responded: {} chars", rawJson != null ? rawJson.length() : 0);

            if (rawJson == null || rawJson.isBlank()) {
                return fallbackInsight(anomaly);
            }

            AnomalyAnalysis analysis = objectMapper.readValue(rawJson, AnomalyAnalysis.class);

            return new LlmInsight(
                    UUID.randomUUID().toString(),
                    anomaly.id(),
                    anomaly.severity(),
                    anomaly.score(),
                    analysis.interpretation() != null ? analysis.interpretation() : "LLM analysis unavailable",
                    analysis.classification() != null ? analysis.classification().trim() : "Unknown",
                    analysis.probableCauses() != null ? analysis.probableCauses() : List.of("Unknown"),
                    analysis.recommendations() != null ? analysis.recommendations() : List.of("Investigate manually"),
                    rawJson,
                    Instant.now(),
                    Map.of("service", anomaly.service(), "metric", anomaly.metric())
            );
        } catch (Exception e) {
            log.error("LLM analysis failed: {}", e.getMessage(), e);
            return fallbackInsight(anomaly);
        }
    }

    private LlmInsight fallbackInsight(AnomalyEvent anomaly) {
        return new LlmInsight(
                UUID.randomUUID().toString(),
                anomaly.id(),
                anomaly.severity(),
                anomaly.score(),
                "LLM service unavailable",
                "Unknown",
                List.of("LLM service unavailable — check Ollama connection"),
                List.of("Ensure Ollama is running and accessible"),
                "",
                Instant.now(),
                Map.of("service", anomaly.service(), "metric", anomaly.metric())
        );
    }
}
