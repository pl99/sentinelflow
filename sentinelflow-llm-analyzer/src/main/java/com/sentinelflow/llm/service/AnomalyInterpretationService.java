package com.sentinelflow.llm.service;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.LlmInsight;
import com.sentinelflow.llm.client.OllamaClient;
import com.sentinelflow.llm.prompt.PromptTemplates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class AnomalyInterpretationService {

    private static final Logger log = LoggerFactory.getLogger(AnomalyInterpretationService.class);

    private final OllamaClient ollamaClient;
    private final String language;

    public AnomalyInterpretationService(OllamaClient ollamaClient,
                                        @Value("${sentinelflow.llm.language}") String language) {
        this.ollamaClient = ollamaClient;
        this.language = PromptTemplates.resolveLanguage(language);
    }

    public LlmInsight analyze(AnomalyEvent anomaly) {
        String prompt = PromptTemplates.anomalyInterpretation(
                language,
                anomaly.service(),
                anomaly.metric(),
                anomaly.score(),
                anomaly.severity(),
                anomaly.description(),
                anomaly.details() != null ? anomaly.details().toString() : ""
        );

        log.debug("Calling Ollama for anomaly {}/{}", anomaly.service(), anomaly.metric());
        String response = ollamaClient.generate(prompt);
        log.debug("Ollama returned response={}", response != null ? "non-null (" + response.length() + " chars)" : "null");
        if (response != null && !response.isBlank()) {
            log.info("RAW_OLLAMA_RESPONSE: {}", response.replace("\n", "\\n"));
        }

        if (response == null) {
            return fallbackInsight(anomaly);
        }

        return parseInsight(anomaly, response);
    }

    private LlmInsight parseInsight(AnomalyEvent anomaly, String rawResponse) {
        String interpretation = firstLine(extractSection(rawResponse, "INTERPRETATION"));
        String classification = firstLine(extractSection(rawResponse, "CLASSIFICATION"));
        String causesText = extractSection(rawResponse, "PROBABLE_CAUSES");
        String recommendationsText = extractSection(rawResponse, "RECOMMENDATIONS");

        List<String> causes = causesText != null
                ? Stream.of(causesText.split("\\n")).map(String::trim).filter(s -> !s.isEmpty()).toList()
                : List.of("Unknown");
        if (causes.isEmpty()) causes = List.of("Unknown");

        List<String> recommendations = recommendationsText != null
                ? Stream.of(recommendationsText.split("\\n")).map(String::trim).filter(s -> !s.isEmpty()).toList()
                : List.of("Investigate manually");
        if (recommendations.isEmpty()) recommendations = List.of("Investigate manually");

        return new LlmInsight(
                UUID.randomUUID().toString(),
                anomaly.id(),
                interpretation != null ? interpretation : "LLM analysis unavailable",
                classification != null ? classification.trim() : "Unknown",
                causes,
                recommendations,
                rawResponse,
                Instant.now(),
                Map.of("service", anomaly.service(), "metric", anomaly.metric())
        );
    }

    private LlmInsight fallbackInsight(AnomalyEvent anomaly) {
        return new LlmInsight(
                UUID.randomUUID().toString(),
                anomaly.id(),
                "LLM service unavailable",
                "Unknown",
                List.of("LLM service unavailable — check Ollama connection"),
                List.of("Ensure Ollama is running and accessible"),
                "",
                Instant.now(),
                Map.of("service", anomaly.service(), "metric", anomaly.metric())
        );
    }

    private String extractSection(String text, String sectionName) {
        if (text == null) return null;
        String cleanText = text.replace("**", "");
        String prefix = sectionName + ":";
        int start = cleanText.indexOf(prefix);
        if (start < 0) {
            prefix = sectionName + " ";
            start = cleanText.indexOf(prefix);
        }
        if (start < 0) {
            prefix = "\n" + sectionName;
            start = cleanText.indexOf(prefix);
        }
        if (start < 0) return null;
        start += prefix.length();
        int end = -1;
        for (int i = start; i < cleanText.length(); i++) {
            if (cleanText.charAt(i) == '\n' && i + 2 < cleanText.length()
                    && Character.isDigit(cleanText.charAt(i + 1))
                    && cleanText.charAt(i + 2) == '.') {
                end = i;
                break;
            }
        }
        if (end < 0) {
            end = cleanText.length();
        }
        return cleanText.substring(start, end).trim();
    }

    private String firstLine(String text) {
        if (text == null) return null;
        int end = text.indexOf("\n");
        return end > 0 ? text.substring(0, end).trim() : text.trim();
    }
}
