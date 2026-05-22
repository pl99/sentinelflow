package com.sentinelflow.llm.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class OllamaClient {

    private static final Logger log = LoggerFactory.getLogger(OllamaClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String baseUrl;
    private final String model;
    private final Duration timeout;

    public OllamaClient(String baseUrl, String model, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
        this.timeout = timeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String generate(String prompt) {
        try {
            var body = Map.of(
                    "model", model,
                    "prompt", prompt,
                    "stream", false
            );
            String json = mapper.writeValueAsString(body);
            log.debug("Ollama request to {} model={}", baseUrl + "/api/generate", model);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/generate"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(timeout)
                    .build();
            long t0 = System.currentTimeMillis();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long elapsed = System.currentTimeMillis() - t0;
            log.debug("Ollama responded in {}ms with status {}", elapsed, response.statusCode());
            if (response.statusCode() != 200) {
                log.warn("Ollama returned {}: {}", response.statusCode(), response.body());
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> result = mapper.readValue(response.body(), Map.class);
            log.debug("Ollama response keys: {}", result.keySet());
            Object resp = result.get("response");
            if (resp == null) {
                log.warn("Ollama response missing 'response' field, body: {}", response.body());
            }
            return resp != null ? resp.toString() : null;
        } catch (Exception e) {
            log.error("Ollama request failed: {}", e.getMessage(), e);
            return null;
        }
    }
}
