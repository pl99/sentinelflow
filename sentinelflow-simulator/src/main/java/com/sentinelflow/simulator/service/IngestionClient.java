package com.sentinelflow.simulator.service;

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

public class IngestionClient {

    private static final Logger log = LoggerFactory.getLogger(IngestionClient.class);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String ingestionUrl;

    public IngestionClient(String ingestionUrl) {
        this.ingestionUrl = ingestionUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public boolean sendEvent(Map<String, Object> event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ingestionUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 202) {
                return true;
            }
            log.warn("Ingestion returned {}: {}", response.statusCode(), response.body());
            return false;
        } catch (Exception e) {
            log.error("Failed to send event to {}: {}", ingestionUrl, e.getMessage());
            return false;
        }
    }
}
