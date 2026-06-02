package com.sentinelflow.dashboard.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

@Service
public class DashboardAggregationService {

    private static final Logger log = LoggerFactory.getLogger(DashboardAggregationService.class);

    private final HttpClient httpClient;
    private final String storageQueryUrl;

    public DashboardAggregationService(@Value("${sentinelflow.storage.query-url}") String storageQueryUrl) {
        this.storageQueryUrl = storageQueryUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public Map<String, Object> fetchSummary() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(storageQueryUrl + "/api/v1/query/summary"))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return Map.of("storageAvailable", true, "status", "connected");
            }
        } catch (Exception e) {
            log.warn("Storage service unavailable: {}", e.getMessage());
        }
        return Map.of("storageAvailable", false, "status", "unavailable");
    }
}
