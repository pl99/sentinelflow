package com.sentinelflow.flink.detector;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.EnrichedTelemetryEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StatisticalDetector
        extends ProcessWindowFunction<EnrichedTelemetryEvent, AnomalyEvent, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<EnrichedTelemetryEvent> elements,
                        Collector<AnomalyEvent> out) {

        List<EnrichedTelemetryEvent> enrichedEvents = new ArrayList<>();
        elements.forEach(enrichedEvents::add);

        if (enrichedEvents.size() < 5) return;

        List<String> correlatedEventIds = enrichedEvents.stream()
                .flatMap(e -> e.correlatedEventIds().stream())
                .distinct()
                .sorted()
                .toList();

        List<String> servicesInChain = enrichedEvents.stream()
                .flatMap(e -> e.servicesInChain().stream())
                .distinct()
                .sorted()
                .toList();

        List<TelemetryEvent> events = enrichedEvents.stream()
                .map(EnrichedTelemetryEvent::original)
                .toList();

        List<Double> values = new ArrayList<>();
        for (var e : events) {
            if (e.payload() != null && e.payload().get("value") instanceof Number n) {
                values.add(n.doubleValue());
            }
        }

        if (values.size() < 5) return;

        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0);
        double stddev = Math.sqrt(variance);

        if (stddev < 0.001) return;

        String[] parts = key.split(":", 2);
        String service = parts[0];
        String metric = parts.length > 1 ? parts[1] : "unknown";

        Map<String, Object> enrichmentMeta = Map.of(
                "correlatedEventIds", correlatedEventIds,
                "servicesInChain", servicesInChain,
                "correlatedCount", correlatedEventIds.size(),
                "uniqueServices", servicesInChain.size()
        );

        for (var event : events) {
            if (event.payload() == null) continue;
            Object valObj = event.payload().get("value");
            if (!(valObj instanceof Number n)) continue;

            double value = n.doubleValue();
            double zScore = Math.abs((value - mean) / stddev);

            if (zScore > 3.0) {
                String severity = zScore > 5.0 ? "CRITICAL" : "WARNING";
                Map<String, Object> details = new java.util.HashMap<>(Map.of(
                        "value", value,
                        "mean", mean,
                        "stddev", stddev,
                        "zScore", zScore,
                        "windowSize", events.size()
                ));
                details.putAll(enrichmentMeta);
                out.collect(new AnomalyEvent(
                        UUID.randomUUID().toString(),
                        service,
                        metric,
                        zScore,
                        severity,
                        String.format("Z-score %.2f for %s (value=%.1f, mean=%.1f, stddev=%.1f)",
                                zScore, metric, value, mean, stddev),
                        Instant.now(),
                        details,
                        event.correlationId()
                ));
            }
        }
    }
}
