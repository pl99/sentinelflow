package com.sentinelflow.flink.detector;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Z-score anomaly detector that works directly on raw {@link TelemetryEvent} metrics.
 * <p>
 * Reads numeric {@code "value"} from {@code event.payload()}, groups events
 * by {@code source:metric} key inside a tumbling window, and emits an
 * {@link AnomalyEvent} for any value whose absolute Z-score exceeds the threshold.
 * <p>
 * This is the raw-events counterpart of {@link StatisticalDetector}, which operates
 * on {@code EnrichedTelemetryEvent} from the enrichment pipeline. Since standalone
 * sensor metrics have no correlation chain, they are detected here rather than
 * going through the enrichment flow.
 */
public class RawMetricDetector
        extends ProcessWindowFunction<TelemetryEvent, AnomalyEvent, String, TimeWindow> {

    /** Minimum number of values required to compute a meaningful Z-score. */
    static final int MIN_SAMPLE_SIZE = 5;

    /** Z-score threshold — any value with |z| > this is flagged. */
    static final double Z_SCORE_THRESHOLD = 3.0;

    /** Values with |z| > this are classified as CRITICAL instead of WARNING. */
    static final double Z_SCORE_CRITICAL = 5.0;

    /** Below this standard deviation the data is treated as constant (no detection). */
    static final double MIN_STDDEV = 0.001;

    @Override
    public void process(String key,
                        Context context,
                        Iterable<TelemetryEvent> elements,
                        Collector<AnomalyEvent> out) {

        List<TelemetryEvent> events = new ArrayList<>();
        elements.forEach(events::add);

        if (events.size() < MIN_SAMPLE_SIZE) return;

        List<Double> values = new ArrayList<>();
        for (var e : events) {
            if (e.payload() != null && e.payload().get("value") instanceof Number n) {
                values.add(n.doubleValue());
            }
        }

        if (values.size() < MIN_SAMPLE_SIZE) return;

        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0);
        double stddev = Math.sqrt(variance);

        if (stddev < MIN_STDDEV) return;

        String[] parts = key.split(":", 2);
        String service = parts[0];
        String metric = parts.length > 1 ? parts[1] : "unknown";

        for (var event : events) {
            if (event.payload() == null) continue;
            Object valObj = event.payload().get("value");
            if (!(valObj instanceof Number n)) continue;

            double value = n.doubleValue();
            double zScore = Math.abs((value - mean) / stddev);

            if (zScore > Z_SCORE_THRESHOLD) {
                String severity = zScore > Z_SCORE_CRITICAL ? "CRITICAL" : "WARNING";
                Map<String, Object> details = new HashMap<>(Map.of(
                        "value", value,
                        "mean", mean,
                        "stddev", stddev,
                        "zScore", zScore,
                        "windowSize", events.size()
                ));
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