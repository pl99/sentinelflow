package com.sentinelflow.flink.detector;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.EnrichedTelemetryEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StatisticalDetectorWithEnrichedEventTest {

    private TelemetryEvent metric(String source, String metric, double value) {
        return new TelemetryEvent(
                java.util.UUID.randomUUID().toString(),
                source,
                "metric",
                null,
                Map.of("metric", metric, "value", value),
                Instant.now(),
                "corr-" + source,
                Map.of()
        );
    }

    private EnrichedTelemetryEvent enriched(TelemetryEvent original) {
        return new EnrichedTelemetryEvent(
                original,
                List.of(original.id()),
                List.of(original.source()),
                Instant.now(),
                Instant.now().plusSeconds(60),
                Map.of("correlatedCount", 1, "uniqueServices", 1)
        );
    }

    private ProcessWindowFunction<EnrichedTelemetryEvent, AnomalyEvent, String, TimeWindow>.Context ctx(
            TimeWindow window) {
        StatisticalDetector f = new StatisticalDetector();
        return f.new Context() {
            public TimeWindow window() { return window; }
            public long currentProcessingTime() { return 0; }
            public long currentWatermark() { return 0; }
            public <X> void output(OutputTag<X> outputTag, X value) {}
            public KeyedStateStore windowState() { return null; }
            public KeyedStateStore globalState() { return null; }
        };
    }

    private Collector<AnomalyEvent> collector(List<AnomalyEvent> results) {
        return new Collector<>() {
            @Override
            public void collect(AnomalyEvent record) { results.add(record); }
            @Override
            public void close() {}
        };
    }

    @Test
    void shouldEmitWarningForZScoreAbove3() throws Exception {
        StatisticalDetector detector = new StatisticalDetector();
        List<AnomalyEvent> results = new ArrayList<>();
        List<EnrichedTelemetryEvent> events = new ArrayList<>();

        // 19 values of 100 + 1 extreme outlier at 3000 → Z-score ≈ 4.4
        for (int i = 0; i < 19; i++) {
            events.add(enriched(metric("engine-1", "rpm", 100.0)));
        }
        events.add(enriched(metric("engine-1", "rpm", 3000.0)));

        var ctx = ctx(new TimeWindow(0, 60000));
        detector.process("engine-1:rpm", ctx, events, collector(results));

        assertEquals(1, results.size(), "Should emit one anomaly for the outlier");
        AnomalyEvent anomaly = results.get(0);
        assertEquals("engine-1", anomaly.service());
        assertEquals("rpm", anomaly.metric());
        assertEquals("WARNING", anomaly.severity());
        assertTrue(anomaly.score() > 3.0);
    }

    @Test
    void shouldEmitCriticalForZScoreAbove5() throws Exception {
        StatisticalDetector detector = new StatisticalDetector();
        List<AnomalyEvent> results = new ArrayList<>();
        List<EnrichedTelemetryEvent> events = new ArrayList<>();

        // 50 values of 100 + 1 extreme outlier → Z-score ≈ 7.0 (> 5.0)
        for (int i = 0; i < 50; i++) {
            events.add(enriched(metric("pump-1", "flow", 100.0)));
        }
        events.add(enriched(metric("pump-1", "flow", 50000.0)));

        var ctx = ctx(new TimeWindow(0, 60000));
        detector.process("pump-1:flow", ctx, events, collector(results));

        assertEquals(1, results.size());
        assertEquals("CRITICAL", results.get(0).severity());
        assertTrue(results.get(0).score() > 5.0);
    }

    @Test
    void shouldSkipWindowWithLessThan5Events() throws Exception {
        StatisticalDetector detector = new StatisticalDetector();
        List<AnomalyEvent> results = new ArrayList<>();
        List<EnrichedTelemetryEvent> events = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            events.add(enriched(metric("engine-1", "temp", 100.0)));
        }

        var ctx = ctx(new TimeWindow(0, 60000));
        detector.process("engine-1:temp", ctx, events, collector(results));

        assertTrue(results.isEmpty(), "Should skip windows with fewer than 5 events");
    }

    @Test
    void shouldSkipWhenAllValuesAreWithinRange() throws Exception {
        StatisticalDetector detector = new StatisticalDetector();
        List<AnomalyEvent> results = new ArrayList<>();
        List<EnrichedTelemetryEvent> events = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            events.add(enriched(metric("engine-1", "temp", 100.0 + i * 0.5)));
        }

        var ctx = ctx(new TimeWindow(0, 60000));
        detector.process("engine-1:temp", ctx, events, collector(results));

        assertTrue(results.isEmpty(), "Should not emit anomaly when all values are close to mean");
    }

    @Test
    void shouldIncludeEnrichmentContextInDetails() throws Exception {
        StatisticalDetector detector = new StatisticalDetector();
        List<AnomalyEvent> results = new ArrayList<>();
        List<EnrichedTelemetryEvent> events = new ArrayList<>();

        for (int i = 0; i < 19; i++) {
            events.add(enriched(metric("engine-1", "rpm", 100.0)));
        }
        events.add(enriched(metric("engine-1", "rpm", 3000.0)));

        var ctx = ctx(new TimeWindow(0, 60000));
        detector.process("engine-1:rpm", ctx, events, collector(results));

        assertEquals(1, results.size());
        Map<String, Object> details = results.get(0).details();
        assertNotNull(details, "Details should not be null");
        assertTrue(details.containsKey("correlatedEventIds"), "Should include correlatedEventIds");
        assertTrue(details.containsKey("servicesInChain"), "Should include servicesInChain");
        assertTrue(details.containsKey("correlatedCount"), "Should include correlatedCount");
        assertTrue(details.containsKey("uniqueServices"), "Should include uniqueServices");
    }
}
