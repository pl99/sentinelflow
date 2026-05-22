package com.sentinelflow.flink.detector;

import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class RuleEngineFunction extends ProcessFunction<TelemetryEvent, AnomalyEvent> {

    @Override
    public void processElement(TelemetryEvent event, Context context, Collector<AnomalyEvent> out) {
        detectErrorSpike(event, out);
        detectLatencyDegradation(event, out);
    }

    private void detectErrorSpike(TelemetryEvent event, Collector<AnomalyEvent> out) {
        if (!"log".equals(event.type())) return;
        if (event.payload() == null) return;

        Object level = event.payload().get("level");
        if (!"ERROR".equals(level) && !"FATAL".equals(level)) return;

        out.collect(new AnomalyEvent(
                UUID.randomUUID().toString(),
                event.source(),
                "error-rate",
                1.0,
                "WARNING",
                String.format("Error detected: %s", event.payload().get("message")),
                Instant.now(),
                Map.of("level", level, "message", event.payload().get("message")),
                event.correlationId()
        ));
    }

    private void detectLatencyDegradation(TelemetryEvent event, Collector<AnomalyEvent> out) {
        if (!"trace".equals(event.type())) return;
        if (event.payload() == null) return;

        Object durationObj = event.payload().get("durationMs");
        if (!(durationObj instanceof Number n)) return;

        long duration = n.longValue();
        if (duration < 1000) return;

        out.collect(new AnomalyEvent(
                UUID.randomUUID().toString(),
                event.source(),
                "latency",
                duration / 1000.0,
                duration > 5000 ? "CRITICAL" : "WARNING",
                String.format("High latency: %dms for %s", duration, event.payload().get("operation")),
                Instant.now(),
                Map.of("durationMs", duration, "operation", event.payload().get("operation")),
                event.correlationId()
        ));
    }
}
