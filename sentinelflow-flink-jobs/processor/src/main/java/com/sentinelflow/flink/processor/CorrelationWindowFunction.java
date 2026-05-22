package com.sentinelflow.flink.processor;

import com.sentinelflow.common.event.EnrichedTelemetryEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CorrelationWindowFunction
        extends ProcessWindowFunction<TelemetryEvent, EnrichedTelemetryEvent, String, TimeWindow> {

    @Override
    public void process(String correlationId,
                        Context context,
                        Iterable<TelemetryEvent> elements,
                        Collector<EnrichedTelemetryEvent> out) {

        List<TelemetryEvent> events = new ArrayList<>();
        elements.forEach(events::add);

        List<String> eventIds = events.stream()
                .map(TelemetryEvent::id)
                .toList();

        List<String> services = events.stream()
                .map(TelemetryEvent::source)
                .distinct()
                .sorted()
                .toList();

        for (TelemetryEvent event : events) {
            out.collect(new EnrichedTelemetryEvent(
                    event,
                    eventIds,
                    services,
                    java.time.Instant.ofEpochMilli(context.window().getStart()),
                    java.time.Instant.ofEpochMilli(context.window().getEnd()),
                    Map.of(
                            "correlatedCount", events.size(),
                            "uniqueServices", services.size()
                    )
            ));
        }
    }
}
