package com.sentinelflow.simulator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class AnomalyGeneratorService {

    private static final Logger log = LoggerFactory.getLogger(AnomalyGeneratorService.class);
    private static final Random RNG = new Random();

    private static final String[] METRICS = {"temperature", "pressure", "rpm", "vibration", "current"};
    private static final String[] SOURCES = {"engine-1", "engine-2", "pump-1", "compressor-1"};

    private final IngestionClient client;
    private final long delayMs;
    private final AtomicLong counter = new AtomicLong(0);

    public AnomalyGeneratorService(IngestionClient client, long delayMs) {
        this.client = client;
        this.delayMs = delayMs;
    }

    public int generate(int cycleCount) {
        int total = 0;
        for (int cycle = 0; cycle < cycleCount; cycle++) {
            String correlationId = UUID.randomUUID().toString();
            for (String source : SOURCES) {
                for (String metric : METRICS) {
                    total += sendMetricBatch(source, metric, correlationId);
                    sleep(delayMs);
                }
            }
            total += sendErrorLog(correlationId);
            sleep(delayMs);
            total += sendHighLatencyTrace(correlationId);
            sleep(delayMs);
            if ((cycle + 1) % 5 == 0) {
                log.info("Anomaly cycle {}/{} complete, total sent: {}", cycle + 1, cycleCount, total);
            }
        }
        log.info("Anomaly generation complete: {} events sent in {} cycles", total, cycleCount);
        return total;
    }

    public void generateForever() {
        log.info("Starting continuous anomaly generation (delay={}ms)...", delayMs);
        long cycle = 0;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String correlationId = UUID.randomUUID().toString();
                for (String source : SOURCES) {
                    for (String metric : METRICS) {
                        sendMetricBatch(source, metric, correlationId);
                        sleep(delayMs);
                    }
                }
                sendErrorLog(correlationId);
                sleep(delayMs);
                sendHighLatencyTrace(correlationId);
                sleep(delayMs);
                cycle++;
                if (cycle % 10 == 0) {
                    log.info("Anomaly cycle {} complete", cycle);
                }
            } catch (Exception e) {
                log.error("Anomaly generation error: {}", e.getMessage());
                sleep(1000);
            }
        }
    }

    private int sendMetricBatch(String source, String metric, String correlationId) {
        int sent = 0;
        int batchSize = 8 + RNG.nextInt(5);
        double anomalyValue = 500 + RNG.nextDouble() * 1000;
        for (int i = 0; i < batchSize; i++) {
            double normalValue = 50 + RNG.nextDouble() * 50;
            Map<String, Object> event = buildMetricEvent(source, metric, normalValue, correlationId);
            if (client.sendEvent(event)) sent++;
        }
        Map<String, Object> anomaly = buildMetricEvent(source, metric, anomalyValue, correlationId);
        if (client.sendEvent(anomaly)) sent++;
        return sent;
    }

    private int sendErrorLog(String correlationId) {
        String[] messages = {
            "Connection timeout to database",
            "Null pointer in data processing pipeline",
            "Disk I/O error writing to checkpoint",
            "Memory limit exceeded for window operation",
            "Authentication failed for upstream service"
        };
        String msg = messages[RNG.nextInt(messages.length)];
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("source", "anomaly-gen");
        event.put("type", "log");
        event.put("subtype", "error");
        event.put("payload", Map.of("level", "ERROR", "message", msg));
        event.put("correlationId", correlationId);
        event.put("tags", Map.of("service", "anomaly-gen"));
        event.put("timestamp", Instant.now().toString());
        return client.sendEvent(event) ? 1 : 0;
    }

    private int sendHighLatencyTrace(String correlationId) {
        long duration = 1000 + RNG.nextLong(4000);
        String[] operations = {"/api/data/query", "/api/process/batch", "/api/storage/write", "/api/analyze"};
        String op = operations[RNG.nextInt(operations.length)];
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("traceId", "trace-" + counter.incrementAndGet());
        payload.put("spanId", "span-" + System.nanoTime());
        payload.put("operation", op);
        payload.put("durationMs", duration);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("source", "anomaly-gen");
        event.put("type", "trace");
        event.put("subtype", op.replace("/", "_"));
        event.put("payload", payload);
        event.put("correlationId", correlationId);
        event.put("tags", Map.of("service", "anomaly-gen"));
        event.put("timestamp", Instant.now().toString());
        return client.sendEvent(event) ? 1 : 0;
    }

    private Map<String, Object> buildMetricEvent(String source, String metric, double value, String correlationId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("metric", metric);
        payload.put("value", value);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("source", source);
        event.put("type", "metric");
        event.put("subtype", metric + "_reading");
        event.put("payload", payload);
        event.put("correlationId", correlationId);
        event.put("tags", Map.of("unit", metric.equals("temperature") ? "celsius" : "raw"));
        event.put("timestamp", Instant.now().toString());
        return event;
    }

    private void sleep(long ms) {
        if (ms <= 0) return;
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
