package com.sentinelflow.simulator.service;

import com.sentinelflow.simulator.reader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataSimulatorService {

    private static final Logger log = LoggerFactory.getLogger(DataSimulatorService.class);

    private final IngestionClient ingestionClient;
    private final String termoDataPath;
    private final String fuelDataPath;
    private final long delayMs;

    public DataSimulatorService(IngestionClient ingestionClient,
                                String termoDataPath,
                                String fuelDataPath,
                                long delayMs) {
        this.ingestionClient = ingestionClient;
        this.termoDataPath = termoDataPath;
        this.fuelDataPath = fuelDataPath;
        this.delayMs = delayMs;
    }

    public int runSimulation() {
        int total = 0;

        List<com.sentinelflow.simulator.model.TermoDataRecord> termoRecords =
                CsvReader.read(termoDataPath, fields -> new com.sentinelflow.simulator.model.TermoDataRecord(
                        Long.parseLong(fields[0]),
                        java.time.LocalDateTime.parse(fields[1].replace(" ", "T")),
                        Long.parseLong(fields[2]),
                        Long.parseLong(fields[3]),
                        Long.parseLong(fields[4]),
                        Double.parseDouble(fields[5]),
                        Integer.parseInt(fields[6])
                ));
        log.info("Loaded {} termo records", termoRecords.size());

        List<com.sentinelflow.simulator.model.FuelSensorRecord> fuelRecords =
                CsvReader.read(fuelDataPath, fields -> new com.sentinelflow.simulator.model.FuelSensorRecord(
                        Long.parseLong(fields[0]),
                        java.time.LocalDateTime.parse(fields[1].replace(" ", "T")),
                        Long.parseLong(fields[2]),
                        Long.parseLong(fields[3]),
                        Integer.parseInt(fields[4]),
                        Integer.parseInt(fields[5]),
                        Integer.parseInt(fields[6]),
                        Boolean.parseBoolean(fields[7])
                ));
        log.info("Loaded {} fuel sensor records", fuelRecords.size());

        var termoProcessor = new TermoDataProcessor();
        var fuelProcessor = new FuelSensorProcessor();

        int maxLen = Math.max(termoRecords.size(), fuelRecords.size());
        for (int i = 0; i < maxLen; i++) {
            if (i < termoRecords.size()) {
                var record = termoRecords.get(i);
                Map<String, Object> event = buildEvent(termoProcessor, record);
                if (ingestionClient.sendEvent(event)) total++;
                sleep(delayMs);
            }
            if (i < fuelRecords.size()) {
                var record = fuelRecords.get(i);
                Map<String, Object> event = buildEvent(fuelProcessor, record);
                if (ingestionClient.sendEvent(event)) total++;
                sleep(delayMs);
            }
            if ((i + 1) % 100 == 0) {
                log.info("Progress: {}/{} records sent", i + 1, maxLen);
            }
        }

        log.info("Simulation complete. Sent {} events", total);
        return total;
    }

    private <T> Map<String, Object> buildEvent(EventProcessor<T> processor, T record) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("source", processor.source());
        event.put("type", "metric");
        event.put("subtype", processor.metricType());
        event.put("payload", processor.toEventPayload(record));
        event.put("tags", processor.extractTags(record));
        event.put("timestamp", processor.extractTimestamp(record).toString());
        return event;
    }

    private void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
