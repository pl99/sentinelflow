package com.sentinelflow.simulator.service;

import com.sentinelflow.simulator.model.FuelSensorRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

public class FuelSensorProcessor implements EventProcessor<FuelSensorRecord> {

    @Override
    public Map<String, Object> toEventPayload(FuelSensorRecord record) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("packet_id", record.packetId());
        payload.put("level_mm", record.levelMm());
        payload.put("level_l", record.levelL());
        payload.put("temperature", record.temperature());
        payload.put("switched_on", record.switchedOn());
        return payload;
    }

    @Override
    public String source() {
        return "fuel-sensor";
    }

    @Override
    public String metricType() {
        return "fuel-level";
    }

    @Override
    public Map<String, String> extractTags(FuelSensorRecord record) {
        return Map.of(
                "sensor_id", String.valueOf(record.sensorId()),
                "tr_id", String.valueOf(record.trId())
        );
    }

    @Override
    public Instant extractTimestamp(FuelSensorRecord record) {
        return record.eventTime().atZone(ZoneId.of("UTC")).toInstant();
    }
}
