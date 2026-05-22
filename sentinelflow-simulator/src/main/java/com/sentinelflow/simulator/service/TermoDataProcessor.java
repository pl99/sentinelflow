package com.sentinelflow.simulator.service;

import com.sentinelflow.simulator.model.TermoDataRecord;

import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

public class TermoDataProcessor implements EventProcessor<TermoDataRecord> {

    @Override
    public Map<String, Object> toEventPayload(TermoDataRecord record) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("packet_id", record.packetId());
        payload.put("val", record.val());
        payload.put("status", record.status());
        return payload;
    }

    @Override
    public String source() {
        return "termo-sensor";
    }

    @Override
    public String metricType() {
        return "temperature";
    }

    @Override
    public Map<String, String> extractTags(TermoDataRecord record) {
        return Map.of(
                "tr_id", String.valueOf(record.trId()),
                "unit_id", String.valueOf(record.unitId()),
                "sensor_id", String.valueOf(record.sensorId())
        );
    }

    @Override
    public Instant extractTimestamp(TermoDataRecord record) {
        return record.eventTime().atZone(ZoneId.of("UTC")).toInstant();
    }
}
