package com.sentinelflow.simulator.model;

import java.time.LocalDateTime;

public record TermoDataRecord(
        long packetId,
        LocalDateTime eventTime,
        long trId,
        long unitId,
        long sensorId,
        double val,
        int status
) {}
