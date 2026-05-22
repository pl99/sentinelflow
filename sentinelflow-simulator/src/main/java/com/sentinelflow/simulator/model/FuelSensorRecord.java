package com.sentinelflow.simulator.model;

import java.time.LocalDateTime;

public record FuelSensorRecord(
        long packetId,
        LocalDateTime eventTime,
        long sensorId,
        long trId,
        int levelMm,
        int levelL,
        int temperature,
        boolean switchedOn
) {}
