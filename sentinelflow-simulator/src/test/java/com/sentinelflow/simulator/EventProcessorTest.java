package com.sentinelflow.simulator;

import com.sentinelflow.simulator.model.FuelSensorRecord;
import com.sentinelflow.simulator.model.TermoDataRecord;
import com.sentinelflow.simulator.service.FuelSensorProcessor;
import com.sentinelflow.simulator.service.TermoDataProcessor;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class EventProcessorTest {

    @Test
    void shouldConvertTermoRecordToEvent() {
        var processor = new TermoDataProcessor();
        var record = new TermoDataRecord(-5517330598926187525L,
                LocalDateTime.of(2025, 11, 10, 0, 0, 5),
                129222L, 880833L, 851208L, 5.0, 1);

        assertThat(processor.source()).isEqualTo("termo-sensor");
        assertThat(processor.metricType()).isEqualTo("temperature");

        var payload = processor.toEventPayload(record);
        assertThat(payload).containsEntry("val", 5.0);

        var tags = processor.extractTags(record);
        assertThat(tags).containsEntry("sensor_id", "851208");
    }

    @Test
    void shouldConvertFuelRecordToEvent() {
        var processor = new FuelSensorProcessor();
        var record = new FuelSensorRecord(-5517330598926187446L,
                LocalDateTime.of(2025, 11, 10, 0, 0, 19),
                689716L, 129222L, 3881, 3881, 0, true);

        assertThat(processor.source()).isEqualTo("fuel-sensor");
        assertThat(processor.metricType()).isEqualTo("fuel-level");

        var payload = processor.toEventPayload(record);
        assertThat(payload).containsEntry("level_mm", 3881);
        assertThat(payload).containsEntry("switched_on", true);

        var tags = processor.extractTags(record);
        assertThat(tags).containsEntry("sensor_id", "689716");
    }
}
