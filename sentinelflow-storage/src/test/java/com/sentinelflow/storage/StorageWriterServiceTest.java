package com.sentinelflow.storage;

import com.sentinelflow.common.event.TelemetryEvent;
import com.sentinelflow.storage.entity.MetricPoint;
import com.sentinelflow.storage.entity.TelemetryEventEntity;
import com.sentinelflow.storage.repository.MetricPointRepository;
import com.sentinelflow.storage.repository.TelemetryEventRepository;
import com.sentinelflow.storage.service.StorageWriterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StorageWriterServiceTest {

    @Mock
    private TelemetryEventRepository eventRepo;

    @Mock
    private MetricPointRepository metricRepo;

    @Captor
    private ArgumentCaptor<List<TelemetryEventEntity>> eventBatchCaptor;

    @Captor
    private ArgumentCaptor<List<MetricPoint>> metricBatchCaptor;

    private StorageWriterService writer;

    @BeforeEach
    void setUp() {
        writer = new StorageWriterService(eventRepo, metricRepo);
    }

    @Test
    void shouldSaveEvent() {
        var event = new TelemetryEvent("e1", "svc", "log", "error",
                Map.of("msg", "timeout"), Instant.now(), null, null);

        writer.writeEvent(event);

        verify(eventRepo).save(any(TelemetryEventEntity.class));
    }

    @Test
    void shouldSaveMetricAndEvent() {
        var event = new TelemetryEvent("e2", "svc", "metric", "latency",
                Map.of("metric", "p99", "value", 150), Instant.now(), null, null);

        writer.writeEvent(event);

        verify(eventRepo).save(any(TelemetryEventEntity.class));
        verify(metricRepo).save(any(MetricPoint.class));
    }

    @Test
    void shouldBatchSaveEvents() {
        var events = List.of(
                new TelemetryEvent("e1", "svc", "log", null, Map.of(), Instant.now(), null, null),
                new TelemetryEvent("e2", "svc", "metric", "cpu",
                        Map.of("metric", "cpu", "value", 85.0), Instant.now(), null, null),
                new TelemetryEvent("e3", "svc", "metric", "mem",
                        Map.of("metric", "mem", "value", 512.0), Instant.now(), null, null)
        );

        when(eventRepo.saveAll(any())).thenReturn(List.of());
        when(metricRepo.saveAll(any())).thenReturn(List.of());

        writer.writeBatch(events);

        verify(eventRepo).saveAll(eventBatchCaptor.capture());
        assertThat(eventBatchCaptor.getValue()).hasSize(3);

        verify(metricRepo).saveAll(metricBatchCaptor.capture());
        assertThat(metricBatchCaptor.getValue()).hasSize(2);
    }
}
