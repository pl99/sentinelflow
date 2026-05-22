package com.sentinelflow.storage.service;

import com.sentinelflow.common.event.TelemetryEvent;
import com.sentinelflow.storage.entity.MetricPoint;
import com.sentinelflow.storage.entity.TelemetryEventEntity;
import com.sentinelflow.storage.repository.MetricPointRepository;
import com.sentinelflow.storage.repository.TelemetryEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class StorageWriterService {

    private static final Logger log = LoggerFactory.getLogger(StorageWriterService.class);

    private final TelemetryEventRepository eventRepo;
    private final MetricPointRepository metricRepo;

    public StorageWriterService(TelemetryEventRepository eventRepo, MetricPointRepository metricRepo) {
        this.eventRepo = eventRepo;
        this.metricRepo = metricRepo;
    }

    @Transactional
    public void writeEvent(TelemetryEvent event) {
        saveEvent(event);
        if ("metric".equals(event.type())) {
            saveMetricPoint(event);
        }
    }

    @Transactional
    public void writeBatch(List<TelemetryEvent> events) {
        var eventEntities = new ArrayList<TelemetryEventEntity>(events.size());
        var metricEntities = new ArrayList<MetricPoint>(events.size());

        for (var event : events) {
            eventEntities.add(toEntity(event));
            if ("metric".equals(event.type())) {
                metricEntities.add(toMetricPoint(event));
            }
        }

        eventRepo.saveAll(eventEntities);
        if (!metricEntities.isEmpty()) {
            metricRepo.saveAll(metricEntities);
        }

        log.debug("Batch saved: {} events, {} metrics", eventEntities.size(), metricEntities.size());
    }

    private void saveEvent(TelemetryEvent event) {
        eventRepo.save(toEntity(event));
    }

    private void saveMetricPoint(TelemetryEvent event) {
        metricRepo.save(toMetricPoint(event));
    }

    private TelemetryEventEntity toEntity(TelemetryEvent event) {
        return new TelemetryEventEntity(
                event.id(), event.source(), event.type(), event.subtype(),
                event.timestamp(), Instant.now(), event.correlationId(),
                event.payload(), event.tags()
        );
    }

    private MetricPoint toMetricPoint(TelemetryEvent event) {
        Object metricName = event.payload() != null ? event.payload().get("metric") : null;
        Object value = event.payload() != null ? event.payload().get("value") : null;
        String name = metricName != null ? metricName.toString() : event.subtype();
        double val = value instanceof Number n ? n.doubleValue() : 0.0;

        return new MetricPoint(
                event.id() + "-m", event.timestamp(),
                event.source(), name, val, event.tags()
        );
    }
}
