package com.sentinelflow.storage.repository;

import com.sentinelflow.storage.entity.TelemetryEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TelemetryEventRepository extends JpaRepository<TelemetryEventEntity, String> {
}
