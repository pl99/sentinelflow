package com.sentinelflow.storage.repository;

import com.sentinelflow.storage.entity.MetricPoint;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MetricPointRepository extends JpaRepository<MetricPoint, MetricPoint.MetricPointId> {
}
