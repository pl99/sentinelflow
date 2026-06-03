package com.sentinelflow.dashboard.repository;

import com.sentinelflow.dashboard.entity.Alert;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface AlertRepository extends JpaRepository<Alert, String> {

    List<Alert> findByStatusOrderByDetectedAtDesc(String status);

    List<Alert> findBySeverityOrderByDetectedAtDesc(String severity);

    long countByStatus(String status);

    @Query("SELECT a.severity, COUNT(a) FROM Alert a WHERE a.status = 'OPEN' GROUP BY a.severity")
    List<Object[]> countOpenBySeverity();

    long countBySeverity(String severity);

    long countByDetectedAtAfter(Instant after);

    @Query("SELECT a.source, COUNT(a) FROM Alert a GROUP BY a.source ORDER BY COUNT(a) DESC")
    List<Object[]> countBySource();

    @Query("SELECT a.severity, COUNT(a) FROM Alert a GROUP BY a.severity")
    List<Object[]> countTotalBySeverity();

    @Query("SELECT a.status, COUNT(a) FROM Alert a GROUP BY a.status")
    List<Object[]> countByStatusGrouped();

    @Query("SELECT FUNCTION('DATE_TRUNC', 'hour', a.detectedAt), COUNT(a) FROM Alert a " +
           "WHERE a.detectedAt >= :since GROUP BY FUNCTION('DATE_TRUNC', 'hour', a.detectedAt) ORDER BY 1")
    List<Object[]> countByHour(@Param("since") Instant since);

    @Query("SELECT a FROM Alert a WHERE a.detectedAt >= :since ORDER BY a.detectedAt DESC")
    List<Alert> findByDetectedAtAfter(@Param("since") Instant since);

    @Query("SELECT a FROM Alert a WHERE a.detectedAt >= :since AND a.detectedAt < :until ORDER BY a.detectedAt DESC")
    List<Alert> findByDetectedAtBetween(@Param("since") Instant since, @Param("until") Instant until);

    Optional<Alert> findByAnomalyId(String anomalyId);
}
