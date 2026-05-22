package com.sentinelflow.dashboard.service;

import com.sentinelflow.dashboard.entity.Alert;
import com.sentinelflow.dashboard.repository.AlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
public class AlertManagementService {

    private static final Logger log = LoggerFactory.getLogger(AlertManagementService.class);

    private static final Set<String> VALID_TRANSITIONS = Set.of("OPEN→ACK", "ACK→RESOLVED", "OPEN→RESOLVED");

    private final AlertRepository alertRepository;

    public AlertManagementService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    @Transactional
    public Alert createAlert(String title, String severity, String source,
                             String description, String recommendation, String anomalyId) {
        String id = UUID.randomUUID().toString();
        var alert = new Alert(id, title, severity, "OPEN", source,
                description, recommendation, anomalyId, Instant.now(), null);
        alertRepository.save(alert);
        log.info("Created alert {}: {} [{}]", id, title, severity);
        return alert;
    }

    @Transactional
    public Optional<Alert> updateStatus(String id, String newStatus) {
        return alertRepository.findById(id).map(alert -> {
            String transition = alert.getStatus() + "→" + newStatus;
            if (!VALID_TRANSITIONS.contains(transition)) {
                log.warn("Invalid status transition: {} (current: {}, requested: {})", id, alert.getStatus(), newStatus);
                throw new IllegalArgumentException(
                        "Invalid transition from " + alert.getStatus() + " to " + newStatus);
            }
            alert.setStatus(newStatus);
            if ("RESOLVED".equals(newStatus)) {
                alert.setResolvedAt(Instant.now());
            }
            alertRepository.save(alert);
            log.info("Alert {} status changed to {}", id, newStatus);
            return alert;
        });
    }

    public List<Alert> listAlerts(String status, String severity, String since, String until, int limit) {
        if (status != null && !status.isBlank()) {
            return alertRepository.findByStatusOrderByDetectedAtDesc(status);
        }
        if (severity != null && !severity.isBlank()) {
            return alertRepository.findBySeverityOrderByDetectedAtDesc(severity);
        }
        if (since != null && until != null) {
            Instant s = Instant.parse(since);
            Instant u = Instant.parse(until);
            return alertRepository.findByDetectedAtBetween(s, u).stream()
                    .limit(limit > 0 ? limit : 100)
                    .toList();
        }
        if (since != null) {
            Instant s = Instant.parse(since);
            return alertRepository.findByDetectedAtAfter(s).stream()
                    .limit(limit > 0 ? limit : 100)
                    .toList();
        }
        return alertRepository.findAll().stream()
                .sorted((a, b) -> b.getDetectedAt().compareTo(a.getDetectedAt()))
                .limit(limit > 0 ? limit : 100)
                .toList();
    }

    public Map<String, Object> getSummary() {
        long openCount = alertRepository.countByStatus("OPEN");
        long ackCount = alertRepository.countByStatus("ACK");
        long resolvedCount = alertRepository.countByStatus("RESOLVED");
        List<Object[]> bySeverity = alertRepository.countOpenBySeverity();

        return Map.of(
                "openCount", openCount,
                "acknowledgedCount", ackCount,
                "resolvedCount", resolvedCount,
                "openBySeverity", bySeverity.stream()
                        .map(row -> Map.of("severity", row[0], "count", row[1]))
                        .toList()
        );
    }

    public Map<String, Object> getStatistics() {
        Instant now = Instant.now();
        long total = alertRepository.count();
        long critical = alertRepository.countBySeverity("CRITICAL");
        long warning = alertRepository.countBySeverity("WARNING");
        long lastHour = alertRepository.countByDetectedAtAfter(now.minusSeconds(3600));
        long lastDay = alertRepository.countByDetectedAtAfter(now.minusSeconds(86400));

        List<Map<String, Object>> bySource = alertRepository.countBySource().stream()
                .limit(10)
                .map(row -> Map.of("source", row[0], "count", row[1]))
                .toList();

        List<Map<String, Object>> totalsBySeverity = alertRepository.countTotalBySeverity().stream()
                .map(row -> Map.of("severity", row[0], "count", row[1]))
                .toList();

        List<Map<String, Object>> byStatus = alertRepository.countByStatusGrouped().stream()
                .map(row -> Map.of("status", row[0], "count", row[1]))
                .toList();

        List<Map<String, Object>> hourly = alertRepository.countByHour(now.minusSeconds(86400)).stream()
                .map(row -> Map.of("hour", row[0] != null ? row[0].toString() : "", "count", row[1]))
                .toList();

        return Map.of(
                "total", total,
                "critical", critical,
                "warning", warning,
                "lastHour", lastHour,
                "lastDay", lastDay,
                "bySource", bySource,
                "bySeverity", totalsBySeverity,
                "byStatus", byStatus,
                "hourly", hourly
        );
    }
}
