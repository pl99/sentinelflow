package com.sentinelflow.dashboard.controller;

import com.sentinelflow.dashboard.entity.Alert;
import com.sentinelflow.dashboard.service.AlertManagementService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/alerts")
public class AlertController {

    private final AlertManagementService alertService;

    public AlertController(AlertManagementService alertService) {
        this.alertService = alertService;
    }

    @GetMapping
    public List<Alert> listAlerts(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String since,
            @RequestParam(required = false) String until,
            @RequestParam(defaultValue = "100") int limit) {
        return alertService.listAlerts(status, severity, since, until, limit);
    }

    @PatchMapping("/{id}/status")
    public ResponseEntity<Map<String, Object>> updateStatus(
            @PathVariable String id,
            @RequestParam String status) {
        return alertService.updateStatus(id, status)
                .map(alert -> ResponseEntity.<Map<String, Object>>ok(Map.of(
                        "id", alert.getId(),
                        "status", alert.getStatus(),
                        "updated", true
                )))
                .orElse(ResponseEntity.notFound().build());
    }
}
