package com.sentinelflow.dashboard.controller;

import com.sentinelflow.dashboard.service.AlertManagementService;
import com.sentinelflow.dashboard.service.DashboardAggregationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private final AlertManagementService alertService;
    private final DashboardAggregationService aggregationService;

    public DashboardController(AlertManagementService alertService,
                               DashboardAggregationService aggregationService) {
        this.alertService = alertService;
        this.aggregationService = aggregationService;
    }

    @GetMapping("/summary")
    public Map<String, Object> summary() {
        var alertSummary = alertService.getSummary();
        var storageStatus = aggregationService.fetchSummary();
        return Map.of(
                "alerts", alertSummary,
                "storage", storageStatus
        );
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return alertService.getStatistics();
    }
}
