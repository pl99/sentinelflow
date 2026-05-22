package com.sentinelflow.dashboard.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "alerts")
public class Alert {

    @Id
    private String id;

    @Column(nullable = false)
    private String title;

    @Column(nullable = false)
    private String severity;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private String source;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(columnDefinition = "TEXT")
    private String recommendation;

    private String anomalyId;

    private Instant detectedAt;
    private Instant resolvedAt;

    public Alert() {}

    public Alert(String id, String title, String severity, String status, String source,
                 String description, String recommendation, String anomalyId,
                 Instant detectedAt, Instant resolvedAt) {
        this.id = id;
        this.title = title;
        this.severity = severity;
        this.status = status;
        this.source = source;
        this.description = description;
        this.recommendation = recommendation;
        this.anomalyId = anomalyId;
        this.detectedAt = detectedAt;
        this.resolvedAt = resolvedAt;
    }

    public String getId() { return id; }
    public String getTitle() { return title; }
    public String getSeverity() { return severity; }
    public String getStatus() { return status; }
    public String getSource() { return source; }
    public String getDescription() { return description; }
    public String getRecommendation() { return recommendation; }
    public String getAnomalyId() { return anomalyId; }
    public Instant getDetectedAt() { return detectedAt; }
    public Instant getResolvedAt() { return resolvedAt; }

    public void setStatus(String status) { this.status = status; }
    public void setResolvedAt(Instant resolvedAt) { this.resolvedAt = resolvedAt; }
}
