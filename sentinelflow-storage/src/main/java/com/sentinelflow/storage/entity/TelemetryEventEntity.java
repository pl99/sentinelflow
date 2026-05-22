package com.sentinelflow.storage.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "telemetry_events")
public class TelemetryEventEntity {

    @Id
    private String id;
    private String source;
    private String type;
    private String subtype;
    private Instant timestamp;
    private Instant receivedAt;
    private String correlationId;

    @JdbcTypeCode(SqlTypes.JSON)
    @jakarta.persistence.Column(columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @JdbcTypeCode(SqlTypes.JSON)
    @jakarta.persistence.Column(columnDefinition = "jsonb")
    private Map<String, String> tags;

    public TelemetryEventEntity() {}

    public TelemetryEventEntity(String id, String source, String type, String subtype,
                                Instant timestamp, Instant receivedAt, String correlationId,
                                Map<String, Object> payload, Map<String, String> tags) {
        this.id = id;
        this.source = source;
        this.type = type;
        this.subtype = subtype;
        this.timestamp = timestamp;
        this.receivedAt = receivedAt;
        this.correlationId = correlationId;
        this.payload = payload;
        this.tags = tags;
    }

    public String getId() { return id; }
    public String getSource() { return source; }
    public String getType() { return type; }
    public String getSubtype() { return subtype; }
    public Instant getTimestamp() { return timestamp; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getCorrelationId() { return correlationId; }
    public Map<String, Object> getPayload() { return payload; }
    public Map<String, String> getTags() { return tags; }
}
