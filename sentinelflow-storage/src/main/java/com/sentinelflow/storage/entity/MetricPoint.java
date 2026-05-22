package com.sentinelflow.storage.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "metric_points")
@IdClass(MetricPoint.MetricPointId.class)
public class MetricPoint {

    public static class MetricPointId implements Serializable {
        private Instant time;
        private String id;

        public MetricPointId() {}
        public MetricPointId(Instant time, String id) { this.time = time; this.id = id; }

        public Instant getTime() { return time; }
        public String getId() { return id; }
        public void setTime(Instant time) { this.time = time; }
        public void setId(String id) { this.id = id; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricPointId that = (MetricPointId) o;
            return java.util.Objects.equals(time, that.time) && java.util.Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() { return java.util.Objects.hash(time, id); }
    }

    @Id
    private Instant time;
    @Id
    private String id;
    private String service;
    private String metricName;
    private double value;

    @JdbcTypeCode(SqlTypes.JSON)
    @jakarta.persistence.Column(columnDefinition = "jsonb")
    private Map<String, String> tags;

    public MetricPoint() {}

    public MetricPoint(String id, Instant time, String service, String metricName,
                       double value, Map<String, String> tags) {
        this.id = id;
        this.time = time;
        this.service = service;
        this.metricName = metricName;
        this.value = value;
        this.tags = tags;
    }

    public String getId() { return id; }
    public Instant getTime() { return time; }
    public String getService() { return service; }
    public String getMetricName() { return metricName; }
    public double getValue() { return value; }
    public Map<String, String> getTags() { return tags; }
}
