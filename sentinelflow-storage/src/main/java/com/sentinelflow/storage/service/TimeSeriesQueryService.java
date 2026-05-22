package com.sentinelflow.storage.service;

import com.sentinelflow.storage.dto.AggregatedMetric;
import com.sentinelflow.storage.dto.EventQueryResponse;
import com.sentinelflow.storage.dto.SummaryResponse;
import com.sentinelflow.storage.entity.TelemetryEventEntity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class TimeSeriesQueryService {

    private final EntityManager em;

    public TimeSeriesQueryService(EntityManager em) {
        this.em = em;
    }

    public List<AggregatedMetric> queryMetrics(
            String service, String metricName, Instant start, Instant end,
            String bucket, int limit) {

        String bucketInterval = (bucket != null && !bucket.isBlank()) ? bucket : '1' + " minute";

        var sql = new StringBuilder("""
                SELECT time_bucket(CAST(:bucket AS interval), mp.time) AS bucket,
                       mp.service, mp.metric_name,
                       AVG(mp.value) AS avg, MIN(mp.value) AS min, MAX(mp.value) AS max,
                       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY mp.value) AS p95,
                       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY mp.value) AS p99,
                       COUNT(*) AS cnt
                FROM metric_points mp
                WHERE mp.time BETWEEN :start AND :end
                """);
        if (service != null && !service.isBlank()) {
            sql.append(" AND mp.service = :service");
        }
        if (metricName != null && !metricName.isBlank()) {
            sql.append(" AND mp.metric_name = :metricName");
        }
        sql.append("""
                GROUP BY bucket, mp.service, mp.metric_name
                ORDER BY bucket DESC
                LIMIT :limit
                """);

        var query = em.createNativeQuery(sql.toString(), Object[].class);
        query.setParameter("bucket", bucketInterval);
        query.setParameter("start", start);
        query.setParameter("end", end);
        if (service != null && !service.isBlank()) {
            query.setParameter("service", service);
        }
        if (metricName != null && !metricName.isBlank()) {
            query.setParameter("metricName", metricName);
        }
        query.setParameter("limit", limit > 0 ? limit : 100);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = query.getResultList();

        return rows.stream().map(r -> new AggregatedMetric(
                toInstant(r[0]),
                (String) r[1],
                (String) r[2],
                toDouble(r[3]), toDouble(r[4]), toDouble(r[5]),
                toDouble(r[6]), toDouble(r[7]),
                toLong(r[8])
        )).toList();
    }

    public List<EventQueryResponse> queryEvents(
            String source, String type, String subtype,
            Instant start, Instant end, String searchText, int limit) {

        String jpql = """
                SELECT e FROM TelemetryEventEntity e
                WHERE 1=1
                  AND (:source IS NULL OR e.source = :source)
                  AND (:type IS NULL OR e.type = :type)
                  AND (:subtype IS NULL OR e.subtype = :subtype)
                  AND e.timestamp BETWEEN :start AND :end
                  AND (:searchText IS NULL
                       OR CAST(e.payload AS string) LIKE :searchPattern
                       OR CAST(e.tags AS string) LIKE :searchPattern)
                ORDER BY e.timestamp DESC
                """;

        TypedQuery<TelemetryEventEntity> query = em.createQuery(jpql, TelemetryEventEntity.class);
        query.setParameter("source", source);
        query.setParameter("type", type);
        query.setParameter("subtype", subtype);
        query.setParameter("start", start);
        query.setParameter("end", end);
        query.setParameter("searchText", searchText);
        query.setParameter("searchPattern", searchText != null ? '%' + searchText + '%' : null);
        query.setMaxResults(limit > 0 ? limit : 100);

        return query.getResultStream()
                .map(e -> new EventQueryResponse(
                        e.getId(), e.getSource(), e.getType(), e.getSubtype(),
                        e.getTimestamp(), e.getCorrelationId(),
                        e.getPayload(), e.getTags()
                ))
                .toList();
    }

    public SummaryResponse summary() {
        Long totalEvents = em.createQuery(
                "SELECT COUNT(e) FROM TelemetryEventEntity e", Long.class).getSingleResult();
        Long totalMetrics = em.createQuery(
                "SELECT COUNT(m) FROM MetricPoint m", Long.class).getSingleResult();

        Instant oneHourAgo = Instant.now().minusSeconds(3600);
        Long eventsLastHour = em.createQuery(
                        "SELECT COUNT(e) FROM TelemetryEventEntity e WHERE e.timestamp >= :since", Long.class)
                .setParameter("since", oneHourAgo)
                .getSingleResult();
        Long metricsLastHour = em.createQuery(
                        "SELECT COUNT(m) FROM MetricPoint m WHERE m.time >= :since", Long.class)
                .setParameter("since", oneHourAgo)
                .getSingleResult();

        return new SummaryResponse(
                totalEvents, totalMetrics,
                eventsLastHour, metricsLastHour);
    }

    private static Instant toInstant(Object v) {
        if (v instanceof Instant i) return i;
        if (v instanceof java.sql.Timestamp ts) return ts.toInstant();
        if (v instanceof java.time.OffsetDateTime odt) return odt.toInstant();
        if (v instanceof java.time.LocalDateTime ldt) return ldt.atZone(java.time.ZoneOffset.UTC).toInstant();
        return Instant.now();
    }

    private static double toDouble(Object v) {
        return v instanceof Number n ? n.doubleValue() : 0.0;
    }

    private static long toLong(Object v) {
        return v instanceof Number n ? n.longValue() : 0L;
    }
}
