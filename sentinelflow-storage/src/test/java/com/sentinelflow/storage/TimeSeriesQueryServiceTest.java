package com.sentinelflow.storage;

import com.sentinelflow.storage.dto.SummaryResponse;
import com.sentinelflow.storage.service.TimeSeriesQueryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimeSeriesQueryServiceTest {

    @Mock
    private EntityManager em;

    @Mock
    private TypedQuery<Long> countQuery;

    private TimeSeriesQueryService service;

    @BeforeEach
    void setUp() {
        lenient().when(countQuery.setParameter(anyString(), any())).thenReturn(countQuery);
        service = new TimeSeriesQueryService(em);
    }

    @Test
    void shouldReturnSummary() {
        when(em.createQuery("SELECT COUNT(e) FROM TelemetryEventEntity e", Long.class))
                .thenReturn(countQuery);
        when(em.createQuery("SELECT COUNT(m) FROM MetricPoint m", Long.class))
                .thenReturn(countQuery);
        when(em.createQuery(
                "SELECT COUNT(e) FROM TelemetryEventEntity e WHERE e.timestamp >= :since", Long.class))
                .thenReturn(countQuery);
        when(em.createQuery(
                "SELECT COUNT(m) FROM MetricPoint m WHERE m.time >= :since", Long.class))
                .thenReturn(countQuery);

        when(countQuery.getSingleResult()).thenReturn(100L, 50L, 10L, 5L);

        SummaryResponse result = service.summary();

        assertThat(result.totalEvents()).isEqualTo(100);
        assertThat(result.totalMetrics()).isEqualTo(50);
        assertThat(result.eventsLastHour()).isEqualTo(10);
        assertThat(result.metricsLastHour()).isEqualTo(5);
    }
}
