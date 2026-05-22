package com.sentinelflow.storage;

import com.sentinelflow.storage.controller.QueryController;
import com.sentinelflow.storage.dto.AggregatedMetric;
import com.sentinelflow.storage.dto.EventQueryResponse;
import com.sentinelflow.storage.dto.SummaryResponse;
import com.sentinelflow.storage.service.TimeSeriesQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(QueryController.class)
class QueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TimeSeriesQueryService queryService;

    @Test
    void shouldReturnSummary() throws Exception {
        when(queryService.summary()).thenReturn(
                new SummaryResponse(100, 50, 10, 5));

        mockMvc.perform(get("/api/v1/query/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalEvents").value(100))
                .andExpect(jsonPath("$.totalMetrics").value(50))
                .andExpect(jsonPath("$.eventsLastHour").value(10));
    }

    @Test
    void shouldReturnMetrics() throws Exception {
        Instant now = Instant.parse("2026-05-20T12:00:00Z");
        when(queryService.queryMetrics(any(), any(), any(), any(), any(), anyInt()))
                .thenReturn(List.of(
                        new AggregatedMetric(now, "svc", "cpu", 50.0, 10.0, 90.0, 85.0, 95.0, 10)));

        mockMvc.perform(get("/api/v1/query/metrics")
                        .param("start", "2026-05-20T10:00:00Z")
                        .param("end", "2026-05-20T12:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].service").value("svc"))
                .andExpect(jsonPath("$[0].metricName").value("cpu"))
                .andExpect(jsonPath("$[0].avg").value(50.0));
    }

    @Test
    void shouldReturnEvents() throws Exception {
        when(queryService.queryEvents(any(), any(), any(), any(), any(), any(), anyInt()))
                .thenReturn(List.of(
                        new EventQueryResponse("e1", "svc", "log", "error",
                                Instant.parse("2026-05-20T11:00:00Z"), null, Map.of(), Map.of())));

        mockMvc.perform(get("/api/v1/query/events")
                        .param("start", "2026-05-20T10:00:00Z")
                        .param("end", "2026-05-20T12:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value("e1"))
                .andExpect(jsonPath("$[0].source").value("svc"))
                .andExpect(jsonPath("$[0].type").value("log"));
    }
}
