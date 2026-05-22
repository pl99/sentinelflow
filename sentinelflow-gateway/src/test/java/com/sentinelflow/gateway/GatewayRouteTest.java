package com.sentinelflow.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class GatewayRouteTest {

    @Autowired
    private RouteDefinitionLocator routeLocator;

    @Test
    void shouldDefineIngestionRoute() {
        var routes = collectRoutes();

        assertThat(routes)
                .anyMatch(r -> r.getId().equals("ingestion")
                        && r.getUri().toString().contains("8081"));
    }

    @Test
    void shouldDefineStorageQueryRoute() {
        var routes = collectRoutes();

        assertThat(routes)
                .anyMatch(r -> r.getId().equals("storage-query")
                        && r.getUri().toString().contains("8086"));
    }

    @Test
    void shouldDefineDashboardRoute() {
        var routes = collectRoutes();

        assertThat(routes)
                .anyMatch(r -> r.getId().equals("dashboard")
                        && r.getUri().toString().contains("8085"));
    }

    @Test
    void shouldHaveRateLimiterOnIngestionRoute() {
        var routes = collectRoutes();

        var ingestion = routes.stream()
                .filter(r -> r.getId().equals("ingestion"))
                .findFirst().orElseThrow();

        var filterNames = ingestion.getFilters().stream()
                .map(FilterDefinition::getName)
                .toList();

        assertThat(filterNames).contains("IngestionRateLimiter");
    }

    private List<RouteDefinition> collectRoutes() {
        var flux = routeLocator.getRouteDefinitions();
        var list = new java.util.ArrayList<RouteDefinition>();
        StepVerifier.create(flux)
                .recordWith(() -> list)
                .expectNextCount(3)
                .verifyComplete();
        return list;
    }
}
