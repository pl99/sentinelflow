package com.sentinelflow.gateway.config;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Configuration
public class GatewayRateLimiterConfig {

    @Bean
    public KeyResolver principalNameKeyResolver() {
        return exchange -> Mono.just(
                exchange.getRequest().getRemoteAddress() != null
                        ? exchange.getRequest().getRemoteAddress().getAddress().getHostAddress()
                        : "unknown"
        );
    }
}

@Component
class RateLimitFilterFactory extends AbstractGatewayFilterFactory<Object> {

    private final RateLimiter rateLimiter;

    public RateLimitFilterFactory() {
        super(Object.class);
        var config = RateLimiterConfig.custom()
                .limitForPeriod(100)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(100))
                .build();
        this.rateLimiter = RateLimiter.of("ingestion-rate-limiter", config);
    }

    @Override
    public GatewayFilter apply(Object config) {
        return (exchange, chain) -> {
            try {
                boolean permitted = rateLimiter.executeCallable(() -> true);
                if (permitted) {
                    return chain.filter(exchange);
                }
                exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                return exchange.getResponse().setComplete();
            } catch (Exception e) {
                return chain.filter(exchange);
            }
        };
    }

    @Override
    public String name() {
        return "IngestionRateLimiter";
    }
}
