package com.sentinelflow.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Rate-limiting configuration for the ingestion gateway.
 * <p>
 * The {@link InMemoryRateLimiter} filter (declared in application.yml) uses
 * this {@link KeyResolver} to apply per-client rate limits based on the
 * client's IP address.
 * <p>
 * Each client is limited independently at 100 requests/second by default.
 * Stale client buckets are purged after 5 minutes of inactivity.
 */
@Configuration
public class GatewayRateLimiterConfig {

    @Bean
    public KeyResolver clientIpKeyResolver() {
        return exchange -> Mono.justOrEmpty(
                exchange.getRequest().getRemoteAddress()
        ).map(addr -> addr.getAddress().getHostAddress());
    }
}