package com.sentinelflow.gateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-client rate-limiting gateway filter using a local token-bucket algorithm.
 * <p>
 * Each unique client key (resolved by {@link KeyResolver}) gets its own
 * token bucket.  Stale buckets are periodically purged so memory does not
 * grow unbounded.
 * <p>
 * <strong>Single-instance only.</strong> For horizontal scaling replace this
 * filter with the Redis-backed {@code RequestRateLimiter} filter.
 */
@Component
public class InMemoryRateLimiter
        extends AbstractGatewayFilterFactory<InMemoryRateLimiter.Config> {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRateLimiter.class);

    static final long IDLE_TIMEOUT_MS   = 300_000L; // 5 min

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final KeyResolver keyResolver;

    public InMemoryRateLimiter(KeyResolver keyResolver) {
        super(Config.class);
        this.keyResolver = keyResolver;
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> keyResolver.resolve(exchange)
                .defaultIfEmpty("unknown")
                .flatMap(key -> {
                    TokenBucket bucket = buckets.computeIfAbsent(key, k -> new TokenBucket(config));
                    if (bucket.tryConsume()) {
                        return chain.filter(exchange);
                    }
                    log.warn("Rate limit exceeded for client: {}", key);
                    exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                    return exchange.getResponse().setComplete();
                });
    }

    @Override
    public String name() {
        return "InMemoryRateLimiter";
    }

    // --- Stale bucket cleanup --------------------------------------------------

    // Called periodically — the filter itself acts as a @Component
    void purgeStaleBuckets() {
        long deadline = System.currentTimeMillis() - IDLE_TIMEOUT_MS;
        buckets.values().removeIf(b -> b.lastAccessTime < deadline);
    }

    // --- Token bucket (per client) ---------------------------------------------

    static final class TokenBucket {
        private final long   maxTokens;
        private final AtomicLong tokens;
        private volatile long lastRefillTime;
        private volatile long lastAccessTime;

        TokenBucket(Config config) {
            this.maxTokens = config.burstCapacity;
            this.tokens = new AtomicLong(config.burstCapacity);
            this.lastRefillTime = System.currentTimeMillis();
            this.lastAccessTime = System.currentTimeMillis();
        }

        boolean tryConsume() {
            lastAccessTime = System.currentTimeMillis();
            refill();
            while (true) {
                long current = tokens.get();
                if (current <= 0) return false;
                if (tokens.compareAndSet(current, current - 1)) return true;
            }
        }

        private void refill() {
            long now = System.currentTimeMillis();
            long elapsed = now - lastRefillTime;
            if (elapsed < 1000L) return;
            long addedTokens = (elapsed / 1000L) * maxTokens;
            if (addedTokens > 0) {
                lastRefillTime = now;
                tokens.updateAndGet(t -> Math.min(t + addedTokens, maxTokens));
            }
        }
    }

    // --- Filter configuration --------------------------------------------------

    public record Config(int replenishRate, int burstCapacity) {
        public Config {
            if (replenishRate <= 0 || burstCapacity <= 0) {
                throw new IllegalArgumentException("replenishRate and burstCapacity must be positive");
            }
        }
    }
}