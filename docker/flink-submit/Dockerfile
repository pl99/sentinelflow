# Stage 1: Use pre-built JARs from the local filesystem
FROM scratch AS builder
COPY sentinelflow-flink-jobs/processor/target/sentinelflow-flink-processor-0.1.0-SNAPSHOT.jar /build/sentinelflow-flink-jobs/processor/target/sentinelflow-flink-processor-0.1.0-SNAPSHOT.jar
COPY sentinelflow-flink-jobs/detector/target/sentinelflow-flink-detector-0.1.0-SNAPSHOT.jar /build/sentinelflow-flink-jobs/detector/target/sentinelflow-flink-detector-0.1.0-SNAPSHOT.jar

# Stage 2: Flink submission image
FROM flink:2.0.2-java21

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy fat JARs from builder
COPY --from=builder /build/sentinelflow-flink-jobs/processor/target/sentinelflow-flink-processor-0.1.0-SNAPSHOT.jar /jobs/sentinelflow-processor.jar
COPY --from=builder /build/sentinelflow-flink-jobs/detector/target/sentinelflow-flink-detector-0.1.0-SNAPSHOT.jar /jobs/sentinelflow-detector.jar

# Copy entrypoint
COPY docker/flink-submit/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
