# Stage 1: Build Flink JARs
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build

# Copy all POM files — Maven validates all modules even with -pl
COPY pom.xml pom.xml
COPY sentinelflow-common/pom.xml sentinelflow-common/pom.xml
COPY sentinelflow-flink-jobs/pom.xml sentinelflow-flink-jobs/pom.xml
COPY sentinelflow-flink-jobs/processor/pom.xml sentinelflow-flink-jobs/processor/pom.xml
COPY sentinelflow-flink-jobs/detector/pom.xml sentinelflow-flink-jobs/detector/pom.xml
COPY sentinelflow-gateway/pom.xml sentinelflow-gateway/pom.xml
COPY sentinelflow-ingestion/pom.xml sentinelflow-ingestion/pom.xml
COPY sentinelflow-llm-analyzer/pom.xml sentinelflow-llm-analyzer/pom.xml
COPY sentinelflow-dashboard/pom.xml sentinelflow-dashboard/pom.xml
COPY sentinelflow-storage/pom.xml sentinelflow-storage/pom.xml
COPY sentinelflow-simulator/pom.xml sentinelflow-simulator/pom.xml

# Create stub source directories for non-Flink modules
RUN for d in gateway ingestion llm-analyzer dashboard storage simulator; do \
      mkdir -p "sentinelflow-${d}/src/main/java" "sentinelflow-${d}/src/main/resources"; \
    done && \
    for d in processor detector; do \
      mkdir -p "sentinelflow-flink-jobs/${d}/src/main/java"; \
    done

# Download dependencies
RUN mvn dependency:go-offline -pl sentinelflow-flink-jobs/processor,sentinelflow-flink-jobs/detector -am -B || true

# Copy source code (common + flink-jobs only)
COPY sentinelflow-common/src ./sentinelflow-common/src
COPY sentinelflow-flink-jobs/processor/src ./sentinelflow-flink-jobs/processor/src
COPY sentinelflow-flink-jobs/detector/src ./sentinelflow-flink-jobs/detector/src

# Build fat JARs (skip tests - they need Docker infrastructure)
RUN mvn package -pl sentinelflow-flink-jobs/processor,sentinelflow-flink-jobs/detector -am -DskipTests -Dspring-boot.repackage.skip=true -Dmaven.compiler.release=17 -B

# Stage 2: Flink submission image
FROM flink:1.18-java17

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy fat JARs from builder (exclude original- prefix)
COPY --from=builder /build/sentinelflow-flink-jobs/processor/target/sentinelflow-flink-processor-0.1.0-SNAPSHOT.jar /jobs/sentinelflow-processor.jar
COPY --from=builder /build/sentinelflow-flink-jobs/detector/target/sentinelflow-flink-detector-0.1.0-SNAPSHOT.jar /jobs/sentinelflow-detector.jar

# Copy entrypoint
COPY docker/flink-submit/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
