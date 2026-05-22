FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-ingestion/target/sentinelflow-ingestion-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
