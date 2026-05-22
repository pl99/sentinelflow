FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-storage/target/sentinelflow-storage-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "app.jar"]
