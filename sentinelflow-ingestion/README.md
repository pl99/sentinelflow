# SentinelFlow Ingestion

Сервис приёма телеметрических данных. Предоставляет REST API для приёма событий, логов, метрик и трейсов, публикует их в Kafka-топик `raw-events` для дальнейшей обработки.

## Архитектура

```
Client ──POST──> IngestionController ──> IngestionService ──> TelemetryProducer ──> Kafka (raw-events)
                        │                       │
                   @Valid DTO              Normalize to
                   (validation)            TelemetryEvent
```

Сервис не имеет базы данных — это лёгкий HTTP-to-Kafka мост.

## API

Базовый путь: `/api/v1/ingest`

| Method | Path | Описание |
|--------|------|----------|
| POST | `/api/v1/ingest` | Принять одно событие |
| POST | `/api/v1/ingest/batch` | Принять несколько событий |
| POST | `/api/v1/ingest/logs` | Принять лог-запись |
| POST | `/api/v1/ingest/metrics` | Принять метрику |
| POST | `/api/v1/ingest/traces` | Принять трейс (span) |

Все запросы принимаются как `application/json`. Валидация — Jakarta Bean Validation (`@NotBlank`, `@NotNull`). Успешный ответ — `202 Accepted` с `{"accepted": true, "id": "<uuid>"}` (для batch — `count` и `ids`). Ошибки валидации — `400 Bad Request`.

### Пример: событие

**POST /api/v1/ingest**

```json
{
  "source": "order-service",
  "type": "log",
  "subtype": "ERROR",
  "payload": {
    "message": "Connection timeout"
  },
  "correlationId": "corr-123",
  "tags": {
    "env": "prod"
  }
}
```

### Пример: метрика

**POST /api/v1/ingest/metrics**

```json
{
  "source": "order-service",
  "name": "p99-latency",
  "value": 1420.5,
  "tags": {
    "host": "web-01"
  }
}
```

### Пример: трейс

**POST /api/v1/ingest/traces**

```json
{
  "traceId": "abc123",
  "spanId": "span-001",
  "parentSpanId": "span-000",
  "service": "order-service",
  "operation": "processOrder",
  "durationMs": 450,
  "startTime": "2026-05-22T12:00:00Z"
}
```

## Kafka

Публикует в топик `raw-events`. Ключ сообщения — `{source}:{type}`. Сериализация — JSON (`JsonSerializer`).

## Конфигурация

| Параметр | ENV | По умолчанию |
|----------|-----|-------------|
| `server.port` | — | `8081` |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |

## Технологии

- Spring Boot 3.5.x
- Spring Kafka (producer)
- Jakarta Bean Validation
- sentinelflow-common (TelemetryEvent, KafkaTopics)

## Сборка и запуск

```bash
# Сборка
mvn clean package -pl sentinelflow-ingestion

# Запуск
java -jar target/sentinelflow-ingestion-0.1.0-SNAPSHOT.jar
```

## Docker

```dockerfile
FROM eclipse-temurin:21-jre
COPY sentinelflow-ingestion/target/sentinelflow-ingestion-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Порт: `8081`. Контейнеру требуется доступ к Kafka.
