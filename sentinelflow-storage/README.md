# SentinelFlow Storage

Сервис хранения и запроса телеметрических данных. Потребляет события из Kafka (`raw-events`), сохраняет в PostgreSQL с TimescaleDB, предоставляет REST API для агрегированных запросов.

## Архитектура

```
Kafka (raw-events) ──> EventConsumer ──> StorageWriterService ──> PostgreSQL + TimescaleDB
                                                                         │
                                               QueryController <── TimeSeriesQueryService
```

- **EventConsumer**: батчевое потребление (до 50 записей), ручной ack
- **StorageWriterService**: запись событий и метрик, `@Transactional`
- **TimeSeriesQueryService**: TimescaleDB `time_bucket`, JPQL поиск, сводка

## API

Базовый путь: `/api/v1/query`

| Method | Path | Параметры | Описание |
|--------|------|-----------|----------|
| GET | `/api/v1/query/metrics` | `service`, `metricName`, `start`*, `end`*, `bucket`, `limit` | Агрегированные метрики (AVG, MIN, MAX, P95, P99, COUNT) |
| GET | `/api/v1/query/events` | `source`, `type`, `subtype`, `start`*, `end`*, `search`, `limit` | Поиск событий с фильтрацией |
| GET | `/api/v1/query/summary` | — | Сводка: всего событий, метрик, за последний час |

Параметры `start`/`end` — ISO 8601 (обязательные). `bucket` — по умолчанию `1 minute`.

Swagger UI: `http://localhost:8086/swagger-ui.html`

## База данных

### telemetry_events

| Колонка | Тип | Описание |
|---------|-----|----------|
| id | VARCHAR(64) PK | ID события |
| source | VARCHAR(255) | Источник |
| type | VARCHAR(64) | Тип (metric, log, trace) |
| subtype | VARCHAR(255) | Подтип |
| timestamp | TIMESTAMPTZ | Время события |
| received_at | TIMESTAMPTZ | Время получения |
| correlation_id | VARCHAR(255) | Correlation ID |
| payload | JSONB | Произвольные данные |
| tags | JSONB | Теги |

### metric_points (hypertable TimescaleDB)

| Колонка | Тип | Описание |
|---------|-----|----------|
| time | TIMESTAMPTZ PK | Время метрики |
| id | VARCHAR(64) PK | ID |
| service | VARCHAR(255) | Сервис |
| metric_name | VARCHAR(255) | Имя метрики |
| value | DOUBLE PRECISION | Значение |
| tags | JSONB | Теги |

## Конфигурация

| Параметр | ENV | По умолчанию |
|----------|-----|-------------|
| `server.port` | — | `8086` |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `spring.datasource.url` | `DB_HOST`, `DB_PORT`, `DB_NAME` | `jdbc:postgresql://localhost:5432/streaming` |
| `spring.datasource.username` | `DB_USER` | `postgres` |
| `spring.datasource.password` | `DB_PASSWORD` | `password` |

Kafka consumer group: `storage-writer`, топик: `raw-events`.

## Технологии

- Spring Boot 3.5.x, Spring Data JPA, Spring Kafka
- PostgreSQL + TimescaleDB (hypertable для метрик)
- Flyway (миграции), SpringDoc OpenAPI (Swagger)
- sentinelflow-common (TelemetryEvent)

## Сборка и запуск

```bash
mvn clean package -pl sentinelflow-storage
java -jar target/sentinelflow-storage-0.1.0-SNAPSHOT.jar
```

## Docker

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-storage/target/sentinelflow-storage-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Порт: `8086`. Требуются Kafka и PostgreSQL (TimescaleDB).
