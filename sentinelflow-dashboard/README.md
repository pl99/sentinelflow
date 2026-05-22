# SentinelFlow Dashboard

Веб-интерфейс для просмотра и управления аномалиями. Потребляет `LlmInsight` из Kafka (`llm-insights`), сохраняет алерты в PostgreSQL и транслирует их в реальном времени через WebSocket. Предоставляет REST API для управления и статистики.

## Архитектура

```
Kafka (llm-insights)
       │
       ▼
InsightConsumer ──> AlertManagementService ──> AlertRepository ──> PostgreSQL (alerts)
       │
       ▼
AlertWebSocketHandler (broadcast)
       │
       ▼
WebSocket /ws/alerts ──> index.html (SPA, dark theme)
                              │
                         (polling)
                              ▼
                    AlertController  ── /api/v1/alerts
                    DashboardController ── /api/v1/dashboard/stats
                    DashboardAggregationService ──> Storage API (health check)
```

## API

### Alerts

Базовый путь: `/api/v1/alerts`

| Method | Path | Параметры | Описание |
|--------|------|-----------|----------|
| GET | `/api/v1/alerts` | `status`, `severity`, `since`, `until`, `limit` (100) | Список алертов с фильтрацией |
| PATCH | `/api/v1/alerts/{id}/status` | `status=ACK\|RESOLVED` | Смена статуса (OPEN→ACK, OPEN→RESOLVED, ACK→RESOLVED) |

### Dashboard

Базовый путь: `/api/v1/dashboard`

| Method | Path | Описание |
|--------|------|----------|
| GET | `/api/v1/dashboard/stats` | Полная статистика: total, critical, warning, lastHour, lastDay, bySource, bySeverity, byStatus, hourly |
| GET | `/api/v1/dashboard/summary` | Сводка: openCount, acknowledgedCount, resolvedCount, openBySeverity + health check storage |

## WebSocket

- Эндпоинт: `/ws/alerts`
- Протокол: raw WebSocket (не STOMP)
- При получении нового `LlmInsight` из Kafka отправляет сообщение `NEW_ALERT` всем подключённым клиентам
- Клиенты после получения обновляют таблицу алертов и виджеты

## Frontend (SPA)

Единый `index.html` без фреймворков, тёмная тема.

**Виджеты:**
- 6 summary-карточек: Open, Critical, Total, Last Hour, Acknowledged, Resolved
- Гистограмма источников (top-10)
- Панели Severity / Status
- Почасовой график за 24ч (клик по бару фильтрует алерты)
- Таблица алертов с обновлением каждые 10с
- Модальное окно: при клике на заголовок — LLM Interpretation + Recommendation

**Вызовы API из JS:**
| Endpoint | Частота |
|----------|---------|
| `GET /api/v1/alerts?limit=50` | 10с |
| `GET /api/v1/dashboard/summary` | 10с |
| `GET /api/v1/dashboard/stats` | 30с |
| `PATCH /api/v1/alerts/{id}/status` | по клику |

## База данных

### alerts

| Колонка | Тип | Описание |
|---------|-----|----------|
| id | VARCHAR(64) PK | UUID |
| title | VARCHAR(512) | Заголовок: "Anomaly {id}: {classification}" |
| severity | VARCHAR(32) | CRITICAL / WARNING / INFO |
| status | VARCHAR(32) | OPEN / ACK / RESOLVED |
| source | VARCHAR(255) | Источник |
| description | TEXT | LLM interpretation |
| recommendation | TEXT | LLM recommendations |
| anomaly_id | VARCHAR(64) | ID исходной аномалии |
| detected_at | TIMESTAMPTZ | Время создания |
| resolved_at | TIMESTAMPTZ | Время закрытия |

Индексы: `status`, `severity`, `detected_at DESC`, `source`.

## Конфигурация

| Параметр | ENV | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `server.port` | — | `8085` | HTTP-порт |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap |
| `spring.datasource.url` | `DB_HOST`, `DB_PORT`, `DB_NAME` | `jdbc:postgresql://localhost:5432/streaming` | JDBC URL |
| `spring.datasource.username` | `DB_USER` | `postgres` | Пользователь БД |
| `spring.datasource.password` | `DB_PASSWORD` | `password` | Пароль БД |
| `sentinelflow.storage.query-url` | `STORAGE_QUERY_URL` | `http://localhost:8086` | URL storage для health check |

Kafka consumer group: `dashboard`, топик: `llm-insights`.

## Технологии

- Spring Boot 3.5.x, Spring Web, Spring WebSocket, Spring Data JPA, Spring Kafka
- PostgreSQL + Flyway (миграции, отключены по умолчанию)
- Vanilla JS SPA (тёмная тема, WebSocket, polling)
- sentinelflow-common (LlmInsight, KafkaTopics)

## Сборка и запуск

```bash
mvn clean package -pl sentinelflow-dashboard
java -jar target/sentinelflow-dashboard-0.1.0-SNAPSHOT.jar
```

## Docker

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-dashboard/target/sentinelflow-dashboard-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Порт: `8085`. Требуются Kafka и PostgreSQL.

## Статусы алертов

```
OPEN ──> ACK ──> RESOLVED
  │               ▲
  └───────────────┘
```
