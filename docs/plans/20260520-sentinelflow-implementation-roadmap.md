# SentinelFlow — Implementation Roadmap

## Overview

SentinelFlow — интеллектуальная платформа потокового анализа телеметрии и обнаружения аномалий. Принимает логи, метрики, трассировки и события из микросервисов/K8s, обрабатывает в реальном времени через Apache Flink, обнаруживает аномалии статистическими/ML-методами и LLM (Ollama), визуализирует через Dashboard.

**Стек:** Java 21, Spring Boot 3.5.x, Maven  
**Брокер:** Apache Kafka  
**Потоковая обработка:** Apache Flink 1.18 (DataStream API, CEP)  
**Хранилище:** TimescaleDB (PostgreSQL + time-series)  
**LLM:** Ollama (локально, Docker)  
**Инфраструктура:** Docker Compose  

## Архитектура — Микросервисы

```
                         ┌─────────────┐
                         │   Gateway   │
                         │ (Cloud GW)  │
                         └──────┬──────┘
                                │
                    ┌───────────┴───────────┐
                    │     Ingestion API     │
                    │  (REST → Kafka)       │
                    └───────────┬───────────┘
                                │ raw-events
                                ▼
                    ┌───────────────────────┐
                    │   Flink Cluster       │
                    │  ┌─────────────────┐  │
                    │  │ Processor Job   │  │
                    │  │ (correlation,   │  │
                    │  │  windowing)     │  │
                    │  └────────┬────────┘  │
                    │           │ enriched  │
                    │  ┌────────▼────────┐  │
                    │  │ Detector Job    │  │
                    │  │ (CEP, rules,    │  │
                    │  │  Z-score, IQR)  │  │
                    │  └────────┬────────┘  │
                    └───────────┼───────────┘
                                │ anomaly-events
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                  ▼
       ┌────────────┐   ┌──────────────┐   ┌──────────────┐
       │   Storage  │   │ LLM Analyzer │   │  Dashboard   │
       │ Writer→TSDB│   │  (Ollama)    │   │ (REST+WS+UI) │
       └────────────┘   └──────────────┘   └──────────────┘
```

### Сервисы

| # | Сервис | Тип | Порт | Назначение |
|---|--------|-----|------|------------|
| 1 | `gateway` | Spring Boot | 8080 | Spring Cloud Gateway — роутинг |
| 2 | `ingestion` | Spring Boot | 8081 | REST приём телеметрии, валидация → `raw-events` |
| 3 | `flink-cluster` | Flink | 8081 (JM) | Flink JobManager + TaskManager |
| 4 | `processor-job` | Flink JAR | — | DataStream — корреляция, windowing, enrichment → `enriched-events` |
| 5 | `detector-job` | Flink JAR | — | CEP + правила + статистика → `anomaly-events` |
| 6 | `llm-analyzer` | Spring Boot | 8084 | Чтение anomaly-events, вызов Ollama → `llm-insights` |
| 7 | `dashboard` | Spring Boot | 8085 | Dashboard REST API + WebSocket + SPA, управление алертами |
| 8 | `storage` | Spring Boot | 8086 | TimescaleDB — Kafka consumer + Query REST API |

### Топики Kafka

| Топик | Ключ | Продюсер | Описание |
|-------|------|----------|----------|
| `raw-events` | source+type | Ingestion | Сырые события телеметрии |
| `enriched-events` | correlationId | Flink Processor | Обогащённые, скоррелированные события |
| `anomaly-events` | service+metric | Flink Detector | Обнаруженные аномалии |
| `llm-insights` | anomalyId | LLM Analyzer | Результаты LLM-анализа |
| `alerts` (опционально) | alertId | Dashboard | Сгенерированные алерты (если нужен отдельный топик для нотификаций) |

## Development Approach

- **Testing approach:** Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **every task MUST include tests** for new/changed code
- **all tests must pass before starting next task**
- update this plan when scope changes

## Testing Strategy

- **Unit tests:** required for every task — cover success + error cases
- **Integration tests:** @SpringBootTest for repository/service layers
- **Contract tests:** for inter-service Kafka schemas
- **E2E:** Docker Compose smoke test

## Фазы реализации

### Фаза 0: Scaffolding и инфраструктура
### Фаза 1: Приём и хранение телеметрии
### Фаза 2: Flink-обработка и обнаружение аномалий
### Фаза 3: LLM-анализ
### Фаза 4: Dashboard Service
### Фаза 5: Query API и интеграция
### Фаза 6: Docker Compose и observability

---

## Phase 0 — Project Scaffolding & Infrastructure

**Цель:** создать multi-module Maven проект, docker-compose.yml для инфраструктуры, общий модуль с DTO/event-схемами.

### Task 0.1: Maven parent + module structure

**Файлы:**
- Create: `pom.xml` (parent)
- Create: `sentinelflow-common/pom.xml`
- Create: `sentinelflow-common/src/main/java/...`
- Create: `.mvn/jvm.config`

- [x] создать parent POM (Spring Boot 3.5.x, Java 21, modules: common, gateway, ingestion, flink-jobs, llm-analyzer, dashboard, storage)
- [x] настроить `spring-boot-starter-parent`, `spring-cloud-dependencies`
- [x] создать `sentinelflow-common` модуль — shared DTO, event schemas (Java Records)
- [x] добавить базовые event-классы: TelemetryEvent, AnomalyEvent, LlmInsight, AlertEvent
- [x] создать .mvn/jvm.config
- [x] написать тесты сериализации DTO + KafkaTopics constants
- [x] запустить `mvn test -pl sentinelflow-common -am` — успех

### Task 0.2: Docker Compose — инфраструктура ✅

**Файлы:**
- ✅ `docker-compose.yaml` (обновлён — healthchecks, Kafka topics init, сети)

- [x] Kafka (confluentinc/cp-kafka, KRaft mode) — порт 9092
- [x] TimescaleDB (timescale/timescaledb:latest-pg16) — порт 5432, БД `streaming`
- [x] Flink 1.18 (JobManager 8081 + TaskManager)
- [x] Ollama (ollama/ollama:latest) — порт 11434
- [x] добавить healthcheck для всех сервисов (Kafka, Postgres, Flink, Ollama)
- [x] добавить kafka-init-topics — создание топиков при старте (raw-events, enriched-events, anomaly-events, llm-insights, alerts)
- [x] настроить sentinelflow-net для container-to-container коммуникации
- [x] настроить dual-listener для Kafka (localhost:9092 для хоста, kafka:9093 для контейнеров)

### Task 0.3: Общий модуль — конфигурация Kafka

**Файлы:**
- Modify: `sentinelflow-common/src/main/java/...`
- Create: `sentinelflow-common/src/main/java/.../config/KafkaConfig.java`

- [ ] создать KafkaConfig с JsonSerializer/JsonDeserializer для event-классов
- [ ] добавить топик-константы (RAW_EVENTS, ENRICHED_EVENTS, ANOMALY_EVENTS, LLM_INSIGHTS, ALERTS)
- [ ] написать тесты сериализации/десериализации event-ов
- [ ] запустить `mvn test -pl sentinelflow-common` — успех

### Task 0.4: Docker Compose — сервисная сеть ✅

**Файлы:**
- Modify: `docker-compose.yaml` (добавлены stub-сервисы приложений)

- [x] добавить gateway, ingestion, llm-analyzer, dashboard, storage — stub-сервисы с eclipse-temurin:21-jre
- [x] настроить depends_on от Kafka/Postgres
- [x] настроить sentinelflow-net для container-to-container коммуникации
- [ ] ➕ Flink job submission — отложено до Phase 6 (фактическая сборка JAR и submit)

---

## Phase 1 — Telemetry Ingestion & Storage

**Цель:** приём телеметрии через REST/gRPC → Kafka, запись в TimescaleDB через Storage Writer.

### Task 1.1: Ingestion Service — REST API ✅

**Файлы:**
- ✅ `sentinelflow-ingestion/pom.xml`
- ✅ `sentinelflow-ingestion/src/main/java/.../SentinelFlowIngestionApplication.java`
- ✅ `sentinelflow-ingestion/src/main/java/.../controller/IngestionController.java`
- ✅ `sentinelflow-ingestion/src/main/java/.../dto/IngestEventRequest.java`, `IngestLogRequest.java`, `IngestMetricRequest.java`, `IngestTraceRequest.java`
- ✅ `sentinelflow-ingestion/src/main/java/.../service/IngestionService.java`
- ✅ `sentinelflow-ingestion/src/main/java/.../kafka/TelemetryProducer.java`
- ✅ `sentinelflow-ingestion/src/main/resources/application.yml`

- [x] создать Spring Boot приложение sentinelflow-ingestion
- [x] реализовать POST `/api/v1/ingest`, `/api/v1/ingest/batch`
- [x] реализовать POST `/api/v1/ingest/logs`, `/api/v1/ingest/metrics`, `/api/v1/ingest/traces`
- [x] IngestionService — валидация, enrichment (timestamp, eventId)
- [x] TelemetryProducer — отправка в Kafka `raw-events`
- [x] настроить application.yml
- [x] unit-тесты контроллера (9 тестов — валидация, все endpoint-ы)
- [x] unit-тесты сервиса (4 теста — все типы событий)
- [x] запустить `mvn test -pl sentinelflow-ingestion` — 13/13 успех

### Task 1.2: Storage Service — TimescaleDB схема + Writer ✅

**Файлы:**
- ✅ `sentinelflow-storage/pom.xml` (+ Flyway)
- ✅ `sentinelflow-storage/src/main/java/.../SentinelFlowStorageApplication.java`
- ✅ `sentinelflow-storage/src/main/java/.../entity/TelemetryEventEntity.java`, `MetricPoint.java`, `JsonbConverter.java`
- ✅ `sentinelflow-storage/src/main/java/.../repository/TelemetryEventRepository.java`, `MetricPointRepository.java`
- ✅ `sentinelflow-storage/src/main/java/.../kafka/EventConsumer.java` (batch-aware, MANUAL_IMMEDIATE ack)
- ✅ `sentinelflow-storage/src/main/java/.../service/StorageWriterService.java` (batch-вставка)
- ✅ `sentinelflow-storage/src/main/java/.../config/KafkaConsumerConfig.java`
- ✅ `sentinelflow-storage/src/main/resources/db/migration/V1__init_schema.sql` (hypertable + индексы)
- ✅ `sentinelflow-storage/src/main/resources/application.yml`

- [x] создать Spring Boot приложение sentinelflow-storage
- [x] TelemetryEventEntity (id, source, type, payload JSONB, timestamp, receivedAt) + JsonbConverter
- [x] MetricPoint (time, service, metricName, value, tags JSONB) — с гипертаблицей TimescaleDB через Flyway
- [x] JPA репозитории (TelemetryEventRepository, MetricPointRepository)
- [x] EventConsumer — читает raw-events, batch-запись через StorageWriterService
- [x] StorageWriterService — batch-вставка (50 записей), отдельно metric_points для metric-типа
- [x] Flyway миграция V1: hypertable metric_points, индексы
- [x] настроить application.yml (datasource, kafka manual ack, flyway, hibernate batch)
- [x] unit-тесты StorageWriterService (3 теста — single event, metric+event, batch)
- [x] запустить `mvn test -pl sentinelflow-storage` — успех

### Task 1.3: Gateway — роутинг ✅

**Файлы:**
- ✅ `sentinelflow-gateway/pom.xml` (+ reactor-test)
- ✅ `sentinelflow-gateway/src/main/java/.../SentinelFlowGatewayApplication.java`
- ✅ `sentinelflow-gateway/src/main/resources/application.yml` (3 routes + RewritePath)

- [x] создать Spring Cloud Gateway приложение (порт 8080)
- [x] настроить route: `/ingest/**` → RewritePath → ingestion:8081/api/v1/ingest/**
- [x] настроить route: `/query/**` → RewritePath → storage:8086/api/v1/query/**
- [x] настроить route: `/dashboard/**` → RewritePath → dashboard:8085/api/v1/**
- [x] написать тест конфигурации роутинга (3 route definitions)
- [x] запустить `mvn test -pl sentinelflow-gateway` — 3/3 успех

---

## Phase 2 — Flink Stream Processing & Anomaly Detection

**Цель:** Flink DataStream jobs для корреляции событий и обнаружения аномалий. Оба Flink job-а запускаются на общем Flink-кластере (JobManager + TaskManager из docker-compose).

### Task 2.1: Processor Flink Job — Correlation + Windowing ✅

**Файлы:**
- ✅ `sentinelflow-flink-jobs/processor/pom.xml` (+ maven-shade-plugin)
- ✅ `sentinelflow-flink-jobs/processor/src/main/java/.../ProcessorJob.java`
- ✅ `sentinelflow-flink-jobs/processor/src/main/java/.../CorrelationWindowFunction.java`
- ✅ `sentinelflow-flink-jobs/processor/src/main/java/.../serialization/FlinkSerialization.java`

- [x] ProcessorJob — DataStream API: Kafka source (raw-events) → correlation → Kafka sink (enriched-events)
- [x] CorrelationWindowFunction — grouping по correlationId, tumbling window (1 мин), вывод списков eventId + сервисов
- [x] настроить checkpointing (filesystem, раз в 30с)
- [x] FlinkSerialization — KafkaRecordDeserializationSchema через Jackson
- [x] сборка `mvn compile -pl sentinelflow-flink-jobs/processor -am` — SUCCESS

### Task 2.2: Detector Flink Job — CEP + Rules + Statistics ✅

**Файлы:**
- ✅ `sentinelflow-flink-jobs/detector/pom.xml` (+ flink-cep)
- ✅ `sentinelflow-flink-jobs/detector/src/main/java/.../DetectorJob.java`
- ✅ `sentinelflow-flink-jobs/detector/src/main/java/.../StatisticalDetector.java`
- ✅ `sentinelflow-flink-jobs/detector/src/main/java/.../RuleEngineFunction.java`
- ✅ `sentinelflow-flink-jobs/detector/src/main/java/.../serialization/FlinkSerialization.java`

- [x] DetectorJob — DataStream: Kafka source (raw-events + enriched-events) → statistical + rules → union → Kafka sink (anomaly-events)
- [x] StatisticalDetector — Z-score в 1мин окнах (Z > 3 → WARNING, Z > 5 → CRITICAL)
- [x] RuleEngineFunction — error-spike для log/ERROR, latency-degradation для trace > 1s
- [x] сборка `mvn compile -pl sentinelflow-flink-jobs/detector -am` — SUCCESS

---

## Phase 3 — LLM Analysis

**Цель:** LLM-анализ аномалий через Ollama — интерпретация логов, объяснение причин, генерация рекомендаций.

### Task 3.1: LLM Analyzer Service — Ollama Integration

**Файлы:**
- Create: `sentinelflow-llm-analyzer/pom.xml`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../SentinelFlowLlmAnalyzerApplication.java`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../service/LlmClientService.java`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../service/AnomalyInterpretationService.java`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../model/OllamaRequest.java`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../kafka/AnomalyConsumer.java`
- Create: `sentinelflow-llm-analyzer/src/main/java/.../kafka/InsightProducer.java`
- Create: `sentinelflow-llm-analyzer/src/main/resources/application.yml`

- [ ] создать Spring Boot приложение sentinelflow-llm-analyzer (порт 8084)
- [ ] реализовать LlmClientService — HTTP клиент к Ollama REST API (генерация + чат)
- [ ] разработать промпты для: интерпретация логов, классификация инцидентов, причины аномалий, рекомендации
- [ ] реализовать AnomalyInterpretationService — чтение anomaly-events, вызов Ollama, структурирование ответа
- [ ] реализовать InsightProducer — отправка результатов в `llm-insights`
- [ ] настроить application.yml (Ollama URL, model name, timeout)
- [ ] написать unit-тесты с WireMock для Ollama API
- [ ] написать integration-тесты consumer → producer цикла
- [ ] запустить `mvn test -pl sentinelflow-llm-analyzer` — успех

---

## Phase 4 — Dashboard Service

**Цель:** сервис дашбордов — агрегация данных из всех источников, управление алертами, WebSocket для real-time, SPA frontend.

### Task 4.1: Dashboard Service — REST API + WebSocket

**Файлы:**
- Create: `sentinelflow-dashboard/pom.xml`
- Create: `sentinelflow-dashboard/src/main/java/.../SentinelFlowDashboardApplication.java`
- Create: `sentinelflow-dashboard/src/main/java/.../controller/DashboardController.java`
- Create: `sentinelflow-dashboard/src/main/java/.../controller/AlertController.java`
- Create: `sentinelflow-dashboard/src/main/java/.../service/DashboardAggregationService.java`
- Create: `sentinelflow-dashboard/src/main/java/.../service/AlertManagementService.java`
- Create: `sentinelflow-dashboard/src/main/java/.../entity/Alert.java`
- Create: `sentinelflow-dashboard/src/main/java/.../repository/AlertRepository.java`
- Create: `sentinelflow-dashboard/src/main/java/.../dto/DashboardSummary.java`
- Create: `sentinelflow-dashboard/src/main/java/.../kafka/InsightConsumer.java`
- Create: `sentinelflow-dashboard/src/main/java/.../websocket/AlertWebSocketHandler.java`
- Create: `sentinelflow-dashboard/src/main/resources/static/index.html`
- Create: `sentinelflow-dashboard/src/main/resources/db/migration/V1__init_alerts.sql`
- Create: `sentinelflow-dashboard/src/main/resources/application.yml`

- [ ] создать Spring Boot приложение sentinelflow-dashboard (порт 8085)
- [ ] создать entity Alert (id, title, severity, status, source, description, recommendation, detectedAt, resolvedAt)
- [ ] создать AlertRepository (JPA + спецификации для фильтрации)
- [ ] реализовать AlertManagementService — CRUD, статусная машина (OPEN → ACK → RESOLVED), deduplication
- [ ] реализовать InsightConsumer — чтение llm-insights, обогащение Alert
- [ ] реализовать DashboardAggregationService — агрегация метрик/событий из storage service (HTTP client)
- [ ] реализовать GET /api/v1/dashboard/summary — сводка: активные алерты, volume событий, топ-метрик
- [ ] реализовать GET /api/v1/alerts — список алертов (фильтрация, пагинация)
- [ ] реализовать PATCH /api/v1/alerts/{id}/status — смена статуса (ack/resolve)
- [ ] реализовать WebSocket /ws/alerts — push новых алертов в real-time
- [ ] создать минимальный SPA frontend (index.html + htmx или vanilla JS) — таблица алертов, сводка
- [ ] написать SQL миграцию для alerts
- [ ] написать unit-тесты AlertManagementService (статусная машина, dedup)
- [ ] написать integration-тесты контроллеров
- [ ] написать тесты WebSocket handler
- [ ] запустить `mvn test -pl sentinelflow-dashboard` — успех

---

## Phase 5 — Query API & Integration

**Цель:** REST API для внешних систем, агрегации временных рядов, исторического анализа.

### Task 4.1: Storage — Query REST API

**Файлы:**
- Modify: `sentinelflow-storage/pom.xml` (добавить Web dependency)
- Create: `sentinelflow-storage/src/main/java/.../controller/QueryController.java`
- Create: `sentinelflow-storage/src/main/java/.../service/TimeSeriesQueryService.java`
- Create: `sentinelflow-storage/src/main/java/.../dto/TimeSeriesResponse.java`
- Create: `sentinelflow-storage/src/main/java/.../dto/AggregatedMetric.java`

- [ ] реализовать TimeSeriesQueryService — агрегации (AVG, P95, P99, COUNT) с TimescaleDB time_bucket
- [ ] реализовать GET /api/v1/query/metrics — временной ряд для метрики (service, metricName, start, end, bucket)
- [ ] реализовать GET /api/v1/query/events — поиск событий с фильтрацией (source, type, time range, full-text в JSONB)
- [ ] реализовать GET /api/v1/query/anomalies — история аномалий с пагинацией
- [ ] добавить спецификацию OpenAPI (springdoc-openapi)
- [ ] написать тесты QueryController с Testcontainers
- [ ] написать тесты для SQL-запросов (time_bucket, JSONB queries)
- [ ] запустить `mvn test -pl sentinelflow-storage` — успех

### Task 5.2: Gateway — полная конфигурация

**Файлы:**
- Modify: `sentinelflow-gateway/src/main/resources/application.yml`

- [ ] добавить rate limiting на /ingest (чтобы не перегружать)
- [ ] настроить CORS
- [ ] добавитьCircuitBreaker (Resilience4j) для вызовов к микросервисам
- [ ] написать тест конфигурации (rate limit, CORS)
- [ ] запустить `mvn test -pl sentinelflow-gateway` — успех

---

## Phase 6 — Docker Compose & Observability

**Цель:** полный Docker Compose, healthchecks, logging, мониторинг самих микросервисов.

### Task 6.1: Dockerfile для сервисов + Flink job submission

**Файлы:**
- Create: `sentinelflow-gateway/Dockerfile`
- Create: `sentinelflow-ingestion/Dockerfile`
- Create: `sentinelflow-llm-analyzer/Dockerfile`
- Create: `sentinelflow-dashboard/Dockerfile`
- Create: `sentinelflow-storage/Dockerfile`
- Create: `sentinelflow-flink-jobs/Dockerfile` (build JAR-ы + submit скрипт)

- [ ] создать Dockerfile для Spring Boot сервисов (multi-stage: mvn build → eclipse-temurin:21-jre)
- [ ] создать Dockerfile для Flink job submission (build JAR-ы → volume mount в Flink)
- [ ] настроить JVM опции (Xmx, Xms, GC)
- [ ] проверить сборку `docker compose build`

### Task 6.2: Observability (метрики + логи + трейсинг)

**Файлы:**
- Modify: каждый сервис `application.yml`
- Modify: `docker-compose/infrastructure.yml`

- [ ] добавить Actuator во все сервисы (health, prometheus, info)
- [ ] добавить Micrometer + Micrometer Tracing
- [ ] настроить сбор метрик Prometheus (в каждом application.yml)
- [ ] добавить Loki для агрегации логов через Docker driver
- [ ] добавить Tempo или Zipkin для распределённой трассировки
- [ ] добавить Grafana дашборды (дашборд для метрик каждого сервиса)
- [ ] обновить docker-compose с observability стеком

### Task 6.3: End-to-End тест

**Файлы:**
- Create: `tests/e2e/smoke-test.sh`
- Create: `tests/e2e/docker-compose.e2e.yml`

- [ ] создать smoke-test скрипт: отправка телеметрии → проверка аномалии в dashboard
- [ ] реализовать E2E тест: curl POST /ingest, wait, GET /dashboard/summary, verify alert count
- [ ] добавить Flink-клиент для submit job-ов при старте (entrypoint.sh)
- [ ] добавить healthcheck для каждого сервиса (wait-for-it)
- [ ] документировать запуск в README

---

## Post-Completion

**Manual verification:**
- проверить ingestion через curl/Postman с разными типами телеметрии
- проверить детекцию аномалий (симулировать error-spike)
- проверить LLM-анализ (Ollama должна быть запущена)
- открыть dashboard в браузере — проверить отображение алертов и метрик

**External system updates:**
- настройка Ollama (pull модели: `ollama pull llama3`)
- настройка Prometheus targets для сбора метрик
