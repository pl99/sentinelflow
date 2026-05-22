# SentinelFlow LLM Analyzer

Сервис анализа аномалий с помощью LLM (Ollama). Потребляет аномальные события из Kafka (`anomaly-events`), отправляет их в локальную языковую модель для интерпретации и публикует результат (`LlmInsight`) в топик `llm-insights`.

## Архитектура

```
Kafka (anomaly-events)
       │
       ▼
AnomalyConsumer ──> AnomalyInterpretationService ──> OllamaClient ──HTTP POST──> Ollama API (qwen3:1.7b)
       │                                                      │
       ▼                                                      ▼
Kafka (llm-insights)                                   Локальная LLM
```

- **AnomalyConsumer** — асинхронный Kafka consumer, пул 1-2 потока (`ThreadPoolTaskExecutor` + `CallerRunsPolicy`)
- **AnomalyInterpretationService** — формирует prompt, вызывает Ollama, парсит ответ
- **OllamaClient** — HTTP-клиент к Ollama REST API (`/api/generate`)
- **PromptTemplates** — шаблоны с настраиваемым языком ответа (ISO-коды: `ru`, `en`, `de`, `fr`, `es` и др.)

## Prompt

Модели передаётся контекст аномалии (сервис, метрика, severity, описание, детали). Ожидаемый формат ответа:

```
1. INTERPRETATION: описание на человеческом языке
2. CLASSIFICATION: LatencyIssue / ErrorSpike / ThroughputDegradation / ResourceExhaustion / Unknown
3. PROBABLE_CAUSES: 2-3 вероятные причины
4. RECOMMENDATIONS: 1-2 действия для расследования
```

Язык ответа настраивается через `LLM_LANGUAGE` (по умолчанию `ru`). Поддерживаются ISO-коды: `en`, `ru`, `de`, `fr`, `es`, `it`, `pt`, `zh`, `ja`, `ko`.

## Парсинг ответа

- Секции извлекаются по заголовкам (INTERPRETATION, CLASSIFICATION, PROBABLE_CAUSES, RECOMMENDATIONS)
- `extractSection()` ищет раздел по префиксу, определяет конец по следующей нумерованной секции
- `firstLine()` берёт первую строку для полей, ожидающих одно значение
- Если LLM недоступен — возвращается fallback-ответ `"LLM service unavailable"`
- Если ответ пустой — `"LLM analysis unavailable"`

## Конфигурация

| Параметр | ENV | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `server.port` | — | `8084` | HTTP-порт |
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap |
| `sentinelflow.llm.language` | `LLM_LANGUAGE` | `ru` | Язык ответа (ISO-код) |
| `sentinelflow.llm.ollama.url` | `OLLAMA_URL` | `http://localhost:11434` | URL Ollama API |
| `sentinelflow.llm.ollama.model` | `OLLAMA_MODEL` | `qwen3:1.7b` | Модель Ollama |
| `sentinelflow.llm.ollama.timeout` | — | `300s` | Таймаут запроса к Ollama |

Kafka consumer group: `llm-analyzer`, потребляет топик `anomaly-events`, публикует в `llm-insights`.

## Технологии

- Spring Boot 3.5.x, Spring Kafka
- Ollama REST API (HTTP-клиент на Java `HttpClient`)
- Jackson (сериализация/десериализация)
- sentinelflow-common (AnomalyEvent, LlmInsight, KafkaTopics)

## Сборка и запуск

```bash
mvn clean package -pl sentinelflow-llm-analyzer
java -jar target/sentinelflow-llm-analyzer-0.1.0-SNAPSHOT.jar
```

## Docker

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-llm-analyzer/target/sentinelflow-llm-analyzer-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Порт: `8084`. Требуются Kafka и Ollama.

## Пример

При аномалии с score 4.2, `qwen3:1.7b` в режиме `ru` может ответить:

```
1. INTERPRETATION: Обнаружен всплеск задержки p99 в сервисе order-service
2. CLASSIFICATION: LatencyIssue
3. PROBABLE_CAUSES: Перегрузка пула соединений с БД, медленный запрос к внешнему API
4. RECOMMENDATIONS: Проверить PgBouncer, проанализировать медленные запросы в БД
```
