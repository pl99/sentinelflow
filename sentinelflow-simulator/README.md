# SentinelFlow Simulator

Генератор тестовых телеметрических данных. Читает CSV-файлы с показателями датчиков и воспроизводит их через HTTP POST в сервис приёма (`sentinelflow-ingestion`). Также умеет генерировать синтетические аномалии для проверки пайплайна детекции.

## Архитектура

```
CSV файлы ──> CsvReader ──> DataSimulatorService ──> IngestionClient ──HTTP POST──> /api/v1/ingest
                                                                                              
AnomalyGeneratorService ───────────────────────────────> IngestionClient ──HTTP POST──> /api/v1/ingest
```

Модуль не имеет встроенного веб-сервера (`spring.main.web-application-type: none`) — запускается как CLI-приложение и завершается после отправки данных (или работает бесконечно в режиме аномалий).

## Режимы работы

### 1. Воспроизведение CSV

Читает два CSV-файла и отправляет записи как метрики:

| Файл | Источник | Тип метрики | Поля |
|------|----------|-------------|------|
| `termo_data.csv` | `termo-sensor` | `temperature` | `packet_id`, `val`, `status` |
| `fuel_sensor_data.csv` | `fuel-sensor` | `fuel-level` | `packet_id`, `level_mm`, `level_l`, `temperature`, `switched_on` |

Записи из обоих файлов отправляются поочерёдно (interleaved) с задержкой между отправками.

### 2. Генерация аномалий

Создаёт синтетические аномальные события трёх типов:

- **Metric**: для 4 источников (`engine-1`, `engine-2`, `pump-1`, `compressor-1`) и 5 метрик (`temperature`, `pressure`, `rpm`, `vibration`, `current`) — 8-12 нормальных значений (50-100) и 1 выброс (500-1500)
- **Log**: случайные сообщения об ошибках (`Connection timeout`, `Null pointer`, `Disk I/O error`, `Memory limit`, `Auth failure`)
- **Trace**: трейсы с высокой задержкой (1000-5000ms)

### 3. Совместный режим

CSV + аномалии запускаются последовательно: сначала воспроизведение CSV, затем генерация аномалий.

## Формат события

```json
{
  "source": "termo-sensor",
  "type": "metric",
  "subtype": "temperature",
  "payload": {
    "packet_id": 123,
    "val": 5.0,
    "status": 1
  },
  "tags": {
    "tr_id": "129222",
    "unit_id": "880833",
    "sensor_id": "851208"
  },
  "timestamp": "2025-11-10T00:00:05Z"
}
```

## Конфигурация

| Параметр | ENV | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `simulator.ingestion.url` | `SIMULATOR_INGESTION_URL` | `http://localhost:8081/api/v1/ingest` | URL сервиса приёма |
| `simulator.data.termo-file` | `TERMO_DATA_PATH` | — | Путь к CSV температуры |
| `simulator.data.fuel-file` | `FUEL_DATA_PATH` | — | Путь к CSV топлива |
| `simulator.delay-ms` | `SIMULATOR_DELAY_MS` | `100` | Задержка между событиями (мс) |
| `simulator.auto-start` | `SIMULATOR_AUTO_START` | `false` | Автостарт при запуске |
| `simulator.anomaly.enabled` | `SIMULATOR_ANOMALY_ENABLED` | `false` | Генерация аномалий |
| `simulator.anomaly.cycles` | `SIMULATOR_ANOMALY_CYCLES` | `0` | Количество циклов (0 = нет) |
| `simulator.anomaly.continuous` | `SIMULATOR_ANOMALY_CONTINUOUS` | `false` | Бесконечная генерация |

## Формат CSV

### termo_data.csv
```
packet_id,event_time,tr_id,unit_id,sensor_id,val,status
```

### fuel_sensor_data.csv
```
packet_id,event_time,sensor_id,tr_id,level_mm,level_l,temperature,switched_on
```

## Технологии

- Spring Boot 3.5.x (core, без веб-сервера)
- Jackson (JSON-сериализация)
- `java.net.http.HttpClient` (HTTP-отправка)

## Сборка и запуск

```bash
mvn clean package -pl sentinelflow-simulator

# Пример: воспроизведение CSV + аномалии
java -jar target/sentinelflow-simulator-0.1.0-SNAPSHOT.jar ^
  -DSIMULATOR_AUTO_START=true ^
  -DTERMO_DATA_PATH=sample-data/termo_data.csv ^
  -DFUEL_DATA_PATH=sample-data/fuel_sensor_data.csv ^
  -DSIMULATOR_ANOMALY_ENABLED=true ^
  -DSIMULATOR_ANOMALY_CYCLES=3
```

## Docker

```dockerfile
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY sentinelflow-simulator/target/sentinelflow-simulator-0.1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

В Docker CSV-файлы монтируются через volume: `./sentinelflow-simulator/sample-data:/app/sample-data`.
