# Connect enriched-events Topic to DetectorJob

## Overview
- **Problem:** DetectorJob reads `raw-events` directly, bypassing the Processor's enrichment pipeline. The `enriched-events` topic is produced but never consumed.
- **Goal:** Make StatisticalDetector read from `enriched-events` (getting correlation context), while RuleEngineFunction keeps reading `raw-events` (since log/trace events may lack correlationId).
- **Benefit:** Detector benefits from correlation data (correlatedEventIds, servicesInChain, window context), enabling richer anomaly analysis and better debugging context.

## Context
- `EnrichedTelemetryEvent` wraps the original `TelemetryEvent` in its `original` field + adds `correlatedEventIds`, `servicesInChain`, `windowStart`, `windowEnd`, `enrichmentMetadata`
- Both are Java records implementing `Serializable`, serialized via Jackson ObjectMapper
- `KafkaTopics.ENRICHED_EVENTS` = `"enriched-events"` already defined in common module
- Detector's `FlinkSerialization` is generic — works with any Jackson-serializable class
- Processor's `CorrelationWindowFunction` filters by `correlationId != null && !isBlank` — events without correlationId never reach `enriched-events`
- `StatisticalDetector` uses `ProcessWindowFunction<TelemetryEvent, AnomalyEvent, String, TimeWindow>` — reads events by `source:metric` key, computes Z-score over 1-min windows
- `RuleEngineFunction` uses `ProcessFunction<TelemetryEvent, AnomalyEvent>` — checks each event for error spikes (`type="log"`, level=ERROR) and latency degradation (`type="trace"`, duration>1000ms)

## Development Approach
- **Testing approach:** TDD
- Complete each task fully before moving to the next
- Make small, focused changes
- All tests must pass before starting next task
- Maintain backward compatibility

## Testing Strategy
- Unit tests for StatisticalDetector with EnrichedTelemetryEvent input
- Unit tests for new DetectorJob pipeline wiring
- Integration test (via existing e2e or manual compose verification)
- RuleEngineFunction tests unchanged (still reads TelemetryEvent)

## Solution Overview
DetectorJob gets a **second Kafka source** for `enriched-events`, deserialized as `EnrichedTelemetryEvent`. The stream is mapped to extract `.original()` (TelemetryEvent), then filtered by `type="metric"` and fed into the existing `StatisticalDetector` via tumbling windows (same 1-min, keyed by `source:metric`). The existing `raw-events` source continues feeding `RuleEngineFunction`. Both outputs are `.union()`ed into a single `AnomalyEvent` stream → `anomaly-events`.

### Architecture change
```
Before:
  raw-events ──→ DetectorJob ──→ anomaly-events
                    ├── StatisticalDetector (metric)
                    └── RuleEngineFunction (log, trace)

After:
  enriched-events ──→ DetectorJob ──→ anomaly-events
  raw-events ──────→    ├── StatisticalDetector (metric, from enriched)
                        └── RuleEngineFunction (log, trace, from raw)
```

## What Goes Where

### Implementation Steps

### Task 1: Add TDD test for StatisticalDetector with EnrichedTelemetryEvent

**Files:**
- Create: `sentinelflow-flink-jobs/detector/src/test/java/com/sentinelflow/flink/detector/StatisticalDetectorWithEnrichedEventTest.java`

- [x] write test that verifies StatisticalDetector correctly extracts `.original()` from EnrichedTelemetryEvent and computes Z-score
- [x] write test for window with <5 events (should skip)
- [x] write test for Z-score < 3.0 (no anomaly emitted)
- [x] write test for Z-score > 3.0 (WARNING emitted)
- [x] write test for Z-score > 5.0 (CRITICAL emitted)
- [x] run tests — must pass before Task 2

### Task 2: Refactor StatisticalDetector to accept EnrichedTelemetryEvent

**Files:**
- Modify: `sentinelflow-flink-jobs/detector/src/main/java/com/sentinelflow/flink/detector/StatisticalDetector.java`

- [x] change `StatisticalDetector` signature from `ProcessWindowFunction<TelemetryEvent, ...>` to `ProcessWindowFunction<EnrichedTelemetryEvent, ...>`
- [x] in `process()`, extract `element.original()` to get TelemetryEvent for existing logic (value extraction, source/metric key parsing)
- [x] add enrichment context to AnomalyEvent details — include `correlatedEventIds` and `servicesInChain` from EnrichedTelemetryEvent in the `details` map

### Task 3: Add enriched-events source to DetectorJob (combined with Task 2 — tightly coupled)

**Files:**
- Modify: `sentinelflow-flink-jobs/detector/src/main/java/com/sentinelflow/flink/detector/DetectorJob.java`

- [x] add `KafkaSource<EnrichedTelemetryEvent>` for `KafkaTopics.ENRICHED_EVENTS`, groupId `"flink-detector-enriched"`
- [x] filter `type="metric"`, keyBy `source:metric`, window 1-min, process via refactored `StatisticalDetector`
- [x] existing `raw-events` source continues for `RuleEngineFunction` only (no type filter, keep as-is)
- [x] `.union()` both anomaly streams (from enriched StatisticalDetector + raw RuleEngineFunction) before sinking to `anomaly-events`
- [x] pipeline wiring is straightforward — tests at StatisticalDetector level are sufficient
- [x] run tests — must pass before Task 4

### Task 4: Verify acceptance criteria
- [x] verify StatisticalDetector correctly uses enriched data (correlatedEventIds, servicesInChain in anomaly details) — test `shouldIncludeEnrichmentContextInDetails`
- [x] verify RuleEngineFunction behavior is unchanged (still reads raw-events directly)
- [x] verify union of both streams produces correct AnomalyEvent format
- [x] verify backward compatibility — existing tests still pass
- [x] run full test suite: `mvn clean test -pl sentinelflow-flink-jobs/detector` — 5 tests pass
- [ ] verify with docker compose that Flink job submits and runs (manual check on Flink UI)

## Post-Completion
**Manual verification:**
- restart Detector job: `docker compose -f docker-compose.srv.yaml restart flink-submit` (or manual Flink CLI)
- check Flink UI at `http://localhost:8081/` — verify DetectorJob has 2 sources
- verify pipeline: start simulator → check anomaly-events topic for enriched correlation data
- verify dashboard shows anomalies with new context fields
