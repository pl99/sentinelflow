package com.sentinelflow.common.config;

public final class KafkaTopics {

    public static final String RAW_EVENTS = "raw-events";
    public static final String ENRICHED_EVENTS = "enriched-events";
    public static final String ANOMALY_EVENTS = "anomaly-events";
    public static final String LLM_INSIGHTS = "llm-insights";
    public static final String ALERTS = "alerts";

    private KafkaTopics() {}
}
