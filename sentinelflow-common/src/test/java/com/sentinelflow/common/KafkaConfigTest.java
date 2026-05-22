package com.sentinelflow.common;

import com.sentinelflow.common.config.KafkaTopics;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigTest {

    @Test
    void topicsShouldBeDefined() {
        assertThat(KafkaTopics.RAW_EVENTS).isEqualTo("raw-events");
        assertThat(KafkaTopics.ENRICHED_EVENTS).isEqualTo("enriched-events");
        assertThat(KafkaTopics.ANOMALY_EVENTS).isEqualTo("anomaly-events");
        assertThat(KafkaTopics.LLM_INSIGHTS).isEqualTo("llm-insights");
        assertThat(KafkaTopics.ALERTS).isEqualTo("alerts");
    }
}
