package com.sentinelflow.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "sentinelflow.kafka.topics.auto-create", havingValue = "true", matchIfMissing = true)
public class TopicInitializer {

    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

    @Bean
    public List<NewTopic> sentinelFlowTopics() {
        log.info("Creating SentinelFlow Kafka topics");
        return List.of(
                topic(KafkaTopics.RAW_EVENTS, 3),
                topic(KafkaTopics.ENRICHED_EVENTS, 3),
                topic(KafkaTopics.ANOMALY_EVENTS, 2),
                topic(KafkaTopics.LLM_INSIGHTS, 2),
                topic(KafkaTopics.ALERTS, 1)
        );
    }

    private NewTopic topic(String name, int partitions) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}
